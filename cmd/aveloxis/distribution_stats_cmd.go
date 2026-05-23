// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"text/tabwriter"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// distributionStatsCmd is the v0.24.0 operator CLI for inspecting
// DistributionWorker coverage. Read-only; never writes to the
// distribution tables.
//
// Three views:
//
//	aveloxis distribution-stats                     # fleet rollup
//	aveloxis distribution-stats --orphans           # manifest-without-registry list
//	aveloxis distribution-stats --repo owner/repo   # per-repo drill-down
//
// The headline operator question is "which repos declare packaging
// intent but were never published to a registry?" — answered by
// --orphans. The rollup also surfaces "manifest without registry
// evidence" as a single number for fleet-wide tracking.
func distributionStatsCmd(cfgPath *string) *cobra.Command {
	var (
		orphans bool
		repoArg string
		limit   int
	)
	cmd := &cobra.Command{
		Use:   "distribution-stats",
		Short: "Show v0.24.0 package-distribution coverage and orphan analysis",
		Long: `Inspects the v0.24.0 DistributionWorker output. Three views:

  aveloxis distribution-stats                     # fleet rollup
  aveloxis distribution-stats --orphans           # repos with manifest but no registry
  aveloxis distribution-stats --repo owner/repo   # one repo's full distribution rows

Rollup includes:
  - total repos / scanned / with registry / with manifest
  - "manifest without registry evidence" count (the headline metric)
  - per-ecosystem breakdown of distinct repos

--orphans lists repos whose in-repo manifest declares a package
name that doesn't show up in any registry (deps.dev or
ecosyste.ms) for the same ecosystem. This is the "this repo has
a setup.py but was never published to PyPI" cohort, useful for
inventory hygiene.

Read-only: never writes to repo_distribution or
repo_distribution_manifest. Safe to run alongside an active
aveloxis serve.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			// v0.21.5: store.Migrate(ctx) intentionally NOT called
			// here. distribution-stats is read-only and trusts that
			// the operator has already run `aveloxis migrate`.

			switch {
			case repoArg != "":
				return printRepoDetail(ctx, store, repoArg)
			case orphans:
				return printOrphans(ctx, store, limit)
			default:
				return printRollup(ctx, store)
			}
		},
	}
	cmd.Flags().BoolVar(&orphans, "orphans", false, "list repos with manifest evidence but no registry evidence (the headline analysis case)")
	cmd.Flags().StringVar(&repoArg, "repo", "", "per-repo drill-down: owner/repo")
	cmd.Flags().IntVar(&limit, "limit", 200, "max rows to show in list views")
	return cmd
}

func printRollup(ctx context.Context, store *db.PostgresStore) error {
	r, err := store.GetDistributionStats(ctx)
	if err != nil {
		return fmt.Errorf("get distribution stats: %w", err)
	}
	fmt.Println("== Distribution coverage ==")
	fmt.Printf("  Total repos (non-archived):           %d\n", r.TotalRepos)
	fmt.Printf("  Scanned by distribution worker:        %d\n", r.ScannedRepos)
	fmt.Printf("  Repos with registry evidence:          %d\n", r.ReposWithRegistry)
	fmt.Printf("  Repos with manifest evidence:          %d\n", r.ReposWithManifest)
	fmt.Printf("  Manifest WITHOUT registry evidence:    %d  ← headline analysis cohort\n", r.ReposManifestNoEvidence)
	if r.TotalRepos > 0 {
		pct := float64(r.ScannedRepos) / float64(r.TotalRepos) * 100
		fmt.Printf("  Coverage:                              %.1f%%\n", pct)
	}

	if len(r.PerEcosystem) > 0 {
		fmt.Println()
		fmt.Println("== Per-ecosystem (distinct repos) ==")
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "ECOSYSTEM\tREPOS")
		for _, ec := range r.PerEcosystem {
			fmt.Fprintf(tw, "%s\t%d\n", ec.Ecosystem, ec.Repos)
		}
		_ = tw.Flush()
	}
	return nil
}

func printOrphans(ctx context.Context, store *db.PostgresStore, limit int) error {
	rows, err := store.ListDistributionOrphans(ctx, limit)
	if err != nil {
		return fmt.Errorf("list orphans: %w", err)
	}
	if len(rows) == 0 {
		fmt.Println("no orphan manifests found (every manifest has matching registry evidence)")
		return nil
	}
	fmt.Printf("== Manifest WITHOUT registry evidence (showing up to %d) ==\n\n", limit)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "REPO\tMANIFEST_TYPE\tPATH\tDECLARED_NAME")
	for _, o := range rows {
		name := o.PackageNameDeclared
		if name == "" {
			name = "(unparsed)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", o.RepoSlug, o.ManifestType, o.ManifestPath, name)
	}
	return tw.Flush()
}

func printRepoDetail(ctx context.Context, store *db.PostgresStore, slug string) error {
	detail, err := store.GetRepoDistribution(ctx, slug)
	if err != nil {
		return err
	}
	fmt.Printf("== Distribution detail for %s ==\n\n", detail.RepoSlug)

	if len(detail.Distributions) == 0 {
		fmt.Println("No registry evidence.")
	} else {
		fmt.Println("Registry evidence:")
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "  SOURCE\tECOSYSTEM\tPACKAGE\tVERSIONS")
		for _, d := range detail.Distributions {
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%d\n", d.Source, d.Ecosystem, d.PackageName, d.VersionCount)
		}
		_ = tw.Flush()
	}

	fmt.Println()
	if len(detail.Manifests) == 0 {
		fmt.Println("No manifest evidence.")
	} else {
		fmt.Println("Manifest evidence:")
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "  TYPE\tPATH\tDECLARED_NAME")
		for _, m := range detail.Manifests {
			name := m.PackageNameDeclared
			if name == "" {
				name = "(unparsed)"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\n", m.ManifestType, m.ManifestPath, name)
		}
		_ = tw.Flush()
	}
	return nil
}
