// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/importers/numfocus"
	"github.com/spf13/cobra"
)

// loadNumfocusProjectsCmd registers `aveloxis load-numfocus-projects`.
//
// Creates two user_groups under --user-id (default 1) — "NumFocus
// Sponsored" and "NumFocus Affiliated" — and adds each project's
// primary repository to the appropriate group via AddRepoToGroup
// (same path the web UI uses). Idempotent: re-running is a no-op
// for repos already in the group.
//
// The project catalog is the YAML embedded in
// internal/importers/numfocus/data.yaml. Updates require editing
// the YAML and rebuilding the binary. Use --detect-new to find
// projects added to numfocus.org since the YAML was last updated.
//
// needs_review entries (catalog entries the maintainer couldn't
// confidently resolve to a primary GitHub repo) are skipped with
// a WARN listing them in the summary output.
func loadNumfocusProjectsCmd(cfgPath *string) *cobra.Command {
	var (
		userID      int
		dryRun      bool
		detectNew   bool
		catalogFile string
	)

	cmd := &cobra.Command{
		Use:   "load-numfocus-projects",
		Short: "Load NumFocus sponsored + affiliated project repos into per-user groups",
		Long: `Reads the embedded NumFocus catalog and loads each project's primary
GitHub repository into a user_group on the operator's dashboard.

Two groups are created (or reused) under --user-id:
  - "NumFocus Sponsored"  — 63 projects (numfocus.org/sponsored-projects)
  - "NumFocus Affiliated" — 103 projects (numfocus.org/sponsored-projects/affiliated-projects)

Idempotent — re-running is a no-op for repos already in each group.

Use --detect-new to compare the live numfocus.org listing against the
embedded catalog and report projects that need to be added to the
YAML (typical maintenance trigger when NumFocus accepts new projects).

For org-level tracking that picks up new repos automatically, use the
companion ` + "`aveloxis load-numfocus-orgs`" + ` command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadNumfocusProjects(*cfgPath, numfocusLoadOpts{
				UserID:      userID,
				DryRun:      dryRun,
				DetectNew:   detectNew,
				CatalogFile: catalogFile,
			})
		},
	}

	cmd.Flags().IntVar(&userID, "user-id", 1, "aveloxis user_id that the new groups belong to (default 1, the bootstrap admin)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print planned inserts without writing to the database")
	cmd.Flags().BoolVar(&detectNew, "detect-new", false, "scrape numfocus.org and report projects not in the embedded catalog (no DB writes)")
	cmd.Flags().StringVar(&catalogFile, "catalog-file", "", "override the embedded catalog with a YAML file (rare; primarily for local iteration)")

	return cmd
}

// numfocusLoadOpts is shared by both load-numfocus-projects and
// load-numfocus-orgs since their flag surface is identical.
type numfocusLoadOpts struct {
	UserID      int
	DryRun      bool
	DetectNew   bool
	CatalogFile string
}

func runLoadNumfocusProjects(cfgPath string, opts numfocusLoadOpts) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)
	ctx := context.Background()

	catalog, err := loadNumfocusCatalog(opts.CatalogFile)
	if err != nil {
		return err
	}

	// --detect-new is a read-only operation. Doesn't touch the
	// database at all. Run it standalone OR alongside the normal
	// insert pass (the latter prints the drift report after the
	// insert summary). Default in this command: report only.
	if opts.DetectNew {
		return runNumfocusDetectNew(ctx, logger, catalog)
	}

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	// v0.21.5: schema migration intentionally NOT invoked here —
	// reserved for `aveloxis migrate` and `aveloxis serve`. Pinned
	// by a source-contract test that fails if the call returns.

	sponsoredGroupID, err := ensureNumfocusGroup(ctx, store, opts.UserID, "NumFocus Sponsored", opts.DryRun)
	if err != nil {
		return err
	}
	affiliatedGroupID, err := ensureNumfocusGroup(ctx, store, opts.UserID, "NumFocus Affiliated", opts.DryRun)
	if err != nil {
		return err
	}

	tally := numfocusInsertTally{}
	skipped := []numfocus.Project{}

	insertSection := func(projects []numfocus.Project, groupID int64, section string) {
		for _, p := range projects {
			if !p.IsActionable() {
				skipped = append(skipped, p)
				tally.skipped++
				continue
			}
			url := p.PrimaryURL()
			if url == "" {
				skipped = append(skipped, p)
				tally.skipped++
				continue
			}
			if opts.DryRun {
				fmt.Printf("  [%s] %s  →  %s\n", section, p.Name, url)
				tally.planned++
				continue
			}
			if err := store.AddRepoToGroup(ctx, opts.UserID, groupID, url); err != nil {
				logger.Warn("failed to add numfocus project",
					"section", section, "name", p.Name, "url", url, "error", err)
				tally.failed++
				continue
			}
			tally.added++
		}
	}

	fmt.Printf("Loading NumFocus catalog into groups (user_id=%d)\n", opts.UserID)
	insertSection(catalog.Sponsored, sponsoredGroupID, "sponsored")
	insertSection(catalog.Affiliated, affiliatedGroupID, "affiliated")

	if opts.DryRun {
		fmt.Printf("\ndry-run summary: %d planned, %d skipped (needs_review or unresolvable)\n",
			tally.planned, tally.skipped)
	} else {
		fmt.Printf("\nsummary: %d added (or already-present), %d skipped, %d failed\n",
			tally.added, tally.skipped, tally.failed)
	}
	if len(skipped) > 0 {
		fmt.Println("\nSkipped entries (needs_review or unresolvable):")
		for _, p := range skipped {
			note := p.Note
			if note == "" {
				note = "(no note)"
			}
			fmt.Printf("  - %s  [%s]  %s\n", p.Name, p.Confidence, note)
		}
	}
	if tally.failed > 0 {
		return fmt.Errorf("%d entries failed to insert; see warn-level logs above", tally.failed)
	}
	return nil
}

type numfocusInsertTally struct {
	planned int
	added   int
	skipped int
	failed  int
}

// ensureNumfocusGroup returns the group_id for a NumFocus group,
// creating it if it doesn't already exist. CreateUserGroup is
// itself idempotent (`ON CONFLICT (user_id, name) DO UPDATE`), so
// this is just a thin wrapper that handles --dry-run reporting.
func ensureNumfocusGroup(ctx context.Context, store *db.PostgresStore, userID int, name string, dryRun bool) (int64, error) {
	if dryRun {
		fmt.Printf("  [dry-run] would ensure user_group: user_id=%d name=%q\n", userID, name)
		return 0, nil
	}
	gid, err := store.CreateUserGroup(ctx, userID, name)
	if err != nil {
		return 0, fmt.Errorf("ensure user_group %q: %w", name, err)
	}
	return gid, nil
}

// loadNumfocusCatalog resolves the catalog source: --catalog-file
// override if set, else the binary-embedded YAML. Returns the
// parsed Catalog with both sections + needs_review.
func loadNumfocusCatalog(file string) (*numfocus.Catalog, error) {
	if file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read catalog file %q: %w", file, err)
		}
		return numfocus.LoadCatalogFromBytes(data)
	}
	return numfocus.LoadCatalog()
}

// runNumfocusDetectNew scrapes the numfocus.org listings, compares
// against the catalog, and prints a punch list of projects the
// catalog is missing. No DB writes. Exit code 0 even when drift
// found — operator action is to edit the YAML, not to fail CI.
func runNumfocusDetectNew(ctx context.Context, logger *slog.Logger, catalog *numfocus.Catalog) error {
	client := &http.Client{Timeout: 30 * time.Second}
	scraped, err := numfocus.Crawl(ctx, client)
	if err != nil {
		return fmt.Errorf("crawling numfocus.org: %w", err)
	}
	logger.Info("scraped numfocus.org", "count", len(scraped))

	missing := numfocus.DetectNew(scraped, catalog)
	if len(missing) == 0 {
		fmt.Println("No drift: every numfocus.org listing entry has a catalog entry.")
		return nil
	}
	fmt.Printf("Drift detected: %d entries on numfocus.org are NOT in the embedded catalog.\n", len(missing))
	fmt.Println("Add the following to internal/importers/numfocus/data.yaml after resolving the (org, primary_repo):")
	fmt.Println()
	for _, sp := range missing {
		fmt.Printf("  - section: %s\n", sp.Section)
		fmt.Printf("    slug:    %s\n", sp.Slug)
		if sp.Name != "" {
			fmt.Printf("    name:    %q\n", sp.Name)
		}
		fmt.Printf("    url:     %s\n", sp.URL)
		fmt.Println()
	}
	return nil
}
