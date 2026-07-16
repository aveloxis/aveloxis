// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/importers/numfocus"
	"github.com/spf13/cobra"
)

// loadNumfocusOrgsCmd registers `aveloxis load-numfocus-orgs`.
//
// Companion to load-numfocus-projects: creates two user_groups
// for org-level tracking ("NumFocus Sponsored Orgs" and "NumFocus
// Affiliated Orgs") under --user-id and registers each project's
// GitHub org via AddOrgToGroup. The scheduler's refreshUserOrgs
// ticker (v0.19.x) then walks each org periodically and picks up
// any new repos.
//
// This is the "track everything in the org, including future
// additions" mode the operator asked for. Pairs with
// load-numfocus-projects for "track just the flagship repo per
// project" mode.
func loadNumfocusOrgsCmd(cfgPath *string) *cobra.Command {
	var (
		userID      int
		dryRun      bool
		detectNew   bool
		catalogFile string
	)

	cmd := &cobra.Command{
		Use:   "load-numfocus-orgs",
		Short: "Load NumFocus sponsored + affiliated project orgs into per-user groups for ongoing tracking",
		Long: `Reads the embedded NumFocus catalog and registers each project's
GitHub organization for periodic refresh. The scheduler's
refreshUserOrgs ticker picks up any repos added to the org
automatically.

Two groups are created (or reused) under --user-id:
  - "NumFocus Sponsored Orgs"  — orgs behind the 63 sponsored projects
  - "NumFocus Affiliated Orgs" — orgs behind the 103 affiliated projects

When multiple projects share an org (e.g. PyTorch-Ignite at
pytorch/ignite and other pytorch/* affiliated projects), the org
is registered once per group — AddOrgToGroup is idempotent via
ON CONFLICT.

Use --detect-new to compare the live numfocus.org listing against
the embedded catalog and report drift.

For repo-level tracking that's bounded to each project's primary
flagship repo (no auto-pickup of new repos in the org), use the
companion ` + "`aveloxis load-numfocus-projects`" + ` command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadNumfocusOrgs(*cfgPath, numfocusLoadOpts{
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

func runLoadNumfocusOrgs(cfgPath string, opts numfocusLoadOpts) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)
	ctx := context.Background()

	catalog, err := loadNumfocusCatalog(opts.CatalogFile)
	if err != nil {
		return err
	}

	if opts.DetectNew {
		return runNumfocusDetectNew(ctx, logger, catalog)
	}

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	sponsoredGroupID, err := ensureNumfocusGroup(ctx, store, opts.UserID, "NumFocus Sponsored Orgs", opts.DryRun)
	if err != nil {
		return err
	}
	affiliatedGroupID, err := ensureNumfocusGroup(ctx, store, opts.UserID, "NumFocus Affiliated Orgs", opts.DryRun)
	if err != nil {
		return err
	}

	tally := numfocusInsertTally{}
	skipped := []numfocus.Project{}
	// Dedupe orgs within each section — when multiple projects share
	// an org (e.g. several scikit-hep/* affiliated projects), we only
	// want one user_org_requests row per (group, org). AddOrgToGroup
	// itself has ON CONFLICT DO NOTHING, but tracking in Go too keeps
	// the planned/added/skipped tally honest.
	seenSponsored := map[string]bool{}
	seenAffiliated := map[string]bool{}

	insertSection := func(projects []numfocus.Project, groupID int64, section string, seen map[string]bool) {
		for _, p := range projects {
			orgURL := p.OrgURL()
			if orgURL == "" {
				skipped = append(skipped, p)
				tally.skipped++
				continue
			}
			if seen[orgURL] {
				continue
			}
			seen[orgURL] = true
			if opts.DryRun {
				fmt.Printf("  [%s] %s  →  %s\n", section, p.Name, orgURL)
				tally.planned++
				continue
			}
			if _, err := store.AddOrgToGroup(ctx, opts.UserID, groupID, orgURL); err != nil {
				logger.Warn("failed to add numfocus org",
					"section", section, "name", p.Name, "org_url", orgURL, "error", err)
				tally.failed++
				continue
			}
			tally.added++
		}
	}

	fmt.Printf("Loading NumFocus orgs into groups (user_id=%d)\n", opts.UserID)
	insertSection(catalog.Sponsored, sponsoredGroupID, "sponsored", seenSponsored)
	insertSection(catalog.Affiliated, affiliatedGroupID, "affiliated", seenAffiliated)

	if opts.DryRun {
		fmt.Printf("\ndry-run summary: %d planned (deduped), %d skipped (needs_review or other-platform)\n",
			tally.planned, tally.skipped)
	} else {
		fmt.Printf("\nsummary: %d orgs registered (or already-present), %d skipped, %d failed\n",
			tally.added, tally.skipped, tally.failed)
	}
	if len(skipped) > 0 {
		fmt.Println("\nSkipped entries (needs_review or other-platform):")
		for _, p := range skipped {
			note := p.Note
			if note == "" {
				note = "(no note)"
			}
			fmt.Printf("  - %s  [%s]  %s\n", p.Name, p.Confidence, note)
		}
	}
	if tally.failed > 0 {
		return fmt.Errorf("%d orgs failed to register; see warn-level logs above", tally.failed)
	}
	return nil
}
