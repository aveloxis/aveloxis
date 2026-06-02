// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/importers/apache"
	"github.com/spf13/cobra"
)

// loadApacheListsCmd registers `aveloxis load-apache-lists` — Phase 0c of the
// mailing-list work (DOAP-enrichment). It is the piece that gives the
// MailingListWorker something to claim: for each Apache PMC it
//
//   - ensures a per-PMC legacy repo_group (the list-registry FK target, §11),
//   - links the PMC's already-collected primary repo to that group so
//     mailing-list bodies have a NOT-NULL repo_id (GetPrimaryRepoForGroup),
//   - registers the PMC's dev@ and users@ lists in repo_groups_list_serve
//     tagged mlls_system='apache_ponymail'.
//
// Prerequisite: run `load-foundation-core-repos` first so the primary repos
// exist in the catalog. PMCs whose repo isn't found are skipped (reported).
//
// v0.25.7 scope note: list addresses use the Apache naming convention
// (dev@<slug>.apache.org, users@<slug>.apache.org). Registering a list that
// doesn't exist is harmless — the worker's mbox fetch returns 404 (a clean
// zero-result). Full per-domain enumeration (preferences.lua) + the §4
// naming-drift cases (httpd cvs@, hadoop common-dev@) are a documented
// follow-up; the convention covers the common case.
func loadApacheListsCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun        bool
		apacheProjURL string
		apachePodURL  string
	)
	cmd := &cobra.Command{
		Use:   "load-apache-lists",
		Short: "Register Apache PMC mailing lists (dev@/users@) so the MailingListWorker can collect them",
		Long: `For each Apache PMC, ensures a per-PMC repo_group, links the PMC's
primary repo to it, and registers the dev@ and users@ lists for
collection by the MailingListWorker (requires collection.mailing_list_enabled).

Run ` + "`aveloxis load-foundation-core-repos`" + ` first so the primary
repos exist; PMCs whose repo isn't in the catalog are skipped.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLoadApacheLists(*cfgPath, dryRun, apacheProjURL, apachePodURL)
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print planned registrations without writing")
	cmd.Flags().StringVar(&apacheProjURL, "apache-projects-url", apache.DefaultProjectsURL, "override Apache projects.json URL")
	cmd.Flags().StringVar(&apachePodURL, "apache-podlings-url", apache.DefaultPodlingsURL, "override Apache podlings.json URL")
	return cmd
}

func runLoadApacheLists(cfgPath string, dryRun bool, projURL, podURL string) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)
	ctx := context.Background()

	pmcs, err := apache.FetchPMCs(ctx, projURL, podURL)
	if err != nil {
		return fmt.Errorf("fetching Apache PMCs: %w", err)
	}
	logger.Info("fetched Apache PMCs", "count", len(pmcs))

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	const system = "apache_ponymail"
	lists := []string{"dev", "users"}

	var registered, skippedNoRepo, listsAdded int
	for _, pmc := range pmcs {
		if pmc.RepoURL == "" {
			continue
		}
		repoID, rerr := store.FindRepoByURL(ctx, pmc.RepoURL)
		if rerr != nil || repoID == 0 {
			skippedNoRepo++
			continue
		}
		if dryRun {
			for _, l := range lists {
				fmt.Printf("  [%s] %s@%s  →  repo_id=%d\n", pmc.Slug, l, pmc.ListDomain(), repoID)
			}
			registered++
			continue
		}

		groupID, gerr := store.UpsertRepoGroup(ctx, "Apache PMC: "+pmc.Slug, "apache_pmc", pmc.Homepage)
		if gerr != nil {
			logger.Warn("failed to ensure per-PMC repo_group", "slug", pmc.Slug, "error", gerr)
			continue
		}
		if err := store.SetRepoGroup(ctx, repoID, groupID); err != nil {
			logger.Warn("failed to link repo to per-PMC group", "slug", pmc.Slug, "error", err)
			continue
		}
		for _, l := range lists {
			if err := store.RegisterMailingList(ctx, groupID, l+"@"+pmc.ListDomain(), system); err != nil {
				logger.Warn("failed to register list", "slug", pmc.Slug, "list", l, "error", err)
				continue
			}
			listsAdded++
		}
		registered++
	}

	fmt.Printf("\nload-apache-lists summary: %d PMCs registered, %d lists added, %d skipped (no primary repo in catalog — run load-foundation-core-repos first)\n",
		registered, listsAdded, skippedNoRepo)
	if dryRun {
		fmt.Println("(dry-run — nothing written)")
	}
	return nil
}
