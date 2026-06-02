// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/importers/apache"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/spf13/cobra"
)

// loadApacheListsCmd registers `aveloxis load-apache-lists` — Phase 0c of the
// mailing-list work (DOAP-enrichment). It is the piece that gives the
// MailingListWorker something to claim: for each Apache PMC it
//
//   - ensures a per-PMC legacy repo_group (the list-registry FK target, §11),
//   - links the PMC's already-collected primary repo to that group so
//     mailing-list bodies have a NOT-NULL repo_id (GetPrimaryRepoForGroup),
//   - enumerates the PMC's lists (Phase 2.6, via preferences.lua) and
//     registers the ones the §5 policy keeps (dev@/users@ + drift lists like
//     common-dev@; issue-tracker lists for Jira/Bugzilla-primary projects;
//     skip commit/PR mirror lists) in repo_groups_list_serve tagged
//     mlls_system='apache_ponymail'.
//
// Prerequisite: run `load-foundation-core-repos` first so the primary repos
// exist in the catalog. PMCs whose repo isn't found are skipped (reported).
//
// Enumeration (Phase 2.6) surfaces the §4 naming-drift lists automatically;
// if it's unavailable (offline / preferences.lua error) the command falls
// back to the dev@/users@ convention.
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
	backend := mailinglist.NewPonyMail("", "") // lists.apache.org, default UA

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

		// Phase 2.6: enumerate the lists that actually exist for this PMC's
		// domain (surfaces drift lists like common-dev@ automatically) and
		// apply the §5 collect/skip policy. Fall back to the dev@/users@
		// convention if enumeration is unavailable.
		addrs := collectableLists(ctx, backend, pmc.ListDomain(), pmc.BugDatabase, logger)

		if dryRun {
			for _, a := range addrs {
				fmt.Printf("  [%s] %s  →  repo_id=%d\n", pmc.Slug, a, repoID)
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
		for _, a := range addrs {
			if err := store.RegisterMailingList(ctx, groupID, a, system); err != nil {
				logger.Warn("failed to register list", "slug", pmc.Slug, "list", a, "error", err)
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

// collectableLists enumerates the lists for a PMC domain and applies the §5
// collect/skip policy. Falls back to the dev@/users@ convention when
// enumeration is unavailable (offline, preferences.lua error, or empty).
func collectableLists(ctx context.Context, backend *mailinglist.PonyMail, domain, bugDatabase string, logger *slog.Logger) []string {
	infos, err := backend.EnumerateLists(ctx, domain)
	if err != nil || len(infos) == 0 {
		if err != nil {
			logger.Warn("list enumeration failed; falling back to dev@/users@ convention", "domain", domain, "error", err)
		}
		return []string{"dev@" + domain, "users@" + domain}
	}
	var out []string
	for _, li := range infos {
		if shouldCollectList(li.Name, bugDatabase) {
			out = append(out, li.Address)
		}
	}
	return out
}

// shouldCollectList implements the §5 policy: always collect human lists
// (dev/user, incl. drift like common-dev); collect the issue-tracker event
// lists for Jira/Bugzilla-primary projects; skip commit/PR mirror lists
// (we already collect those from GitHub/facade).
func shouldCollectList(name, bugDatabase string) bool {
	n := strings.ToLower(name)
	if strings.Contains(n, "dev") || strings.Contains(n, "user") {
		return true // dev@, users@, common-dev@, dev-*, ...
	}
	bd := strings.ToLower(bugDatabase)
	if strings.Contains(bd, "jira") && (n == "jira" || n == "issues") {
		return true
	}
	if (strings.Contains(bd, "bugzilla") || strings.Contains(bd, "bz.apache")) && strings.Contains(n, "bug") {
		return true
	}
	return false // commits@, notifications@, cvs@, announce@, ...
}
