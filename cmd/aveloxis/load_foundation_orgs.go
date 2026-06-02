// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/importers"
	"github.com/aveloxis/aveloxis/internal/importers/apache"
	"github.com/aveloxis/aveloxis/internal/importers/cncf"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/spf13/cobra"
)

// loadFoundationOrgsCmd registers `aveloxis load-foundation-orgs`.
//
// Companion to load-foundation-core-repos. Where that command loads one
// core repo per project, this registers each foundation's GitHub
// organization(s) as TRACKED ORGANIZATIONS under the operator's user
// (via AddOrgToGroup → aveloxis_ops.user_org_requests). The scheduler's
// existing refreshUserOrgs ticker (v0.19.x) then continuously discovers
// every repo in the org — including future additions. This is the
// "catch all repos, including new ones" mode.
//
// SCALE: tracking the `apache` org pulls ALL ~3,000 apache/* repos into
// collection (not just the ~350 core repos). That is the intended
// completeness trade, but it is a large collection-budget commitment, so
// the command refuses to write without an explicit --yes (or --dry-run).
func loadFoundationOrgsCmd(cfgPath *string) *cobra.Command {
	var (
		userID        int
		dryRun        bool
		yes           bool
		cncfOnly      bool
		apacheOnly    bool
		cncfURL       string
		apacheProjURL string
		apachePodURL  string
	)

	cmd := &cobra.Command{
		Use:   "load-foundation-orgs",
		Short: "Register foundation GitHub orgs as tracked organizations (continuous repo discovery)",
		Long: `Derives the distinct GitHub organization(s) behind each foundation's
projects and registers them for ongoing tracking via the existing
refreshUserOrgs feature. New repos added to the org are picked up
automatically on the next refresh cycle.

Groups created (or reused) under --user-id:
  - "Apache Orgs"  — the apache GitHub org
  - "CNCF Orgs"    — the distinct orgs behind CNCF projects

SCALE WARNING: tracking the apache org pulls ALL ~3,000 apache/* repos
into collection, not just the core repos. This is a large collection
commitment. The command requires --yes (or --dry-run) to proceed.

For bounded tracking of just one core repo per project, use the
companion ` + "`aveloxis load-foundation-core-repos`" + ` command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cncfOnly && apacheOnly {
				return fmt.Errorf("--cncf-only and --apache-only are mutually exclusive")
			}
			return runLoadFoundationOrgs(*cfgPath, foundationOrgsOpts{
				UserID:        userID,
				DryRun:        dryRun,
				Yes:           yes,
				CncfOnly:      cncfOnly,
				ApacheOnly:    apacheOnly,
				CncfURL:       cncfURL,
				ApacheProjURL: apacheProjURL,
				ApachePodURL:  apachePodURL,
			})
		},
	}

	cmd.Flags().IntVar(&userID, "user-id", 1, "aveloxis user_id the org-tracking groups belong to (default 1, the bootstrap admin)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the orgs that would be tracked without writing to the database")
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm tracking entire orgs (pulls every repo in the org into collection) — required to write")
	cmd.Flags().BoolVar(&cncfOnly, "cncf-only", false, "track only CNCF orgs")
	cmd.Flags().BoolVar(&apacheOnly, "apache-only", false, "track only Apache orgs")
	cmd.Flags().StringVar(&cncfURL, "cncf-url", cncf.DefaultLandscapeURL, "override CNCF landscape.yml URL")
	cmd.Flags().StringVar(&apacheProjURL, "apache-projects-url", apache.DefaultProjectsURL, "override Apache projects.json URL")
	cmd.Flags().StringVar(&apachePodURL, "apache-podlings-url", apache.DefaultPodlingsURL, "override Apache podlings.json URL")

	return cmd
}

type foundationOrgsOpts struct {
	UserID                               int
	DryRun, Yes, CncfOnly, ApacheOnly    bool
	CncfURL, ApacheProjURL, ApachePodURL string
}

// orgGroupName maps a foundation key to the tracked-org group name.
func orgGroupName(foundation string) string {
	switch foundation {
	case "cncf":
		return "CNCF Orgs"
	case "apache":
		return "Apache Orgs"
	default:
		if foundation == "" {
			return "Foundation Orgs"
		}
		return strings.ToUpper(foundation[:1]) + foundation[1:] + " Orgs"
	}
}

// orgURLForRepo derives the canonical org URL from a project repo URL,
// e.g. "https://github.com/apache/arrow" → "https://github.com/apache".
// Returns "" for hosts we can't track as an org.
func orgURLForRepo(rurl string) string {
	parsed, err := platform.ParseRepoURL(rurl)
	if err != nil || parsed.Owner == "" {
		return ""
	}
	switch parsed.Platform {
	case model.PlatformGitHub:
		return "https://github.com/" + parsed.Owner
	case model.PlatformGitLab:
		return "https://gitlab.com/" + parsed.Owner
	default:
		return ""
	}
}

func runLoadFoundationOrgs(cfgPath string, opts foundationOrgsOpts) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)
	ctx := context.Background()

	// Fetch the project lists, then derive the distinct set of org URLs per
	// foundation. We track orgs, not individual repos — refreshUserOrgs does
	// the per-repo discovery.
	var projects []importers.Project
	if !opts.ApacheOnly {
		logger.Info("fetching CNCF landscape", "url", opts.CncfURL)
		cncfProjects, ferr := cncf.Fetch(ctx, opts.CncfURL)
		if ferr != nil {
			return fmt.Errorf("fetching CNCF landscape: %w", ferr)
		}
		projects = append(projects, cncfProjects...)
	}
	if !opts.CncfOnly {
		logger.Info("fetching Apache projects", "projects_url", opts.ApacheProjURL, "podlings_url", opts.ApachePodURL)
		apacheProjects, ferr := apache.Fetch(ctx, opts.ApacheProjURL, opts.ApachePodURL)
		if ferr != nil {
			return fmt.Errorf("fetching Apache projects: %w", ferr)
		}
		projects = append(projects, apacheProjects...)
	}

	// foundation → set of distinct org URLs.
	orgsByFoundation := map[string]map[string]bool{}
	for _, p := range projects {
		for _, rurl := range p.RepoURLs {
			org := orgURLForRepo(rurl)
			if org == "" {
				continue
			}
			if orgsByFoundation[p.Foundation] == nil {
				orgsByFoundation[p.Foundation] = map[string]bool{}
			}
			orgsByFoundation[p.Foundation][org] = true
		}
	}

	// Stable ordering for output + dedup reporting.
	foundations := make([]string, 0, len(orgsByFoundation))
	for f := range orgsByFoundation {
		foundations = append(foundations, f)
	}
	sort.Strings(foundations)

	totalOrgs := 0
	for _, set := range orgsByFoundation {
		totalOrgs += len(set)
	}

	if opts.DryRun {
		fmt.Printf("Would track %d distinct org(s) across %d foundation(s):\n", totalOrgs, len(foundations))
		for _, f := range foundations {
			for _, org := range sortedKeys(orgsByFoundation[f]) {
				fmt.Printf("  [%s] %s\n", f, org)
			}
		}
		fmt.Println("\n(dry-run — nothing written)")
		return nil
	}

	// Scale gate — checked BEFORE any write. Tracking an org pulls every
	// repo in it (apache ≈ 3,000) into collection.
	if !opts.Yes {
		fmt.Printf("This will register %d org(s) as tracked organizations.\n", totalOrgs)
		fmt.Println("Tracking an org pulls EVERY repo in it into collection — for the")
		fmt.Println("apache org that is ~3,000 repos, a large collection commitment.")
		fmt.Println("Re-run with --yes to proceed, or --dry-run to preview.")
		return fmt.Errorf("refusing to track orgs without --yes")
	}

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	added, failed := 0, 0
	for _, f := range foundations {
		name := orgGroupName(f)
		groupID, gerr := store.CreateUserGroup(ctx, opts.UserID, name)
		if gerr != nil {
			logger.Warn("failed to create org-tracking group", "name", name, "error", gerr)
			failed++
			continue
		}
		for _, org := range sortedKeys(orgsByFoundation[f]) {
			if err := store.AddOrgToGroup(ctx, opts.UserID, groupID, org); err != nil {
				logger.Warn("failed to track org", "foundation", f, "org", org, "error", err)
				failed++
				continue
			}
			added++
		}
	}

	fmt.Printf("\nsummary: %d org(s) tracked (or already-present), %d failed\n", added, failed)
	fmt.Println("refreshUserOrgs will discover repos in these orgs on its next cycle.")
	if failed > 0 {
		return fmt.Errorf("%d orgs failed to register; see warn-level logs above", failed)
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
