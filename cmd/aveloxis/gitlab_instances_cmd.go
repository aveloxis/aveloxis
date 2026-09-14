// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// gitlabInstancesCmd is the read-only operator view of multi-instance
// GitLab (v0.30.0).
func gitlabInstancesCmd(cfgPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "gitlab-instances",
		Short: "List GitLab instances: platform_id, web and API URLs, keys, repositories",
		Long: `Read-only. For every GitLab instance in the config and in the platforms
registry: its platform_id, web URL, the API URL its keys are sent to, how
many keys it has (config + stored), and how many repositories it has.

Also lists instances registered earlier but no longer configured (their
repositories are collected git-only), stored keys tagged with an instance
that is not configured (never loaded), and misrouted repositories — rows on
platform_id 2 that live under another instance and already hold API data
(reported, never changed).

Run it after a deploy: gitlab.com should be platform_id 2 and nothing should
be misrouted.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			instances, err := cfg.GitLab.EffectiveInstances()
			if err != nil {
				return fmt.Errorf("gitlab config: %w", err)
			}
			registry, err := store.LoadGitLabInstanceRegistry(ctx)
			if err != nil {
				return err
			}
			stored, err := db.LoadAPIKeysByInstance(ctx, store.Pool(), "gitlab", false)
			if err != nil {
				return err
			}
			pools, orphans, err := partitionGitLabTokens(instances, stored)
			if err != nil {
				return err
			}
			repos, err := store.CountReposByPlatform(ctx)
			if err != nil {
				return err
			}
			misrouted, err := store.MisroutedGitLabRepos(ctx)
			if err != nil {
				return err
			}
			renderGitLabInstances(os.Stdout, instances, registry, pools, repos, orphans, misrouted)
			return nil
		},
	}
}

// renderGitLabInstances writes the gitlab-instances report: configured
// instances in config order, then registered instances no longer configured
// (each a tab-separated row), then orphan stored keys and misrouted
// repositories.
func renderGitLabInstances(w io.Writer, instances []config.GitLabInstance, registry map[string]model.Platform,
	pools map[string][]string, repos map[model.Platform]int, orphans map[string]int, misrouted []db.MisroutedRepo) {
	// Tab-separated rows, one per instance: readable in a terminal and easy
	// to cut/awk.
	tw := w
	fmt.Fprintln(tw, "platform_id\tweb_url\tapi_url\tconfigured\tkeys\trepos")
	configured := map[string]bool{}
	for _, in := range instances {
		configured[in.WebBase] = true
		id, ok := registry[in.WebBase]
		idCol, status, count := "-", "NOT REGISTERED — run aveloxis migrate", 0
		if ok {
			idCol = fmt.Sprint(int(id))
			count = repos[id]
			status = "yes"
			if len(pools[in.WebBase]) == 0 {
				status = "yes, NO KEYS"
			}
		}
		if in.Primary && ok {
			status += " (main)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\n", idCol, in.WebBase, in.APIURL, status, len(pools[in.WebBase]), count)
	}
	var leftover []string
	for base := range registry {
		if !configured[base] {
			leftover = append(leftover, base)
		}
	}
	sort.Strings(leftover)
	for _, base := range leftover {
		id := registry[base]
		fmt.Fprintf(tw, "%d\t%s\t-\tregistered, not configured\t-\t%d\n", int(id), base, repos[id])
	}

	if len(orphans) > 0 {
		fmt.Fprintln(w, "\nStored GitLab keys for instances that are not configured (never loaded):")
		tags := make([]string, 0, len(orphans))
		for tag := range orphans {
			tags = append(tags, tag)
		}
		sort.Strings(tags)
		for _, tag := range tags {
			fmt.Fprintf(w, "  %s: %d stored key(s) load nowhere\n", tag, orphans[tag])
		}
	}
	if len(misrouted) > 0 {
		fmt.Fprintln(w, "\nMisrouted repositories (platform_id 2 under another instance, with API data — reported, not changed):")
		for _, m := range misrouted {
			fmt.Fprintf(w, "  repo %d %s (lives under %s)\n", m.RepoID, m.GitURL, m.Instance)
		}
	}
}
