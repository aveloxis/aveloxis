// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

// registerJiraProjectsCmd registers `aveloxis register-jira-projects`:
// seeds aveloxis_ops.jira_project_serve from the synthetic issues the
// mailing-list projection already minted (191 distinct project keys on
// the production aveloxis DB, each mapped to its repo). Idempotent —
// re-running re-registers nothing and never clobbers an operator's
// repo fix. Dead upstream keys (5 of the pilot's 191) are disabled by
// the worker on their first 400, not filtered here (no network calls
// in this command).
//
// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
// Only serve and the migrate subcommand run migrations.
func registerJiraProjectsCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun  bool
		baseURL string
	)
	cmd := &cobra.Command{
		Use:   "register-jira-projects",
		Short: "Register Jira projects for collection, derived from existing synthetic issues",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			_ = logger
			ctx := context.Background()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionStringWithAppName("aveloxis-jira-register"), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			cands, err := store.DeriveJiraProjectsFromSynthetics(ctx)
			if err != nil {
				return err
			}
			if dryRun {
				fmt.Printf("dry run: %d project keys derivable\n", len(cands))
				for i, c := range cands {
					if i >= 20 {
						fmt.Printf("  ... and %d more\n", len(cands)-20)
						break
					}
					fmt.Printf("  %-16s repo_id=%d\n", c.ProjectKey, c.RepoID)
				}
				return nil
			}
			registered := 0
			for _, c := range cands {
				repoID := c.RepoID
				if err := store.RegisterJiraProject(ctx, c.ProjectKey, baseURL, &repoID); err != nil {
					return fmt.Errorf("register %s: %w", c.ProjectKey, err)
				}
				registered++
			}
			fmt.Printf("registered %d Jira projects (base %s)\n", registered, baseURL)
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "list derivable projects without registering")
	cmd.Flags().StringVar(&baseURL, "base-url", "https://issues.apache.org/jira", "Jira Server base URL for the registrations")
	return cmd
}
