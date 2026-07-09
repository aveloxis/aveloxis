// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/spf13/cobra"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// backfillIdentitiesCmd is the one-off identity-attribution repair
// (v0.26.5; plan: summary/identity-attribution-audit-2026-07-09.md).
//
// Phases:
//
//	1 — assignment identity joins (issue_assignees,
//	    pull_request_assignees, pull_request_reviewers) + pr_meta
//	    owners. Pure SQL, no API calls. Measured production coverage
//	    99.87–99.98%.
//	2 — issues.closed_by_id from each issue's latest 'closed' event.
//	    Pure SQL. Run after the v0.26.3 event-healing cohort for best
//	    coverage.
//	3 — closed_by GraphQL timeline sweep for issues whose closers the
//	    history-capped events feed cannot reach (GitHub only; needs
//	    API keys; ~3 points per 100 issues).
func backfillIdentitiesCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun     bool
		batchSize  int
		limit      int64
		phase      string
		sweepBatch int
	)
	cmd := &cobra.Command{
		Use:   "backfill-identities",
		Short: "Backfill missing cntrb_id attribution (assignees, reviewers, meta owners, issue closers)",
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()
			// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
			// Only serve and the migrate subcommand run migrations.

			runAll := phase == "all"
			if runAll || phase == "1" {
				n, err := store.BackfillAssignmentIdentities(ctx, batchSize, limit, dryRun)
				if err != nil {
					return fmt.Errorf("phase 1 (assignments): %w", err)
				}
				fmt.Printf("phase 1 assignments: %d rows %s\n", n, verb(dryRun))
				m, err := store.BackfillPRMetaOwners(ctx, batchSize, limit, dryRun)
				if err != nil {
					return fmt.Errorf("phase 1 (pr_meta owners): %w", err)
				}
				fmt.Printf("phase 1 pr_meta owners: %d rows %s\n", m, verb(dryRun))
			}
			if runAll || phase == "2" {
				n, err := store.BackfillClosedByFromEvents(ctx, batchSize, limit, dryRun)
				if err != nil {
					return fmt.Errorf("phase 2 (closed_by from events): %w", err)
				}
				fmt.Printf("phase 2 closed_by from events: %d rows %s\n", n, verb(dryRun))
			}
			if runAll || phase == "3" {
				ghKeys, _, err := loadKeys(ctx, cfg, store, false, logger)
				if err != nil {
					return fmt.Errorf("phase 3 needs API keys: %w", err)
				}
				client := github.New(cfg.GitHub.BaseURL, ghKeys, logger)
				sweep := collector.NewClosedBySweep(store, client, logger, sweepBatch)
				n, err := sweep.Run(ctx, limit, dryRun)
				if err != nil {
					return fmt.Errorf("phase 3 (timeline sweep): %w", err)
				}
				fmt.Printf("phase 3 closed_by timeline sweep: %d issues %s\n", n, verb(dryRun))
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report candidate counts without writing")
	cmd.Flags().IntVar(&batchSize, "batch-size", 100000, "rows per UPDATE batch (phases 1-2)")
	cmd.Flags().Int64Var(&limit, "limit", 0, "cap rows/issues per phase (0 = unbounded)")
	cmd.Flags().StringVar(&phase, "phase", "all", "which phase to run: all | 1 | 2 | 3")
	cmd.Flags().IntVar(&sweepBatch, "sweep-batch", 100, "issues per GraphQL query in phase 3")
	return cmd
}

func verb(dryRun bool) string {
	if dryRun {
		return "derivable (dry-run)"
	}
	return "updated"
}
