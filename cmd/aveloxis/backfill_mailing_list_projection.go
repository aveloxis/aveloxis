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

// backfillMailingListProjectionCmd registers `aveloxis backfill-mailing-list-projection`
// (Phase 5, summary/12 §8). It projects issue_event mail → issues (link-or-create)
// + thread-inheritance over email_message rows collected BEFORE the projection
// code existed — in place, no re-collection. Three steps, each idempotent and
// looped to convergence: keyed projection first (so thread issues exist), then
// thread-inheritance, then a final sweep marking the rest mailing_list_only.
//
// Does NOT run migrations (v0.21.5 contract — run `aveloxis migrate` first).
func backfillMailingListProjectionCmd(cfgPath *string) *cobra.Command {
	var batch int
	cmd := &cobra.Command{
		Use:   "backfill-mailing-list-projection",
		Short: "Project existing mailing-list issue_event mail onto issues (Phase 5; in-place, idempotent)",
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx := context.Background()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			if batch <= 0 {
				batch = 500
			}

			// Ensure the per-row-lookup indexes exist before draining (CONCURRENTLY,
			// IF NOT EXISTS — instant if present, non-blocking if it must build).
			// Without these the backfill sequential-scans messages/email_message
			// per row and crawls for days; this makes the command self-sufficient
			// rather than depending on a separate `aveloxis migrate` having run.
			fmt.Println("ensuring projection indexes (idx_messages_node_id, idx_email_message_thread_root)...")
			if err := store.EnsureMailingListProjectionIndexes(ctx, logger); err != nil {
				return fmt.Errorf("ensuring projection indexes: %w", err)
			}

			// Step 1: keyed issue_event projection (runs to completion first so
			// every thread with a keyed message has its issue before step 2).
			keyed := 0
			for {
				n, err := store.BackfillKeyedIssueProjection(ctx, batch)
				if err != nil {
					return err
				}
				keyed += n
				if n > 0 {
					fmt.Printf("  keyed projection: %d (running total %d)\n", n, keyed)
				}
				if n == 0 {
					break
				}
			}

			// Step 2: thread-inheritance over the remaining threaded rows.
			threaded := 0
			for {
				n, err := store.BackfillThreadInheritance(ctx, batch)
				if err != nil {
					return err
				}
				threaded += n
				if n > 0 {
					fmt.Printf("  thread inheritance: %d (running total %d)\n", n, threaded)
				}
				if n == 0 {
					break
				}
			}

			// Step 3: mark everything still unprojected mailing_list_only.
			marked, err := store.BackfillMarkRemainingProjected(ctx)
			if err != nil {
				return err
			}

			fmt.Printf("backfill-mailing-list-projection complete: %d keyed issue projections, %d thread-inherited, %d marked mailing_list_only\n",
				keyed, threaded, marked)
			return nil
		},
	}
	cmd.Flags().IntVar(&batch, "batch", 500, "rows per batch")
	return cmd
}
