// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// `aveloxis merge-cntrb-collisions` — v0.22.3 one-shot consolidation
// of the rename-merge cases v0.22.2's data migration left behind.
// Soft-merges each loser/winner pair via the v0.20.2 phase D
// pattern: move identities, copy non-empty fields, insert alias,
// mark loser cntrb_deleted=1.
//
// Operator workflow:
//
//	aveloxis merge-cntrb-collisions --dry-run     # show the plan
//	aveloxis merge-cntrb-collisions               # merge
//
// See CLAUDE.md v0.22.3 for the full rationale.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func mergeCntrbCollisionsCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun    bool
		batchSize int
		limit     int
	)

	cmd := &cobra.Command{
		Use:   "merge-cntrb-collisions",
		Short: "Soft-merge the cntrb_id collisions left over from migrate-cntrb-ids",
		Long: `Opt-in one-shot data consolidation (v0.22.3).

For each rename-merge collision identified by ` + "`aveloxis migrate-cntrb-ids`" + `
(loser row has random cntrb_id, winner row has the deterministic
PlatformUUID target ID, both reference the same gh_user_id):

  1. Move loser's contributor_identities rows to winner so future
     Resolve calls hit the active row.
  2. Merge non-empty fields from loser into winner with prefer-
     existing semantics (cntrb_email, cntrb_canonical, cntrb_company,
     cntrb_location, cntrb_full_name).
  3. Insert a contributors_aliases row mapping loser's email to
     winner so commit-email resolution still routes correctly.
  4. Mark loser cntrb_deleted = 1. Row stays in place; child FKs
     remain valid; read-path queries filter via COALESCE(
     cntrb_deleted, 0) = 0.

No FK rewrites — same R10 (FK integrity) guarantee v0.20.2 phase D
established. Operator can run this any time after migrate-cntrb-ids;
it's independent and idempotent.

Use --dry-run first to see the plan.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMergeCntrbCollisions(*cfgPath, dryRun, batchSize, limit)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show the merge plan without writing")
	cmd.Flags().IntVar(&batchSize, "batch-size", 500, "pairs merged per batch (each pair is one transaction)")
	cmd.Flags().IntVar(&limit, "limit", 0, "cap total pairs merged this run (0 = no cap)")

	return cmd
}

func runMergeCntrbCollisions(cfgPath string, dryRun bool, batchSize, limit int) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	count, err := db.CountCntrbIDCollisions(ctx, store)
	if err != nil {
		return fmt.Errorf("count collisions: %w", err)
	}
	logger.Info("v0.22.3 cntrb_id collision merge plan",
		"unresolved_collisions", count,
		"dry_run", dryRun,
		"batch_size", batchSize,
		"limit_cap", limit)

	if count == 0 {
		logger.Info("no unresolved collisions — nothing to merge")
		return nil
	}

	if dryRun {
		sample, err := db.SampleCntrbIDCollisions(ctx, store, 20)
		if err != nil {
			return fmt.Errorf("sample collisions: %w", err)
		}
		logger.Info("=== sample plan (first 20 pairs) ===")
		for _, c := range sample {
			logger.Info("  collision",
				"loser_login", c.LoserLogin, "winner_login", c.WinnerLogin,
				"platform_user_id", c.PlatformUserID,
				"loser_id", c.LoserID, "winner_id", c.WinnerID)
		}
		logger.Info("dry-run complete — no changes written. Re-run without --dry-run to apply.")
		return nil
	}

	merged := 0
	startedAt := time.Now()
	for {
		if limit > 0 && merged >= limit {
			logger.Info("hit --limit cap, stopping", "merged", merged, "limit", limit)
			break
		}
		thisBatch := batchSize
		if limit > 0 && merged+thisBatch > limit {
			thisBatch = limit - merged
		}

		batchMerged, err := db.MergeCntrbIDCollisionsBatch(ctx, store, thisBatch)
		merged += batchMerged
		if err != nil {
			logger.Error("batch merge errored mid-way — partial progress preserved",
				"batch_merged", batchMerged, "total_merged", merged, "error", err)
			return err
		}
		logger.Info("batch complete",
			"batch_merged", batchMerged,
			"total_merged", merged,
			"elapsed", time.Since(startedAt).Round(time.Second))

		if batchMerged == 0 {
			break
		}
	}

	logger.Info("v0.22.3 cntrb_id collision merge complete",
		"merged", merged,
		"duration", time.Since(startedAt).Round(time.Second))
	return nil
}
