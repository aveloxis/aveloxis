// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// `aveloxis migrate-cntrb-ids` — v0.22.2 one-shot data migration that
// swaps random-UUID cntrb_id values to deterministic PlatformUUID
// form. Opt-in (operator-invoked). Idempotent (re-runs are no-ops).
// Depends on v0.22.1's ON UPDATE CASCADE schema migration.
//
// Operator workflow:
//
//	aveloxis migrate-cntrb-ids --dry-run         # show the plan
//	aveloxis migrate-cntrb-ids                   # do it
//	aveloxis migrate-cntrb-ids --limit 10000     # incremental
//	aveloxis refresh-views                       # rebuild matviews
//
// See CLAUDE.md v0.22.2 for the full rationale.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/spf13/cobra"
)

func migrateCntrbIDsCmd(cfgPath *string) *cobra.Command {
	var (
		dryRun       bool
		batchSize    int
		limit        int
		skipPrecheck bool
	)

	cmd := &cobra.Command{
		Use:   "migrate-cntrb-ids",
		Short: "Migrate random cntrb_id values to deterministic PlatformUUID form",
		Long: `Opt-in one-shot data migration (v0.22.2).

Every contributor whose cntrb_id is a random UUID but whose
contributor_identities row carries a non-zero platform_user_id gets
its cntrb_id rewritten to PlatformUUID(platform_id, platform_user_id).
ON UPDATE CASCADE (added by v0.22.1's schema migration) propagates
the rewrite through every child FK column atomically.

Candidates whose target deterministic cntrb_id already exists on a
DIFFERENT row are skipped — those are rename-merge cases that need
v0.20.2-style soft-merge handling. A collision count is reported so
operators can plan a follow-up if desired.

Use --dry-run first to see the plan. The live migration runs in
batches (default 5000 rows / transaction) to keep lock windows
short on the cascading child tables (messages, pull_request_commits,
etc. each have tens of millions of rows on a large fleet).

Idempotent: already-deterministic rows are filtered out of the
candidate set, so re-running is a no-op once the migration completes.

Pre-check: refuses to run unless every cntrb_id FK has ON UPDATE
CASCADE. Override with --skip-precheck only if you know what you're
doing.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMigrateCntrbIDs(*cfgPath, dryRun, batchSize, limit, skipPrecheck)
		},
	}

	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "show the migration plan without writing")
	cmd.Flags().IntVar(&batchSize, "batch-size", 5000, "rows per UPDATE batch (tunes lock-window length)")
	cmd.Flags().IntVar(&limit, "limit", 0, "cap total rows migrated this run (0 = no cap)")
	cmd.Flags().BoolVar(&skipPrecheck, "skip-precheck", false, "skip the ON UPDATE CASCADE pre-check (DANGEROUS)")

	return cmd
}

func runMigrateCntrbIDs(cfgPath string, dryRun bool, batchSize, limit int, skipPrecheck bool) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()

	if !skipPrecheck {
		missing, err := db.PrecheckCntrbIDCascade(ctx, store, logger)
		if err != nil {
			return fmt.Errorf("pre-check query failed: %w", err)
		}
		if len(missing) > 0 {
			logger.Error("ON UPDATE CASCADE missing on cntrb_id FK(s)",
				"count", len(missing),
				"first_few", missing[:min(len(missing), 5)])
			return fmt.Errorf("%d cntrb_id FK constraint(s) missing ON UPDATE CASCADE "+
				"— run `aveloxis migrate` to add cascade behavior, "+
				"or pass --skip-precheck to override", len(missing))
		}
		logger.Info("pre-check OK — all cntrb_id FKs have ON UPDATE CASCADE")
	}

	counts, err := db.CountCntrbIDMigrationCandidates(ctx, store)
	if err != nil {
		return fmt.Errorf("counting candidates: %w", err)
	}

	logger.Info("v0.22.2 cntrb_id migration plan",
		"total_candidates", counts.Total,
		"safe_to_migrate", counts.Safe,
		"collisions_skipped", counts.Collisions,
		"dry_run", dryRun,
		"batch_size", batchSize,
		"limit_cap", limit)

	if counts.Collisions > 0 {
		logger.Warn("collisions detected — rows skipped",
			"count", counts.Collisions,
			"hint", "these are rename-merge cases; run a v0.20.2-style soft-merge separately if you want them consolidated")
	}

	if counts.Safe == 0 {
		logger.Info("nothing to migrate — DB is already deterministic for all eligible rows")
		return nil
	}

	if dryRun {
		sample, err := db.SampleCntrbIDMigrationCandidates(ctx, store, 20)
		if err != nil {
			return fmt.Errorf("sample query: %w", err)
		}
		logger.Info("=== sample plan (first 20 rows) ===")
		for _, c := range sample {
			logger.Info("  candidate",
				"login", c.Login, "platform_user_id", c.PlatformUserID,
				"old", c.OldID, "new", c.NewID)
		}
		logger.Info("dry-run complete — no changes written. Re-run without --dry-run to apply.")
		return nil
	}

	// Live migration: batched UPDATEs. Each batch is one transaction.
	// The UPDATE locks contributors rows AND cascades through every
	// FK column on every child table. Batching keeps the lock window
	// per transaction bounded.
	migrated := int64(0)
	startedAt := time.Now()
	for {
		if limit > 0 && migrated >= int64(limit) {
			logger.Info("hit --limit cap, stopping", "migrated", migrated, "limit", limit)
			break
		}
		thisBatch := batchSize
		if limit > 0 && migrated+int64(thisBatch) > int64(limit) {
			thisBatch = limit - int(migrated)
		}

		rows, err := db.MigrateCntrbIDsBatch(ctx, store, thisBatch)
		if err != nil {
			return fmt.Errorf("batch UPDATE at offset %d: %w", migrated, err)
		}
		migrated += rows
		logger.Info("batch complete",
			"batch_rows", rows,
			"total_migrated", migrated,
			"elapsed", time.Since(startedAt).Round(time.Second))

		if rows == 0 {
			// Either we've drained all candidates, or all remaining are
			// collisions. Either way, we're done.
			break
		}
	}

	logger.Info("v0.22.2 cntrb_id migration complete",
		"migrated", migrated,
		"collisions_skipped", counts.Collisions,
		"duration", time.Since(startedAt).Round(time.Second),
		"next_step", "run `aveloxis merge-cntrb-collisions` and then, if you think it is a good idea, `aveloxis refresh-views` to rebuild any matviews that depend on cntrb_id joins")

	return nil
}
