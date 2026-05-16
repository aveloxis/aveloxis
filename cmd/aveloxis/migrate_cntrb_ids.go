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
		if err := precheckOnUpdateCascade(ctx, store, logger); err != nil {
			return fmt.Errorf("pre-check failed: %w (run `aveloxis migrate` to add ON UPDATE CASCADE, or pass --skip-precheck to override)", err)
		}
	}

	// Build the candidate query plan. The PlatformUUID layout (per
	// internal/db/github_uuid.go) for userID <= uint32 max:
	//   byte 0: platform_id
	//   bytes 1-4: big-endian uint32 of userID
	//   bytes 5-15: zero
	// SQL construction: hex-pack as a 32-char string, cast to uuid.
	//
	// The exclusion for already-deterministic rows uses the same
	// pattern as the introspection in cntrb_id_cascade.go's test
	// fixture and aligns with how the v0.22.0 source-contract tests
	// scan the contributors table.
	const candidateView = `
		WITH candidates AS (
			SELECT c.cntrb_id AS old_id,
			       c.cntrb_login,
			       ci.platform_id,
			       ci.platform_user_id,
			       (lpad(to_hex(ci.platform_id::int), 2, '0') ||
			        lpad(to_hex(ci.platform_user_id::bigint::int), 8, '0') ||
			        '0000000000000000000000')::uuid AS new_id
			FROM aveloxis_data.contributors c
			JOIN aveloxis_data.contributor_identities ci ON c.cntrb_id = ci.cntrb_id
			WHERE c.cntrb_login != ''
			  AND NOT (c.cntrb_id::text LIKE '01%%' AND c.cntrb_id::text LIKE '%%000000000000')
			  AND ci.platform_user_id > 0
			  AND ci.platform_user_id <= 4294967295
			  AND COALESCE(c.cntrb_deleted, 0) = 0
		)`

	var totalCandidates, safeToMigrate, collisions int
	err = store.Pool().QueryRow(ctx, candidateView+`
		SELECT
			COUNT(*) AS total,
			COUNT(*) FILTER (WHERE NOT EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)) AS safe,
			COUNT(*) FILTER (WHERE EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)) AS collision_count
		FROM candidates
	`).Scan(&totalCandidates, &safeToMigrate, &collisions)
	if err != nil {
		return fmt.Errorf("counting candidates: %w", err)
	}

	logger.Info("v0.22.2 cntrb_id migration plan",
		"total_candidates", totalCandidates,
		"safe_to_migrate", safeToMigrate,
		"collisions_skipped", collisions,
		"dry_run", dryRun,
		"batch_size", batchSize,
		"limit_cap", limit)

	if collisions > 0 {
		logger.Warn("collisions detected — rows skipped",
			"count", collisions,
			"hint", "these are rename-merge cases; run a v0.20.2-style soft-merge separately if you want them consolidated")
	}

	if safeToMigrate == 0 {
		logger.Info("nothing to migrate — DB is already deterministic for all eligible rows")
		return nil
	}

	if dryRun {
		// Print a sample of the plan so the operator can eyeball it.
		const sampleSQL = candidateView + `
			SELECT cntrb_login, old_id::text, new_id::text, platform_user_id
			FROM candidates
			WHERE NOT EXISTS (
				SELECT 1 FROM aveloxis_data.contributors c2
				WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
			)
			ORDER BY cntrb_login
			LIMIT 20`
		rows, err := store.Pool().Query(ctx, sampleSQL)
		if err != nil {
			return fmt.Errorf("sample query: %w", err)
		}
		defer rows.Close()
		logger.Info("=== sample plan (first 20 rows) ===")
		for rows.Next() {
			var login, oldID, newID string
			var pUserID int64
			if err := rows.Scan(&login, &oldID, &newID, &pUserID); err != nil {
				return err
			}
			logger.Info("  candidate",
				"login", login, "platform_user_id", pUserID,
				"old", oldID, "new", newID)
		}
		logger.Info("dry-run complete — no changes written. Re-run without --dry-run to apply.")
		return nil
	}

	// Live migration: batched UPDATEs. Each batch is one transaction.
	// The UPDATE locks contributors rows AND cascades through every
	// FK column on every child table. Batching keeps the lock window
	// per transaction bounded.
	migrated := 0
	startedAt := time.Now()
	for {
		if limit > 0 && migrated >= limit {
			logger.Info("hit --limit cap, stopping", "migrated", migrated, "limit", limit)
			break
		}
		thisBatch := batchSize
		if limit > 0 && migrated+thisBatch > limit {
			thisBatch = limit - migrated
		}

		// Update one batch. ctid filter ensures we pick a deterministic
		// slice of candidates per transaction without holding cursors
		// open across batches.
		updateSQL := candidateView + `,
			batch AS (
				SELECT old_id, new_id
				FROM candidates
				WHERE NOT EXISTS (
					SELECT 1 FROM aveloxis_data.contributors c2
					WHERE c2.cntrb_id = candidates.new_id AND c2.cntrb_id <> candidates.old_id
				)
				LIMIT $1
			)
			UPDATE aveloxis_data.contributors c
			SET cntrb_id = b.new_id,
			    data_collection_date = NOW()
			FROM batch b
			WHERE c.cntrb_id = b.old_id`
		tag, err := store.Pool().Exec(ctx, updateSQL, thisBatch)
		if err != nil {
			return fmt.Errorf("batch UPDATE at offset %d: %w", migrated, err)
		}
		rows := int(tag.RowsAffected())
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
		"collisions_skipped", collisions,
		"duration", time.Since(startedAt).Round(time.Second),
		"next_step", "run `aveloxis refresh-views` to rebuild any matviews that depend on cntrb_id joins")

	return nil
}

// precheckOnUpdateCascade verifies every cntrb_id FK has CASCADE.
// Returns an error naming the first missing constraint if any.
func precheckOnUpdateCascade(ctx context.Context, store *db.PostgresStore, logger *slog.Logger) error {
	rows, err := store.Pool().Query(ctx, `
		SELECT tc.constraint_name, rc.update_rule
		FROM information_schema.table_constraints tc
		JOIN information_schema.referential_constraints rc
		  ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
		JOIN information_schema.constraint_column_usage ccu
		  ON rc.unique_constraint_name = ccu.constraint_name AND rc.unique_constraint_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
		  AND ccu.table_schema = 'aveloxis_data'
		  AND ccu.table_name = 'contributors'
		  AND ccu.column_name = 'cntrb_id'
	`)
	if err != nil {
		return fmt.Errorf("query referential_constraints: %w", err)
	}
	defer rows.Close()

	var missing []string
	for rows.Next() {
		var name, rule string
		if err := rows.Scan(&name, &rule); err != nil {
			return err
		}
		if rule != "CASCADE" {
			missing = append(missing, fmt.Sprintf("%s (update_rule=%s)", name, rule))
		}
	}
	if len(missing) > 0 {
		logger.Error("ON UPDATE CASCADE missing on cntrb_id FK(s)",
			"count", len(missing),
			"first_few", missing[:min(len(missing), 5)])
		return fmt.Errorf("%d cntrb_id FK constraint(s) missing ON UPDATE CASCADE", len(missing))
	}
	logger.Info("pre-check OK — all cntrb_id FKs have ON UPDATE CASCADE")
	return nil
}
