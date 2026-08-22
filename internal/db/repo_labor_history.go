// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// v0.27.7 — one-shot rotation of every NON-latest repo_labor snapshot
// into repo_labor_history.
//
// Pre-v0.27.7, every scc analysis run INSERTed a full fresh per-file
// snapshot into repo_labor with a new rl_analysis_date and nothing was
// ever rotated or deleted — production reached 2.0M live rows / 29 GB
// and grew unboundedly. The forward fix (ReplaceRepoLaborSnapshot)
// keeps the table single-snapshot from here on; this migration moves
// the accumulated historical cohorts out of the current table once.
//
// Batching contract (v0.26.6 lesson — the 9h45m single-batch grind):
// KEYSET WINDOWS over the repo_labor_id primary key, NEVER
// LIMIT-rescan loops and NEVER per-batch DISTINCT ON global sorts.
// Every window is a bounded PK index-range scan; the per-row
// "is this the repo's latest cohort?" check is a correlated MAX
// lookup served by idx_repo_labor_repo_id_analysis_date
// ((repo_id, rl_analysis_date DESC), built in the v0.25.5 block of
// RunMigrations, which executes before this migration). Each window
// is its own implicit transaction, so an interrupted run keeps all
// completed windows and simply resumes on the next migrate.
//
// NULL rl_analysis_date rule (legacy rows predating consistent date
// stamping): NULL sorts as "oldest". Concretely, MAX(rl_analysis_date)
// ignores NULLs, and `NULL IS DISTINCT FROM <non-NULL max>` is TRUE,
// so NULL-dated rows rotate to history whenever the repo has ANY dated
// snapshot. A repo whose rows are ALL NULL-dated has MAX = NULL and
// `NULL IS DISTINCT FROM NULL` is FALSE — those rows stay in the
// current table: with no dates there is exactly one indistinguishable
// cohort, and it is by definition the latest one we have.
//
// Idempotency: after the first pass only each repo's latest cohort
// remains, so the movable predicate matches nothing on re-runs
// (single-snapshot repos never match at all).
//
// Downstream consumers are unaffected: explorer_repo_files /
// explorer_repo_languages and LaborInvestmentSnapshot all filter to
// the latest rl_analysis_date per repo, which is exactly the cohort
// that stays.

// RepoLaborRotateWindowSQL is the per-window statement of the v0.27.7
// one-shot migration. $1 = exclusive lower PK bound, $2 = inclusive
// upper PK bound. Exposed as a named const so it can be extracted and
// run standalone (e.g., timing validation against the production DB
// inside BEGIN ... ROLLBACK with literal bounds substituted).
//
// DELETE ... RETURNING feeding the INSERT keeps the move atomic within
// one statement; column order matches because repo_labor_history is
// declared `LIKE aveloxis_data.repo_labor INCLUDING ALL`.
const RepoLaborRotateWindowSQL = `
WITH moved AS (
    DELETE FROM aveloxis_data.repo_labor l
    WHERE l.repo_labor_id > $1
      AND l.repo_labor_id <= $2
      AND l.rl_analysis_date IS DISTINCT FROM (
          SELECT MAX(l2.rl_analysis_date)
          FROM aveloxis_data.repo_labor l2
          WHERE l2.repo_id = l.repo_id)
    RETURNING l.*
)
INSERT INTO aveloxis_data.repo_labor_history
SELECT * FROM moved`

// RepoLaborRotateBoundsSQL yields the PK range the window loop walks.
// COALESCE(..., 0) makes the empty-table case a clean no-op.
const RepoLaborRotateBoundsSQL = `
SELECT COALESCE(MIN(repo_labor_id), 0), COALESCE(MAX(repo_labor_id), 0)
FROM aveloxis_data.repo_labor`

// RepoLaborRotateWindowSize is the PK-window width for the production
// run. Following the v0.26.6 guidance: window math sized against
// production PK ranges — repo_labor tops out around 2M ids, so 100K-id
// windows mean ~20 bounded statements.
const RepoLaborRotateWindowSize int64 = 100_000

// migrateRepoLaborSnapshotsToHistory is the RunMigrations entry point.
// Honors the v0.19.4 fail-closed contract: any window error is logged
// at ERROR and appended to the collector so `aveloxis serve` refuses
// to start until resolved.
func migrateRepoLaborSnapshotsToHistory(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	const label = "v0.27.7 rotate non-latest repo_labor snapshots to repo_labor_history"
	moved, windows, err := rotateRepoLaborSnapshotWindows(ctx, pg, logger, RepoLaborRotateWindowSize)
	if err != nil {
		logger.Error("schema migration error", "step", label, "error", err)
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
		return
	}
	logger.Info("migration step ok", "label", label, "rows_moved", moved, "windows", windows)
}

// rotateRepoLaborSnapshotWindows is the testable core of the v0.27.7
// migration: walks the repo_labor PK range in keyset windows of
// windowSize ids, executing RepoLaborRotateWindowSQL per window (each
// its own implicit transaction). Returns total rows moved and windows
// executed. On a window error it stops and returns — completed windows
// stay committed and the next run resumes idempotently.
func rotateRepoLaborSnapshotWindows(ctx context.Context, pg *PostgresStore, logger *slog.Logger, windowSize int64) (moved, windows int64, err error) {
	if windowSize <= 0 {
		windowSize = RepoLaborRotateWindowSize
	}
	var lo, hi int64
	if err := pg.pool.QueryRow(ctx, RepoLaborRotateBoundsSQL).Scan(&lo, &hi); err != nil {
		return 0, 0, fmt.Errorf("repo_labor rotation bounds: %w", err)
	}
	if hi == 0 {
		// Empty table (fresh install) — nothing to rotate.
		return 0, 0, nil
	}
	for start := lo - 1; start < hi; start += windowSize {
		end := start + windowSize
		tag, execErr := pg.pool.Exec(ctx, RepoLaborRotateWindowSQL, start, end)
		if execErr != nil {
			return moved, windows, fmt.Errorf("repo_labor rotation window (%d, %d]: %w", start, end, execErr)
		}
		moved += tag.RowsAffected()
		windows++
		if windows%10 == 0 {
			logger.Info("repo_labor snapshot rotation progress",
				"windows_done", windows, "rows_moved", moved, "pk_high_water", end, "pk_max", hi)
		}
	}
	return moved, windows, nil
}

// ensureRepoLaborHistoryIndex builds the plain repo_id index on
// repo_labor_history — the ONE deliberate index on the history table
// (dedup-repos' hygiene delete probes it; 188 measured scans on
// production). Declared under the exact auto-generated name a
// LIKE-INCLUDING-ALL copy produced on fleets whose v0.27.7 migration
// ran after repo_labor had its indexes, so those fleets no-op via
// IF NOT EXISTS.
//
// v0.27.123 (Copilot round 15, suppressed — the v0.27.98 rule): this
// index is MIGRATION-ONLY. A schema.sql declaration executes in base
// DDL before any migration step and would block-build with a plain
// CREATE INDEX on upgraded fleets that LACK the accidental copy —
// blocking rotation writers on a fleet-scale history table for the
// build. Fresh installs get it through this same step (CONCURRENTLY
// on an empty table is instant — the v0.27.98 precedent).
func ensureRepoLaborHistoryIndex(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	execCreateIndexConcurrently(ctx, pg, logger, errs,
		"aveloxis_data", "repo_labor_history_repo_id_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS repo_labor_history_repo_id_idx
		 ON aveloxis_data.repo_labor_history (repo_id)`)
}
