// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// historyRotationAllowlist maps a current-snapshot table to its history
// companion for rotateRepoRowsToHistory. The helper refuses any pair
// not listed here: table names are interpolated into the SQL text
// (identifiers cannot be bind parameters), so the fixed allowlist is
// the injection guard.
//
// Only pairs whose history table is declared
// `LIKE <parent> INCLUDING ALL` (identical column order) may appear —
// the helper's `INSERT INTO history SELECT *` relies on positional
// column matching.
//
// Deliberately NOT migrated onto this helper (follow-up, out of scope
// for v0.27.7): RotateRepoInfoToHistory, RotateLibyearToHistory,
// rotateScancodeRows (multi-table, cross-schema), and the
// distribution rotation inside MarkDistributionComplete (runs inside
// an existing caller-owned transaction with its own quirks).
var historyRotationAllowlist = map[string]string{
	"aveloxis_data.repo_labor":          "aveloxis_data.repo_labor_history",
	"aveloxis_data.repo_deps_scorecard": "aveloxis_data.repo_deps_scorecard_history",
}

// rotationExecer is the slice of pgx.Tx (and *pgxpool.Pool) that
// rotateRepoRowsToHistory needs. Taking the interface lets the caller
// decide the transaction scope — the whole point of the helper is that
// rotation composes into the SAME transaction as the fresh-snapshot
// insert.
type rotationExecer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// rotateRepoRowsToHistory copies a repo's current rows from table into
// historyTable, then deletes them from table. Generic over the
// identical-column history pairs in historyRotationAllowlist; any
// other pair is refused. Same two-statement shape (INSERT ... SELECT
// then DELETE) and ordering as the bespoke Rotate*ToHistory functions
// it consolidates.
func rotateRepoRowsToHistory(ctx context.Context, q rotationExecer, table, historyTable string, repoID int64) error {
	want, ok := historyRotationAllowlist[table]
	if !ok || want != historyTable {
		return fmt.Errorf("rotateRepoRowsToHistory: table pair (%q -> %q) is not in the rotation allowlist", table, historyTable)
	}
	if _, err := q.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s SELECT * FROM %s WHERE repo_id = $1`, historyTable, table), repoID); err != nil {
		return fmt.Errorf("rotate %s to history: %w", table, err)
	}
	if _, err := q.Exec(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE repo_id = $1`, table), repoID); err != nil {
		return fmt.Errorf("clear %s after rotation: %w", table, err)
	}
	return nil
}

// RotateRepoInfoToHistory moves all existing repo_info rows for a repo into
// repo_info_history, then deletes them from the main table. This keeps only the
// latest snapshot in repo_info while preserving full history in repo_info_history.
//
// Called before InsertRepoInfo so the main table always has exactly one row
// per repo (the most recent collection).
func (s *PostgresStore) RotateRepoInfoToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Copy existing rows to history — INSERT ... SELECT preserves all columns.
		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_info_history
			SELECT * FROM aveloxis_data.repo_info
			WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		// Delete from main table so only the new snapshot lives there.
		_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
}

// RotateLibyearToHistory moves all existing libyear rows for a repo into
// repo_deps_libyear_history, then deletes them from the main table. This ensures
// the main table always has the latest snapshot with current license data.
// Called before inserting fresh libyear data during each analysis pass.
// Without rotation, old rows with empty licenses persist because the INSERT
// uses ON CONFLICT DO NOTHING which skips existing rows.
func (s *PostgresStore) RotateLibyearToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_libyear_history
			SELECT * FROM aveloxis_data.repo_deps_libyear
			WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
}

// ClearRepoDependencies deletes all repo_dependencies rows for a repo before
// re-inserting fresh data. Unlike libyear/scorecard, repo_dependencies has no
// history table — the table is a snapshot of current dependencies only. Without
// clearing, re-collection would either silently duplicate rows (no unique
// constraint) or skip them (ON CONFLICT DO NOTHING with stale data persisting).
func (s *PostgresStore) ClearRepoDependencies(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			DELETE FROM aveloxis_data.repo_dependencies WHERE repo_id = $1`, repoID)
		return err
	})
}

// RotateScorecardToHistory moves all existing scorecard rows for a repo into
// repo_deps_scorecard_history, then deletes them from the main table.
// Called before inserting new scorecard results.
//
// v0.27.7: delegates to rotateRepoRowsToHistory. Behavior-preserving
// refactor — identical SQL effect (INSERT ... SELECT * WHERE repo_id,
// then DELETE WHERE repo_id), identical transaction semantics
// (withRetry + single tx), identical rows moved. The only delta is on
// FAILURE paths, where errors now carry a "rotate/clear <table>"
// prefix identifying the failing statement.
func (s *PostgresStore) RotateScorecardToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if err := rotateRepoRowsToHistory(ctx, tx,
			"aveloxis_data.repo_deps_scorecard",
			"aveloxis_data.repo_deps_scorecard_history", repoID); err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
}

// rotateScancodeRows moves a repo's current scancode rows to the
// history tables inside the caller's transaction. Shared so
// ReplaceScancodeSnapshot can fuse the rotation with its inserts
// without a second copy of the statements (v0.28.19).
func rotateScancodeRows(ctx context.Context, tx pgx.Tx, repoID int64) error {

	// Rotate file results first (no FK, but logically dependent on scan).
	_, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_scan.scancode_file_results_history
			SELECT * FROM aveloxis_scan.scancode_file_results
			WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_scan.scancode_file_results WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}

	// Rotate scan metadata.
	_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_scan.scancode_scans_history
			SELECT * FROM aveloxis_scan.scancode_scans
			WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_scan.scancode_scans WHERE repo_id = $1`, repoID)
	if err != nil {
		return err
	}

	return nil
}
