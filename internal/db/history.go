package db

import "context"

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
func (s *PostgresStore) RotateScorecardToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_deps_scorecard_history
			SELECT * FROM aveloxis_data.repo_deps_scorecard
			WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx, `
			DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id = $1`, repoID)
		if err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
}

// RotateScancodeToHistory moves all existing scancode data (both scan metadata
// and per-file results) for a repo into the corresponding history tables, then
// deletes from the main tables. Called before inserting new scancode results.
func (s *PostgresStore) RotateScancodeToHistory(ctx context.Context, repoID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Rotate file results first (no FK, but logically dependent on scan).
		_, err = tx.Exec(ctx, `
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

		return tx.Commit(ctx)
	})
}
