// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RefreshRepoAggregates recomputes the dm_repo_annual, dm_repo_monthly, and
// dm_repo_weekly tables for a single repo by aggregating commit data.
// This is the equivalent of Augur's facade post-processing.
//
// Each table is refreshed inside a transaction (DELETE old + INSERT new) so
// readers never see a half-updated state.
func (s *PostgresStore) RefreshRepoAggregates(ctx context.Context, repoID int64) error {
	type aggQuery struct {
		name   string
		delete string
		insert string
	}

	queries := []aggQuery{
		{
			name:   "dm_repo_annual",
			delete: `DELETE FROM aveloxis_data.dm_repo_annual WHERE repo_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_annual
					(repo_id, email, affiliation, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					repo_id,
					cmt_author_email AS email,
					COALESCE(NULLIF(cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(YEAR FROM cmt_author_timestamp)::smallint AS year,
					SUM(cmt_added) AS added,
					SUM(cmt_removed) AS removed,
					SUM(cmt_whitespace) AS whitespace,
					COUNT(DISTINCT cmt_filename) AS files,
					COUNT(DISTINCT cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits
				WHERE repo_id = $1
				  AND cmt_author_timestamp IS NOT NULL
				GROUP BY repo_id, cmt_author_email, cmt_author_affiliation,
				         EXTRACT(YEAR FROM cmt_author_timestamp)`,
		},
		{
			name:   "dm_repo_monthly",
			delete: `DELETE FROM aveloxis_data.dm_repo_monthly WHERE repo_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_monthly
					(repo_id, email, affiliation, month, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					repo_id,
					cmt_author_email AS email,
					COALESCE(NULLIF(cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(MONTH FROM cmt_author_timestamp)::smallint AS month,
					EXTRACT(YEAR FROM cmt_author_timestamp)::smallint AS year,
					SUM(cmt_added) AS added,
					SUM(cmt_removed) AS removed,
					SUM(cmt_whitespace) AS whitespace,
					COUNT(DISTINCT cmt_filename) AS files,
					COUNT(DISTINCT cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits
				WHERE repo_id = $1
				  AND cmt_author_timestamp IS NOT NULL
				GROUP BY repo_id, cmt_author_email, cmt_author_affiliation,
				         EXTRACT(MONTH FROM cmt_author_timestamp),
				         EXTRACT(YEAR FROM cmt_author_timestamp)`,
		},
		{
			name:   "dm_repo_weekly",
			delete: `DELETE FROM aveloxis_data.dm_repo_weekly WHERE repo_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_weekly
					(repo_id, email, affiliation, week, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					repo_id,
					cmt_author_email AS email,
					COALESCE(NULLIF(cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(WEEK FROM cmt_author_timestamp)::smallint AS week,
					EXTRACT(YEAR FROM cmt_author_timestamp)::smallint AS year,
					SUM(cmt_added) AS added,
					SUM(cmt_removed) AS removed,
					SUM(cmt_whitespace) AS whitespace,
					COUNT(DISTINCT cmt_filename) AS files,
					COUNT(DISTINCT cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits
				WHERE repo_id = $1
				  AND cmt_author_timestamp IS NOT NULL
				GROUP BY repo_id, cmt_author_email, cmt_author_affiliation,
				         EXTRACT(WEEK FROM cmt_author_timestamp),
				         EXTRACT(YEAR FROM cmt_author_timestamp)`,
		},
	}

	for _, q := range queries {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", q.name, err)
		}
		if _, err := tx.Exec(ctx, q.delete, repoID); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("deleting %s for repo %d: %w", q.name, repoID, err)
		}
		if _, err := tx.Exec(ctx, q.insert, repoID); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("inserting %s for repo %d: %w", q.name, repoID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing %s for repo %d: %w", q.name, repoID, err)
		}
	}
	return nil
}

// RefreshRepoGroupAggregates recomputes the dm_repo_group_annual/monthly/weekly
// tables for the repo group that contains the given repo.
func (s *PostgresStore) RefreshRepoGroupAggregates(ctx context.Context, repoID int64) error {
	// Look up the repo_group_id.
	var rgID *int64
	err := s.pool.QueryRow(ctx,
		`SELECT repo_group_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID,
	).Scan(&rgID)
	if err != nil {
		// A lookup ERROR is not "no group" (SR-5) — RefreshAllRepoAggregates
		// counts it, or a canceled pass would report failed_groups=0.
		return fmt.Errorf("repo_group for repo %d: %w", repoID, err)
	}
	if rgID == nil {
		return nil // no group, nothing to aggregate
	}
	return s.RefreshGroupAggregates(ctx, *rgID)
}

// RefreshGroupAggregates rebuilds the dm_repo_group_* tables for ONE
// group. Every statement below is scoped `WHERE r.repo_group_id = $1`,
// so the work is proportional to the whole GROUP, not to any one repo
// in it — which is why RefreshAllRepoAggregates drives this per group
// and not per repo (v0.28.19).
func (s *PostgresStore) RefreshGroupAggregates(ctx context.Context, groupID int64) error {
	rgID := &groupID

	type aggQuery struct {
		name   string
		delete string
		insert string
	}

	queries := []aggQuery{
		{
			name:   "dm_repo_group_annual",
			delete: `DELETE FROM aveloxis_data.dm_repo_group_annual WHERE repo_group_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_group_annual
					(repo_group_id, email, affiliation, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					r.repo_group_id,
					c.cmt_author_email AS email,
					COALESCE(NULLIF(c.cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(YEAR FROM c.cmt_author_timestamp)::smallint AS year,
					SUM(c.cmt_added) AS added,
					SUM(c.cmt_removed) AS removed,
					SUM(c.cmt_whitespace) AS whitespace,
					COUNT(DISTINCT c.cmt_filename) AS files,
					COUNT(DISTINCT c.cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits c
				JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id
				WHERE r.repo_group_id = $1
				  AND c.cmt_author_timestamp IS NOT NULL
				GROUP BY r.repo_group_id, c.cmt_author_email, c.cmt_author_affiliation,
				         EXTRACT(YEAR FROM c.cmt_author_timestamp)`,
		},
		{
			name:   "dm_repo_group_monthly",
			delete: `DELETE FROM aveloxis_data.dm_repo_group_monthly WHERE repo_group_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_group_monthly
					(repo_group_id, email, affiliation, month, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					r.repo_group_id,
					c.cmt_author_email AS email,
					COALESCE(NULLIF(c.cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(MONTH FROM c.cmt_author_timestamp)::smallint AS month,
					EXTRACT(YEAR FROM c.cmt_author_timestamp)::smallint AS year,
					SUM(c.cmt_added) AS added,
					SUM(c.cmt_removed) AS removed,
					SUM(c.cmt_whitespace) AS whitespace,
					COUNT(DISTINCT c.cmt_filename) AS files,
					COUNT(DISTINCT c.cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits c
				JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id
				WHERE r.repo_group_id = $1
				  AND c.cmt_author_timestamp IS NOT NULL
				GROUP BY r.repo_group_id, c.cmt_author_email, c.cmt_author_affiliation,
				         EXTRACT(MONTH FROM c.cmt_author_timestamp),
				         EXTRACT(YEAR FROM c.cmt_author_timestamp)`,
		},
		{
			name:   "dm_repo_group_weekly",
			delete: `DELETE FROM aveloxis_data.dm_repo_group_weekly WHERE repo_group_id = $1`,
			insert: `
				INSERT INTO aveloxis_data.dm_repo_group_weekly
					(repo_group_id, email, affiliation, week, year, added, removed, whitespace, files, patches,
					 tool_source, data_source)
				SELECT
					r.repo_group_id,
					c.cmt_author_email AS email,
					COALESCE(NULLIF(c.cmt_author_affiliation,''), '(Unknown)') AS affiliation,
					EXTRACT(WEEK FROM c.cmt_author_timestamp)::smallint AS week,
					EXTRACT(YEAR FROM c.cmt_author_timestamp)::smallint AS year,
					SUM(c.cmt_added) AS added,
					SUM(c.cmt_removed) AS removed,
					SUM(c.cmt_whitespace) AS whitespace,
					COUNT(DISTINCT c.cmt_filename) AS files,
					COUNT(DISTINCT c.cmt_commit_hash) AS patches,
					'aveloxis-facade', 'git'
				FROM aveloxis_data.commits c
				JOIN aveloxis_data.repos r ON r.repo_id = c.repo_id
				WHERE r.repo_group_id = $1
				  AND c.cmt_author_timestamp IS NOT NULL
				GROUP BY r.repo_group_id, c.cmt_author_email, c.cmt_author_affiliation,
				         EXTRACT(WEEK FROM c.cmt_author_timestamp),
				         EXTRACT(YEAR FROM c.cmt_author_timestamp)`,
		},
	}

	for _, q := range queries {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", q.name, err)
		}
		if _, err := tx.Exec(ctx, q.delete, *rgID); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("deleting %s for group %d: %w", q.name, *rgID, err)
		}
		if _, err := tx.Exec(ctx, q.insert, *rgID); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("inserting %s for group %d: %w", q.name, *rgID, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing %s for group %d: %w", q.name, *rgID, err)
		}
	}
	return nil
}

// DMAggregatesAdvisoryLockID serializes dm_ aggregate rebuilds across
// processes ("AVLXDAGG" packed into 64 bits). The dm_* tables have no
// PK/UNIQUE and each repo is a DELETE then INSERT, so two concurrent
// passes (the weekly scheduler rebuild and `aveloxis refresh-views
// --aggregates`) would interleave and duplicate aggregate rows.
const DMAggregatesAdvisoryLockID int64 = 0x41564C5844414747

// ErrAggregateRebuildRunning is returned when another process holds the
// dm_ aggregate advisory lock.
var ErrAggregateRebuildRunning = errors.New("another dm_ aggregate rebuild is already running (advisory lock held)")

// RefreshAllRepoAggregates recomputes dm_repo_annual/monthly/weekly and
// dm_repo_group_annual/monthly/weekly for ALL repos. Two callers: the
// weekly matview rebuild (collection claims paused under
// MatviewRebuildActive) and `aveloxis refresh-views --aggregates`
// (v0.28.18). The pass holds DMAggregatesAdvisoryLockID for its whole
// duration and returns ErrAggregateRebuildRunning if another holder
// exists — the layer enforces the "one pass at a time" invariant (SR-18)
// so neither caller has to know about the other.
//
// Shape (pass 28 rewrote a doc that claimed bulk SQL): two per-repo
// DELETE+INSERT loops — the repo tables, then the group tables via each
// repo's group (same-group repos are refreshed redundantly; per-group is a
// future optimization). One bad repo never aborts the fleet: failures are
// WARN'd, counted, and returned bounded; a canceled ctx is one exit.
func (s *PostgresStore) RefreshAllRepoAggregates(ctx context.Context, logger interface {
	Info(string, ...any)
	Warn(string, ...any)
}) error {
	lockConn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquiring connection for the dm_ aggregate lock: %w", err)
	}
	var locked bool
	if err := lockConn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, DMAggregatesAdvisoryLockID).Scan(&locked); err != nil {
		lockConn.Release()
		return fmt.Errorf("dm_ aggregate advisory lock: %w", err)
	}
	if !locked {
		lockConn.Release()
		return ErrAggregateRebuildRunning
	}
	defer func() {
		// Unlock on a fresh bounded context — the caller's ctx may be the
		// one that just got canceled (the v0.27.130 lesson); an unconfirmed
		// unlock destroys the session so the lock cannot leak.
		unlockCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var unlocked bool
		if err := lockConn.QueryRow(unlockCtx, `SELECT pg_advisory_unlock($1)`, DMAggregatesAdvisoryLockID).Scan(&unlocked); err != nil || !unlocked {
			_ = lockConn.Conn().Close(unlockCtx)
		}
		lockConn.Release()
	}()

	// Get all repo IDs that have commits.
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT repo_id FROM aveloxis_data.commits WHERE cmt_author_timestamp IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("querying repos with commits: %w", err)
	}
	defer rows.Close()

	var repoIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scanning repo id: %w", err)
		}
		repoIDs = append(repoIDs, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(repoIDs) == 0 {
		return nil
	}

	logger.Info("refreshing dm_ aggregate tables", "repos", len(repoIDs))

	// Copilot round 4 (v0.28.18): per-repo / per-group failures used to
	// be logged and dropped, so the pass returned nil over stale dm_
	// rows and `refresh-views --aggregates` exited 0. Keep going (one
	// bad repo must not abort the fleet) but ACCUMULATE and return them.
	var failed []error
	failedRepos, failedGroups := 0, 0
	for _, repoID := range repoIDs {
		if ctx.Err() != nil {
			// One exit, not ~140K "context canceled" WARNs (a `stop serve`
			// inside the multi-day weekly pass is the common shape).
			return ctx.Err()
		}
		if err := s.RefreshRepoAggregates(ctx, repoID); err != nil {
			// Copilot round 8: shutdown is not a per-repo failure. The
			// loop-top guard runs BEFORE the call; the call that
			// observed the cancellation is already past it, so without
			// this every `stop serve` mid-pass emits a WARN and records
			// a stale-row failure for the repo it was working on.
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			logger.Warn("aggregate refresh failed", "repo_id", repoID, "error", err)
			failedRepos++
			failed = append(failed, fmt.Errorf("repo %d: %w", repoID, err))
			continue // Don't abort all repos if one fails.
		}
	}

	// Group tables: once per GROUP, not once per repo.
	//
	// Each dm_repo_group_* statement is scoped `WHERE r.repo_group_id
	// = $1`, so driving it from repos rebuilt the entire group for
	// every member: after the v0.27.17 consolidation collapsed 93,912
	// per-repo `Default` groups into one canonical group, a group
	// holding N repos did N identical DELETE + full-group-aggregate +
	// INSERT passes across three tables (measured locally: one group
	// with 8,766 repos). All of it under MatviewRebuildActive, which
	// pauses collection claims — the v0.16.5 per-repo-aggregate class,
	// reintroduced by consolidation (Copilot, v0.28.19).
	//
	// The group ids are read into a slice BEFORE the loop: the
	// v0.25.36-era `SELECT DISTINCT repo_group_id` that used to sit
	// here held its Rows open across the whole loop, pinning a pool
	// connection, and buried its own error behind ten repo failures
	// (pass 28). Both of those are the consumption pattern, not the
	// query.
	groupIDs, err := s.distinctRepoGroupIDs(ctx)
	if err != nil {
		logger.Warn("could not enumerate repo groups; dm_repo_group_* not refreshed", "error", err)
		failedGroups++
		failed = append(failed, fmt.Errorf("enumerating repo groups: %w", err))
	}
	for _, groupID := range groupIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := s.RefreshGroupAggregates(ctx, groupID); err != nil {
			// Copilot round 8: same shutdown misclassification as the
			// repo loop above.
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			logger.Warn("group aggregate refresh failed", "repo_group_id", groupID, "error", err)
			failedGroups++
			failed = append(failed, fmt.Errorf("group %d: %w", groupID, err))
		}
	}

	if ctx.Err() != nil {
		// A cancel inside the LAST item never reaches a loop-top guard;
		// a killed pass must not log "refreshed" nor hide the cancel
		// behind a truncated join.
		return ctx.Err()
	}
	logger.Info("dm_ aggregate tables refreshed", "repos", len(repoIDs), "groups", len(groupIDs), "failed_repos", failedRepos, "failed_groups", failedGroups)
	if len(failed) > 0 {
		return boundedJoin(fmt.Sprintf("dm_ aggregate refresh left stale rows: %d repo and %d group refreshes failed across %d repos", failedRepos, failedGroups, len(repoIDs)), failed, partialFailureSample)
	}
	return nil
}

// partialFailureSample bounds how many per-item failures ride a
// keep-going pass's returned error (the dm_ aggregate and matview
// refreshes) — a fleet-wide outage would otherwise join 140K errors
// into one message. The counts in the prefix stay exact.
const partialFailureSample = 10

// boundedJoin wraps the first `keep` errors (errors.Is / errors.As reach
// them through the join) under a prefix that carries the full count.
func boundedJoin(prefix string, errs []error, keep int) error {
	if len(errs) == 0 {
		return nil
	}
	n := len(errs)
	if keep > 0 && keep < n {
		n = keep
	}
	return fmt.Errorf("%s (first %d of %d): %w", prefix, n, len(errs), errors.Join(errs[:n]...))
}

// distinctRepoGroupIDs returns every group that has at least one repo.
// Fully consumed into a slice before returning: holding the Rows open
// across the refresh loop pinned a pool connection for its whole
// duration (pass 28).
func (s *PostgresStore) distinctRepoGroupIDs(ctx context.Context) ([]int64, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT DISTINCT repo_group_id FROM aveloxis_data.repos WHERE repo_group_id IS NOT NULL ORDER BY repo_group_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
