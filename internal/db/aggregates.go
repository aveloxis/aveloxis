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
// This is more efficient than per-repo refresh because it can use bulk SQL
// without per-repo DELETE+INSERT cycles. For the repo_group tables, it
// refreshes each distinct group once.
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
			logger.Warn("aggregate refresh failed", "repo_id", repoID, "error", err)
			failedRepos++
			failed = append(failed, fmt.Errorf("repo %d: %w", repoID, err))
			continue // Don't abort all repos if one fails.
		}
	}

	// Refresh repo group aggregates for each distinct group.
	groupRows, err := s.pool.Query(ctx,
		`SELECT DISTINCT repo_group_id FROM aveloxis_data.repos WHERE repo_group_id IS NOT NULL`)
	if err != nil {
		// The repo failures already accumulated must ride this exit too.
		return boundedJoin(fmt.Sprintf("dm_ aggregate refresh aborted before the group pass (%d repo refreshes had failed)", failedRepos),
			append(failed, fmt.Errorf("querying repo groups: %w", err)), partialFailureSample)
	}
	defer groupRows.Close()
	// NOTE: the DISTINCT repo_group_id result set is intentionally not
	// iterated — RefreshRepoGroupAggregates derives the group from each
	// repo. The query remains only as a connectivity check; the dead
	// groupIDs accumulation it once fed was removed in v0.25.36
	// (staticcheck SA4010). Refreshing per-group instead of per-repo is
	// a future optimization (redundant refreshes for same-group repos).

	for _, repoID := range repoIDs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// RefreshRepoGroupAggregates looks up the group from the repo.
		if err := s.RefreshRepoGroupAggregates(ctx, repoID); err != nil {
			logger.Warn("group aggregate refresh failed", "repo_id", repoID, "error", err)
			failedGroups++
			failed = append(failed, fmt.Errorf("group of repo %d: %w", repoID, err))
		}
	}

	logger.Info("dm_ aggregate tables refreshed", "repos", len(repoIDs), "failed_repos", failedRepos, "failed_groups", failedGroups)
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
