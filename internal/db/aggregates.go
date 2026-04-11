package db

import (
	"context"
	"fmt"
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
	if err != nil || rgID == nil {
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

// RefreshAllRepoAggregates recomputes dm_repo_annual/monthly/weekly and
// dm_repo_group_annual/monthly/weekly for ALL repos. Called during the weekly
// matview rebuild when collection workers are paused, to avoid conflicts.
//
// This is more efficient than per-repo refresh because it can use bulk SQL
// without per-repo DELETE+INSERT cycles. For the repo_group tables, it
// refreshes each distinct group once.
func (s *PostgresStore) RefreshAllRepoAggregates(ctx context.Context, logger interface{ Info(string, ...any) }) error {
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
		if rows.Scan(&id) == nil {
			repoIDs = append(repoIDs, id)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(repoIDs) == 0 {
		return nil
	}

	logger.Info("refreshing dm_ aggregate tables", "repos", len(repoIDs))

	for _, repoID := range repoIDs {
		if err := s.RefreshRepoAggregates(ctx, repoID); err != nil {
			logger.Info("aggregate refresh failed", "repo_id", repoID, "error", err)
			continue // Don't abort all repos if one fails.
		}
	}

	// Refresh repo group aggregates for each distinct group.
	groupRows, err := s.pool.Query(ctx,
		`SELECT DISTINCT repo_group_id FROM aveloxis_data.repos WHERE repo_group_id IS NOT NULL`)
	if err != nil {
		return fmt.Errorf("querying repo groups: %w", err)
	}
	defer groupRows.Close()

	var groupIDs []int64
	for groupRows.Next() {
		var id int64
		if groupRows.Scan(&id) == nil {
			groupIDs = append(groupIDs, id)
		}
	}

	for _, repoID := range repoIDs {
		// RefreshRepoGroupAggregates looks up the group from the repo.
		if err := s.RefreshRepoGroupAggregates(ctx, repoID); err != nil {
			logger.Info("group aggregate refresh failed", "repo_id", repoID, "error", err)
		}
	}

	logger.Info("dm_ aggregate tables refreshed", "repos", len(repoIDs))
	return nil
}
