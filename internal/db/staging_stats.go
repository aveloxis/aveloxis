// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"
)

// StagingStatsRow is one row of the read-only summary surfaced by the
// `aveloxis staging-stats` subcommand. v0.22.4 item 6.
//
// Mirrors the operator output shape directly: one row per (repo,
// entity_type), with row counts, age bounds, and approximate byte
// size of the JSONB payloads. The query is intentionally read-only —
// nothing about it modifies the staging table.
type StagingStatsRow struct {
	RepoID      int64
	RepoOwner   string
	RepoName    string
	EntityType  string
	Rows        int64
	Processed   int64
	Unprocessed int64
	Oldest      time.Time
	Newest      time.Time
	Bytes       int64
}

// StagingStats returns aggregated staging-table state. When repoFilter
// is empty, returns the top `limit` (repo, entity_type) pairs by total
// rows. When repoFilter is "owner/name", returns every entity_type for
// that repo (limit is ignored).
//
// The query joins against aveloxis_data.repos to get owner/name for
// display. `pg_column_size(payload)` is approximate per-row payload
// size; SUM over the cohort gives a usable disk-space proxy without
// needing to scan the actual TOAST chunks.
func (s *PostgresStore) StagingStats(ctx context.Context, repoFilter string, limit int) ([]StagingStatsRow, error) {
	if limit <= 0 {
		limit = 10
	}

	const baseQuery = `
		SELECT
		  s.repo_id,
		  COALESCE(r.repo_owner, ''),
		  COALESCE(r.repo_name, ''),
		  s.entity_type,
		  COUNT(*),
		  COUNT(*) FILTER (WHERE s.processed),
		  COUNT(*) FILTER (WHERE NOT s.processed),
		  MIN(s.created_at),
		  MAX(s.created_at),
		  COALESCE(SUM(pg_column_size(s.payload))::bigint, 0)
		FROM aveloxis_ops.staging s
		LEFT JOIN aveloxis_data.repos r ON r.repo_id = s.repo_id`

	var rows []StagingStatsRow

	if repoFilter != "" {
		// Per-repo drill-in. Filter on owner/name join.
		q := baseQuery + `
		WHERE r.repo_owner || '/' || r.repo_name = $1
		GROUP BY s.repo_id, r.repo_owner, r.repo_name, s.entity_type
		ORDER BY COUNT(*) DESC`
		r, err := s.pool.Query(ctx, q, repoFilter)
		if err != nil {
			return nil, fmt.Errorf("staging stats query (per-repo): %w", err)
		}
		defer r.Close()
		for r.Next() {
			var row StagingStatsRow
			if err := r.Scan(&row.RepoID, &row.RepoOwner, &row.RepoName,
				&row.EntityType, &row.Rows, &row.Processed, &row.Unprocessed,
				&row.Oldest, &row.Newest, &row.Bytes); err != nil {
				return nil, fmt.Errorf("staging stats scan: %w", err)
			}
			rows = append(rows, row)
		}
		return rows, r.Err()
	}

	// Top-N view.
	q := baseQuery + `
		GROUP BY s.repo_id, r.repo_owner, r.repo_name, s.entity_type
		ORDER BY COUNT(*) DESC
		LIMIT $1`
	r, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("staging stats query (top): %w", err)
	}
	defer r.Close()
	for r.Next() {
		var row StagingStatsRow
		if err := r.Scan(&row.RepoID, &row.RepoOwner, &row.RepoName,
			&row.EntityType, &row.Rows, &row.Processed, &row.Unprocessed,
			&row.Oldest, &row.Newest, &row.Bytes); err != nil {
			return nil, fmt.Errorf("staging stats scan: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, r.Err()
}
