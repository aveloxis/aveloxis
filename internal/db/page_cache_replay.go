// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// PageCacheReplayKey names one request on the API's re-warm list: a cached
// route's request URI of one repository.
type PageCacheReplayKey struct {
	RepoID int64
	URI    string
}

// PageCacheReplayRow is one entry of the re-warm list and its last
// computation cost in milliseconds (the re-warm replays the costliest first).
type PageCacheReplayRow struct {
	Key    PageCacheReplayKey
	CostMS int64
}

// LoadPageCacheReplay reads the API's saved re-warm list (0.29.122, summary/63
// §8). The rows are a hint written by the API; it validates each before it
// replays one.
func (s *PostgresStore) LoadPageCacheReplay(ctx context.Context) ([]PageCacheReplayRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT repo_id, uri, cost_ms FROM aveloxis_ops.page_cache_replay`)
	if err != nil {
		return nil, fmt.Errorf("load page cache replay list: %w", err)
	}
	defer rows.Close()
	var out []PageCacheReplayRow
	for rows.Next() {
		var r PageCacheReplayRow
		if err := rows.Scan(&r.Key.RepoID, &r.Key.URI, &r.CostMS); err != nil {
			return nil, fmt.Errorf("load page cache replay list: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load page cache replay list: %w", err)
	}
	return out, nil
}

// SavePageCacheReplay applies one difference to the saved re-warm list in one
// transaction: each upsert stores its row (a key already saved takes the new
// cost), each delete removes its key. An empty difference writes nothing.
func (s *PostgresStore) SavePageCacheReplay(ctx context.Context, upserts []PageCacheReplayRow, deletes []PageCacheReplayKey) error {
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("save page cache replay list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if len(deletes) > 0 {
		var ids []int64
		var uris []string
		for _, k := range deletes {
			ids, uris = append(ids, k.RepoID), append(uris, k.URI)
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM aveloxis_ops.page_cache_replay p
			USING unnest($1::bigint[], $2::text[]) AS d(repo_id, uri)
			WHERE p.repo_id = d.repo_id AND p.uri = d.uri`, ids, uris); err != nil {
			return fmt.Errorf("save page cache replay list (delete): %w", err)
		}
	}
	if len(upserts) > 0 {
		var ids, costs []int64
		var uris []string
		for _, r := range upserts {
			ids, uris, costs = append(ids, r.Key.RepoID), append(uris, r.Key.URI), append(costs, r.CostMS)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.page_cache_replay (repo_id, uri, cost_ms)
			SELECT repo_id, uri, GREATEST(cost_ms, 0) FROM unnest($1::bigint[], $2::text[], $3::bigint[]) AS u(repo_id, uri, cost_ms)
			ON CONFLICT (repo_id, uri) DO UPDATE SET cost_ms = EXCLUDED.cost_ms, recorded_at = NOW()`, ids, uris, costs); err != nil {
			return fmt.Errorf("save page cache replay list (upsert): %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("save page cache replay list: %w", err)
	}
	return nil
}
