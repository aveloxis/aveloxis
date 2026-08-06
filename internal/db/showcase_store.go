// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// GetRepoShowcaseMeta returns the repo-level presentation fields the
// public showcase repo snapshot pages render (generate-showcase,
// 2026-08-02). Deliberately repo-scoped only — no user names, stars,
// or group ownership can flow through this read, matching the
// showcase privacy contract (queries run anonymously).
func (s *PostgresStore) GetRepoShowcaseMeta(ctx context.Context, repoID int64) (description, primaryLanguage string, archived bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(repo_description, ''), COALESCE(primary_language, ''), COALESCE(repo_archived, FALSE)
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID,
	).Scan(&description, &primaryLanguage, &archived)
	return description, primaryLanguage, archived, err
}

// GetForkStatusBatch reports, for each requested repo, whether it is
// a fork (repos.forked_from non-empty — populated by Phase 0 since
// v0.27.78). The showcase generator's featured-repo selection skips
// forks: sorting collections by commits surfaces high-commit
// mirrors/forks that aren't the collection's own flagship work.
// Missing IDs are simply absent from the map.
func (s *PostgresStore) GetForkStatusBatch(ctx context.Context, repoIDs []int64) (map[int64]bool, error) {
	out := make(map[int64]bool, len(repoIDs))
	if len(repoIDs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, COALESCE(forked_from, '') <> ''
		FROM aveloxis_data.repos WHERE repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var isFork bool
		if err := rows.Scan(&id, &isFork); err != nil {
			return nil, err
		}
		out[id] = isFork
	}
	return out, rows.Err()
}
