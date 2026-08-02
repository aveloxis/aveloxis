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
