// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "context"

// BackfillGitLabCommitCount patches the latest aveloxis_data.repo_info row
// for the given repo when:
//   - the existing row's commit_count is 0 (API reported no data), AND
//   - aveloxis_data.commits has a non-zero distinct-hash count (facade ran).
//
// The rule is strict on purpose — only "API returned 0 AND facade observed
// at least one commit" writes — because GitLab's `statistics.commit_count`
// is 0 for two reasons that look identical from the client side: the token
// lacks Reporter+ access on a private project (`statistics` is nil), or the
// async stats worker has not computed the count yet for a freshly imported,
// mirrored or recently pushed project. In both the facade's `git log` walk
// has the correct count. The statement below IS the rule
// (TestBackfillGitLabCommitCountFourCases drives its four cases, v0.29.98);
// there is no Go copy of it to drift.
//
// Safe by construction:
//   - scoped to a single repo_id via $1,
//   - only touches the latest repo_info snapshot (highest data_collection_date),
//   - WHERE commit_count = 0 means a real non-zero API count is never overwritten,
//   - idempotent: a second call after a successful backfill is a no-op because
//     commit_count is no longer 0.
//
// Returns (updated, err). `updated` is true iff a row was actually written.
// Callers should treat any error as non-fatal for the collection job — a
// failed backfill leaves the API-reported zero in place, which is what
// would have happened anyway before this feature existed.
func (s *PostgresStore) BackfillGitLabCommitCount(ctx context.Context, repoID int64) (bool, error) {
	var gathered int
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT cmt_commit_hash)
		FROM aveloxis_data.commits
		WHERE repo_id = $1`, repoID).Scan(&gathered); err != nil {
		return false, err
	}
	if gathered == 0 {
		return false, nil
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.repo_info
		SET commit_count = $2
		WHERE repo_id = $1
		  AND commit_count = 0
		  AND repo_info_id = (
		      SELECT repo_info_id
		      FROM aveloxis_data.repo_info
		      WHERE repo_id = $1
		      ORDER BY data_collection_date DESC
		      LIMIT 1
		  )`, repoID, gathered)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
