package db

import "context"

// ShouldBackfillCommitCount returns true when repo_info.commit_count should
// be patched with the facade-derived gathered count. The rule is strict:
// only fire when the API returned 0 AND facade has observed at least one
// commit. This avoids overwriting a real non-zero API count and avoids
// writing a no-op zero when facade also saw nothing.
//
// Used specifically for GitLab, where the API's `statistics.commit_count`
// can be 0 for two reasons that look identical from the client side:
//  1. token lacks Reporter+ access on a private project (`statistics` is nil),
//  2. GitLab's async stats worker has not yet computed the count for a
//     freshly-imported / mirrored / recently-pushed project.
//
// In both cases the facade's `git log` walk produces the correct count.
func ShouldBackfillCommitCount(apiCommitCount, gatheredCommitCount int) bool {
	return apiCommitCount == 0 && gatheredCommitCount > 0
}

// BackfillGitLabCommitCount patches the latest aveloxis_data.repo_info row
// for the given repo when:
//   - the existing row's commit_count is 0 (API reported no data), AND
//   - aveloxis_data.commits has a non-zero distinct-hash count (facade ran).
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
