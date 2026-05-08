// Store methods supporting the v0.19.2 search-resolve background task.
//
// Pipeline:
//   1. Scheduler ticker fires (default once per hour).
//   2. runSearchResolve calls GetContributorsNeedingSearch(limit=N).
//   3. For each row: SearchUserByEmail. On hit, LinkContributorToGitHubUser.
//      On no-hit / error, MarkContributorSearchAttempted.
//   4. cntrb_last_search_attempted_at stamps the row, excluding it
//      from future batches until the cooldown elapses.
//
// Critical contract for LinkContributorToGitHubUser: the function
// must NOT change cntrb_id (would orphan FK references in 16+ child
// tables) or cntrb_login (would re-enter the partial-unique-index
// collision class this whole feature is designed to avoid). Only
// gh_user_id, gh_login, and the audit columns are touched.

package db

import (
	"context"
	"fmt"
)

// SearchResolveCandidate is a contributor row eligible for
// search-API resolution: has an email, lacks a platform user ID.
type SearchResolveCandidate struct {
	CntrbID string
	Email   string
}

// SearchResolveCooldown is the minimum interval between search
// attempts for the same contributor. Mirrors EnrichmentCooldown's
// 30-day window — a user whose email genuinely doesn't resolve to
// a GitHub account today is unlikely to suddenly resolve tomorrow,
// and re-trying soon wastes the 30/min/token search budget.
const SearchResolveCooldown = "30 days"

// GetContributorsNeedingSearch returns up to limit contributors with
// non-empty email, no gh_user_id, and either never-attempted or
// past-cooldown last_search_attempted_at. Excludes noreply emails
// (users.noreply.github.com et al.) — they're guaranteed search
// misses and would waste the rate limit.
func (s *PostgresStore) GetContributorsNeedingSearch(ctx context.Context, limit int) ([]SearchResolveCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, cntrb_email
		FROM aveloxis_data.contributors
		WHERE cntrb_email != ''
		  AND gh_user_id IS NULL
		  AND cntrb_email NOT LIKE '%noreply%'
		  AND cntrb_email NOT LIKE '%no-reply%'
		  AND (cntrb_last_search_attempted_at IS NULL
		       OR cntrb_last_search_attempted_at < NOW() - INTERVAL '`+SearchResolveCooldown+`')
		ORDER BY data_collection_date DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get contributors needing search: %w", err)
	}
	defer rows.Close()

	var out []SearchResolveCandidate
	for rows.Next() {
		var c SearchResolveCandidate
		if err := rows.Scan(&c.CntrbID, &c.Email); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// LinkContributorToGitHubUser applies a successful search hit to an
// existing contributor row: backfills gh_user_id and gh_login,
// inserts a contributor_identities row so the next (platform_id,
// platform_user_id) lookup hits the cache directly, and stamps
// cntrb_last_search_attempted_at.
//
// Critical: does NOT modify cntrb_id (would orphan 16+ FK columns)
// or cntrb_login (would trip idx_contributors_login when the
// resolved login differs from the row's stored login). The two
// columns stay where they are; only the platform-identity backfill
// happens here.
//
// If contributor_identities already has a row for (gh, ghUserID)
// pointing to a different cntrb_id, the INSERT silently does
// nothing (ON CONFLICT DO NOTHING) — that's a sign of a
// pre-existing duplicate the caller can't safely merge from here.
func (s *PostgresStore) LinkContributorToGitHubUser(ctx context.Context, cntrbID, login string, ghUserID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Backfill gh_user_id, gh_login, and stamp the audit column.
		// COALESCE preserves any existing values that may have been
		// set since the search batch was selected.
		_, err = tx.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_user_id = COALESCE(gh_user_id, $2),
			    gh_login = COALESCE(NULLIF(gh_login, ''), $3),
			    cntrb_last_search_attempted_at = NOW(),
			    data_collection_date = NOW()
			WHERE cntrb_id = $1::uuid`,
			cntrbID, ghUserID, login)
		if err != nil {
			return fmt.Errorf("link contributor: %w", err)
		}

		// Add identity row so future (platform=1, user_id=ghUserID)
		// lookups hit the cache. ON CONFLICT DO NOTHING because a
		// pre-existing row for that tuple means the user was already
		// deterministically resolved on a different cntrb_id —
		// silently skipping leaves the existing row intact and
		// avoids creating a phantom identity.
		_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email,
				 avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1::uuid, 1, $2, $3, '', '', '', '', '', '', FALSE)
			ON CONFLICT (platform_id, platform_user_id) DO NOTHING`,
			cntrbID, ghUserID, login)
		if err != nil {
			return fmt.Errorf("backfill identity row: %w", err)
		}

		return tx.Commit(ctx)
	})
}

// MarkContributorSearchAttempted stamps cntrb_last_search_attempted_at
// without applying any other changes. Called when search returns no
// hit OR when the search call errored — both cases excluded from
// the next batch until cooldown.
func (s *PostgresStore) MarkContributorSearchAttempted(ctx context.Context, cntrbID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE aveloxis_data.contributors
		 SET cntrb_last_search_attempted_at = NOW()
		 WHERE cntrb_id = $1::uuid`, cntrbID)
	return err
}
