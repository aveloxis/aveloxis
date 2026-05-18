// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
		WHERE COALESCE(cntrb_deleted, 0) = 0
		  AND cntrb_email != ''
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

		// v0.20.2 rename merge: detect ALL active rows that may
		// represent the same person before linking. Three matching
		// criteria — same gh_user_id, same cntrb_login, or the
		// caller-supplied cntrb_id (which we know exists). If only
		// one row matches (the caller-supplied cntrbID), the standard
		// link path runs unchanged. If 2+ rows match, that's the R3
		// rename edge case: pick a winner via pickMergeWinner, copy
		// non-empty fields from losers, mark losers cntrb_deleted=1,
		// link the winner.
		//
		// COALESCE(cntrb_deleted, 0) = 0 filters legacy rows where
		// the column might be NULL (the schema default is 0 but
		// historical data may have NULL).
		candidates, err := loadMergeCandidates(ctx, tx, cntrbID, login, ghUserID)
		if err != nil {
			return fmt.Errorf("load merge candidates: %w", err)
		}

		winner := pickMergeWinner(candidates, ghUserID)
		if winner == nil {
			// Caller-supplied cntrbID isn't in the active set — race
			// between search batch selection and merge. Fall through
			// to the standard update; if the row is gone, the UPDATE
			// affects 0 rows (harmless).
			winner = &mergeCandidate{cntrbID: cntrbID}
		}

		// Process losers — every active row OTHER than the winner.
		for _, c := range candidates {
			if c.cntrbID == winner.cntrbID {
				continue
			}
			// Copy any non-empty fields from loser into winner where
			// winner is empty (best-of merge). The UPDATE uses
			// COALESCE(NULLIF(winner_field, ''), loser_field) on each
			// column so existing winner values are preserved.
			if _, err := tx.Exec(ctx, `
				UPDATE aveloxis_data.contributors AS w
				SET cntrb_email     = COALESCE(NULLIF(w.cntrb_email, ''),     l.cntrb_email),
				    cntrb_canonical = COALESCE(NULLIF(w.cntrb_canonical, ''), l.cntrb_canonical),
				    cntrb_company   = COALESCE(NULLIF(w.cntrb_company, ''),   l.cntrb_company),
				    cntrb_location  = COALESCE(NULLIF(w.cntrb_location, ''),  l.cntrb_location),
				    cntrb_full_name = COALESCE(NULLIF(w.cntrb_full_name, ''), l.cntrb_full_name)
				FROM aveloxis_data.contributors AS l
				WHERE w.cntrb_id = $1::uuid AND l.cntrb_id = $2::uuid`,
				winner.cntrbID, c.cntrbID); err != nil {
				return fmt.Errorf("merge fields from loser %s: %w", c.cntrbID, err)
			}
			// Insert alias mapping so the loser's commit emails still
			// resolve to the winner. ON CONFLICT DO NOTHING — if
			// another alias for that email already exists pointing
			// somewhere, leave it.
			if c.email != "" {
				if _, err := tx.Exec(ctx, `
					INSERT INTO aveloxis_data.contributors_aliases
						(cntrb_id, canonical_email, alias_email, cntrb_active,
						 tool_source, data_source, data_collection_date)
					VALUES (
						$1::uuid,
						COALESCE(
							(SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid),
							$2),
						$2, 1,
						'aveloxis-rename-merge', 'rename-merge', NOW())
					ON CONFLICT (alias_email) DO NOTHING`,
					winner.cntrbID, c.email); err != nil {
					return fmt.Errorf("insert alias for loser %s: %w", c.cntrbID, err)
				}
			}
			// Soft-delete the loser. R2 (identity-key immutability):
			// cntrb_id is preserved. R10 (FK integrity): every child
			// row that referenced this cntrb_id still resolves.
			if _, err := tx.Exec(ctx, `
				UPDATE aveloxis_data.contributors
				SET cntrb_deleted = 1, data_collection_date = NOW()
				WHERE cntrb_id = $1::uuid`,
				c.cntrbID); err != nil {
				return fmt.Errorf("soft-delete loser %s: %w", c.cntrbID, err)
			}
			s.logger.Info("rename-merge: soft-deleted duplicate contributor",
				"winner", winner.cntrbID, "loser", c.cntrbID,
				"login", login, "gh_user_id", ghUserID)
		}

		// Standard link path on the winner.
		if _, err = tx.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_user_id = COALESCE(gh_user_id, $2),
			    gh_login = COALESCE(NULLIF(gh_login, ''), $3),
			    cntrb_last_search_attempted_at = NOW(),
			    data_collection_date = NOW()
			WHERE cntrb_id = $1::uuid`,
			winner.cntrbID, ghUserID, login); err != nil {
			return fmt.Errorf("link contributor: %w", err)
		}

		// Identity row.
		if _, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email,
				 avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1::uuid, 1, $2, $3, '', '', '', '', '', '', FALSE)
			ON CONFLICT (platform_id, platform_user_id) DO NOTHING`,
			winner.cntrbID, ghUserID, login); err != nil {
			return fmt.Errorf("backfill identity row: %w", err)
		}

		return tx.Commit(ctx)
	})
}

// RenameContributorGhLogin applies a rename-detected update to an
// existing contributor row. Same transaction shape as
// LinkContributorToGitHubUser — reuses loadMergeCandidates and
// pickMergeWinner so the v0.20.2 logical-merge contract applies
// uniformly — but differs in one critical detail: the final UPDATE
// sets gh_login = $3 UNCONDITIONALLY, overwriting any stale value.
//
// Use case (v0.22.12): the breadth worker hits HTTP 404 on
// /users/{stored_gh_login}/events, queries /user/{stored_gh_user_id},
// finds a different current login, and calls this function to
// record the rename. Without unconditional overwrite, the breadth
// worker would keep querying the stale login on every cycle.
//
// Critical invariants preserved (per the LinkContributorToGitHubUser
// contract, R1-R10 in docs/architecture/contributor-resolution.md):
//
//   - cntrb_id is NOT modified (would orphan 16+ FK columns).
//   - cntrb_login is NOT modified (would trip idx_contributors_login
//     on the partial-unique index; cntrb_login is the historical
//     identity, gh_login is "what this person is called RIGHT NOW
//     on GitHub").
//   - Loser rows from the merge candidate set are soft-deleted
//     (cntrb_deleted = 1), not physically deleted.
//   - Email aliases for losers are preserved via
//     contributors_aliases inserts.
//
// Logs INFO when the stored gh_login differs from the new login so
// operators can audit rename events from the application log.
// (cntrb_login on the row serves as the durable record of the
// original observation; the application log captures the
// transition.)
func (s *PostgresStore) RenameContributorGhLogin(ctx context.Context, cntrbID, newLogin string, ghUserID int64) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Capture stored gh_login before update for audit log.
		var oldGhLogin string
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(gh_login, '')
			FROM aveloxis_data.contributors
			WHERE cntrb_id = $1::uuid`, cntrbID).Scan(&oldGhLogin); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("read stored gh_login: %w", err)
			}
			// Row missing — race; let candidate scan handle it.
			oldGhLogin = ""
		}

		// Reuse the v0.20.2 merge machinery so a rename that lands
		// on a login already claimed by another row is consolidated
		// correctly.
		candidates, err := loadMergeCandidates(ctx, tx, cntrbID, newLogin, ghUserID)
		if err != nil {
			return fmt.Errorf("load merge candidates: %w", err)
		}
		winner := pickMergeWinner(candidates, ghUserID)
		if winner == nil {
			winner = &mergeCandidate{cntrbID: cntrbID}
		}

		// Process losers — identical to LinkContributorToGitHubUser.
		for _, c := range candidates {
			if c.cntrbID == winner.cntrbID {
				continue
			}
			if _, err := tx.Exec(ctx, `
				UPDATE aveloxis_data.contributors AS w
				SET cntrb_email     = COALESCE(NULLIF(w.cntrb_email, ''),     l.cntrb_email),
				    cntrb_canonical = COALESCE(NULLIF(w.cntrb_canonical, ''), l.cntrb_canonical),
				    cntrb_company   = COALESCE(NULLIF(w.cntrb_company, ''),   l.cntrb_company),
				    cntrb_location  = COALESCE(NULLIF(w.cntrb_location, ''),  l.cntrb_location),
				    cntrb_full_name = COALESCE(NULLIF(w.cntrb_full_name, ''), l.cntrb_full_name)
				FROM aveloxis_data.contributors AS l
				WHERE w.cntrb_id = $1::uuid AND l.cntrb_id = $2::uuid`,
				winner.cntrbID, c.cntrbID); err != nil {
				return fmt.Errorf("merge fields from loser %s: %w", c.cntrbID, err)
			}
			if c.email != "" {
				if _, err := tx.Exec(ctx, `
					INSERT INTO aveloxis_data.contributors_aliases
						(cntrb_id, canonical_email, alias_email, cntrb_active,
						 tool_source, data_source, data_collection_date)
					VALUES (
						$1::uuid,
						COALESCE(
							(SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid),
							$2),
						$2, 1,
						'aveloxis-rename-merge', 'rename-merge', NOW())
					ON CONFLICT (alias_email) DO NOTHING`,
					winner.cntrbID, c.email); err != nil {
					return fmt.Errorf("insert alias for loser %s: %w", c.cntrbID, err)
				}
			}
			if _, err := tx.Exec(ctx, `
				UPDATE aveloxis_data.contributors
				SET cntrb_deleted = 1, data_collection_date = NOW()
				WHERE cntrb_id = $1::uuid`,
				c.cntrbID); err != nil {
				return fmt.Errorf("soft-delete loser %s: %w", c.cntrbID, err)
			}
			s.logger.Info("rename-merge: soft-deleted duplicate contributor",
				"winner", winner.cntrbID, "loser", c.cntrbID,
				"new_login", newLogin, "gh_user_id", ghUserID)
		}

		// The critical difference from LinkContributorToGitHubUser:
		// gh_login is set UNCONDITIONALLY. The caller has just
		// observed via /user/{id} that the current login differs
		// from the stored one. Preserving the stored value (via
		// COALESCE) would defeat the purpose of this function.
		// gh_user_id retains its COALESCE because the caller has
		// the same value the row already holds (we resolved /user/id
		// using it).
		if _, err = tx.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_login = $3,
			    gh_user_id = COALESCE(gh_user_id, $2),
			    data_collection_date = NOW()
			WHERE cntrb_id = $1::uuid`,
			winner.cntrbID, ghUserID, newLogin); err != nil {
			return fmt.Errorf("update gh_login on rename: %w", err)
		}

		// Identity row — same shape as LinkContributorToGitHubUser.
		// ON CONFLICT DO NOTHING because the identity row for this
		// (platform_id, platform_user_id) likely already exists
		// (otherwise we wouldn't have known to look up by id).
		if _, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email,
				 avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1::uuid, 1, $2, $3, '', '', '', '', '', '', FALSE)
			ON CONFLICT (platform_id, platform_user_id) DO NOTHING`,
			winner.cntrbID, ghUserID, newLogin); err != nil {
			return fmt.Errorf("backfill identity row: %w", err)
		}

		if oldGhLogin != "" && oldGhLogin != newLogin {
			s.logger.Info("contributor gh_login renamed",
				"cntrb_id", winner.cntrbID,
				"old_gh_login", oldGhLogin,
				"new_gh_login", newLogin,
				"gh_user_id", ghUserID)
		}

		return tx.Commit(ctx)
	})
}

// mergeCandidate is one row in the candidate set returned by
// loadMergeCandidates. Used by pickMergeWinner to choose the survivor.
type mergeCandidate struct {
	cntrbID  string
	ghUserID *int64
	email    string
	dataDate time.Time
}

// loadMergeCandidates returns every active contributor row matching
// the caller-supplied cntrbID OR the (login, ghUserID) being linked.
// Filters cntrb_deleted = 0 / NULL — already-merged rows are excluded
// so we don't re-merge transitively.
func loadMergeCandidates(ctx context.Context, tx pgx.Tx, cntrbID, login string, ghUserID int64) ([]mergeCandidate, error) {
	rows, err := tx.Query(ctx, `
		SELECT cntrb_id::text, gh_user_id, COALESCE(cntrb_email, ''),
		       COALESCE(data_collection_date, NOW())
		FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0
		  AND (cntrb_id = $1::uuid OR gh_user_id = $2 OR cntrb_login = $3)`,
		cntrbID, ghUserID, login)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mergeCandidate
	for rows.Next() {
		var c mergeCandidate
		if err := rows.Scan(&c.cntrbID, &c.ghUserID, &c.email, &c.dataDate); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// pickMergeWinner selects the survivor among candidate contributor
// rows. Per R1, the deterministic-UUID row wins when present (its
// cntrb_id matches PlatformUUID(1, ghUserID)). When no candidate
// matches the deterministic id, the oldest row wins — it has been
// referenced by FK columns the longest, and demoting it would mean
// touching the most child rows.
//
// Returns nil if the candidate set is empty (caller falls back to a
// trivial winner).
func pickMergeWinner(candidates []mergeCandidate, ghUserID int64) *mergeCandidate {
	if len(candidates) == 0 {
		return nil
	}
	det := PlatformUUID(1, ghUserID).String()
	for i := range candidates {
		if candidates[i].cntrbID == det {
			return &candidates[i]
		}
	}
	// Fallback: oldest data_collection_date.
	winner := &candidates[0]
	for i := 1; i < len(candidates); i++ {
		if candidates[i].dataDate.Before(winner.dataDate) {
			winner = &candidates[i]
		}
	}
	return winner
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
