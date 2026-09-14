// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// UnresolvedCommit is a commit needing author resolution.
type UnresolvedCommit struct {
	Hash  string
	Email string // cmt_author_raw_email
}

// GetUnresolvedCommits returns distinct (hash, email) pairs for commits where
// cmt_author_platform_username is NULL in the given repo.
func (s *PostgresStore) GetUnresolvedCommits(ctx context.Context, repoID int64) ([]UnresolvedCommit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT cmt_commit_hash, cmt_author_raw_email
		FROM aveloxis_data.commits
		WHERE repo_id = $1
		  AND (cmt_author_platform_username IS NULL OR cmt_author_platform_username = '')
		ORDER BY cmt_commit_hash`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []UnresolvedCommit
	for rows.Next() {
		var c UnresolvedCommit
		if err := rows.Scan(&c.Hash, &c.Email); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// SetCommitAuthorLogin sets cmt_author_platform_username on all commit rows
// matching the given repo + hash.
func (s *PostgresStore) SetCommitAuthorLogin(ctx context.Context, repoID int64, hash, login string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.commits
		SET cmt_author_platform_username = $3
		WHERE repo_id = $1
		  AND cmt_commit_hash = $2
		  AND (cmt_author_platform_username IS NULL OR cmt_author_platform_username = '')`,
		repoID, hash, login)
	return err
}

// FindLoginByEmail looks up a GitHub login from a commit email.
// Checks contributors table (cntrb_email, cntrb_canonical) and aliases.
func (s *PostgresStore) FindLoginByEmail(ctx context.Context, email string) (string, error) {
	var login string

	// Check contributors by email. Filter cntrb_deleted = 0 so a
	// merged-loser row's email doesn't shadow the active winner.
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(gh_login, cntrb_login)
		FROM aveloxis_data.contributors
		WHERE (cntrb_email = $1 OR cntrb_canonical = $1)
		  AND (gh_login IS NOT NULL AND gh_login != '')
		  AND COALESCE(cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&login)
	if err == nil && login != "" {
		return login, nil
	}

	// Check aliases. Per R5, an alias_email maps to one cntrb_id;
	// after a v0.20.2 rename merge, that cntrb_id is the winner.
	// Filter on c.cntrb_deleted = 0 defensively in case an alias row
	// somehow points at a since-soft-deleted row.
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(c.gh_login, c.cntrb_login)
		FROM aveloxis_data.contributors_aliases a
		JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
		WHERE a.alias_email = $1
		  AND (c.gh_login IS NOT NULL AND c.gh_login != '')
		  AND COALESCE(c.cntrb_deleted, 0) = 0
		LIMIT 1`, email).Scan(&login)
	if err == nil && login != "" {
		return login, nil
	}

	return "", nil
}

// UpsertContributorFull creates or updates a contributor with the deterministic
// GithubUUID and sets gh_login. Returns true if a new row was created.
func (s *PostgresStore) UpsertContributorFull(ctx context.Context, cntrbID, login string, ghUserID int64, commitEmail string) (bool, string, error) {
	var created bool
	actualID := cntrbID
	err := s.withRetry(ctx, func(ctx context.Context) error {
		// Check by cntrb_id first. The probe fetches cntrb_login rather
		// than a bare 1 so the row-exists branch below can REPORT a
		// rename without a second read — same one-row index lookup,
		// zero extra cost.
		var storedLogin string
		err := s.pool.QueryRow(ctx,
			`SELECT cntrb_login FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, cntrbID,
		).Scan(&storedLogin)

		if err != nil {
			// Not found by ID. Check if login already exists (different cntrb_id).
			var existingID string
			loginErr := s.pool.QueryRow(ctx,
				`SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
				login).Scan(&existingID)
			if loginErr == nil {
				// Login exists under a different cntrb_id — update that row instead.
				actualID = existingID
				_, err = s.pool.Exec(ctx, `
					UPDATE aveloxis_data.contributors
					SET gh_user_id = COALESCE(gh_user_id, $2),
					    gh_login = $3,
					    cntrb_canonical = COALESCE(NULLIF(cntrb_canonical,''), $4),
					    data_collection_date = NOW()
					WHERE cntrb_id = $1::uuid`,
					existingID, ghUserID, login, commitEmail)
				created = false
				return err
			}

			// Truly new — insert. If another worker raced us and the login now
			// exists, catch the error and look up their row instead.
			//
			// NOTE the argument order on cntrb_login in the conflict
			// clause: STORED first, EXCLUDED second — fill-empty-only,
			// the opposite of its siblings on purpose. cntrb_login is
			// the login as FIRST observed (R2) and the only one of
			// these columns under a unique index, so re-writing it both
			// breaks the audit trail and collides with whatever row
			// holds the new login. This arm fires only on a race to the
			// same deterministic cntrb_id, where the logins are normally
			// identical; when they differ, the stored one wins
			// (2026-09-11, F4).
			_, insertErr := s.pool.Exec(ctx, `
				INSERT INTO aveloxis_data.contributors
					(cntrb_id, cntrb_login, gh_login, gh_user_id, cntrb_canonical,
					 tool_source, data_source, data_collection_date)
				VALUES ($1::uuid, $2, $2, $3, $4,
					'aveloxis-commit-resolver', 'GitHub API', NOW())
				ON CONFLICT (cntrb_id) DO UPDATE SET
					gh_login = COALESCE(NULLIF(EXCLUDED.gh_login,''), contributors.gh_login),
					cntrb_login = COALESCE(NULLIF(contributors.cntrb_login,''), EXCLUDED.cntrb_login),
					gh_user_id = COALESCE(EXCLUDED.gh_user_id, contributors.gh_user_id)`,
				cntrbID, login, ghUserID, commitEmail)
			if insertErr != nil {
				// Race: another worker inserted this login. Look it up.
				//
				// This arm absorbed the insert error SILENTLY for the
				// product's whole life (F4 of the 2026-09-11 log
				// analysis): on a successful lookup it returned nil
				// with no record that anything had gone wrong, so a
				// systemic insert failure that happened to coincide
				// with a resolvable login was indistinguishable from a
				// clean first insert. The lookup succeeding is genuine
				// evidence of a race, so this stays non-fatal — but it
				// is now observable.
				var raceID string
				if lookupErr := s.pool.QueryRow(ctx,
					`SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
					login).Scan(&raceID); lookupErr == nil {
					var pgErr *pgconn.PgError
					sqlState := ""
					if errors.As(insertErr, &pgErr) {
						sqlState = pgErr.Code
					}
					s.logger.Debug("commit resolver lost an insert race — adopting the winner's row",
						"cntrb_login", login, "winner_cntrb_id", raceID,
						"sqlstate", sqlState, "insert_error", insertErr)
					actualID = raceID
					created = false
					return nil
				}
				return insertErr
			}
			actualID = cntrbID
			created = true
			return nil
		}

		// Row exists by ID — the deterministic UUID makes this the same
		// person by construction. Update gh_login (the current
		// display-name mirror) and backfill gh_user_id / canonical.
		//
		// cntrb_login is deliberately NOT written. R2
		// (docs/architecture/contributor-resolution.md): cntrb_login is
		// the durable audit trail of the login as FIRST observed, and
		// the other three rename paths already obey it — the v0.22.13
		// batch recovery has a hard NEGATIVE pin against the write,
		// v0.22.12's RenameContributorGhLogin documents leaving it
		// alone, and the login-exists-under-a-different-id branch above
		// touches only gh_login. This branch was the sole dissenter.
		//
		// It was also the sole source of ALL 555 idx_contributors_login
		// unique violations in the 2026-09-06..09-11 production log
		// (measured: 555 of 555; the ON CONFLICT arm above contributed
		// zero). Writing cntrb_login collides whenever another row
		// already holds the new login — routine after a rename, because
		// a lazy-resolver row stamps the post-rename login under its
		// own random UUID. Worse than the noise: the 23505 fallback
		// that recovered those collisions omitted gh_login TOO, so for
		// every one of those 555 events the rename was lost entirely
		// and the display-name mirror stayed stale. Not writing
		// cntrb_login means gh_login now lands on the first attempt.
		//
		// With cntrb_login out of the SET list this statement can no
		// longer raise 23505 at all: contributors carries exactly two
		// unique indexes — contributors_pkey (cntrb_id, not written
		// here) and idx_contributors_login (cntrb_login) — and
		// gh_login / gh_user_id / cntrb_canonical are indexed
		// non-uniquely. So the v0.19.2 recovery branch that used to sit
		// here is GONE rather than kept for a case it can no longer
		// reach; a 23505 from this statement would be a genuine new
		// defect and must surface instead of being silently absorbed.
		_, err = s.pool.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_login = $2,
			    gh_user_id = COALESCE(gh_user_id, $3),
			    cntrb_canonical = COALESCE(NULLIF(cntrb_canonical,''), $4),
			    data_collection_date = NOW()
			WHERE cntrb_id = $1::uuid`,
			cntrbID, login, ghUserID, commitEmail)
		if err == nil && storedLogin != "" && storedLogin != login {
			// The rename stays visible to operators — as one ordinary
			// observation, not as a recovered Postgres ERROR logged at
			// Debug. Mirrors the batch path's "contributor rename
			// recovered in batch upsert" INFO.
			s.logger.Info("contributor rename observed by commit resolver",
				"cntrb_id", cntrbID,
				"cntrb_login", storedLogin,
				"new_gh_login", login,
				"note", "cntrb_login preserved as the first-observed login (R2); gh_login carries the current name")
		}
		actualID = cntrbID
		created = false
		return err
	})
	// Backfill gh_* columns from contributor_identities if not already set.
	if err == nil && actualID != "" {
		s.backfillGHColumns(ctx, actualID)
	}
	return created, actualID, err
}

// backfillGHColumns copies GitHub identity data to the denormalized gh_* columns
// on the contributors row if they're empty.
func (s *PostgresStore) backfillGHColumns(ctx context.Context, cntrbID string) {
	// v0.25.36: error captured + logged. This ran as a bare Exec for
	// years — a failed backfill UPDATE (constraint, connection blip)
	// vanished silently, violating the "everything that errors should
	// be logged" rule.
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors c SET
			gh_node_id = COALESCE(NULLIF(c.gh_node_id,''), ci.node_id),
			gh_avatar_url = COALESCE(NULLIF(c.gh_avatar_url,''), ci.avatar_url),
			gh_url = COALESCE(NULLIF(c.gh_url,''), ci.profile_url),
			gh_html_url = COALESCE(NULLIF(c.gh_html_url,''), ci.profile_url),
			gh_type = COALESCE(NULLIF(c.gh_type,''), ci.user_type)
		FROM aveloxis_data.contributor_identities ci
		WHERE ci.cntrb_id = c.cntrb_id
		  AND c.cntrb_id = $1::uuid
		  AND ci.platform_id = 1
		  AND (c.gh_node_id IS NULL OR c.gh_node_id = ''
		    OR c.gh_avatar_url IS NULL OR c.gh_avatar_url = '')`,
		cntrbID)
	if err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			s.logger.Warn("backfillGHColumns failed", "cntrb_id", cntrbID, "error", err)
		}
	}
}

// InsertUnresolvedEmail records a commit email that could not be resolved to
// any platform user. Stored for future resolution attempts or manual review.
// Uses a single INSERT with a duplicate check in the WHERE clause to avoid
// the race condition inherent in check-then-insert.
func (s *PostgresStore) InsertUnresolvedEmail(ctx context.Context, email string) {
	// v0.25.36: error captured + logged (was a bare Exec).
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.unresolved_commit_emails
			(email, tool_source, tool_version, data_source, data_collection_date)
		SELECT $1, 'aveloxis-commit-resolver', $2, 'git', NOW()
		WHERE NOT EXISTS (
			SELECT 1 FROM aveloxis_data.unresolved_commit_emails WHERE email = $1
		)`,
		email, ToolVersion)
	if err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			s.logger.Warn("InsertUnresolvedEmail failed", "email", email, "error", err)
		}
	}
}

// contributorAliasInsertSQL + contributorAliasRepairSQL are the ONE
// alias-upsert spelling (SR-17), shared by EnsureContributorAlias (pool)
// and ensureAliasTx (the CreateEmailOnlyContributor transaction twin).
//
// Copilot round 29 on PR #193: a bare ON CONFLICT DO NOTHING left a STALE
// alias in place when its owner was a soft-deleted merge loser (the
// v0.22.3 merge path used to never repoint the loser's own alias rows) —
// the resolver's alias arm filters to active owners, so the alias resolved
// nothing while its existence blocked the new contributor from ever
// owning it.
//
// Code-review round (2026-09-06) — why this is TWO statements and not one
// ON CONFLICT DO UPDATE ... WHERE:
//  1. SNAPSHOT RACE (empirically reproduced): a DO UPDATE's correlated
//     WHERE subquery evaluates under the STATEMENT snapshot, while the
//     conflict arbitration re-reads the newest alias row. A rival that
//     blocked on a concurrent creator's commit resumed, could not SEE the
//     just-committed owner row, read "missing = dead", and STOLE the
//     alias from an active owner. The repair below is a SEPARATE
//     statement: it begins after any blocking commit, so its fresh
//     READ COMMITTED snapshot sees the committed owner and the dead-owner
//     guard evaluates correctly.
//  2. HOT PATH: EnsureContributorAlias fires once per resolved commit
//     email across up to 120 workers. DO UPDATE locks the conflicting
//     row BEFORE evaluating its WHERE — a per-call tuple lock plus a
//     contributors subselect that DO NOTHING never paid. The fast path
//     is conflict-free again; the repair runs only when the insert
//     conflicted, and its UPDATE locks nothing when the guard is false.
//  3. NULL SEMANTICS: cntrb_deleted IS NULL means ACTIVE everywhere in
//     this repo (the pre-v0.20.2 cohort; 46 sites spell
//     COALESCE(cntrb_deleted, 0) = 0). The first spelling treated a
//     NULL-deleted owner as DEAD and stole active legacy owners' aliases.
//  4. canonical_email can never be DOWNGRADED to the empty string —
//     the repair's NULLIF keeps the $2 fallback when the contributor's
//     canonical is empty (a TEXT-defaulted-empty production cohort).
const contributorAliasInsertSQL = `
		INSERT INTO aveloxis_data.contributors_aliases
			(cntrb_id, canonical_email, alias_email, cntrb_active,
			 tool_source, data_source, data_collection_date)
		VALUES (
			$1::uuid,
			COALESCE(
				NULLIF((SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid), ''),
				$2
			),
			$2, 1,
			$3, $4, NOW())
		ON CONFLICT (alias_email) DO NOTHING`

// contributorAliasRepairSQL reassigns the alias ONLY when its current
// owner is soft-deleted (an ACTIVE owner is never stolen). Run as its
// own statement AFTER a conflicted insert — see the rationale above.
const contributorAliasRepairSQL = `
		UPDATE aveloxis_data.contributors_aliases a
		SET cntrb_id = $1::uuid,
		    canonical_email = COALESCE(
		        NULLIF((SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid), ''),
		        $2),
		    tool_source = $3,
		    data_source = $4,
		    data_collection_date = NOW()
		FROM aveloxis_data.contributors dead
		WHERE a.alias_email = $2
		  AND dead.cntrb_id = a.cntrb_id
		  AND COALESCE(dead.cntrb_deleted, 0) <> 0`

// EnsureContributorAlias creates an alias linking a commit email to a contributor.
// The canonical_email is looked up from the contributor row; alias_email is the
// commit email that differs from the canonical.
// v0.29.0 Part E: toolSource/dataSource are parameters now — the old
// hardcoded 'aveloxis-commit-resolver'/'GitHub API' stamped
// mailing-list-origin aliases with commit-resolver provenance, which
// misled every provenance audit that trusted the columns.
// Round 29 + code-review round: a stale alias (soft-deleted owner) is
// REASSIGNED to the caller's contributor via the separate repair
// statement; an active owner's alias is never touched.
func (s *PostgresStore) EnsureContributorAlias(ctx context.Context, cntrbID, aliasEmail, toolSource, dataSource string) error {
	tag, err := s.pool.Exec(ctx, contributorAliasInsertSQL,
		cntrbID, aliasEmail, toolSource, dataSource)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil // inserted fresh — nothing to repair
	}
	// Conflicted: repair iff the existing owner is soft-deleted.
	_, err = s.pool.Exec(ctx, contributorAliasRepairSQL,
		cntrbID, aliasEmail, toolSource, dataSource)
	return err
}

// FindContributorIDByLogin returns the cntrb_id for a given gh_login, or "" if not found.
//
// Filters cntrb_deleted = 0 so a v0.20.2 rename-merge loser row
// doesn't shadow the active winner. Per R3 / Phase D semantics.
func (s *PostgresStore) FindContributorIDByLogin(ctx context.Context, login string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors
		 WHERE gh_login = $1 AND COALESCE(cntrb_deleted, 0) = 0
		 LIMIT 1`,
		login).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// BackfillCommitAuthorIDs sets cmt_ght_author_id from contributor gh_login matches.
// This is a pure SQL operation — no API calls.
//
// v0.20.12 (Fix H): the JOIN is case-insensitive. GitHub treats logins
// as case-insensitive but case-preserving; commit metadata may store
// the display case ("NitishT") while the contributors row was inserted
// under a lowercased login from a different endpoint ("nitisht"), or
// vice versa. A case-sensitive equality silently drops those commits
// into a perpetual NULL-author state. Production diagnostic on the
// live aveloxis_large DB showed 1,919 additional commits recoverable
// under the case-insensitive comparison.
//
// v0.27.25 — two fixes after a live 2-day-2-hour orphaned run of this
// statement on aveloxis_large (2026-07-20 diagnostic):
//
//  1. BOTH sides now exclude empty strings. Postgres "" = "" is TRUE
//     (the v0.25.6 explorer_new_contributors lesson), and production
//     carries 10,636 contributors with gh_login = "" — a repo whose
//     unresolved commits include ""-username rows cross-products
//     against all of them inside the join.
//  2. The v0.20.12 note below said an expression index on
//     LOWER(gh_login) was "the next step" if this profiled as a
//     bottleneck. Two days is that profile:
//     idx_contributors_gh_login_lower (migrate.go, CONCURRENTLY) now
//     serves the join and gives the planner expression statistics
//     (the unindexed form estimated an 86M-row join from 7K commits).
//
// Safe to cancel at any point: single atomic statement, and the
// cmt_ght_author_id IS NULL predicate makes the next pass redo only
// what didn't land. With SIGTERM wired (v0.27.25), `aveloxis stop`
// cancels this statement server-side instead of orphaning it.
func (s *PostgresStore) BackfillCommitAuthorIDs(ctx context.Context, repoID int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.commits c
		SET cmt_ght_author_id = cn.cntrb_id
		FROM aveloxis_data.contributors cn
		WHERE c.repo_id = $1
		  AND LOWER(c.cmt_author_platform_username) = LOWER(cn.gh_login)
		  AND c.cmt_ght_author_id IS NULL
		  AND c.cmt_author_platform_username IS NOT NULL
		  AND c.cmt_author_platform_username != ''
		  AND cn.gh_login != ''`,
		repoID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ContributorMissingCanonical is a contributor needing email enrichment.
type ContributorMissingCanonical struct {
	ID    string // cntrb_id
	Login string // gh_login
}

// CanonicalBatchSize limits how many contributors are processed per
// ResolveEmailsToCanonical pass. Without this, every contributor with
// gh_login but no canonical email is queried — unbounded API calls per pass,
// many for users with private emails that will never return data.
const CanonicalBatchSize = 500

// GetContributorsMissingCanonical returns contributors that have gh_login
// but no cntrb_canonical email and haven't been recently enriched.
// The cntrb_last_enriched_at filter skips contributors already processed by
// EnrichThinContributors (which now sets canonical from email). Users with
// private emails get their enrichment timestamp set, so they won't be
// re-queried until the cooldown expires.
func (s *PostgresStore) GetContributorsMissingCanonical(ctx context.Context) ([]ContributorMissingCanonical, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, gh_login
		FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0
		  AND gh_login IS NOT NULL AND gh_login != ''
		  AND (cntrb_canonical IS NULL OR length(cntrb_canonical) < 2)
		  AND (cntrb_last_enriched_at IS NULL
		       OR cntrb_last_enriched_at < NOW() - INTERVAL '30 days')
		ORDER BY gh_login
		LIMIT $1`, CanonicalBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ContributorMissingCanonical
	for rows.Next() {
		var c ContributorMissingCanonical
		if err := rows.Scan(&c.ID, &c.Login); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// SetContributorCanonical sets cntrb_canonical on a contributor.
func (s *PostgresStore) SetContributorCanonical(ctx context.Context, cntrbID, email string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_canonical = $2
		WHERE cntrb_id = $1::uuid
		  AND (cntrb_canonical IS NULL OR length(cntrb_canonical) < 2)`,
		cntrbID, email)
	return err
}

// MarkContributorEnriched sets cntrb_last_enriched_at to NOW() for the given
// login, recording that enrichment was attempted. Called after both
// EnrichThinContributors and ResolveEmailsToCanonical to prevent wasteful
// re-querying of users with genuinely empty profiles or private emails.
func (s *PostgresStore) MarkContributorEnriched(ctx context.Context, login string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_last_enriched_at = NOW()
		WHERE cntrb_login = $1`, login)
	return err
}
