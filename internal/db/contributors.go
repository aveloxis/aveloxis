// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ContributorResolver resolves platform user references to contributor UUIDs.
// It caches lookups to avoid repeated DB queries during a collection run.
type ContributorResolver struct {
	store *PostgresStore
	cache map[contributorKey]string // platform+userID -> cntrb_id UUID string
}

type contributorKey struct {
	platformID int16
	userID     int64
	// login participates in the key ONLY for userID == 0 refs
	// (v0.27.106, ultrareview/Copilot PR #184): login-only refs (GitLab
	// group owners, GraphQL Bot actors whose fragments carry no
	// databaseId) all shared the key {plat, 0}, so after the first one
	// resolved, EVERY subsequent login-only ref returned the FIRST
	// ref's cntrb_id from cache — collapsing distinct identities onto
	// one contributor. For userID > 0 the login stays out of the key so
	// renames keep hitting the same cache entry.
	login string
}

// resolverCacheKey builds the cache key per the rule above.
func resolverCacheKey(platformID int16, userID int64, login string) contributorKey {
	if userID > 0 {
		return contributorKey{platformID: platformID, userID: userID}
	}
	return contributorKey{platformID: platformID, login: login}
}

// NewContributorResolver creates a resolver backed by the given store.
func NewContributorResolver(store *PostgresStore) *ContributorResolver {
	return &ContributorResolver{
		store: store,
		cache: make(map[contributorKey]string),
	}
}

// Resolve looks up or creates a contributor for the given platform user,
// returning the cntrb_id UUID as a string. Results are cached for the
// lifetime of the resolver.
func (r *ContributorResolver) Resolve(ctx context.Context, platformID int16, userID int64, login, name, email, avatarURL, profileURL, nodeID, userType string) (string, error) {
	key := resolverCacheKey(platformID, userID, login)

	// 1. Check in-memory cache.
	if id, ok := r.cache[key]; ok {
		return id, nil
	}

	// 2. Look up in contributor_identities — ONLY for real platform ids.
	// A (platform, 0) probe would match whatever identity row happens
	// to carry platform_user_id = 0 and collapse every login-only ref
	// onto it (the DB-side twin of the cache-key bug above); login-only
	// refs resolve by login in step 2.5 instead.
	var cntrbID string
	if userID > 0 {
		// v0.27.112 (wrongly-suppressed Copilot finding): the v0.27.103
		// node_id/user_type heal lived only in the ON CONFLICT clauses —
		// UNREACHABLE for the rows it targets, because an existing
		// identity returns right here and never reaches those inserts.
		// Read the fields alongside cntrb_id (free) and heal in place
		// when the row is empty and the caller has values; the cache
		// makes this a once-per-run check per identity.
		var haveNodeID, haveType string
		err := r.store.pool.QueryRow(ctx, `
			SELECT cntrb_id::text, COALESCE(node_id, ''), COALESCE(user_type, '')
			FROM aveloxis_data.contributor_identities
			WHERE platform_id = $1 AND platform_user_id = $2`,
			platformID, userID,
		).Scan(&cntrbID, &haveNodeID, &haveType)

		if err == nil {
			if (haveNodeID == "" && nodeID != "") || (haveType == "" && userType != "") {
				if _, herr := r.store.pool.Exec(ctx, `
					UPDATE aveloxis_data.contributor_identities
					SET node_id = COALESCE(NULLIF(node_id, ''), $3),
					    user_type = COALESCE(NULLIF(user_type, ''), $4)
					WHERE platform_id = $1 AND platform_user_id = $2`,
					platformID, userID, nodeID, userType); herr != nil {
					// Best-effort — resolution itself succeeded — but
					// never silent (v0.25.36), and v0.27.117 (Copilot
					// round 11, suppressed): do NOT cache on a failed
					// heal. A cached identity exits at the step-1
					// cache lookup on every later observation, so the
					// "next observation retries" promise would be
					// false for the process lifetime and the fields
					// would stay dark until restart. Uncached, the
					// next observation re-runs this branch (indexed
					// point read) and retries the heal.
					r.store.logger.Warn("identity node_id/user_type heal failed",
						"platform_id", platformID, "platform_user_id", userID, "error", herr)
					return cntrbID, nil
				}
			}
			r.cache[key] = cntrbID
			return cntrbID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}
	}

	// 2.5. Not found by (platform, user_id), but the login may already
	// exist in contributors from a prior lazy resolution. Look up by
	// cntrb_login before generating a new UUID — without this step,
	// every UserRef whose login already lives in the table creates a
	// fresh row that races the existing one on idx_contributors_login.
	// The race is what produced the "duplicate key value violates
	// unique constraint idx_contributors_login" floods in production
	// logs from 2026-05-02. Skipped when login is empty (matches the
	// partial unique index's WHERE clause and avoids a meaningless
	// query).
	if login != "" {
		var existingID string
		err := r.store.pool.QueryRow(ctx, `
			SELECT cntrb_id::text
			FROM aveloxis_data.contributors
			WHERE cntrb_login = $1
			  AND COALESCE(cntrb_deleted, 0) = 0`, login).Scan(&existingID)
		if err == nil {
			// Reuse the existing row. If we have a real platform_user_id,
			// backfill the contributor_identities row so future lookups
			// by (platform_id, platform_user_id) hit the cache directly
			// instead of falling through to login lookup again.
			if userID > 0 {
				_, _ = r.store.pool.Exec(ctx, `
					INSERT INTO aveloxis_data.contributor_identities
						(cntrb_id, platform_id, platform_user_id, login, name, email,
						 avatar_url, profile_url, node_id, user_type, is_admin)
					VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, FALSE)
					ON CONFLICT (platform_id, platform_user_id) DO UPDATE SET
						login = EXCLUDED.login,
						name = EXCLUDED.name,
						email = COALESCE(NULLIF(EXCLUDED.email,''), contributor_identities.email),
						-- v0.27.103: pre-fix GraphQL-rail rows carry empty
						-- node_id/user_type; refresh prefer-nonempty so they
						-- heal on re-observation (empty never clobbers).
						node_id = COALESCE(NULLIF(EXCLUDED.node_id, ''), contributor_identities.node_id),
						user_type = COALESCE(NULLIF(EXCLUDED.user_type, ''), contributor_identities.user_type)`,
					existingID, platformID, userID, login, name, email,
					avatarURL, profileURL, nodeID, userType)
			}
			r.cache[key] = existingID
			return existingID, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}
	}

	// 3. Not found — create contributor + identity in a transaction.
	//
	// The cntrb_id UUID is deterministic per (platform_id, userID) via
	// PlatformUUID. This is what CLAUDE.md promises and what verification
	// tooling (shadow-diff equivalence tests, periodic re-collection
	// cross-checks) assumes: two independent collections of the same
	// GitHub user produce the same UUID on both sides, so joined tables
	// referencing cntrb_id match across databases without content drift.
	//
	// For userID == 0 — email-only contributors created when a commit
	// author has no linked GitHub/GitLab user — there is no platform ID
	// to derive from. Fall back to a random UUID; this is the one case
	// where cntrb_id is inherently non-deterministic, and it's small
	// enough (usually <1% of contributors) that the drift is tolerable.
	var newID string
	if userID > 0 {
		newID = PlatformUUID(int(platformID), userID).String()
	} else {
		newID = uuid.New().String()
	}
	err := r.store.withRetry(ctx, func(ctx context.Context) error {
		tx, err := r.store.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Two upsert SQL flavors depending on whether the contributor has
		// a numeric platform user ID (deterministic cntrb_id) or not
		// (random UUID). Postgres has no multi-target ON CONFLICT, so
		// we branch.
		//
		// userID > 0: cntrb_id is PlatformUUID(platformID, userID),
		// deterministic per platform user. The natural unique key here
		// is cntrb_id — concurrent inserts of the same numeric user
		// under different login strings (historical login drift across
		// repos, GitHub renames, two workers seeing the same hot user
		// at once) all collide on cntrb_id, so ON CONFLICT (cntrb_id)
		// routes them all to DO UPDATE. The DO UPDATE clause also
		// updates cntrb_login from EXCLUDED so a renamed user's row
		// picks up the new login on next observation.
		//
		// At v0.18.28 this used ON CONFLICT (cntrb_login) — which
		// failed to match when login strings differed across observers,
		// letting INSERT proceed and trip contributors_pkey, raising
		// thousands of WARNs/day on a 40K-repo fleet.
		//
		// userID == 0: cntrb_id is a random UUID per call (email-only
		// contributor, no platform user). Two observations of the
		// same person generate different cntrb_ids but the same login,
		// so ON CONFLICT (cntrb_login) WHERE cntrb_login != '' is the
		// right target.
		if userID > 0 {
			err = tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.contributors
					(cntrb_id, cntrb_login, cntrb_email, cntrb_full_name, cntrb_created_at)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (cntrb_id) DO UPDATE SET
					cntrb_login = COALESCE(NULLIF(EXCLUDED.cntrb_login,''), contributors.cntrb_login),
					cntrb_email = COALESCE(NULLIF(EXCLUDED.cntrb_email,''), contributors.cntrb_email),
					cntrb_full_name = COALESCE(NULLIF(EXCLUDED.cntrb_full_name,''), contributors.cntrb_full_name),
					data_collection_date = NOW()
				RETURNING cntrb_id::text`,
				newID, login, email, name, time.Now(),
			).Scan(&cntrbID)
		} else {
			err = tx.QueryRow(ctx, `
				INSERT INTO aveloxis_data.contributors
					(cntrb_id, cntrb_login, cntrb_email, cntrb_full_name, cntrb_created_at)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (cntrb_login) WHERE cntrb_login != ''
				DO UPDATE SET
					cntrb_email = COALESCE(NULLIF(EXCLUDED.cntrb_email,''), contributors.cntrb_email),
					cntrb_full_name = COALESCE(NULLIF(EXCLUDED.cntrb_full_name,''), contributors.cntrb_full_name),
					data_collection_date = NOW()
				RETURNING cntrb_id::text`,
				newID, login, email, name, time.Now(),
			).Scan(&cntrbID)
		}
		if err != nil {
			return err
		}

		// v0.27.108 (Copilot PR #184 round 3): identity rows are keyed by
		// (platform_id, platform_user_id) — a userID==0 ref has no such
		// identity, and inserting one funnels EVERY login-only ref into
		// the single (platform, 0) row: each new login overwrites its
		// metadata while cntrb_id stays pinned to the first contributor
		// (production carried exactly one such poisoned row — the
		// codecov Bot actor). Login-only contributors get NO identity
		// row; they resolve by login (step 2.5) every time.
		if userID > 0 {
			_, err = tx.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_identities
				(cntrb_id, platform_id, platform_user_id, login, name, email,
				 avatar_url, profile_url, node_id, user_type, is_admin)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (platform_id, platform_user_id) DO UPDATE SET
				login = EXCLUDED.login,
				name = EXCLUDED.name,
				email = COALESCE(NULLIF(EXCLUDED.email,''), contributor_identities.email),
				avatar_url = EXCLUDED.avatar_url,
				profile_url = EXCLUDED.profile_url,
				-- v0.27.103: heal empty node_id/user_type on re-observation.
				node_id = COALESCE(NULLIF(EXCLUDED.node_id, ''), contributor_identities.node_id),
				user_type = COALESCE(NULLIF(EXCLUDED.user_type, ''), contributor_identities.user_type)`,
				cntrbID, platformID, userID, login, name, email,
				avatarURL, profileURL, nodeID, userType, false,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	})
	if err != nil {
		return "", err
	}

	r.cache[key] = cntrbID
	return cntrbID, nil
}

// EnrichmentCooldown is the minimum interval between enrichment attempts for
// the same contributor. Users with genuinely empty GitHub profiles (no company,
// no location) would otherwise be re-enriched on every collection pass, wasting
// API tokens. 30 days balances freshness with token efficiency.
const EnrichmentCooldown = "30 days"

// GetThinContributorLogins returns logins of contributors that lack enrichment
// data (empty company AND location) and haven't been enriched recently.
// Contributors discovered via issue/PR/message UserRefs start with only basic
// data; this query finds those needing full profile data from GET /users/{login}.
// The cntrb_last_enriched_at filter prevents re-enriching users with genuinely
// empty GitHub profiles on every pass — they are retried after 30 days in case
// the user updates their profile.
func (r *ContributorResolver) GetThinContributorLogins(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.store.pool.Query(ctx, `
		SELECT cntrb_login FROM aveloxis_data.contributors
		WHERE COALESCE(cntrb_deleted, 0) = 0
			AND cntrb_login != ''
			AND (cntrb_company = '' OR cntrb_company IS NULL)
			AND (cntrb_location = '' OR cntrb_location IS NULL)
			AND (cntrb_last_enriched_at IS NULL
			     OR cntrb_last_enriched_at < NOW() - INTERVAL '`+EnrichmentCooldown+`')
		ORDER BY data_collection_date DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logins []string
	for rows.Next() {
		var login string
		if err := rows.Scan(&login); err != nil {
			return nil, err
		}
		if login != "" {
			logins = append(logins, login)
		}
	}
	return logins, rows.Err()
}

// MarkContributorEnriched sets cntrb_last_enriched_at to NOW() for the given
// login, recording that enrichment was attempted. This prevents wasteful
// re-enrichment of users with genuinely empty GitHub/GitLab profiles.
func (r *ContributorResolver) MarkContributorEnriched(ctx context.Context, login string) error {
	return r.store.MarkContributorEnriched(ctx, login)
}

// ResolveIfKnown performs a cache-only lookup and returns the cntrb_id if
// the contributor was previously resolved. Returns ("", false, nil) if
// the contributor is not in the cache.
func (r *ContributorResolver) ResolveIfKnown(ctx context.Context, platformID int16, userID int64) (string, bool, error) {
	key := resolverCacheKey(platformID, userID, "")
	if id, ok := r.cache[key]; ok {
		return id, true, nil
	}
	return "", false, nil
}
