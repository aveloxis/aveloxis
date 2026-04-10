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
	key := contributorKey{platformID: platformID, userID: userID}

	// 1. Check in-memory cache.
	if id, ok := r.cache[key]; ok {
		return id, nil
	}

	// 2. Look up in contributor_identities.
	var cntrbID string
	err := r.store.pool.QueryRow(ctx, `
		SELECT cntrb_id::text
		FROM aveloxis_data.contributor_identities
		WHERE platform_id = $1 AND platform_user_id = $2`,
		platformID, userID,
	).Scan(&cntrbID)

	if err == nil {
		r.cache[key] = cntrbID
		return cntrbID, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}

	// 3. Not found — create contributor + identity in a transaction.
	newID := uuid.New().String()
	err = r.store.withRetry(ctx, func(ctx context.Context) error {
		tx, err := r.store.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Use the partial unique index form — NOT ON CONSTRAINT, because
		// idx_contributors_login is an index (not a constraint) and the
		// ON CONSTRAINT form doesn't accept a WHERE clause. The previous
		// syntax caused a SQL error on every lazy contributor creation,
		// resulting in 131K+ messages with NULL cntrb_id.
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
		if err != nil {
			return err
		}

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
				profile_url = EXCLUDED.profile_url`,
			cntrbID, platformID, userID, login, name, email,
			avatarURL, profileURL, nodeID, userType, false,
		)
		if err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
	if err != nil {
		return "", err
	}

	r.cache[key] = cntrbID
	return cntrbID, nil
}

// GetThinContributorLogins returns logins of contributors that lack enrichment
// data (empty company). These are contributors discovered via issue/PR/message
// UserRefs but never enriched with full profile data from GET /users/{login}.
// Limited to avoid excessive API calls — enrichment runs incrementally.
func (r *ContributorResolver) GetThinContributorLogins(ctx context.Context, limit int) ([]string, error) {
	rows, err := r.store.pool.Query(ctx, `
		SELECT cntrb_login FROM aveloxis_data.contributors
		WHERE cntrb_login != ''
			AND (cntrb_company = '' OR cntrb_company IS NULL)
			AND (cntrb_location = '' OR cntrb_location IS NULL)
		ORDER BY data_collection_date DESC NULLS LAST
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logins []string
	for rows.Next() {
		var login string
		if rows.Scan(&login) == nil && login != "" {
			logins = append(logins, login)
		}
	}
	return logins, rows.Err()
}

// ResolveIfKnown performs a cache-only lookup and returns the cntrb_id if
// the contributor was previously resolved. Returns ("", false, nil) if
// the contributor is not in the cache.
func (r *ContributorResolver) ResolveIfKnown(ctx context.Context, platformID int16, userID int64) (string, bool, error) {
	key := contributorKey{platformID: platformID, userID: userID}
	if id, ok := r.cache[key]; ok {
		return id, true, nil
	}
	return "", false, nil
}
