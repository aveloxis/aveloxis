// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.82 — operator-issued API tokens (operator decisions 2026-10-08).
//
// A token is granted to an existing account on the admin page and
// authenticates as that account (its repository scope). Instead of the
// per-IP rate limit it is counted against its own hourly allowance
// (rate_limit_per_hour). It is shown once, when it is granted; the table
// keeps only its SHA-256 hex. A revoked or expired token is refused. The
// defaults the admin page offers (5,000 calls per hour, 30 days) live in
// aveloxis_ops.api_token_settings and are editable there.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// APITokenPrefix starts every API token, so the API can tell one from a
// session token without a lookup and a leaked one is recognisable.
const APITokenPrefix = "avx_"

// The defaults the migrate seeds (operator decision 2026-10-08).
const (
	DefaultAPITokenRateLimitPerHour = 5000
	DefaultAPITokenLifetimeDays     = 30
)

// MaxAPITokenLifetimeDays is the longest a token may live, for a grant and
// for the default (operator decision 2026-10-08, the ASVS review's A3: a
// credential that never expires is a forgotten one).
const MaxAPITokenLifetimeDays = 365

// MaxAPITokenLabelLength bounds a token's label, in characters (it is shown
// in the admin list; ASVS V2.2.1).
const MaxAPITokenLabelLength = 200

// ErrInvalidAPIToken is an API token that is unknown, revoked or expired.
var ErrInvalidAPIToken = errors.New("invalid, revoked or expired API token")

// ErrAPITokenNotFound is a token id that names no token.
var ErrAPITokenNotFound = errors.New("API token not found")

// ErrAPITokenOwnerNotFound is a grant whose owner account does not exist.
var ErrAPITokenOwnerNotFound = errors.New("no account with that user id")

type adminPrivilegeDroppedKey struct{}

// WithoutAdminPrivilege marks a request's context as acting without admin
// privilege: IsUserAdmin answers false under it, so every store-level admin
// decision the request reaches (approval bypass on an add, org
// registration) treats the caller as a non-admin. The API applies it to
// every request that carries an API token — a token works as its owner for
// data, never with admin privilege (the 0.29.82 ASVS review A1 and its L10
// round: requireAdmin was not the only admin check).
func WithoutAdminPrivilege(ctx context.Context) context.Context {
	return context.WithValue(ctx, adminPrivilegeDroppedKey{}, true)
}

// AdminPrivilegeDropped reports whether ctx was marked WithoutAdminPrivilege.
func AdminPrivilegeDropped(ctx context.Context) bool {
	v, _ := ctx.Value(adminPrivilegeDroppedKey{}).(bool)
	return v
}

// hashToken is the one spelling of a stored token (session and API): the
// SHA-256 of the raw token, hex-encoded.
func hashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// APITokenGrant is what the admin page asks for.
type APITokenGrant struct {
	UserID           int // the account the token authenticates as
	Label            string
	CreatedBy        int
	Lifetime         time.Duration
	RateLimitPerHour int
}

// APIToken is one granted token, as the admin page lists it (never the
// token itself).
type APIToken struct {
	TokenID          int64      `json:"token_id"`
	UserID           int        `json:"user_id"`
	OwnerLogin       string     `json:"owner_login"`
	Label            string     `json:"label"`
	CreatedBy        int        `json:"created_by"`
	CreatedByLogin   string     `json:"created_by_login"`
	CreatedAt        time.Time  `json:"created_at"`
	ExpiresAt        time.Time  `json:"expires_at"`
	RateLimitPerHour int        `json:"rate_limit_per_hour"`
	LastUsedAt       *time.Time `json:"last_used_at"`
	RevokedAt        *time.Time `json:"revoked_at"`
}

// APITokenIdentity is what a validated API token resolves to.
type APITokenIdentity struct {
	TokenID          int64
	UserID           int
	RateLimitPerHour int
}

// CreateAPIToken grants a token and returns it raw — the only time it is
// ever available — with its stored record.
func (s *PostgresStore) CreateAPIToken(ctx context.Context, g APITokenGrant) (string, APIToken, error) {
	label := strings.TrimSpace(g.Label)
	switch {
	case g.UserID <= 0:
		return "", APIToken{}, fmt.Errorf("an API token needs an owner account")
	case label == "":
		return "", APIToken{}, fmt.Errorf("an API token needs a label")
	case utf8.RuneCountInString(label) > MaxAPITokenLabelLength:
		return "", APIToken{}, fmt.Errorf("an API token's label is at most %d characters", MaxAPITokenLabelLength)
	case g.Lifetime <= 0:
		return "", APIToken{}, fmt.Errorf("an API token's lifetime must be positive")
	case g.Lifetime > MaxAPITokenLifetimeDays*24*time.Hour:
		return "", APIToken{}, fmt.Errorf("an API token lives at most %d days", MaxAPITokenLifetimeDays)
	case g.RateLimitPerHour <= 0:
		return "", APIToken{}, fmt.Errorf("an API token's hourly allowance must be positive")
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", APIToken{}, fmt.Errorf("API token entropy: %w", err)
	}
	raw := APITokenPrefix + hex.EncodeToString(buf)
	var createdBy *int
	if g.CreatedBy > 0 {
		createdBy = &g.CreatedBy
	}
	t := APIToken{UserID: g.UserID, Label: label, RateLimitPerHour: g.RateLimitPerHour}
	err := s.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.api_tokens (token_hash, user_id, label, created_by, expires_at, rate_limit_per_hour)
		SELECT $1, u.user_id, $3, $4, NOW() + make_interval(secs => $5), $6
		FROM aveloxis_ops.users u WHERE u.user_id = $2
		RETURNING token_id, created_at, expires_at`,
		hashToken(raw), g.UserID, label, createdBy, g.Lifetime.Seconds(), g.RateLimitPerHour).
		Scan(&t.TokenID, &t.CreatedAt, &t.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", APIToken{}, fmt.Errorf("user id %d: %w", g.UserID, ErrAPITokenOwnerNotFound)
	}
	if err != nil {
		return "", APIToken{}, fmt.Errorf("create API token: %w", err)
	}
	t.CreatedBy = g.CreatedBy
	return raw, t, nil
}

// ValidateAPIToken resolves a raw API token, stamping its last use, or
// returns ErrInvalidAPIToken (unknown, revoked or expired). Any other error
// is the store's failure (SR-5).
func (s *PostgresStore) ValidateAPIToken(ctx context.Context, raw string) (APITokenIdentity, error) {
	if !strings.HasPrefix(raw, APITokenPrefix) {
		return APITokenIdentity{}, ErrInvalidAPIToken
	}
	var id APITokenIdentity
	err := s.pool.QueryRow(ctx, `
		UPDATE aveloxis_ops.api_tokens SET last_used_at = NOW()
		WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > NOW()
		RETURNING token_id, user_id, rate_limit_per_hour`, hashToken(raw)).
		Scan(&id.TokenID, &id.UserID, &id.RateLimitPerHour)
	if errors.Is(err, pgx.ErrNoRows) {
		return APITokenIdentity{}, ErrInvalidAPIToken
	}
	if err != nil {
		return APITokenIdentity{}, fmt.Errorf("validate API token: %w", err)
	}
	return id, nil
}

// APITokenActive reports whether a raw API token is still valid (known,
// not revoked, not expired) — the per-call recheck of a cached token (the
// ASVS review's A6), one lookup on the unique token_hash, no write.
func (s *PostgresStore) APITokenActive(ctx context.Context, raw string) (bool, error) {
	var active bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM aveloxis_ops.api_tokens
		               WHERE token_hash = $1 AND revoked_at IS NULL AND expires_at > NOW())`, hashToken(raw)).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("recheck API token: %w", err)
	}
	return active, nil
}

// ListAPITokens lists every granted token, newest first, with the owner's
// and granter's logins.
func (s *PostgresStore) ListAPITokens(ctx context.Context) ([]APIToken, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.token_id, t.user_id, COALESCE(o.login_name, ''), t.label,
		       COALESCE(t.created_by, 0), COALESCE(c.login_name, ''), t.created_at, t.expires_at,
		       t.rate_limit_per_hour, t.last_used_at, t.revoked_at
		FROM aveloxis_ops.api_tokens t
		LEFT JOIN aveloxis_ops.users o ON o.user_id = t.user_id
		LEFT JOIN aveloxis_ops.users c ON c.user_id = t.created_by
		ORDER BY t.created_at DESC, t.token_id DESC`)
	if err != nil {
		return nil, fmt.Errorf("list API tokens: %w", err)
	}
	defer rows.Close()
	var out []APIToken
	for rows.Next() {
		var t APIToken
		if err := rows.Scan(&t.TokenID, &t.UserID, &t.OwnerLogin, &t.Label, &t.CreatedBy, &t.CreatedByLogin,
			&t.CreatedAt, &t.ExpiresAt, &t.RateLimitPerHour, &t.LastUsedAt, &t.RevokedAt); err != nil {
			return nil, fmt.Errorf("list API tokens: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list API tokens: %w", err)
	}
	return out, nil
}

// RevokeAPIToken revokes a token. Revoking a revoked token is a no-op; an
// unknown id is ErrAPITokenNotFound.
func (s *PostgresStore) RevokeAPIToken(ctx context.Context, tokenID int64, revokedBy int) error {
	var by *int
	if revokedBy > 0 {
		by = &revokedBy
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `
		WITH upd AS (
		    UPDATE aveloxis_ops.api_tokens SET revoked_at = NOW(), revoked_by = $2
		    WHERE token_id = $1 AND revoked_at IS NULL
		    RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM aveloxis_ops.api_tokens WHERE token_id = $1)`, tokenID, by).Scan(&exists)
	if err != nil {
		return fmt.Errorf("revoke API token %d: %w", tokenID, err)
	}
	if !exists {
		return ErrAPITokenNotFound
	}
	return nil
}

// APITokenSettings are the defaults the admin page offers.
type APITokenSettings struct {
	DefaultRateLimitPerHour int       `json:"default_rate_limit_per_hour"`
	DefaultLifetimeDays     int       `json:"default_lifetime_days"`
	UpdatedBy               int       `json:"updated_by"`
	UpdatedAt               time.Time `json:"updated_at"`
}

// GetAPITokenSettings reads the defaults (the migrate seeds the row).
func (s *PostgresStore) GetAPITokenSettings(ctx context.Context) (APITokenSettings, error) {
	var st APITokenSettings
	err := s.pool.QueryRow(ctx, `
		SELECT default_rate_limit_per_hour, default_lifetime_days, COALESCE(updated_by, 0), updated_at
		FROM aveloxis_ops.api_token_settings WHERE id = 1`).
		Scan(&st.DefaultRateLimitPerHour, &st.DefaultLifetimeDays, &st.UpdatedBy, &st.UpdatedAt)
	if err != nil {
		return APITokenSettings{}, fmt.Errorf("read API token settings: %w", err)
	}
	return st, nil
}

// SetAPITokenSettings changes the defaults; both must be positive.
func (s *PostgresStore) SetAPITokenSettings(ctx context.Context, st APITokenSettings, updatedBy int) error {
	if st.DefaultRateLimitPerHour <= 0 || st.DefaultLifetimeDays <= 0 {
		return fmt.Errorf("the default hourly allowance and lifetime must both be positive")
	}
	if st.DefaultLifetimeDays > MaxAPITokenLifetimeDays {
		return fmt.Errorf("the default lifetime is at most %d days", MaxAPITokenLifetimeDays)
	}
	var by *int
	if updatedBy > 0 {
		by = &updatedBy
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.api_token_settings (id, default_rate_limit_per_hour, default_lifetime_days, updated_at, updated_by)
		VALUES (1, $1, $2, NOW(), $3)
		ON CONFLICT (id) DO UPDATE SET default_rate_limit_per_hour = EXCLUDED.default_rate_limit_per_hour,
		    default_lifetime_days = EXCLUDED.default_lifetime_days, updated_at = NOW(), updated_by = EXCLUDED.updated_by`,
		st.DefaultRateLimitPerHour, st.DefaultLifetimeDays, by)
	if err != nil {
		return fmt.Errorf("update API token settings: %w", err)
	}
	return nil
}
