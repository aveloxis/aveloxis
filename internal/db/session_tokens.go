// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.1 — DB-backed session tokens + per-user repo scope
// (plan: summary/api-analytics-plan-2026-07-10.md §2/§2b).
//
// The aveloxis_ops.user_session_tokens table (present in the schema
// since the Augur-compat era, previously unused) becomes the shared
// session store: the web process mints a token at OAuth login, the
// api process validates the same token from the SPA's Authorization
// Bearer header. DB-backed also fixes the long-standing "restart web
// and everyone is logged out" limitation of in-memory sessions.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidSessionToken is returned for unknown or expired tokens.
var ErrInvalidSessionToken = errors.New("invalid or expired session token")

// DefaultSessionTokenLifetime is how long an SPA session token lives.
const DefaultSessionTokenLifetime = 30 * 24 * time.Hour

// CreateSessionToken mints a cryptographically random token for the
// user and stores it with the given lifetime (zero = default 30
// days). Expired tokens are purged opportunistically.
func (s *PostgresStore) CreateSessionToken(ctx context.Context, userID int, lifetime time.Duration) (string, error) {
	if lifetime <= 0 {
		lifetime = DefaultSessionTokenLifetime
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("session token entropy: %w", err)
	}
	token := hex.EncodeToString(raw)
	now := time.Now().Unix()
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration)
		VALUES ($1, $2, $3, $4)`, token, userID, now, now+int64(lifetime.Seconds()))
	if err != nil {
		return "", fmt.Errorf("create session token: %w", err)
	}
	// Opportunistic hygiene — keeps the table from accumulating
	// expired rows without a dedicated ticker.
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.user_session_tokens WHERE expiration < $1`, now)
	return token, nil
}

// ValidateSessionToken resolves a token to its user id, or
// ErrInvalidSessionToken for unknown/expired tokens.
func (s *PostgresStore) ValidateSessionToken(ctx context.Context, token string) (int, error) {
	var userID int
	err := s.pool.QueryRow(ctx, `
		SELECT user_id FROM aveloxis_ops.user_session_tokens
		WHERE token = $1 AND expiration > $2`, token, time.Now().Unix()).Scan(&userID)
	if err != nil {
		return 0, ErrInvalidSessionToken
	}
	return userID, nil
}

// DeleteSessionToken revokes one token (logout).
func (s *PostgresStore) DeleteSessionToken(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_ops.user_session_tokens WHERE token = $1`, token)
	return err
}

// GetUserRepoScope returns the repo ids the user may see: every repo
// linked into one of their APPROVED groups (v0.19.0 approval
// workflow; pre-status rows count as approved). Admins are unscoped —
// callers check IsUserAdmin and skip this entirely (§2b).
func (s *PostgresStore) GetUserRepoScope(ctx context.Context, userID int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ur.repo_id
		FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_ops.user_groups g USING (group_id)
		WHERE g.user_id = $1
		  AND COALESCE(g.status, 'approved') = 'approved'`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}
