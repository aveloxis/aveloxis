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
//
// v0.29.82: the table holds only the SHA-256 hex of each token
// (hashToken); the raw token exists only in the caller's hands. Every
// lookup hashes what it is given.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
		INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration, token_hashed)
		VALUES ($1, $2, $3, $4, TRUE)`, hashToken(token), userID, now, now+int64(lifetime.Seconds()))
	if err != nil {
		return "", fmt.Errorf("create session token: %w", err)
	}
	// Opportunistic hygiene — keeps the table from accumulating
	// expired rows without a dedicated ticker.
	if _, err := s.pool.Exec(ctx, deleteSessionsSQL("expiration < $1"), now); err != nil && !errors.Is(err, context.Canceled) {
		// The token was created; only the hygiene failed (old problem O2).
		s.logger.Warn("expired session tokens could not be deleted — they are removed with the next token", "error", err)
	}
	return token, nil
}

// ValidateSessionToken resolves a token to its user id, or
// ErrInvalidSessionToken for unknown/expired tokens.
func (s *PostgresStore) ValidateSessionToken(ctx context.Context, token string) (int, error) {
	var userID int
	err := s.pool.QueryRow(ctx, `
		SELECT user_id FROM aveloxis_ops.user_session_tokens
		WHERE token = $1 AND expiration > $2`, hashToken(token), time.Now().Unix()).Scan(&userID)
	if err != nil {
		// Only "no such row" is the sentinel (SR-5; worklist follow-up 6,
		// review round 1): a lost connection or a cancelled context is the
		// store's failure, and the API answers it 503, not "sign in again".
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrInvalidSessionToken
		}
		return 0, fmt.Errorf("validate session token: %w", err)
	}
	return userID, nil
}

// DeleteSessionToken revokes one token (logout).
func (s *PostgresStore) DeleteSessionToken(ctx context.Context, token string) error {
	_, err := s.pool.Exec(ctx, deleteSessionsSQL("token = $1"), hashToken(token))
	return err
}

// deleteSessionsSQL deletes the sessions matching where together with the
// aveloxis_ops.refresh_tokens rows that reference them, in one statement.
// refresh_tokens is an Augur leftover Aveloxis never writes, but a fleet
// carried over from Augur may hold rows; its foreign key is checked at
// commit, so deleting a session alone failed (logout answered 500 and the
// token stayed valid; Copilot review 5476192626 on PR #228). where is a
// constant predicate over user_session_tokens, never caller input.
func deleteSessionsSQL(where string) string {
	// It answers the number of sessions deleted (one row).
	return `WITH gone AS (
		DELETE FROM aveloxis_ops.user_session_tokens WHERE ` + where + ` RETURNING token
	), refreshed AS (
		DELETE FROM aveloxis_ops.refresh_tokens WHERE user_session_token IN (SELECT token FROM gone) RETURNING 1
	)
	SELECT count(*) FROM gone`
}

// GetUserRepoScope returns every repo in ANY of the user's groups —
// pending groups included. Operator clarification (2026-07-14):
// approval exists to gate NEW COLLECTION (so nobody bulk-adds 50,000
// uncollected repos), never to gate visibility of already-collected
// data. A pending group's uncollected repos being "in scope" grants
// nothing (there is no data until an admin approves the collection),
// so the status filter was pure friction. Admins are unscoped —
// callers check IsUserAdmin and skip this entirely (§2b).
func (s *PostgresStore) GetUserRepoScope(ctx context.Context, userID int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ur.repo_id
		FROM aveloxis_ops.user_repos ur
		JOIN aveloxis_ops.user_groups g USING (group_id)
		WHERE g.user_id = $1`, userID)
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
