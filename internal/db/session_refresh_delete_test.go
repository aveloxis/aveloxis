// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Copilot review 5476192626 on PR #228 (HIGH): aveloxis_ops.refresh_tokens
// (an Augur leftover Aveloxis never writes) references
// user_session_tokens.token. A session with a refresh row could not be
// deleted — logout failed with a foreign-key error and the token stayed
// valid; the expired-token sweep failed the same way. Every session delete
// takes its refresh rows with it, in one statement.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestSessionDeletesTakeTheirRefreshRows(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ('avx-it-refresh-del', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.refresh_tokens WHERE id LIKE 'avx-it-refresh-%'`)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})

	// Logout of a session that has a refresh row.
	tok, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-refresh-live', $1)`, hashToken(tok)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSessionToken(ctx, tok); err != nil {
		t.Fatalf("logout of a session with a refresh row failed: %v", err)
	}
	if _, err := store.ValidateSessionToken(ctx, tok); err == nil {
		t.Fatal("the session is still valid after logout")
	}

	// The expired-token sweep (it runs when a token is created).
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.user_session_tokens (token, user_id, created_at, expiration) VALUES ('avx-it-expired-hash', $1, 1, 2)`, userID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.refresh_tokens (id, user_session_token) VALUES ('avx-it-refresh-old', 'avx-it-expired-hash')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSessionToken(ctx, userID, time.Hour); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_session_tokens WHERE token = 'avx-it-expired-hash'`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatal("the expired session with a refresh row survived the sweep")
	}
}
