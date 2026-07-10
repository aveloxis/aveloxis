// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.1 — DB-backed session tokens + per-user repo scope.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestSessionTokensAndScopeEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	pool := store.Pool()

	// Seed a user + approved group with one repo + a PENDING group
	// with another repo (pending must be OUT of scope).
	var userID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider)
		VALUES ('avx-it-session-user', 'github')
		ON CONFLICT (login_name) DO UPDATE SET oauth_provider = EXCLUDED.oauth_provider
		RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var repoA, repoB int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/aveloxis-it/scope-a', 'aveloxis-it', 'scope-a', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_owner = EXCLUDED.repo_owner
		RETURNING repo_id`).Scan(&repoA); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/aveloxis-it/scope-b', 'aveloxis-it', 'scope-b', 1)
		ON CONFLICT (repo_git) DO UPDATE SET repo_owner = EXCLUDED.repo_owner
		RETURNING repo_id`).Scan(&repoB); err != nil {
		t.Fatal(err)
	}
	var gApproved, gPending int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, 'avx-it-approved', 'approved')
		ON CONFLICT (user_id, name) DO UPDATE SET status = 'approved' RETURNING group_id`, userID).Scan(&gApproved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status) VALUES ($1, 'avx-it-pending', 'pending')
		ON CONFLICT (user_id, name) DO UPDATE SET status = 'pending' RETURNING group_id`, userID).Scan(&gPending); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]int64{{gApproved, repoA}, {gPending, repoB}} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)
			ON CONFLICT DO NOTHING`, pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		c := context.Background()
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN ($1,$2)`, gApproved, gPending)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_groups WHERE group_id IN ($1,$2)`, gApproved, gPending)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		_, _ = pool.Exec(c, `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1,$2)`, repoA, repoB)
	})

	// Token lifecycle.
	token, err := store.CreateSessionToken(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.ValidateSessionToken(ctx, token)
	if err != nil || got != userID {
		t.Fatalf("validate: got (%d, %v), want (%d, nil)", got, err, userID)
	}
	if _, err := store.ValidateSessionToken(ctx, "not-a-token"); !errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("unknown token must return ErrInvalidSessionToken, got %v", err)
	}
	// A 1ns lifetime truncates to +0 seconds — expired on arrival.
	// (Negative/zero lifetimes clamp to the 30-day default by design.)
	expired, err := store.CreateSessionToken(ctx, userID, time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateSessionToken(ctx, expired); !errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("expired token must be invalid, got %v", err)
	}
	if err := store.DeleteSessionToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ValidateSessionToken(ctx, token); !errors.Is(err, ErrInvalidSessionToken) {
		t.Errorf("deleted token must be invalid, got %v", err)
	}

	// Scope: approved group's repo in; pending group's repo out.
	scope, err := store.GetUserRepoScope(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	in := map[int64]bool{}
	for _, id := range scope {
		in[id] = true
	}
	if !in[repoA] {
		t.Errorf("approved-group repo %d must be in scope %v", repoA, scope)
	}
	if in[repoB] {
		t.Errorf("PENDING-group repo %d must NOT be in scope %v (v0.19.0 approval workflow)", repoB, scope)
	}
}
