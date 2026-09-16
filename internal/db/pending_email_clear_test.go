// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestClearUserPendingEmailIfEndToEnd (AVELOXIS_TEST_DB): the clear after a
// failed confirmation send touches email_pending only while it still holds
// that submission's address, so a newer submission from a second tab
// survives (v0.29.30).
func TestClearUserPendingEmailIfEndToEnd(t *testing.T) {
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
	testMigrate(ctx, t, store)

	const login = "_avpending_clear_probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.SetUserPendingEmail(ctx, uid, "b@example.com"); err != nil {
		t.Fatal(err)
	}
	// Tab A's failed send tries to clear its own, older address.
	if err := store.ClearUserPendingEmailIf(ctx, uid, "a@example.com"); err != nil {
		t.Fatalf("a clear that matches nothing is not an error: %v", err)
	}
	if got := rawPending(ctx, t, store, uid); got != "b@example.com" {
		t.Errorf("another submission's pending address was cleared: got %q", got)
	}
	// Exact match: a case variant of the stored address is a different
	// string to this clear (callers pass the same parsed value they stored).
	if err := store.SetUserPendingEmail(ctx, uid, "B@Example.com"); err != nil {
		t.Fatal(err)
	}
	if err := store.ClearUserPendingEmailIf(ctx, uid, "b@example.com"); err != nil {
		t.Fatal(err)
	}
	if got := rawPending(ctx, t, store, uid); got != "B@Example.com" {
		t.Errorf("a case-variant clear must not match: got %q", got)
	}
	if err := store.ClearUserPendingEmailIf(ctx, uid, "B@Example.com"); err != nil {
		t.Fatal(err)
	}
	if got := rawPending(ctx, t, store, uid); got != "" {
		t.Errorf("the matching pending address was not cleared: got %q", got)
	}
}

// rawPending reads users.email_pending directly, token or not.
func rawPending(ctx context.Context, t *testing.T, store *PostgresStore, uid int) string {
	t.Helper()
	var p *string
	if err := store.pool.QueryRow(ctx, `SELECT email_pending FROM aveloxis_ops.users WHERE user_id = $1`, uid).Scan(&p); err != nil {
		t.Fatal(err)
	}
	if p == nil {
		return ""
	}
	return *p
}

// TestConfirmEmailTokenEndToEnd (AVELOXIS_TEST_DB): a confirmation link is
// consumed only by the account it was issued to, in one transaction with
// the promotion, and a pending address counts only while a live token
// backs it (round-6 review: another account's click burned the link, a
// failed promotion burned it too, and an expired link kept the dashboard
// saying "click the link in that email").
func TestConfirmEmailTokenEndToEnd(t *testing.T) {
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
	testMigrate(ctx, t, store)

	logins := []string{"_avconfirm_token_a", "_avconfirm_token_b"}
	clean := func() {
		for _, l := range logins {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.email_confirmations WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, l)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, l)
		}
	}
	clean()
	t.Cleanup(clean)
	a, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: logins[0], Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: logins[1], Provider: "gitlab"})
	if err != nil {
		t.Fatal(err)
	}

	if got, _ := store.GetUserLivePendingEmail(ctx, a); got != "" {
		t.Fatalf("no pending address yet, got %q", got)
	}
	if err := store.SetUserPendingEmail(ctx, a, "a@example.com"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetUserLivePendingEmail(ctx, a); got != "" {
		t.Errorf("a pending address with no token is not live, got %q", got)
	}
	token, err := store.CreateEmailConfirmation(ctx, a, "a@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetUserLivePendingEmail(ctx, a); got != "a@example.com" {
		t.Errorf("a pending address with a live token must be returned, got %q", got)
	}

	// Another account's click neither confirms nor burns A's link.
	if _, err := store.ConfirmEmailToken(ctx, token, b); !errors.Is(err, ErrConfirmationTokenInvalid) {
		t.Errorf("ConfirmEmailToken by another user = %v, want ErrConfirmationTokenInvalid", err)
	}
	if got, _ := store.GetUserLivePendingEmail(ctx, a); got != "a@example.com" {
		t.Errorf("another account's click must leave A's link live, got %q", got)
	}

	email, err := store.ConfirmEmailToken(ctx, token, a)
	if err != nil || email != "a@example.com" {
		t.Fatalf("ConfirmEmailToken by its owner = %q, %v", email, err)
	}
	var confirmed *string
	if err := store.pool.QueryRow(ctx, `SELECT email FROM aveloxis_ops.users WHERE user_id = $1`, a).Scan(&confirmed); err != nil || confirmed == nil || *confirmed != "a@example.com" {
		t.Errorf("users.email after confirmation = %v, %v", confirmed, err)
	}
	if got := rawPending(ctx, t, store, a); got != "" {
		t.Errorf("email_pending after confirmation = %q, want cleared", got)
	}
	if _, err := store.ConfirmEmailToken(ctx, token, a); !errors.Is(err, ErrConfirmationTokenInvalid) {
		t.Errorf("a used token must be invalid, got %v", err)
	}

	// An expired token is not live and does not confirm.
	if err := store.SetUserPendingEmail(ctx, b, "b@example.com"); err != nil {
		t.Fatal(err)
	}
	old, err := store.CreateEmailConfirmation(ctx, b, "b@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.email_confirmations SET expires_at = NOW() - INTERVAL '1 minute' WHERE token = $1`, old); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetUserLivePendingEmail(ctx, b); got != "" {
		t.Errorf("a pending address whose only token expired is not live, got %q", got)
	}
	if _, err := store.ConfirmEmailToken(ctx, old, b); !errors.Is(err, ErrConfirmationTokenInvalid) {
		t.Errorf("an expired token must be invalid, got %v", err)
	}
}
