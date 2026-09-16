// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
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
	if got, _ := store.GetUserPendingEmail(ctx, uid); got != "b@example.com" {
		t.Errorf("another submission's pending address was cleared: got %q", got)
	}
	if err := store.ClearUserPendingEmailIf(ctx, uid, "b@example.com"); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetUserPendingEmail(ctx, uid); got != "" {
		t.Errorf("the matching pending address was not cleared: got %q", got)
	}
}
