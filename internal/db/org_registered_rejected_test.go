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

// TestOrgRegistrationInARejectedGroupAutoApprovesNothing (AVELOXIS_TEST_DB):
// a registration in a rejected group is not scanned, so it must not count as
// "already registered" for the v0.27.84 auto-approve — otherwise a non-admin
// whose group was rejected re-tracks the same org from a fresh group with no
// review, around RejectGroup (round-12 review).
func TestOrgRegistrationInARejectedGroupAutoApprovesNothing(t *testing.T) {
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

	const login = "_avorg_rejected_probe"
	const orgURL = "https://github.com/_avorg-rejected-probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if admin, err := store.IsUserAdmin(ctx, uid); err != nil || admin {
		t.Fatalf("fixture user must be a non-admin (admin=%v, err=%v)", admin, err)
	}
	first, err := store.CreateUserGroup(ctx, uid, "rejected org probe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform) VALUES ($1, $2, $3, '_avorg-rejected-probe', 'github')`, uid, first, orgURL); err != nil {
		t.Fatal(err)
	}
	if registered, err := store.IsOrgRegisteredAnywhere(ctx, orgURL); err != nil || !registered {
		t.Fatalf("a registration in an approved group: registered=%v err=%v, want true", registered, err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.user_groups SET status = 'rejected' WHERE group_id = $1`, first); err != nil {
		t.Fatal(err)
	}
	if registered, err := store.IsOrgRegisteredAnywhere(ctx, orgURL); err != nil || registered {
		t.Errorf("a registration only in a rejected group: registered=%v err=%v, want false", registered, err)
	}

	fresh, err := store.CreateUserGroup(ctx, uid, "fresh org probe")
	if err != nil {
		t.Fatal(err)
	}
	out, err := store.AddOrgToGroup(ctx, uid, fresh, orgURL)
	if err != nil || out.RequestID == 0 {
		t.Fatalf("AddOrgToGroup from a fresh group = %+v, %v; want a request", out, err)
	}
	var status string
	var inFresh int
	_ = store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, out.RequestID).Scan(&status)
	_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, fresh).Scan(&inFresh)
	if status != "pending" || inFresh != 0 {
		t.Errorf("re-adding an org whose only registration is in a rejected group: request %q, %d registrations in the fresh group; want pending, 0", status, inFresh)
	}
}
