// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestAddRefusesURLsWithUserinfo (AVELOXIS_TEST_DB) — v0.29.57, Copilot
// review 5261384568. Every add path — non-admin pending, auto-approved,
// admin direct, and the org registration — refuses a URL carrying
// credentials with platform.ErrURLUserinfo BEFORE anything is written: no
// request row, no item, no repos row, no org registration. The portal API
// reaches these writers without the web validator, and a pending item is
// shown to the admin, so the store is where the refusal has to live.
func TestAddRefusesURLsWithUserinfo(t *testing.T) {
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

	const userLogin, adminLogin = "_avurl_userinfo_user_probe", "_avurl_userinfo_admin_probe"
	const marker = "_avurl-userinfo-probe"
	clean := func() {
		for _, login := range []string{userLogin, adminLogin} {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		}
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $1 || '%')`, marker)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $1 || '%'`, marker)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: userLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	adminID, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: adminLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAdmin(ctx, adminID, true); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "userinfo probe")
	if err != nil {
		t.Fatal(err)
	}
	adminGID, err := store.CreateUserGroup(ctx, adminID, "userinfo probe, admin")
	if err != nil {
		t.Fatal(err)
	}

	written := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `
			SELECT (SELECT count(*) FROM aveloxis_ops.collection_add_requests WHERE user_id IN ($1, $2))
			     + (SELECT count(*) FROM aveloxis_data.repos WHERE repo_git LIKE '%' || $3 || '%')
			     + (SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE org_url LIKE '%' || $3 || '%')`,
			uid, adminID, marker).Scan(&n); err != nil {
			t.Fatalf("count writes: %v", err)
		}
		return n
	}
	before := written()
	okURL := "https://git.example.invalid/" + marker + "/fine"
	badRepo := "https://user:token@git.example.invalid/" + marker + "/creds"
	badHub := "https://token@github.com/" + marker + "-owner/creds"
	badOrg := "https://user:token@github.com/" + marker + "-org"
	for _, tc := range []struct {
		name string
		add  func() error
	}{
		{"non-admin repos, beside a clean URL", func() error {
			_, err := store.AddReposToGroup(ctx, uid, gid, []string{okURL, badRepo}, 0)
			return err
		}},
		{"auto-approved repos", func() error {
			_, err := store.AddReposToGroup(ctx, uid, gid, []string{badHub}, 5)
			return err
		}},
		{"admin repos", func() error {
			_, err := store.AddReposToGroup(ctx, adminID, adminGID, []string{badRepo}, 0)
			return err
		}},
		{"admin org", func() error {
			_, err := store.AddOrgToGroup(ctx, adminID, adminGID, badOrg, "")
			return err
		}},
		{"non-admin org", func() error {
			_, err := store.AddOrgToGroup(ctx, uid, gid, badOrg, "")
			return err
		}},
	} {
		if err := tc.add(); !errors.Is(err, platform.ErrURLUserinfo) {
			t.Errorf("%s: %v; want platform.ErrURLUserinfo", tc.name, err)
		}
	}
	if after := written(); after != before {
		t.Errorf("refused adds wrote %d rows (requests, repos or org registrations)", after-before)
	}
	// The clean URL beside the refused one was not added either: a refused
	// add changes nothing.
	if id, err := store.FindRepoByURL(ctx, okURL); err != nil || id != 0 {
		t.Errorf("the clean URL of a refused batch was stored (id %d, %v)", id, err)
	}
}

// TestApprovingALegacyCredentialedOrgRequestIsRefused (AVELOXIS_TEST_DB) —
// Copilot reviews 5267193512/5267408933: a pending org request written
// before the store refused credentialed URLs bypasses AddOrgToGroup at
// approval; registerApprovedOrg refuses it and the approval's transaction
// never commits: the request stays pending (the admin rejects it), nothing
// is registered.
func TestApprovingALegacyCredentialedOrgRequestIsRefused(t *testing.T) {
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
	const userLogin, adminLogin = "_avlegacy_org_user_probe", "_avlegacy_org_admin_probe"
	const orgURL = "https://user:s3cret@github.com/_avlegacy-org-probe"
	clean := func() {
		for _, login := range []string{userLogin, adminLogin} {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		}
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: userLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	adminID, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: adminLogin, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetUserAdmin(ctx, adminID, true); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "legacy org probe")
	if err != nil {
		t.Fatal(err)
	}
	// The legacy row: planted directly, since every writer refuses it now.
	var reqID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, org_url, status, item_count)
		VALUES ($1, $2, 'org', $3, 'pending', 0) RETURNING request_id`, uid, gid, orgURL).Scan(&reqID); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.DecideAddRequest(ctx, reqID, adminID, true, "")
	if !errors.Is(err, platform.ErrURLUserinfo) {
		t.Fatalf("approving a legacy credentialed org request = %v; want platform.ErrURLUserinfo", err)
	}
	if err != nil && strings.Contains(err.Error(), "s3cret") {
		t.Errorf("the refusal's text carries the credential: %v", err)
	}
	var registered int
	var status string
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&registered); err != nil || registered != 0 {
		t.Errorf("the refused org was registered (%d rows, %v)", registered, err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, reqID).Scan(&status); err != nil || status != "pending" {
		t.Errorf("the refused request's status = %q, %v; want pending (the admin rejects it)", status, err)
	}
	// Rejecting it works.
	if _, _, err := store.DecideAddRequest(ctx, reqID, adminID, false, ""); err != nil {
		t.Errorf("rejecting the legacy request: %v", err)
	}
}
