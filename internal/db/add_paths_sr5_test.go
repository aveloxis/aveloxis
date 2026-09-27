// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func openSR5Store(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
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
	return store, ctx
}

// TestOwnershipLookupErrorsAreNotNotYours pins worklist follow-up 12 at the
// store: GetPortalGroupReposForUser and GetPortalGroupOrgsForUser read ANY
// ownership-lookup error as "group N is not yours" (an untyped error; since
// batch 5b it is ErrGroupNotOwned, wrapped), and CopyCollectionToGroup
// turned any verifyGroupOwned error into ErrNotGroupOwner. Only "no such
// owned group" is that answer; a failed lookup is returned as itself (SR-5).
func TestOwnershipLookupErrorsAreNotNotYours(t *testing.T) {
	store, ctx := openSR5Store(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := store.GetPortalGroupReposForUser(canceled, 1, 1, false, 1, 10, "", ""); err == nil || errors.Is(err, ErrGroupNotOwned) {
		t.Errorf("GetPortalGroupReposForUser on a failed lookup = %v; want the store's error, not \"not yours\"", err)
	}
	if _, err := store.GetPortalGroupOrgsForUser(canceled, 1, 1, false); err == nil || errors.Is(err, ErrGroupNotOwned) {
		t.Errorf("GetPortalGroupOrgsForUser on a failed lookup = %v; want the store's error, not \"not yours\"", err)
	}
	if _, err := store.CopyCollectionToGroup(canceled, 1, 1, 1); err == nil || errors.Is(err, ErrNotGroupOwner) {
		t.Errorf("CopyCollectionToGroup on a failed lookup = %v; want the store's error, not ErrNotGroupOwner", err)
	}
	// The real answer is unchanged: a group nobody owns.
	if _, _, err := store.GetPortalGroupReposForUser(ctx, 1, 1<<40, false, 1, 10, "", ""); !errors.Is(err, ErrGroupNotOwned) {
		t.Errorf("an unowned group = %v; want ErrGroupNotOwned", err)
	}
	if _, err := store.GetPortalGroupOrgsForUser(ctx, 1, 1<<40, false); !errors.Is(err, ErrGroupNotOwned) {
		t.Errorf("an unowned group (orgs) = %v; want ErrGroupNotOwned", err)
	}
	if _, err := store.CopyCollectionToGroup(ctx, 1, 1, 1<<40); !errors.Is(err, ErrNotGroupOwner) {
		t.Errorf("an unowned target group = %v; want ErrNotGroupOwner", err)
	}
}

// TestOverLongURLsAreRefusedAtEveryWriter pins worklist follow-up 14: a
// pending org request whose URL is too long for the registration's unique
// index failed at approval forever (DecideAddRequest applied no limit), and
// `aveloxis add-repo` and every other UpsertRepo path had no limit at all.
// The owning writers refuse: registerApprovedOrg (approval and
// auto-approval alike) and UpsertRepo answer ErrURLTooLong, so the admin is
// told to reject the request and the CLI reports the URL.
func TestOverLongURLsAreRefusedAtEveryWriter(t *testing.T) {
	store, ctx := openSR5Store(t)
	const login = "_avoverlong_probe"
	// Past the INDEX bound, not merely the entry limit: a pending request of
	// 1,343–2,684 bytes has always approved and still must.
	tooLong := "https://github.com/_avoverlong-" + strings.Repeat("x", maxIndexedURLBytes)
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_url LIKE 'https://github.com/_avoverlong-%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE 'https://github.com/_avoverlong-%'`)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "over-long probe")
	if err != nil {
		t.Fatal(err)
	}
	// A pending org request created before v0.29.54's limit existed.
	reqID, err := store.createAddRequest(ctx, uid, gid, "org", tooLong, nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.DecideAddRequest(ctx, reqID, uid, true, ""); !errors.Is(err, ErrURLTooLong) {
		t.Errorf("approving an over-long org URL = %v; want ErrURLTooLong (the admin is told to reject it)", err)
	}
	// A legacy request between the entry limit and the index bound approves.
	legacy := "https://github.com/_avoverlong-" + strings.Repeat("y", MaxAddURLBytes+100)
	legacyID, err := store.createAddRequest(ctx, uid, gid, "org", legacy, nil, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.DecideAddRequest(ctx, legacyID, uid, true, ""); err != nil {
		t.Errorf("approving a legacy org URL between the entry limit and the index bound = %v; want approved", err)
	}
	var status string
	if err := store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, reqID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Errorf("the refused approval left the request %q; want pending (nothing flipped)", status)
	}
	if _, err := store.UpsertRepo(ctx, &model.Repo{GitURL: tooLong, Owner: "_avoverlong", Name: "x", Platform: model.PlatformGitHub}); !errors.Is(err, ErrURLTooLong) {
		t.Errorf("UpsertRepo with an over-long URL = %v; want ErrURLTooLong", err)
	}
}

// TestOrgRegistrationIsCaseInsensitive pins worklist follow-up 3: the
// "already registered" check compares LOWER(org_url) but the unique key is
// case-sensitive, so re-adding `github.com/CHAOSS` beside `github.com/chaoss`
// made a second registration in the same group. The org URL is stored in
// one canonical spelling (lowercase host and path — GitHub and GitLab paths
// are case-insensitive), so the unique key and the check agree.
func TestOrgRegistrationIsCaseInsensitive(t *testing.T) {
	store, ctx := openSR5Store(t)
	const login = "_avorgcase_probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE LOWER(org_url) LIKE 'https://github.com/_avorgcase-%'`)
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
	if err := store.SetUserAdmin(ctx, uid, true); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "org case probe")
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{"https://github.com/_AVORGCASE-Org", "github.com/_avorgcase-org/", "HTTPS://GitHub.com/_avorgcase-ORG"} {
		if _, err := store.AddOrgToGroup(ctx, uid, gid, u, ""); err != nil {
			t.Fatalf("AddOrgToGroup(%q): %v", u, err)
		}
	}
	var n int
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT count(*), min(org_url) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n, &stored); err != nil {
		t.Fatal(err)
	}
	if n != 1 || stored != "https://github.com/_avorgcase-org" {
		t.Errorf("%d registrations, first %q; want 1 registration stored as https://github.com/_avorgcase-org", n, stored)
	}
}

// TestLegacyMixedCaseOrgRowIsNotDuplicated pins the write side of follow-up
// 3 (batch 5 review round 1): CanonicalOrgURL lowercases a NEW spelling, but
// the registration's arbiter is the exact key, so a row stored mixed-case
// before v0.29.68 plus an identical re-paste — the most common input —
// inserted a second, lowercase row beside it, where the old exact-key
// conflict inserted nothing. Both writers (the admin path and the
// auto-approved non-admin path) go through registerApprovedOrg, which now
// asks the group for the org case-insensitively first. The bridge
// GetUserGroupIDsForOrgURL compares case-insensitively too, so a legacy
// row and a canonical row are both found from a typed spelling.
func TestLegacyMixedCaseOrgRowIsNotDuplicated(t *testing.T) {
	store, ctx := openSR5Store(t)
	const login = "_avorglegacy_probe"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE LOWER(org_url) LIKE 'https://github.com/_avorglegacy-%'`)
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
	gid, err := store.CreateUserGroup(ctx, uid, "org legacy probe")
	if err != nil {
		t.Fatal(err)
	}
	const legacy = "https://github.com/_AVORGLEGACY-Org"
	// How AddOrgToGroup stored a pasted URL before v0.29.68.
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name, platform) VALUES ($1, $2, $3, '_AVORGLEGACY-Org', 'github')`, uid, gid, legacy); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.user_org_requests WHERE group_id = $1`, gid).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// The non-admin owner re-pastes it: registered anywhere → auto-approved
	// → registerApprovedOrg.
	if _, err := store.AddOrgToGroup(ctx, uid, gid, legacy, ""); err != nil {
		t.Fatal(err)
	}
	if n := count(); n != 1 {
		t.Errorf("after a non-admin's identical re-add: %d registrations in the group; want the legacy row alone", n)
	}
	if err := store.SetUserAdmin(ctx, uid, true); err != nil {
		t.Fatal(err)
	}
	for _, u := range []string{legacy, "github.com/_avorglegacy-org/"} {
		if _, err := store.AddOrgToGroup(ctx, uid, gid, u, ""); err != nil {
			t.Fatalf("AddOrgToGroup(%q): %v", u, err)
		}
	}
	if n := count(); n != 1 {
		t.Errorf("after an admin's re-adds in two spellings: %d registrations in the group; want the legacy row alone", n)
	}
	// The bridge finds the legacy row from the typed spelling, and a
	// canonical row from a mixed-case spelling.
	if ids, err := store.GetUserGroupIDsForOrgURL(ctx, legacy); err != nil || !slices.Contains(ids, gid) {
		t.Errorf("GetUserGroupIDsForOrgURL(legacy spelling) = %v, %v; want the group", ids, err)
	}
	gid2, err := store.CreateUserGroup(ctx, uid, "org canonical probe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddOrgToGroup(ctx, uid, gid2, "https://github.com/_AVORGLEGACY-Other", ""); err != nil {
		t.Fatal(err)
	}
	if ids, err := store.GetUserGroupIDsForOrgURL(ctx, "https://github.com/_AVORGLEGACY-Other/"); err != nil || !slices.Contains(ids, gid2) {
		t.Errorf("GetUserGroupIDsForOrgURL(a typed spelling with a trailing slash) = %v, %v; want the group (review round 5)", ids, err)
	}
	if ids, err := store.GetUserGroupIDsForOrgURL(ctx, "https://github.com/_AVORGLEGACY-Other"); err != nil || !slices.Contains(ids, gid2) {
		t.Errorf("GetUserGroupIDsForOrgURL(mixed-case spelling of a canonical row) = %v, %v; want the group", ids, err)
	}
}
