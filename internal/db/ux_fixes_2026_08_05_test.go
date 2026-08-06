// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// ux_fixes_2026_08_05_test.go — TDD suite for the v0.27.84 UX round
// (operator testing with a non-admin account, 2026-08-05):
//
//   A1  already-REGISTERED orgs auto-approve for non-admins (the org's
//       repos are tracked and its future repos already auto-enqueue via
//       the existing registration + the v0.27.83 scan dedup, so the
//       duplicate registration adds ZERO new enqueue reachability — the
//       v0.27.20 "approval gates collection" principle holds).
//   A2  /me identity: GetUserIdentity (login, display name, avatar_url)
//       + OAuth logins refresh the stored display name.
//   A3  RepoStats carries last_collected so the GUI can distinguish
//       "queued for first collection" from "collected, zero activity"
//       (the kubernetes/api misread: honest zeros looked like an auth
//       bug).
//   A5  groups list carries pending_adds so the GUI can render the
//       REAL state (group approved, additions pending).

import (
	"strings"
	"testing"
	"time"
)

// --- A1: org auto-approve ---

func TestOrgAutoApproveSourceContract(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddOrgToGroup")
	if !strings.Contains(body, "IsOrgRegisteredAnywhere") {
		t.Error("AddOrgToGroup's non-admin branch must consult IsOrgRegisteredAnywhere — " +
			"an already-registered org adds zero new collection (v0.27.83 dedup) and must not pend")
	}
	src := readSourceFile(t, "web_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) IsOrgRegisteredAnywhere(") {
		t.Fatal("IsOrgRegisteredAnywhere helper missing")
	}
	if !strings.Contains(src, "LOWER(org_url)") {
		t.Error("IsOrgRegisteredAnywhere must match case-insensitively (org URLs are stored case-preserved)")
	}
}

func TestOrgAutoApproveWhenAlreadyRegisteredEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	suffix := time.Now().UnixNano()

	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE name LIKE '_avorgauto%')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_url ILIKE 'https://github.com/_avorgauto%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE '_avorgauto%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '_avorgauto%'`)
	}
	clean()
	t.Cleanup(clean)

	var adminID, plainID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, TRUE) RETURNING user_id`,
		"_avorgauto_admin").Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, FALSE) RETURNING user_id`,
		"_avorgauto_plain").Scan(&plainID); err != nil {
		t.Fatal(err)
	}
	mkGroup := func(userID int, name string) int64 {
		var gid int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
			VALUES ($1, $2, 'approved') RETURNING group_id`, userID, name).Scan(&gid); err != nil {
			t.Fatal(err)
		}
		return gid
	}
	adminGroup := mkGroup(adminID, "_avorgauto_admingrp")
	plainGroup := mkGroup(plainID, "_avorgauto_plaingrp")

	org := "_avorgauto_tracked" + itoa64(suffix)
	// Admin registers the org first (the pre-existing registration).
	out, err := store.AddOrgToGroup(ctx, adminID, adminGroup, "https://github.com/"+org)
	if err != nil || !out.Registered {
		t.Fatalf("admin registration failed: out=%+v err=%v", out, err)
	}

	// (1) Non-admin adds the SAME org as a CASE VARIANT: must register
	// immediately (no pending), with an auto-approved audit row.
	variant := "https://github.com/" + strings.ToUpper(org[:3]) + org[3:]
	out, err = store.AddOrgToGroup(ctx, plainID, plainGroup, variant)
	if err != nil {
		t.Fatalf("non-admin add of already-registered org: %v", err)
	}
	if !out.Registered {
		t.Fatal("already-registered org must auto-approve for a non-admin (v0.27.84)")
	}
	if out.RequestID == 0 {
		t.Error("auto-approve must still leave an audit row (RequestID)")
	}
	var auditStatus string
	var decidedBy int
	if err := store.pool.QueryRow(ctx, `
		SELECT status, COALESCE(decided_by, -1) FROM aveloxis_ops.collection_add_requests
		WHERE request_id = $1`, out.RequestID).Scan(&auditStatus, &decidedBy); err != nil {
		t.Fatal(err)
	}
	if auditStatus != "approved" || decidedBy != 0 {
		t.Errorf("audit row must be status='approved' decided_by=0 (the auto-approve pattern), got %s/%d", auditStatus, decidedBy)
	}
	var regs int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.user_org_requests
		WHERE group_id = $1 AND LOWER(org_url) = LOWER($2)`, plainGroup, variant).Scan(&regs); err != nil {
		t.Fatal(err)
	}
	if regs != 1 {
		t.Errorf("the non-admin's group must gain its own registration row, got %d", regs)
	}

	// (2) Non-admin adds an UNREGISTERED org: must still pend (the
	// v0.27.20 principle — a new org is an unbounded future commitment).
	out, err = store.AddOrgToGroup(ctx, plainID, plainGroup, "https://github.com/_avorgauto_new"+itoa64(suffix))
	if err != nil {
		t.Fatal(err)
	}
	if out.Registered {
		t.Fatal("an UNREGISTERED org must still pend for non-admins — regression of the approval gate")
	}
	if out.RequestID == 0 {
		t.Error("pending path must return the request id")
	}
	var pendStatus string
	if err := store.pool.QueryRow(ctx, `
		SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`,
		out.RequestID).Scan(&pendStatus); err != nil {
		t.Fatal(err)
	}
	if pendStatus != "pending" {
		t.Errorf("unregistered org request must be pending, got %q", pendStatus)
	}
}

func itoa64(n int64) string {
	if n < 0 {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
		if n == 0 {
			break
		}
	}
	return string(b[i:])
}

// --- A2: /me identity ---

func TestGetUserIdentityReplacesGetUserLogin(t *testing.T) {
	src := readSourceFile(t, "portal_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) GetUserIdentity(") {
		t.Fatal("GetUserIdentity missing — /me needs login + display name + avatar_url")
	}
	for _, needle := range []string{"first_name", "last_name", "avatar_url"} {
		if !strings.Contains(src, needle) {
			t.Errorf("GetUserIdentity must read %s", needle)
		}
	}
	if strings.Contains(src, "func (s *PostgresStore) GetUserLogin(") {
		t.Error("GetUserLogin should be REMOVED (single caller replaced; house remove-don't-deprecate)")
	}
}

func TestUpsertOAuthUserRefreshesDisplayName(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "UpsertOAuthUser")
	// The UPDATE path must refresh first/last name (non-empty guard) —
	// pre-v0.27.84 the display name was written only at first signup and
	// went stale after provider-side renames.
	upd := body[strings.Index(body, "UPDATE aveloxis_ops.users"):]
	for _, needle := range []string{"first_name", "last_name"} {
		if !strings.Contains(upd, needle) {
			t.Errorf("UpsertOAuthUser's UPDATE path must refresh %s from the fresh OAuth name", needle)
		}
	}
}

func TestUpsertOAuthUserNameRefreshEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	login := "_avidentity_" + itoa64(time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	})

	id1, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Name: "Old Name", AvatarURL: "https://a/1.png", Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	// Same user logs in again after renaming on the provider.
	id2, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Name: "Aaron Newname", AvatarURL: "https://a/2.png", Provider: "github"})
	if err != nil || id1 != id2 {
		t.Fatalf("second login must hit the same row: %d/%d err=%v", id1, id2, err)
	}
	login2, name, avatar, err := store.GetUserIdentity(ctx, id1)
	if err != nil {
		t.Fatal(err)
	}
	if login2 != login || name != "Aaron Newname" || avatar != "https://a/2.png" {
		t.Errorf("identity must reflect the FRESH login: login=%q name=%q avatar=%q", login2, name, avatar)
	}
	// An empty provider name must not clobber the stored one.
	if _, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Name: "", AvatarURL: "https://a/2.png", Provider: "github"}); err != nil {
		t.Fatal(err)
	}
	_, name, _, err = store.GetUserIdentity(ctx, id1)
	if err != nil || name != "Aaron Newname" {
		t.Errorf("empty OAuth name must preserve the stored display name, got %q (err=%v)", name, err)
	}
}

// --- A3: RepoStats.last_collected ---

func TestRepoStatsCarriesLastCollected(t *testing.T) {
	src := readSourceFile(t, "repo_stats.go")
	if !strings.Contains(src, `json:"last_collected,omitempty"`) {
		t.Fatal("RepoStats must carry last_collected (omitempty) — the GUI's only way to " +
			"distinguish 'queued for first collection' from 'collected, zero activity'")
	}
	for _, fn := range []string{"GetRepoStats", "GetRepoStatsBatch"} {
		body := extractFunctionBody(t, "repo_stats.go", fn)
		if !strings.Contains(body, "last_collected") {
			t.Errorf("%s must read collection_queue.last_collected", fn)
		}
	}
}

func TestRepoStatsLastCollectedEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	suffix := itoa64(time.Now().UnixNano())
	owner := "_avstatslc" + suffix

	mkRepo := func(name string, collected bool) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			"https://github.com/"+owner+"/"+name, owner, name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		lc := "NULL"
		if collected {
			lc = "NOW()"
		}
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at, last_collected)
			VALUES ($1, 'queued', 100, NOW(), `+lc+`)`, id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	queued := mkRepo("queued", false)
	collected := mkRepo("done", true)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2)`, queued, collected)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, owner)
	})

	sq, err := store.GetRepoStats(ctx, queued)
	if err != nil {
		t.Fatal(err)
	}
	if sq.LastCollected != nil {
		t.Error("never-collected repo must have nil LastCollected")
	}
	sc, err := store.GetRepoStats(ctx, collected)
	if err != nil {
		t.Fatal(err)
	}
	if sc.LastCollected == nil {
		t.Error("collected repo must carry LastCollected")
	}
	batch, err := store.GetRepoStatsBatch(ctx, []int64{queued, collected})
	if err != nil {
		t.Fatal(err)
	}
	if batch[queued] == nil || batch[queued].LastCollected != nil {
		t.Error("batch: queued repo must have nil LastCollected")
	}
	if batch[collected] == nil || batch[collected].LastCollected == nil {
		t.Error("batch: collected repo must carry LastCollected")
	}
}

// --- A5: groups list pending_adds ---

func TestGetUserGroupsCarriesPendingAdds(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "GetUserGroups")
	for _, needle := range []string{"collection_add_requests", "'pending'"} {
		if !strings.Contains(body, needle) {
			t.Errorf("GetUserGroups must count pending add-requests per group (missing %q) — "+
				"the GUI needs it to render the real 'additions awaiting approval' state", needle)
		}
	}
	src := readSourceFile(t, "web_store.go")
	if !strings.Contains(src, "PendingAdds") {
		t.Error("UserGroup must carry PendingAdds")
	}
}

func TestGroupsPendingAddsEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	login := "_avpendcnt_" + itoa64(time.Now().UnixNano())
	var userID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, FALSE) RETURNING user_id`,
		login).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
	})
	gid, err := store.CreateUserGroup(ctx, userID, "_avpendcnt_grp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.createAddRequest(ctx, userID, gid, "org", "https://github.com/_avpendcnt_org", nil, "pending"); err != nil {
		t.Fatal(err)
	}
	groups, err := store.GetUserGroups(ctx, userID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, g := range groups {
		if g.GroupID == gid {
			found = true
			if g.PendingAdds != 1 {
				t.Errorf("group with one pending add-request must report PendingAdds=1, got %d", g.PendingAdds)
			}
		}
	}
	if !found {
		t.Fatal("seeded group missing from GetUserGroups")
	}
}
