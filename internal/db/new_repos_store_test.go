// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"
)

// v0.27.62 — GetNewRepos: the "New Repositories" home feed. Fleet arm
// = repos added since the window start whose owner matches an org
// registered by an ADMIN user (the "main user" generalized) with a
// non-rejected owning group; mine arm = same, filtered to orgs the
// CALLER registered. Source pins on the two gating decisions, then an
// integration test that seeds both arms.

func newReposSQL(t *testing.T) string {
	t.Helper()
	src := readSourceFile(t, "new_repos_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) GetNewRepos(") {
		t.Fatal("GetNewRepos missing from new_repos_store.go")
	}
	return src
}

// The fleet arm is gated on ADMIN-registered orgs (u.admin) whose
// owning group is not rejected — the same rejected-group refusal the
// org-scan paths honor (v0.27.20: RejectGroup is the abuse lever, so
// a rejected group's orgs must not surface anywhere).
func TestGetNewReposFleetArmGating(t *testing.T) {
	src := newReposSQL(t)
	if !strings.Contains(src, "u.admin") {
		t.Error("fleet arm must gate on users.admin (admin-registered orgs only)")
	}
	if !strings.Contains(src, "<> 'rejected'") {
		t.Error("org set must exclude orgs whose owning group is rejected")
	}
	if !strings.Contains(src, "added_at") {
		t.Error("feed must window on repos.added_at (v0.27.60)")
	}
	if !strings.Contains(src, "repo_archived") {
		t.Error("archived repos don't belong in a new-repos feed")
	}
}

// Owner matching is case-insensitive: GitHub org logins are
// case-preserving but case-insensitive, and org_url casing is
// whatever the registrant typed.
func TestGetNewReposOwnerMatchCaseInsensitive(t *testing.T) {
	src := newReposSQL(t)
	if !strings.Contains(src, "LOWER(r.repo_owner)") {
		t.Error("owner ↔ org matching must be case-insensitive (LOWER on both sides)")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestGetNewReposEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_url LIKE 'https://github.com/_avnewr%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE '_avnewr%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner ILIKE '_avnewr%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '_avnewr%'`)
	}
	clean()
	t.Cleanup(clean)

	var adminID, plainID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avnewr_admin', TRUE) RETURNING user_id`).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avnewr_plain', FALSE) RETURNING user_id`).Scan(&plainID); err != nil {
		t.Fatal(err)
	}

	mkGroup := func(userID int, name, status string) int64 {
		var gid int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
			VALUES ($1, $2, $3) RETURNING group_id`, userID, name, status).Scan(&gid); err != nil {
			t.Fatal(err)
		}
		return gid
	}
	adminGroup := mkGroup(adminID, "_avnewr_admingrp", "approved")
	plainGroup := mkGroup(plainID, "_avnewr_plaingrp", "approved")
	rejGroup := mkGroup(adminID, "_avnewr_rejgrp", "rejected")

	mkOrg := func(userID int, gid int64, org string) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url, org_name)
			VALUES ($1, $2, $3, $4)`, userID, gid, "https://github.com/"+org, org); err != nil {
			t.Fatal(err)
		}
	}
	mkOrg(adminID, adminGroup, "_avnewr_fleetorg")
	mkOrg(plainID, plainGroup, "_avnewr_mineorg")
	mkOrg(adminID, rejGroup, "_avnewr_rejorg")

	mkRepo := func(owner, name string, age time.Duration) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, added_at)
			VALUES ($1, $2, $3, 1, NOW() - $4::interval)`,
			"https://github.com/"+owner+"/"+name, owner, name, age.String()); err != nil {
			t.Fatal(err)
		}
	}
	// Case-variant owner spelling proves the LOWER match.
	mkRepo("_AVNEWR_fleetorg", "fresh", 24*time.Hour)
	mkRepo("_avnewr_fleetorg", "stale", 200*24*time.Hour) // outside any window
	mkRepo("_avnewr_mineorg", "mine1", 24*time.Hour)
	mkRepo("_avnewr_rejorg", "hidden", 24*time.Hour) // rejected group → never surfaces

	since := time.Now().AddDate(0, 0, -30)

	// Plain user: fleet = admin-org repos (fresh only), mine = their own
	// org's repo. The rejected-group org must appear in NEITHER.
	fleet, mine, err := store.GetNewRepos(ctx, plainID, since, 100)
	if err != nil {
		t.Fatalf("GetNewRepos: %v", err)
	}
	find := func(rows []NewRepo, name string) bool {
		for _, r := range rows {
			if r.Name == name {
				return true
			}
		}
		return false
	}
	if !find(fleet, "fresh") {
		t.Errorf("fleet arm must include the fresh admin-org repo (case-variant owner): %+v", fleet)
	}
	if find(fleet, "stale") {
		t.Error("fleet arm must window on added_at — 200-day-old repo leaked in")
	}
	if find(fleet, "hidden") || find(mine, "hidden") {
		t.Error("rejected-group org repos must never surface")
	}
	if find(fleet, "mine1") {
		t.Error("fleet arm is admin-registered orgs only — plain user's org leaked in")
	}
	if !find(mine, "mine1") {
		t.Errorf("mine arm must include the caller's own org repos: %+v", mine)
	}
	if find(mine, "fresh") {
		t.Error("mine arm is the CALLER's orgs only")
	}
}
