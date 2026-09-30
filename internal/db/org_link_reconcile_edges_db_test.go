// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
)

// TestReconcileOrgRepoLinksKeyedJoinKeepsItsResult — v0.29.71: the join
// gained a host/owner key (a hash join; 0.7 s instead of 330 s on kate)
// with starts_with kept as the exact test, so the result must be exactly
// the prefix rule's: a GitLab subgroup's direct and nested projects, a case
// variant, an org URL with a trailing slash, and NOT a sibling org whose
// name merely starts with the org's name, nor another host's same path.
func TestReconcileOrgRepoLinksKeyedJoinKeepsItsResult(t *testing.T) {
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
	const login = "_avrecon_keyed_probe"
	repos := map[string]int{
		"https://gitlab.com/_avrk-g/sub/direct":       2,
		"https://gitlab.com/_avrk-g/sub/deeper/proj":  2,
		"https://gitlab.com/_avrk-g/subsibling/other": 2,
		"https://GitHub.com/_AvRk-Org/Mixed":          1,
		"https://github.com/_avrk-org/plain":          1,
		"https://github.com/_avrk-orgx/sibling":       1,
		"https://git.example.org/_avrk-org/samepath":  3,
	}
	clean := func() {
		for u := range repos {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, u)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1)`, u)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, u)
		}
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "keyed reconcile probe")
	if err != nil {
		t.Fatal(err)
	}
	for u, platform := range repos {
		var id int64
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id) VALUES ($1, 'o', 'n', $2) RETURNING repo_id`, u, platform).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id) VALUES ($1) ON CONFLICT DO NOTHING`, id); err != nil {
			t.Fatal(err)
		}
	}
	for _, org := range []string{"https://gitlab.com/_avrk-g/sub", "https://github.com/_avrk-org/"} {
		if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url) VALUES ($1, $2, $3)`, uid, gid, org); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ReconcileOrgRepoLinks(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := store.pool.Query(ctx, `SELECT r.repo_git FROM aveloxis_ops.user_repos ur JOIN aveloxis_data.repos r USING (repo_id) WHERE ur.group_id = $1 ORDER BY 1`, gid)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		got = append(got, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []string{"https://GitHub.com/_AvRk-Org/Mixed", "https://github.com/_avrk-org/plain", "https://gitlab.com/_avrk-g/sub/deeper/proj", "https://gitlab.com/_avrk-g/sub/direct"}
	sort.Strings(want)
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("linked %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("linked %v, want %v", got, want)
		}
	}
}
