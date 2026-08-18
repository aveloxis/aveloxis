// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// The 2026-08-18 production drift check found 227 repos under tracked-org
// namespaces with no user_repos link for the registering groups. Mechanism:
// the org scan links only what GitHub's /orgs/{org}/repos listing
// ENUMERATES, so repos entering the catalog through any other door —
// mailing-list loaders, foundation importers, renames, GitLab orgs the
// scanner doesn't enumerate — are never linked. ReconcileOrgRepoLinks is
// the structural fix: a set-based self-heal linking every TRACKED
// (queue-row-bearing) repo under a registered org into that org's groups,
// regardless of which path created the repo.

// TestReconcileOrgRepoLinksShape pins the load-bearing SQL properties.
func TestReconcileOrgRepoLinksShape(t *testing.T) {
	src := readFileForTest(t, "org_link_reconcile.go")

	needles := map[string]string{
		"INSERT INTO aveloxis_ops.user_repos": "the reconciler is a pure user_repos link",
		"JOIN aveloxis_ops.collection_queue":  "the queue join is the tracked-only gate — it structurally excludes dead/sidelined rows (218 of the 227 drift repos were GitHub-404 residue that must NOT be linked)",
		"<> 'rejected'":                       "rejected groups' orgs are never linked (v0.27.20 abuse lever)",
		"starts_with(":                        "prefix match must be starts_with, not LIKE — org names can contain LIKE metacharacters (_ is legal in GitLab paths)",
		"LOWER(":                              "URL prefix match must be case-insensitive (v0.25.32)",
		"ON CONFLICT DO NOTHING":              "idempotent — re-running each pass must be a no-op for existing links",
	}
	for needle, why := range needles {
		if !strings.Contains(src, needle) {
			t.Errorf("org_link_reconcile.go missing %q — %s", needle, why)
		}
	}
}

// TestReconcileOrgRepoLinksNeverTouchesCollectionMachinery is the v0.27.20
// safety tripwire (the v0.27.82 pattern): the reconciler LINKS existing
// tracked repos only. It must never enqueue, create add-requests, or write
// the queue/org tables — otherwise it would become an approval bypass.
// Comment-stripped; bans WRITE operations, not identifiers (the SELECT
// legitimately reads collection_queue and user_org_requests).
func TestReconcileOrgRepoLinksNeverTouchesCollectionMachinery(t *testing.T) {
	data, err := os.ReadFile("org_link_reconcile.go")
	if err != nil {
		t.Fatalf("read org_link_reconcile.go: %v", err)
	}
	src := stripLineComments(string(data))
	for _, banned := range []string{
		"EnqueueRepo",
		"collection_add_requests",
		"INSERT INTO aveloxis_ops.collection_queue",
		"UPDATE aveloxis_ops.collection_queue",
		"INSERT INTO aveloxis_ops.user_org_requests",
		"UPDATE aveloxis_ops.user_org_requests",
	} {
		if strings.Contains(src, banned) {
			t.Errorf("org_link_reconcile.go contains banned operation %q — "+
				"the reconciler must be a pure user_repos link with zero "+
				"collection-machinery reachability (v0.27.20 invariant)", banned)
		}
	}
}

// TestReconcileOrgRepoLinksEndToEnd proves the reconciler's three-way
// behavior on a live database: a tracked-but-unlinked repo under a
// registered org gets linked; a dead (queue-less) repo does not; a
// rejected group's org repos do not. Second run is a no-op.
func TestReconcileOrgRepoLinksEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	const orgURL = "https://github.com/avtest-reconcile-org"
	cleanup := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, orgURL+"/%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_org_requests WHERE org_url = $1`, orgURL)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1)`, orgURL+"/%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1`, orgURL+"/%")
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name IN
			('avtest-reconcile-approved', 'avtest-reconcile-rejected')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = 'avtest-reconcile-user'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	var userID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name) VALUES ('avtest-reconcile-user')
		RETURNING user_id`).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	var approvedGID, rejectedGID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, 'avtest-reconcile-approved', 'approved') RETURNING group_id`, userID).Scan(&approvedGID); err != nil {
		t.Fatalf("seed approved group: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, 'avtest-reconcile-rejected', 'rejected') RETURNING group_id`, userID).Scan(&rejectedGID); err != nil {
		t.Fatalf("seed rejected group: %v", err)
	}
	for _, gid := range []int64{approvedGID, rejectedGID} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_org_requests (user_id, group_id, org_url)
			VALUES ($1, $2, $3)`, userID, gid, orgURL); err != nil {
			t.Fatalf("seed org request for group %d: %v", gid, err)
		}
	}

	seedRepo := func(name string, tracked bool) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
			VALUES ($1, 'avtest-reconcile-org', $2, 1, 1) RETURNING repo_id`,
			orgURL+"/"+name, name).Scan(&id); err != nil {
			t.Fatalf("seed repo %s: %v", name, err)
		}
		if tracked {
			if _, err := store.pool.Exec(ctx, `
				INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority)
				VALUES ($1, 'queued', 100) ON CONFLICT (repo_id) DO NOTHING`, id); err != nil {
				t.Fatalf("seed queue row for %s: %v", name, err)
			}
		}
		return id
	}
	trackedID := seedRepo("tracked-unlinked", true)
	deadID := seedRepo("dead-no-queue", false)

	linked, err := store.ReconcileOrgRepoLinks(ctx)
	if err != nil {
		t.Fatalf("ReconcileOrgRepoLinks: %v", err)
	}
	if linked < 1 {
		t.Fatalf("expected at least 1 link inserted, got %d", linked)
	}

	countLinks := func(gid, repoID int64) int {
		var n int
		if err := store.pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM aveloxis_ops.user_repos WHERE group_id = $1 AND repo_id = $2`,
			gid, repoID).Scan(&n); err != nil {
			t.Fatalf("count links: %v", err)
		}
		return n
	}
	if countLinks(approvedGID, trackedID) != 1 {
		t.Error("tracked-but-unlinked repo under the org must be linked into the approved group")
	}
	if countLinks(approvedGID, deadID) != 0 {
		t.Error("queue-less (dead/sidelined) repo must NOT be linked — 218 of the " +
			"227 production drift repos were GitHub-404 residue")
	}
	if countLinks(rejectedGID, trackedID) != 0 {
		t.Error("rejected group's org must never receive links (v0.27.20 abuse lever)")
	}

	// Idempotency: the fixture's links are all in place, so a second run
	// must insert nothing for this org (fleet-wide count may be nonzero on
	// a shared scratch DB, so assert on the fixture's links directly).
	if _, err := store.ReconcileOrgRepoLinks(ctx); err != nil {
		t.Fatalf("second ReconcileOrgRepoLinks: %v", err)
	}
	if countLinks(approvedGID, trackedID) != 1 {
		t.Error("second run must be a no-op for already-linked repos (ON CONFLICT DO NOTHING)")
	}
}
