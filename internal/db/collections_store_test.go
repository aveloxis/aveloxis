// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.63 — collections: admin-curated groups-of-groups. Source pins
// on the three load-bearing decisions (groups-join model, admin-owned
// link rule, copy-never-enqueues), then an integration test covering
// the full lifecycle.

func collectionsSQL(t *testing.T) string {
	t.Helper()
	return readSourceFile(t, "collections_store.go")
}

// Collections join to LIVE user_groups (not frozen repo lists):
// admin org-groups auto-grow via org scans, so collections stay
// fresh for free. A future refactor that snapshots repo ids into the
// collection defeats the design.
func TestCollectionsAreGroupJoins(t *testing.T) {
	src := collectionsSQL(t)
	if !strings.Contains(src, "aveloxis_ops.collection_groups") {
		t.Error("collections must join through collection_groups to live user_groups")
	}
	if !strings.Contains(src, "aveloxis_ops.user_repos") {
		t.Error("collection repos must resolve through user_repos at read time (live membership)")
	}
}

// Link-time rule: only ADMIN-owned groups may join a collection.
// Collections are a fleet-curation surface; linking an arbitrary
// user's group would let its owner mutate what every user sees.
func TestAddGroupToCollectionRequiresAdminOwner(t *testing.T) {
	src := collectionsSQL(t)
	if !strings.Contains(src, "u.admin") {
		t.Error("AddGroupToCollection must verify the member group is admin-owned (users.admin)")
	}
}

// THE copy invariant (v0.27.20 approval interplay): collections
// contain only already-tracked repos, so copying into a user's group
// is a direct user_repos INSERT…SELECT — it must NEVER touch the
// collection queue or the add-request tables. Comment-stripped scan
// so prose mentioning the words can't false-match.
func TestCopyCollectionNeverEnqueues(t *testing.T) {
	src := collectionsSQL(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	joined := strings.Join(code, "\n")
	for _, banned := range []string{"EnqueueRepo", "collection_queue", "collection_add_requests", "AddReposToGroup("} {
		if strings.Contains(joined, banned) {
			t.Errorf("collections_store.go must not reference %q — copy is a pure user_repos INSERT…SELECT (v0.27.20: approval gates NEW collection; collection repos are already tracked)", banned)
		}
	}
	if !strings.Contains(joined, "ON CONFLICT DO NOTHING") {
		t.Error("CopyCollectionToGroup must be idempotent (ON CONFLICT DO NOTHING on the user_repos PK)")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestCollectionsEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.Close()

	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collections WHERE name LIKE '_avcoll%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE name LIKE '_avcoll%')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE '_avcoll%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avcoll'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '_avcoll%'`)
	}
	clean()
	t.Cleanup(clean)

	var adminID, plainID int
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avcoll_admin', TRUE) RETURNING user_id`).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ('_avcoll_plain', FALSE) RETURNING user_id`).Scan(&plainID); err != nil {
		t.Fatal(err)
	}

	// Two admin groups sharing one repo (dedup check) + a plain-user group.
	mkGroup := func(userID int, name string) int64 {
		var gid int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.user_groups (user_id, name) VALUES ($1, $2) RETURNING group_id`, userID, name).Scan(&gid); err != nil {
			t.Fatal(err)
		}
		return gid
	}
	g1 := mkGroup(adminID, "_avcoll_g1")
	g2 := mkGroup(adminID, "_avcoll_g2")
	plainGroup := mkGroup(plainID, "_avcoll_target")
	nonAdminGroup := mkGroup(plainID, "_avcoll_notadmin")

	mkRepo := func(name string) int64 {
		var rid int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, '_avcoll', $2, 1) RETURNING repo_id`, "https://github.com/_avcoll/"+name, name).Scan(&rid); err != nil {
			t.Fatal(err)
		}
		return rid
	}
	r1, r2, r3 := mkRepo("one"), mkRepo("two"), mkRepo("three")
	link := func(gid, rid int64) {
		if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, gid, rid); err != nil {
			t.Fatal(err)
		}
	}
	link(g1, r1)
	link(g1, r2)
	link(g2, r2) // shared with g1 — dedup proof
	link(g2, r3)

	// Create + link.
	collID, err := store.CreateCollection(ctx, "_avcoll_main", "the test collection", 1, adminID)
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := store.AddGroupToCollection(ctx, collID, g1); err != nil {
		t.Fatalf("AddGroupToCollection g1: %v", err)
	}
	if err := store.AddGroupToCollection(ctx, collID, g2); err != nil {
		t.Fatalf("AddGroupToCollection g2: %v", err)
	}
	// Non-admin-owned group must be refused.
	if err := store.AddGroupToCollection(ctx, collID, nonAdminGroup); err == nil {
		t.Error("AddGroupToCollection must refuse a group owned by a non-admin")
	}

	// List: our collection shows 2 groups / 3 distinct repos.
	sums, err := store.ListCollections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, c := range sums {
		if c.Name == "_avcoll_main" {
			found = true
			if c.GroupCount != 2 {
				t.Errorf("group count = %d, want 2", c.GroupCount)
			}
			if c.RepoCount != 3 {
				t.Errorf("repo count = %d, want 3 (deduped across groups)", c.RepoCount)
			}
		}
	}
	if !found {
		t.Fatalf("collection not listed: %+v", sums)
	}

	// Detail repos: deduped, 3 rows.
	repos, total, err := store.GetCollectionRepos(ctx, collID, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if total != 3 || len(repos) != 3 {
		t.Errorf("detail repos = %d rows / total %d, want 3/3", len(repos), total)
	}

	// Copy into the plain user's group: 3 links, no enqueue.
	added, err := store.CopyCollectionToGroup(ctx, collID, plainID, plainGroup)
	if err != nil {
		t.Fatalf("CopyCollectionToGroup: %v", err)
	}
	if added != 3 {
		t.Errorf("copy added = %d, want 3", added)
	}
	// Idempotent: second copy adds zero.
	added, err = store.CopyCollectionToGroup(ctx, collID, plainID, plainGroup)
	if err != nil {
		t.Fatal(err)
	}
	if added != 0 {
		t.Errorf("second copy added = %d, want 0", added)
	}
	// Ownership: copying into someone ELSE's group must fail.
	if _, err := store.CopyCollectionToGroup(ctx, collID, plainID, g1); err == nil {
		t.Error("CopyCollectionToGroup must refuse a target group the caller does not own")
	}
	// The copy created ZERO queue rows for these repos.
	var queued int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2, $3)`, r1, r2, r3).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Errorf("copy must never enqueue collection — found %d queue rows", queued)
	}

	// Remove a group: repo count shrinks to g2's set (r2, r3).
	if err := store.RemoveGroupFromCollection(ctx, collID, g1); err != nil {
		t.Fatal(err)
	}
	_, total, err = store.GetCollectionRepos(ctx, collID, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("after removing g1, total = %d, want 2", total)
	}

	// Delete: join rows cascade.
	if err := store.DeleteCollection(ctx, collID); err != nil {
		t.Fatal(err)
	}
	var joins int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_groups WHERE collection_id = $1`, collID).Scan(&joins); err != nil {
		t.Fatal(err)
	}
	if joins != 0 {
		t.Errorf("collection_groups rows must cascade on delete, %d remain", joins)
	}
}
