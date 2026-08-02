// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
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
//
// 2026-08-02 scope correction: v0.27.74's GetCollectionRepos
// legitimately READS collection_queue (the cached count columns), so
// the bare "collection_queue" ban moved from file-wide to the
// CopyCollectionToGroup body. Queue WRITES stay banned file-wide —
// the invariant was always about mutation, not reads.
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
	// File-wide: no enqueue helpers, no add-request tables, no queue
	// WRITES of any shape.
	for _, banned := range []string{
		"EnqueueRepo", "collection_add_requests", "AddReposToGroup(",
		"INSERT INTO aveloxis_ops.collection_queue",
		"UPDATE aveloxis_ops.collection_queue",
		"DELETE FROM aveloxis_ops.collection_queue",
	} {
		if strings.Contains(joined, banned) {
			t.Errorf("collections_store.go must not reference %q — copy is a pure user_repos INSERT…SELECT (v0.27.20: approval gates NEW collection; collection repos are already tracked)", banned)
		}
	}
	// Copy-path-scoped: the copy function itself may not touch the
	// queue AT ALL, reads included.
	start := strings.Index(joined, "func (s *PostgresStore) CopyCollectionToGroup")
	if start < 0 {
		t.Fatal("CopyCollectionToGroup not found in collections_store.go")
	}
	body := joined[start:]
	if end := strings.Index(body[1:], "\nfunc "); end >= 0 {
		body = body[:end+1]
	}
	if strings.Contains(body, "collection_queue") {
		t.Error("CopyCollectionToGroup must not reference collection_queue in any form")
	}
	if !strings.Contains(body, "ON CONFLICT DO NOTHING") {
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
		// Child rows FIRST (2026-08-02): user_repo_stars.repo_id and
		// collection_queue.repo_id have no ON DELETE action, so a
		// leftover child row silently kills the repos delete (errors
		// here are discarded by design) and the stranded '_avcoll'
		// repos collide with mkRepo on every subsequent run — the
		// chicken-and-egg residue loop this pre-clean exists to break.
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repo_stars WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avcoll')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avcoll')`)
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

	// List: our collection shows 2 groups / 3 distinct repos, and is
	// unstarred for a caller who never starred it.
	sums, err := store.ListCollections(ctx, plainID)
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
			if c.Starred {
				t.Error("collection must not be starred before StarCollection")
			}
		}
	}
	if !found {
		t.Fatalf("collection not listed: %+v", sums)
	}

	// ─── v0.27.70: per-user stars ───────────────────────────────
	// A second collection at a LATER position; starring it as plainID
	// must sort it ahead of _avcoll_main for plainID ONLY.
	coll2, err := store.CreateCollection(ctx, "_avcoll_second", "starred later", 9, adminID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StarCollection(ctx, plainID, coll2); err != nil {
		t.Fatalf("StarCollection: %v", err)
	}
	// Idempotent re-star.
	if err := store.StarCollection(ctx, plainID, coll2); err != nil {
		t.Fatalf("second StarCollection: %v", err)
	}
	orderOf := func(userID int) (secondIdx, mainIdx int, secondStarred bool) {
		secondIdx, mainIdx = -1, -1
		list, lerr := store.ListCollections(ctx, userID)
		if lerr != nil {
			t.Fatal(lerr)
		}
		for i, c := range list {
			switch c.Name {
			case "_avcoll_second":
				secondIdx, secondStarred = i, c.Starred
			case "_avcoll_main":
				mainIdx = i
			}
		}
		return secondIdx, mainIdx, secondStarred
	}
	secondIdx, mainIdx, secondStarred := orderOf(plainID)
	if !secondStarred {
		t.Error("starred collection must report starred=true for the starrer")
	}
	if secondIdx == -1 || mainIdx == -1 || secondIdx > mainIdx {
		t.Errorf("starred collection must sort first for the starrer (second at %d, main at %d)", secondIdx, mainIdx)
	}
	// The star is PER-USER: the admin still sees position order.
	secondIdx, mainIdx, secondStarred = orderOf(adminID)
	if secondStarred {
		t.Error("another user's star must not leak into the admin's listing")
	}
	if secondIdx == -1 || mainIdx == -1 || secondIdx < mainIdx {
		t.Errorf("non-starrer must keep position order (second at %d, main at %d)", secondIdx, mainIdx)
	}
	// Unstar reverts the starrer's ordering.
	if err := store.UnstarCollection(ctx, plainID, coll2); err != nil {
		t.Fatalf("UnstarCollection: %v", err)
	}
	secondIdx, mainIdx, secondStarred = orderOf(plainID)
	if secondStarred || secondIdx < mainIdx {
		t.Errorf("after unstar, position order must return (second at %d starred=%v, main at %d)", secondIdx, secondStarred, mainIdx)
	}
	// Starring a nonexistent collection surfaces the typed sentinel.
	if err := store.StarCollection(ctx, plainID, coll2+999999); !errors.Is(err, ErrCollectionNotFound) {
		t.Errorf("starring a nonexistent collection: got %v, want ErrCollectionNotFound", err)
	}
	if err := store.DeleteCollection(ctx, coll2); err != nil {
		t.Fatal(err)
	}

	// Detail repos: deduped, 3 rows (default name-asc sort).
	repos, total, err := store.GetCollectionRepos(ctx, collID, plainID, 1, 50, "", "")
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

	// ─── v0.27.74: counts + sort + starred on the detail rows ───
	// (AFTER the zero-queue-rows assertion above — this block seeds
	// queue rows deliberately, which would false-fail that check.)
	for _, seed := range []struct {
		rid                  int64
		issues, prs, commits int64
	}{{r1, 10, 5, 100}, {r2, 30, 2, 50}, {r3, 20, 9, 75}} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_issues, last_prs, last_commits, last_collected)
			VALUES ($1, 'queued', $2, $3, $4, NOW())
			ON CONFLICT (repo_id) DO UPDATE SET last_issues = $2, last_prs = $3, last_commits = $4, last_collected = NOW()`,
			seed.rid, seed.issues, seed.prs, seed.commits); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1,$2,$3)`, r1, r2, r3)
	})
	if err := store.StarRepo(ctx, plainID, r3); err != nil {
		t.Fatal(err)
	}
	sorted, _, err := store.GetCollectionRepos(ctx, collID, plainID, 1, 50, "issues", "desc")
	if err != nil {
		t.Fatal(err)
	}
	if len(sorted) != 3 || sorted[0].RepoID != r2 || sorted[0].Issues != 30 {
		t.Errorf("issues-desc sort: first row = %+v, want repo %d with 30 issues", sorted[0], r2)
	}
	if sorted[0].LastCollected == nil {
		t.Error("last_collected must surface from the queue row")
	}
	for _, row := range sorted {
		if row.RepoID == r3 && !row.Starred {
			t.Error("r3 must carry the caller's starred flag")
		}
		if row.RepoID == r1 && row.Starred {
			t.Error("unstarred repo must not be starred")
		}
	}
	// An unknown sort key falls back safely (no SQL error, default order).
	if _, _, err := store.GetCollectionRepos(ctx, collID, plainID, 1, 50, "evil; DROP TABLE x", "desc"); err != nil {
		t.Errorf("unknown sort key must fall back to the default, got error: %v", err)
	}

	// Remove a group: repo count shrinks to g2's set (r2, r3).
	if err := store.RemoveGroupFromCollection(ctx, collID, g1); err != nil {
		t.Fatal(err)
	}
	_, total, err = store.GetCollectionRepos(ctx, collID, plainID, 1, 50, "", "")
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
