// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// shared_with_me_test.go — TDD suite for the v0.27.82 "Shared with
// Me" link-sharing flow. Operator decision (2026-08-04): a signed-in
// user opening a link to a repo outside their groups gets the repo
// auto-added to an implicit "Shared with Me" group (the third leg of
// the Starred / Comparisons pattern) instead of a dead-end 403.
//
// The safety contract is the v0.27.20 approval principle: this flow
// LINKS existing repo rows only. It must never enqueue collection,
// never register org tracking, never create add-requests — a shared
// link is visibility of already-collected data, and approval gates
// collection, never visibility.

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSharedWithMeSourceContract(t *testing.T) {
	src := readSourceFile(t, "shared_with_me.go")
	for _, needle := range []string{
		`SharedWithMeGroupName = "Shared with Me"`,
		"func (s *PostgresStore) EnsureRepoSharedWithUser(",
		"ErrSharedRepoNotFound",
		// creation must route through the shared implicit-group helper
		// so the v0.19.0 status rules apply uniformly
		"findOrCreateNamedGroup(",
		// idempotency is load-bearing: a repo page fires several data
		// calls in parallel and every one runs this
		"ON CONFLICT DO NOTHING",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("shared_with_me.go must contain %q", needle)
		}
	}
}

// TestSharedWithMeNeverTouchesCollectionMachinery is the negative
// tripwire for the v0.27.20 approval principle (mirrors the compare
// auto-add tripwire): NOTHING in this file may reference the
// collection-enqueue or org-tracking machinery. A shared link grants
// visibility of collected data — it must be structurally incapable of
// triggering collection.
func TestSharedWithMeNeverTouchesCollectionMachinery(t *testing.T) {
	src := readSourceFile(t, "shared_with_me.go")
	// Strip // comments so prose can't false-match (v0.21.5 lesson).
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	stripped := b.String()
	for _, forbidden := range []string{
		"EnqueueRepo",
		"collection_queue",
		"user_org_requests",
		"collection_add_requests",
		"AddOrgToGroup",
		"AddReposToGroup",
	} {
		if strings.Contains(stripped, forbidden) {
			t.Errorf("shared_with_me.go must not reference %q — the Shared with Me flow links existing repos only, never collection machinery", forbidden)
		}
	}
}

// TestEnsureRepoSharedWithUserEndToEnd is the excruciating one: every
// behavior AND every must-never-happen, against the real schema.
func TestEnsureRepoSharedWithUserEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	suffix := time.Now().UnixNano()

	seedUser := func(tag string) int {
		t.Helper()
		var id int
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
			VALUES ($1, 'github', '', FALSE) RETURNING user_id`,
			fmt.Sprintf("_avshare_%s_%d", tag, suffix)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, id)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, id)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, id)
		})
		return id
	}
	seedRepo := func(name string, withQueue bool) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, $3, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avshare%d/%s", suffix, name),
			fmt.Sprintf("_avshare%d", suffix), name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if withQueue {
			if _, err := store.pool.Exec(ctx, `
				INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
				VALUES ($1, 'queued', 100, NOW())`, id); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return id
	}

	userA := seedUser("a")
	userB := seedUser("b")
	userC := seedUser("c")
	repoQ := seedRepo("queued", true)      // tracked repo with a live queue row
	repoNoQ := seedRepo("unqueued", false) // repos row only — no queue row

	queueRow := func(repoID int64) string {
		t.Helper()
		var row string
		if err := store.pool.QueryRow(ctx, `
			SELECT COALESCE(row_to_json(q)::text, '') FROM aveloxis_ops.collection_queue q
			WHERE repo_id = $1`, repoID).Scan(&row); err != nil {
			// no row
			return ""
		}
		return row
	}
	countScoped := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, q, args...).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	linkCount := func(userID int, repoID int64) int {
		return countScoped(`
			SELECT COUNT(*) FROM aveloxis_ops.user_repos ur
			JOIN aveloxis_ops.user_groups g USING (group_id)
			WHERE g.user_id = $1 AND g.name = $2 AND ur.repo_id = $3`,
			userID, SharedWithMeGroupName, repoID)
	}

	queueBefore := queueRow(repoQ)
	if queueBefore == "" {
		t.Fatal("fixture: repoQ must have a queue row")
	}

	// (1) First share: link created, group born 'approved'.
	added, err := store.EnsureRepoSharedWithUser(ctx, userA, repoQ)
	if err != nil {
		t.Fatalf("EnsureRepoSharedWithUser: %v", err)
	}
	if !added {
		t.Error("first share must report added=true (drives the one-time GUI notice)")
	}
	if n := linkCount(userA, repoQ); n != 1 {
		t.Fatalf("expected exactly 1 Shared with Me link, got %d", n)
	}
	var status string
	if err := store.pool.QueryRow(ctx, `
		SELECT status FROM aveloxis_ops.user_groups WHERE user_id = $1 AND name = $2`,
		userA, SharedWithMeGroupName).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "approved" {
		t.Errorf("Shared with Me group must be born 'approved' (implicit-group rules), got %q", status)
	}

	// (2) Scope now includes the repo — what the auth cache re-reads
	// after invalidation.
	scope, err := store.GetUserRepoScope(ctx, userA)
	if err != nil {
		t.Fatal(err)
	}
	inScope := false
	for _, id := range scope {
		if id == repoQ {
			inScope = true
		}
	}
	if !inScope {
		t.Error("GetUserRepoScope must include the shared repo after the link")
	}

	// (3) Idempotent re-share: no dup, added=false.
	added, err = store.EnsureRepoSharedWithUser(ctx, userA, repoQ)
	if err != nil {
		t.Fatal(err)
	}
	if added {
		t.Error("re-share must report added=false (no repeated GUI notice)")
	}
	if n := linkCount(userA, repoQ); n != 1 {
		t.Errorf("re-share must not duplicate the link, got %d rows", n)
	}
	if n := countScoped(`SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1 AND name = $2`,
		userA, SharedWithMeGroupName); n != 1 {
		t.Errorf("re-share must not duplicate the group, got %d", n)
	}

	// (4) SAFETY: the queue row is byte-identical; sharing an unqueued
	// repo creates NO queue row; no org tracking; no add-requests.
	if after := queueRow(repoQ); after != queueBefore {
		t.Errorf("sharing must NEVER touch collection_queue.\nbefore: %s\nafter:  %s", queueBefore, after)
	}
	added, err = store.EnsureRepoSharedWithUser(ctx, userA, repoNoQ)
	if err != nil || !added {
		t.Fatalf("sharing an unqueued repo must link it (added=%v err=%v)", added, err)
	}
	if row := queueRow(repoNoQ); row != "" {
		t.Errorf("sharing must NEVER create a collection_queue row, got %s", row)
	}
	if n := countScoped(`
		SELECT COUNT(*) FROM aveloxis_ops.user_org_requests
		WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, userA); n != 0 {
		t.Errorf("sharing must NEVER register org tracking, found %d rows", n)
	}
	if n := countScoped(`
		SELECT COUNT(*) FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, userA); n != 0 {
		t.Errorf("sharing must NEVER create add-requests, found %d rows", n)
	}

	// (5) Nonexistent repo: sentinel error, and CRITICALLY no empty
	// "Shared with Me" group left behind (existence is checked before
	// group creation).
	var maxRepoID int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(MAX(repo_id), 0) FROM aveloxis_data.repos`).Scan(&maxRepoID); err != nil {
		t.Fatal(err)
	}
	added, err = store.EnsureRepoSharedWithUser(ctx, userB, maxRepoID+1000000)
	if !errors.Is(err, ErrSharedRepoNotFound) {
		t.Errorf("nonexistent repo must return ErrSharedRepoNotFound, got added=%v err=%v", added, err)
	}
	if added {
		t.Error("nonexistent repo must not report added")
	}
	if n := countScoped(`SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1`, userB); n != 0 {
		t.Errorf("a failed share must not leave an empty group behind, found %d groups", n)
	}

	// (6) Pre-existing group with the reserved name is reused, never
	// duplicated (unique (user_id, name) backs this; assert anyway).
	if _, err := store.FindOrCreateUserGroupByName(ctx, userC, SharedWithMeGroupName); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureRepoSharedWithUser(ctx, userC, repoQ); err != nil {
		t.Fatal(err)
	}
	if n := countScoped(`SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1 AND name = $2`,
		userC, SharedWithMeGroupName); n != 1 {
		t.Errorf("pre-existing Shared with Me group must be reused, got %d groups", n)
	}

	// (7) Concurrency: a repo page fires several data calls in
	// parallel, each racing this call. All must succeed; exactly one
	// link and one group must exist afterward.
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, userC)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = store.EnsureRepoSharedWithUser(ctx, userC, repoQ)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("concurrent share %d failed: %v", i, err)
		}
	}
	if n := linkCount(userC, repoQ); n != 1 {
		t.Errorf("concurrent shares must converge on exactly 1 link, got %d", n)
	}
	if n := countScoped(`SELECT COUNT(*) FROM aveloxis_ops.user_groups WHERE user_id = $1 AND name = $2`,
		userC, SharedWithMeGroupName); n != 1 {
		t.Errorf("concurrent shares must converge on exactly 1 group, got %d", n)
	}
}
