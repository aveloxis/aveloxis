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

// pendingGroupFixture creates a user (with the given address, or none) and a
// pending group of theirs, and removes both when the test ends.
func pendingGroupFixture(ctx context.Context, t *testing.T, store *PostgresStore, login, email, group string) int64 {
	t.Helper()
	clean := func() {
		// A concurrent migrate converts a pending group into an add-request
		// (migrateLegacyPendingGroups), so those go too.
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
	// No address is a NULL email, as for an OAuth account that gave none.
	var addr any
	if email != "" {
		addr = email
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.users SET email = $2 WHERE user_id = $1`, uid, addr); err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, group)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.user_groups SET status = 'pending' WHERE group_id = $1`, gid); err != nil {
		t.Fatal(err)
	}
	return gid
}

// TestApproveGroupReportsItsRequesterOnce (AVELOXIS_TEST_DB): ApproveGroup
// returns the requester the approval email goes to, read in the statement
// that approves, and says whether THIS call approved the group — so a second
// click mails nobody, and there is no separate lookup whose error a caller
// can drop (round-10 review: both handlers discarded it, and spelled the
// query two ways).
func TestApproveGroupReportsItsRequesterOnce(t *testing.T) {
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

	gid := pendingGroupFixture(ctx, t, store, "_avapprove_requester", "Req@Example.com", "probe group")
	got, approved, err := store.ApproveGroup(ctx, gid, 1)
	want := GroupApproval{RequesterEmail: "Req@Example.com", RequesterLogin: "_avapprove_requester", GroupName: "probe group"}
	if err != nil || !approved || got != want {
		t.Fatalf("ApproveGroup(pending) = %+v, %v, %v; want %+v, true, nil", got, approved, err, want)
	}
	var status string
	if err := store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.user_groups WHERE group_id = $1`, gid).Scan(&status); err != nil || status != "approved" {
		t.Fatalf("group status = %q (%v), want approved", status, err)
	}
	if got, approved, err := store.ApproveGroup(ctx, gid, 1); err != nil || approved || got != (GroupApproval{}) {
		t.Errorf("ApproveGroup(already approved) = %+v, %v, %v; want zero, false, nil", got, approved, err)
	}

	noAddr := pendingGroupFixture(ctx, t, store, "_avapprove_requester_noaddr", "", "probe group")
	if got, approved, err := store.ApproveGroup(ctx, noAddr, 1); err != nil || !approved || got.RequesterEmail != "" || got.RequesterLogin != "_avapprove_requester_noaddr" {
		t.Errorf("ApproveGroup(requester without an address) = %+v, %v, %v; want an empty RequesterEmail, true, nil", got, approved, err)
	}

	if got, approved, err := store.ApproveGroup(ctx, -1, 1); err != nil || approved || got != (GroupApproval{}) {
		t.Errorf("ApproveGroup(unknown group) = %+v, %v, %v; want zero, false, nil", got, approved, err)
	}

	// The approval and its enqueue are one transaction (round-11 review: no
	// test ran either). The group's repos are queued...
	queuedGroup := pendingGroupFixture(ctx, t, store, "_avapprove_enqueue", "q@example.com", "probe group")
	repoID := seedRepoForDeps(t, store, ctx, "_avapprove_enqueue_owner", "_avapprove_enqueue_repo")
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, queuedGroup, repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, queuedGroup)
	})
	if _, err := store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	if _, approved, err := store.ApproveGroup(ctx, queuedGroup, 1); err != nil || !approved {
		t.Fatalf("ApproveGroup(group with a repo) = %v, %v", approved, err)
	}
	var queued int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&queued); err != nil || queued != 1 {
		t.Errorf("approving a group queued %d rows for its repo (%v), want 1", queued, err)
	}

	// ...and a failure at COMMIT (the queue row's deferred repo FK, for a
	// user_repos row whose repo does not exist) leaves the group pending and
	// reports nothing approved — not an approval without its repos.
	failingGroup := pendingGroupFixture(ctx, t, store, "_avapprove_commitfail", "cf@example.com", "probe group")
	var missing int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(MAX(repo_id), 0) + 1000000 FROM aveloxis_data.repos`).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, failingGroup, missing); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id = $1`, failingGroup)
	})
	got, approved, err = store.ApproveGroup(ctx, failingGroup, 1)
	var failedStatus string
	_ = store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.user_groups WHERE group_id = $1`, failingGroup).Scan(&failedStatus)
	if err == nil || approved || got != (GroupApproval{}) || failedStatus != "pending" {
		t.Errorf("ApproveGroup failing at COMMIT = %+v, %v, %v with the group %q; want zero, false, an error, and the group still pending", got, approved, err, failedStatus)
	}

	closed, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if _, approved, err := closed.ApproveGroup(ctx, gid, 1); err == nil || approved {
		t.Errorf("ApproveGroup on a failing store = %v, %v; want an error and not approved", approved, err)
	}
}
