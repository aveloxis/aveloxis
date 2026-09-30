// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestApprovedAddRequestsToRetry — O17 (v0.29.71): an approved add request
// whose processing pass stopped early (a failed stamp or read, a process
// stopped mid-pass) kept its unprocessed items forever — the admin page
// lists pending requests only, and re-approving an approved one does
// nothing. serve retries them; this is the query that finds them: approved
// 'repos' requests with an unprocessed item, decided before the cutoff (so a
// pass still running in web or api is not raced).
func TestApprovedAddRequestsToRetry(t *testing.T) {
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

	const login = "_avaddretry_probe"
	const urlPrefix = "https://github.com/_avaddretry-owner/_avaddretry-"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
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
	gid, err := store.CreateUserGroup(ctx, uid, "add retry probe")
	if err != nil {
		t.Fatal(err)
	}
	mk := func(status, suffix string, decidedAgo time.Duration) int64 {
		t.Helper()
		id, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{urlPrefix + suffix}, status)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_add_requests
			SET decided_at = NOW() - $2::interval, created_at = NOW() - $2::interval WHERE request_id = $1`, id, decidedAgo.String()); err != nil {
			t.Fatal(err)
		}
		return id
	}
	stuck := mk("approved", "stuck", 2*time.Hour)
	fresh := mk("approved", "fresh", time.Minute)
	pending := mk("pending", "pending", 2*time.Hour)
	done := mk("approved", "done", 2*time.Hour)
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_add_request_items SET repo_id = -1 WHERE request_id = $1`, done); err != nil {
		t.Fatal(err)
	}

	ids, err := store.ApprovedAddRequestsToRetry(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(ids, stuck) {
		t.Errorf("the stuck approved request %d must be listed: %v", stuck, ids)
	}
	for name, id := range map[string]int64{"fresh (a pass may still run)": fresh, "pending": pending, "fully processed": done} {
		if slices.Contains(ids, id) {
			t.Errorf("%s request %d must not be listed: %v", name, id, ids)
		}
	}

	// Review round 1 F1: a stuck request of a group an administrator has
	// since rejected is neither listed nor processed — the pass itself
	// refuses it (SR-18), so no caller can resume collection for it.
	gid2, err := store.CreateUserGroup(ctx, uid, "add retry rejected probe")
	if err != nil {
		t.Fatal(err)
	}
	rejReq, err := store.createAddRequest(ctx, uid, gid2, "repos", "", []string{urlPrefix + "rejected"}, "approved")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_add_requests SET decided_at = NOW() - interval '2 hours' WHERE request_id = $1`, rejReq); err != nil {
		t.Fatal(err)
	}
	if err := store.RejectGroup(ctx, gid2, uid); err != nil {
		t.Fatal(err)
	}
	if ids, err := store.ApprovedAddRequestsToRetry(ctx, time.Hour); err != nil || slices.Contains(ids, rejReq) {
		t.Errorf("a rejected group's request must not be listed for retry: %v, %v", ids, err)
	}
	if _, _, err := store.ProcessApprovedAddRequest(ctx, rejReq); !errors.Is(err, ErrGroupRejected) {
		t.Errorf("processing a rejected group's request = %v, want ErrGroupRejected", err)
	}
	var tracked int
	_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repos WHERE repo_git = $1`, urlPrefix+"rejected").Scan(&tracked)
	if tracked != 0 {
		t.Error("a rejected group's URL was added")
	}

	// The retry is the ordinary approved pass: it processes the stuck item.
	processed, _, err := store.ProcessApprovedAddRequest(ctx, stuck)
	if err != nil || processed != 1 {
		t.Fatalf("retrying the stuck request: processed %d, err %v", processed, err)
	}
	if ids, err := store.ApprovedAddRequestsToRetry(ctx, time.Hour); err != nil || slices.Contains(ids, stuck) {
		t.Errorf("a processed request must leave the list: %v, %v", ids, err)
	}
}

// TestRejectedGroupTakesNoApprovalOrMoreItems — v0.29.71 review round 2
// F2/F3: approving a PENDING request of a rejected group used to flip it to
// approved and mail the requester "approved" while the pass then refused
// it, leaving it approved and unprocessed for good; the decision refuses
// instead (ErrGroupRejected, the request stays pending). And a group
// rejected while a pass runs stops taking items at the next one.
func TestRejectedGroupTakesNoApprovalOrMoreItems(t *testing.T) {
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
	const login = "_avrejmid_probe"
	const urlPrefix = "https://github.com/_avrejmid-owner/_avrejmid-"
	const trigger = "_avtest_rejmid"
	clean := func() {
		_, _ = store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_data.repos`)
		_, _ = store.pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_data.`+trigger+`()`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
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

	// F2: a pending request of a rejected group is not approved.
	gid, err := store.CreateUserGroup(ctx, uid, "rejected before decision")
	if err != nil {
		t.Fatal(err)
	}
	pend, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{urlPrefix + "pending"}, "pending")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RejectGroup(ctx, gid, uid); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := store.DecideAddRequest(ctx, pend, uid, true, ""); !errors.Is(err, ErrGroupRejected) || changed {
		t.Errorf("approving a rejected group's pending request = changed %v, %v; want ErrGroupRejected", changed, err)
	}
	var status string
	_ = store.pool.QueryRow(ctx, `SELECT status FROM aveloxis_ops.collection_add_requests WHERE request_id = $1`, pend).Scan(&status)
	if status != "pending" {
		t.Errorf("the request is %q, want pending", status)
	}

	// F3: the group is rejected while the first item is added (a trigger on
	// its repos INSERT); the second item is not taken.
	gid2, err := store.CreateUserGroup(ctx, uid, "rejected mid-pass")
	if err != nil {
		t.Fatal(err)
	}
	req2, err := store.createAddRequest(ctx, uid, gid2, "repos", "", []string{urlPrefix + "first", urlPrefix + "second"}, "approved")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_data.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.repo_git LIKE '%_avrejmid-first' THEN
				UPDATE aveloxis_ops.user_groups SET status = 'rejected' WHERE group_id = `+fmt.Sprint(gid2)+`;
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION aveloxis_data.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ProcessApprovedAddRequest(ctx, req2); !errors.Is(err, ErrGroupRejected) {
		t.Errorf("a pass whose group was rejected mid-way = %v, want ErrGroupRejected", err)
	}
	var secondTracked, secondStamped int
	_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repos WHERE repo_git = $1`, urlPrefix+"second").Scan(&secondTracked)
	_ = store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id IS NOT NULL AND repo_url = $2`, req2, urlPrefix+"second").Scan(&secondStamped)
	if secondTracked != 0 || secondStamped != 0 {
		t.Errorf("the second item was taken after the rejection (tracked %d, stamped %d)", secondTracked, secondStamped)
	}
}

// TestDecideChecksTheGroupInsideTheFlip pins review round 3 F2: the
// rejected-group check runs inside the flip's transaction with the group
// row share-locked, before the UPDATE — a concurrent RejectGroup then waits
// for the flip instead of slipping between a separate check and it. (The
// race itself is not driven; TestRejectedGroupTakesNoApprovalOrMoreItems
// proves the refusal.)
func TestDecideChecksTheGroupInsideTheFlip(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/add_requests.go"))
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) DecideAddRequest(")
	begin := strings.Index(body, "s.pool.Begin(ctx)")
	lock := strings.Index(body, "WHERE group_id = $1 FOR SHARE")
	flip := strings.Index(body, "UPDATE aveloxis_ops.collection_add_requests")
	if begin < 0 || lock < 0 || flip < 0 || begin >= lock || lock >= flip {
		t.Errorf("DecideAddRequest must share-lock and check the group inside its transaction, before the flip (begin %d, lock %d, flip %d)", begin, lock, flip)
	}
	if !strings.Contains(body[begin:flip], "tx.QueryRow(") {
		t.Error("the group check must read through the flip's transaction")
	}
}
