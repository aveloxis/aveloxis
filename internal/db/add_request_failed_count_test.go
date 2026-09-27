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

// TestProcessApprovedAddRequestReportsFailedItems pins worklist follow-up 9
// (the PR #207 review): the admin approval pass returns how many items it
// stamped processed-with-error (-1), so the "approved add-request processed"
// line can report them. v0.29.50 counted them on the auto-approve path only
// (AddOutcome.Failed); the admin path dropped the count. A permanent failure
// (a check violation, SQLSTATE 23514) on one of two items is injected by a
// trigger on the repos INSERT.
func TestProcessApprovedAddRequestReportsFailedItems(t *testing.T) {
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

	const login = "_avfailedcount_probe"
	const urlPrefix = "https://github.com/_avfailedcount-owner/_avfailedcount-"
	const trigger = "_avtest_failed_count_repos"
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
	gid, err := store.CreateUserGroup(ctx, uid, "failed count probe")
	if err != nil {
		t.Fatal(err)
	}
	reqID, err := store.createAddRequest(ctx, uid, gid, "repos", "", []string{urlPrefix + "works", urlPrefix + "fails"}, "approved")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE FUNCTION aveloxis_data.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.repo_git LIKE '%fails' THEN
				RAISE EXCEPTION 'injected permanent failure' USING ERRCODE = '23514';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION aveloxis_data.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	processed, failed, err := store.ProcessApprovedAddRequest(ctx, reqID)
	if err != nil {
		t.Fatalf("ProcessApprovedAddRequest: %v", err)
	}
	if processed != 1 || failed != 1 {
		t.Errorf("ProcessApprovedAddRequest = processed %d, failed %d; want 1 and 1 (one item stamped -1)", processed, failed)
	}
	var stamped int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id = -1`, reqID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped != 1 {
		t.Errorf("%d items stamped -1, want 1", stamped)
	}
}
