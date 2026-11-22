// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"testing"
)

// Statements that one call site sends for each member of a list, of which
// the tests sent only the first member's (0.29.215: the site was "reached",
// and the other members' statements had been judged from their text alone).
// Each member is sent here, and what it did is checked.

// The assignment backfill fills the contributor on ALL THREE assignment
// tables: where an identity carries the row's platform id for the
// repository's platform, and nowhere else.
func TestTheAssignmentBackfillFillsEveryAssignmentTable(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "asg-all", "aveloxis_data.issue_assignees", "aveloxis_data.pull_request_assignees", "aveloxis_data.pull_request_reviewers", "aveloxis_data.issues", "aveloxis_data.pull_requests")
	who := seedR6Contributor(t, store, pathsTag("asg-all"), "", nil)
	other := seedR6Contributor(t, store, pathsTag("asg-all-o"), "", nil)
	uid := pathsUserID()
	// The identity is GitHub's (platform 1, the repository's). The same
	// number under GitLab's platform names somebody else, and must not be
	// taken for it.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 1, $2, 'asg-all'), ($3::uuid, 2, $4, 'asg-all-gl')`, who, uid, other, uid+1)
	issue := pathsIssue(t, store, repoID, 31)
	var pr int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number) VALUES ($1, $2, 32) RETURNING pull_request_id`, repoID, repoID*1000+32).Scan(&pr); err != nil {
		t.Fatal(err)
	}
	// Per table: one row to fill, one whose id only the other platform's
	// identity carries, one with no platform id, one already linked to
	// somebody else (kept).
	seed := func(table, parentCol, platCol string, parent int64) {
		t.Helper()
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.`+table+` (`+parentCol+`, repo_id, `+platCol+`, cntrb_id) VALUES ($1, $2, $3, NULL), ($1, $2, $4, NULL), ($1, $2, 0, NULL), ($1, $2, $5, $6::uuid)`,
			parent, repoID, uid, uid+1, uid+2, other)
	}
	seed("issue_assignees", "issue_id", "platform_assignee_id", issue)
	seed("pull_request_assignees", "pull_request_id", "platform_assignee_id", pr)
	seed("pull_request_reviewers", "pull_request_id", "platform_reviewer_id", pr)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributor_identities (cntrb_id, platform_id, platform_user_id, login) VALUES ($1::uuid, 1, $2, 'asg-all-2')`, who, uid+2)

	// A window of one row: every row is walked in a window of its own.
	n, err := store.BackfillAssignmentIdentities(ctx, 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if n < 3 {
		t.Errorf("%d rows filled, want at least this test's three (one per table)", n)
	}
	for _, c := range []struct{ table, platCol string }{
		{"issue_assignees", "platform_assignee_id"}, {"pull_request_assignees", "platform_assignee_id"}, {"pull_request_reviewers", "platform_reviewer_id"}} {
		got := pathsText(t, store, `SELECT string_agg(`+c.platCol+`::text || '=' || COALESCE(cntrb_id::text, '-'), ',' ORDER BY `+c.platCol+`) FROM aveloxis_data.`+c.table+` WHERE repo_id = $1`, repoID)
		want := "0=-," + itoa64(uid) + "=" + who + "," + itoa64(uid+1) + "=-," + itoa64(uid+2) + "=" + other
		if got != want {
			t.Errorf("%s after the backfill:\n got %s\nwant %s\n(the identity's row filled; the other platform's id, the row with no id and the row already linked left as they were)", c.table, got, want)
		}
	}
	// A second run finds nothing of this test's to fill.
	if _, err := store.BackfillAssignmentIdentities(ctx, 1000, 0, false); err != nil {
		t.Fatal(err)
	}
	if left := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.pull_request_reviewers WHERE repo_id = $1 AND cntrb_id = $2::uuid`, repoID, who); left != 1 {
		t.Errorf("%d reviewer rows carry the contributor after a second run, want 1", left)
	}
}

// The v0.27.15 migration step copies a message's data_source onto the ref
// rows of EACH bridge table that lack one, and leaves a ref that has one.
func TestTheMessageRefSourceBackfillFillsEveryBridgeTable(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "refsrc", "aveloxis_data.issue_message_ref", "aveloxis_data.pull_request_message_ref", "aveloxis_data.messages", "aveloxis_data.issues", "aveloxis_data.pull_requests")
	issue := pathsIssue(t, store, repoID, 41)
	var pr int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number) VALUES ($1, $2, 42) RETURNING pull_request_id`, repoID, repoID*1000+42).Scan(&pr); err != nil {
		t.Fatal(err)
	}
	msg := func(n int64, source string) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_text, data_source) VALUES ($1, $2, 1, 'm', $3) RETURNING msg_id`, repoID, repoID*1000+n, source).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	fromAPI, alreadySaid, unsourced := msg(1, "github api"), msg(2, "github api"), msg(3, "")
	for _, c := range []struct {
		table, parentCol string
		parent           int64
	}{{"issue_message_ref", "issue_id", issue}, {"pull_request_message_ref", "pull_request_id", pr}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.`+c.table+` (`+c.parentCol+`, repo_id, msg_id, data_source) VALUES ($1, $2, $3, ''), ($1, $2, $4, 'mailing list'), ($1, $2, $5, '')`,
			c.parent, repoID, fromAPI, alreadySaid, unsourced)
	}
	var errs []error
	ensureMsgRefMetadata(ctx, store, slog.Default(), &errs)
	if len(errs) != 0 {
		t.Fatalf("the migration step: %v", errs)
	}
	for _, table := range []string{"issue_message_ref", "pull_request_message_ref"} {
		got := pathsText(t, store, `SELECT string_agg(COALESCE(NULLIF(r.data_source, ''), '-'), ',' ORDER BY m.platform_msg_id) FROM aveloxis_data.`+table+` r JOIN aveloxis_data.messages m USING (msg_id) WHERE r.repo_id = $1`, repoID)
		if want := "github api,mailing list,-"; got != want {
			t.Errorf("%s after the step: %q, want %q (the message's source copied where the ref had none; a ref's own source kept; nothing invented for a message with none)", table, got, want)
		}
	}
}
