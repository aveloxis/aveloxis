// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"
)

// TestMessageHealBatchNamesEveryRepositoryTheMessageTouches (Copilot's
// fourth review of PR #246, 0.29.127): the batch resolves ONE parent per
// bridge table as the refetch coordinate, but the healer's cleanup deletes
// EVERY conversation link of the message, and the API's cached answers read
// those links by the message's own repository (the retention series) and by
// the link's (the contributor metrics). A message whose links reach
// repositories no parent names — duplicate repositories sharing a message
// after a rename, a message stored under a repository other than its
// issue's — therefore left those repositories' cached answers stale. Each
// item carries every repository its rows name: the message's, and each
// conversation link's (the bridge row's and its issue's or pull request's).
func TestMessageHealBatchNamesEveryRepositoryTheMessageTouches(t *testing.T) {
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

	const base = int64(944_146_100)
	owner, issueRepoA, issueRepoB, prRepo := base, base+1, base+2, base+3
	platformMsgID := base + 4
	repos := []int64{owner, issueRepoA, issueRepoB, prRepo}
	clear := func(ctx context.Context) {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.message_heal_worklist WHERE msg_id IN
			(SELECT msg_id FROM aveloxis_data.messages WHERE platform_msg_id = $1 AND platform_id = 1)`, platformMsgID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id = ANY($1)`, repos)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.pull_request_message_ref WHERE repo_id = ANY($1)`, repos)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE platform_msg_id = $1 AND platform_id = 1`, platformMsgID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE repo_id = ANY($1)`, repos)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = ANY($1)`, repos)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1)`, repos)
	}
	clear(ctx) // residue of a failed run
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		clear(cctx)
	})
	for i, id := range repos {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, 'https://github.com/_avheal/r' || $2, '_avheal', 'r' || $2, 1)`, id, strconv.Itoa(i))
	}
	// The message is stored under owner — a repository no link names.
	var msgID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
		VALUES ($1, $2, 1, $3, 'heal repos fixture') RETURNING msg_id`, owner, platformMsgID, MsgKindReviewComment).Scan(&msgID); err != nil {
		t.Fatal(err)
	}
	link := func(repoID int64, issue bool) {
		t.Helper()
		if issue {
			var issueID int64
			if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number)
				VALUES ($1, $2, 1) RETURNING issue_id`, repoID, repoID).Scan(&issueID); err != nil {
				t.Fatal(err)
			}
			mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.issue_message_ref (issue_id, repo_id, msg_id) VALUES ($1, $2, $3)`, issueID, repoID, msgID)
			return
		}
		var prID int64
		if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number)
			VALUES ($1, $2, 1) RETURNING pull_request_id`, repoID, repoID).Scan(&prID); err != nil {
			t.Fatal(err)
		}
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.pull_request_message_ref (pull_request_id, repo_id, msg_id) VALUES ($1, $2, $3)`, prID, repoID, msgID)
	}
	link(issueRepoA, true)
	link(issueRepoB, true) // the second issue link: the refetch resolves one, the cleanup deletes both
	link(prRepo, false)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.message_heal_worklist (msg_id)
		VALUES ($1) ON CONFLICT (msg_id) DO UPDATE SET healed_at = NULL`, msgID)

	items, err := store.GetMessageHealBatch(ctx, 0, 100000)
	if err != nil {
		t.Fatal(err)
	}
	var item *MessageHealItem
	for i := range items {
		if items[i].MsgID == msgID {
			item = &items[i]
		}
	}
	if item == nil {
		t.Fatal("the pending worklist row must appear in GetMessageHealBatch")
	}
	if item.IssueRepoID != issueRepoA && item.IssueRepoID != issueRepoB {
		t.Errorf("IssueRepoID = %d, want one of the two linked issues' repositories (%d, %d)", item.IssueRepoID, issueRepoA, issueRepoB)
	}
	if item.PRConvRepoID != prRepo {
		t.Errorf("PRConvRepoID = %d, want %d", item.PRConvRepoID, prRepo)
	}
	got := append([]int64(nil), item.RepoIDs...)
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := append([]int64(nil), repos...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("RepoIDs = %v, want every repository the message's rows name %v (the owner, both issue links, the PR link)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RepoIDs = %v, want %v", got, want)
		}
	}
}
