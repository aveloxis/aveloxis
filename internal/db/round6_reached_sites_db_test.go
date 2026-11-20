// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Review round 6: the write-policy judge sees a statement only when a test
// of this package sends it. These are writers of contributors and their
// identities (tables with registered write policies) and a few one-statement
// writers that no test of this package reached; other packages' tests drive
// them, where the judge is not installed. Each is run here, so that what it
// sends is judged.

func TestTheContributorResolverSendsItsStatements(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 7_000_000_000
	login := fmt.Sprintf("r6-resolver-%d", uid)
	t.Cleanup(func() {
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	// A new identity, with neither node id nor type.
	first, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "", "")
	if err != nil || first == "" {
		t.Fatalf("resolving a new contributor: %q, %v", first, err)
	}
	// A fresh resolver (no cache) meets the stored identity and fills the
	// two columns it now has values for: the fill the policy's Exceptions
	// name (FillEmptyOnly, written with NULLIF).
	second, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "MDQ6VXNlcjE=", "User")
	if err != nil || second != first {
		t.Fatalf("resolving it again: %q (was %q), %v", second, first, err)
	}
	var node, typ string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(node_id, ''), COALESCE(user_type, '') FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&node, &typ); err != nil {
		t.Fatal(err)
	}
	if node != "MDQ6VXNlcjE=" || typ != "User" {
		t.Errorf("the identity's node id and type after the fill: %q, %q", node, typ)
	}
	// And a third time with other values: a filled column keeps its value.
	if _, err := NewContributorResolver(store).Resolve(ctx, 1, uid, login, "R Six", "", "", "", "OTHER", "Bot"); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(node_id, ''), COALESCE(user_type, '') FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&node, &typ); err != nil {
		t.Fatal(err)
	}
	if node != "MDQ6VXNlcjE=" || typ != "User" {
		t.Errorf("a filled node id or type was overwritten: %q, %q", node, typ)
	}
}

// One-statement writers on rows that do not exist: each statement is sent,
// and changes nothing.
func TestOneStatementWritersSendTheirStatements(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	const missing = "01000000-0000-0000-0000-0000000006a6"
	for name, run := range map[string]func() error{
		"MarkContributorEnriched":        func() error { return store.MarkContributorEnriched(ctx, "r6-no-such-login") },
		"MarkContributorSearchAttempted": func() error { return store.MarkContributorSearchAttempted(ctx, missing) },
		"SetIssueClosedBy":               func() error { return store.SetIssueClosedBy(ctx, -6006, missing) },
		"SetCommitAuthorLogin":           func() error { return store.SetCommitAuthorLogin(ctx, -6006, "r6-no-such-hash", "r6-login") },
	} {
		if err := run(); err != nil {
			t.Errorf("%s on a row that does not exist: %v", name, err)
		}
	}
	// This one says so when the row is not there; its statement was sent.
	if err := store.SetForceFullCollect(ctx, -6006, true); err == nil {
		t.Error("SetForceFullCollect on a repository with no queue row reported success")
	}
}

// Round 6, second stage: the writers of registered tables (contributors,
// contributor identities, messages) that no DB-tier test of the whole suite
// reached. Each is driven down the branch that sends its statements.

func seedR6Contributor(t *testing.T, store *PostgresStore, login, email string, ghUserID any) string {
	t.Helper()
	ctx := context.Background()
	var id string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_email, gh_user_id, cntrb_deleted)
		VALUES ($1, $2, $3, 0) RETURNING cntrb_id::text`, login, email, ghUserID).Scan(&id); err != nil {
		t.Fatalf("seed contributor %s: %v", login, err)
	}
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors_aliases WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id = $1::uuid`, id)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	return id
}

func r6Deleted(t *testing.T, store *PostgresStore, id string) int {
	t.Helper()
	var d int
	if err := store.pool.QueryRow(context.Background(), `SELECT COALESCE(cntrb_deleted, 0) FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id).Scan(&d); err != nil {
		t.Fatal(err)
	}
	return d
}

// Linking a contributor to a GitHub user that another contributor row
// already carries: the two are merged (the loser's fields fill the
// winner's empty ones, the loser is soft-deleted) and the winner is linked
// and given its identity row.
func TestLinkingAContributorMergesTheDuplicateAndLinksTheWinner(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 8_000_000_000
	login := fmt.Sprintf("r6-link-%d", uid)
	target := seedR6Contributor(t, store, login+"-a", "", nil)
	other := seedR6Contributor(t, store, login+"-b", login+"@example.org", uid)
	if err := store.LinkContributorToGitHubUser(ctx, target, login, uid); err != nil {
		t.Fatal(err)
	}
	if r6Deleted(t, store, target)+r6Deleted(t, store, other) != 1 {
		t.Errorf("after the link, exactly one of the two rows is soft-deleted: target %d, other %d", r6Deleted(t, store, target), r6Deleted(t, store, other))
	}
	var identities int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.contributor_identities WHERE platform_id = 1 AND platform_user_id = $1`, uid).Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if identities != 1 {
		t.Errorf("%d identity rows for the linked GitHub user, want 1", identities)
	}
}

// The same merge on a rename, which also records the new login.
func TestRenamingAContributorMergesTheDuplicateAndRecordsTheLogin(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 8_500_000_000
	login := fmt.Sprintf("r6-rename-%d", uid)
	target := seedR6Contributor(t, store, login+"-old", "", nil)
	other := seedR6Contributor(t, store, login+"-dup", login+"@example.org", uid)
	if err := store.RenameContributorGhLogin(ctx, target, login+"-new", uid); err != nil {
		t.Fatal(err)
	}
	if r6Deleted(t, store, target)+r6Deleted(t, store, other) != 1 {
		t.Errorf("after the rename, exactly one of the two rows is soft-deleted: target %d, other %d", r6Deleted(t, store, target), r6Deleted(t, store, other))
	}
	var gh string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_login, '') FROM aveloxis_data.contributors WHERE cntrb_id IN ($1::uuid, $2::uuid) AND COALESCE(cntrb_deleted, 0) = 0`, target, other).Scan(&gh); err != nil {
		t.Fatal(err)
	}
	if gh != login+"-new" {
		t.Errorf("the surviving row's gh_login is %q, want the new login", gh)
	}
}

// The commit resolver meets a login that a contributor row already has
// under another id: that row is updated, and its id is the one returned.
func TestUpsertContributorFullUpdatesTheRowThatHasTheLogin(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 9_000_000_000
	login := fmt.Sprintf("r6-full-%d", uid)
	have := seedR6Contributor(t, store, login, "", nil)
	created, id, err := store.UpsertContributorFull(ctx, "0100000a-0000-0000-0000-000000000006", login, uid, login+"@example.org")
	if err != nil {
		t.Fatal(err)
	}
	if created || id != have {
		t.Errorf("created %v, id %s; want the existing row %s, not created", created, id, have)
	}
	var gh int64
	var canonical string
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gh_user_id, 0), COALESCE(cntrb_canonical, '') FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, have).Scan(&gh, &canonical); err != nil {
		t.Fatal(err)
	}
	if gh != uid || canonical != login+"@example.org" {
		t.Errorf("the row after the update: gh_user_id %d, canonical %q", gh, canonical)
	}
}

// A GitLab identity in a contributor batch fills the contributor's GitLab
// columns (the GitHub arm is reached by every other batch test).
func TestAContributorBatchWithAGitLabIdentityFillsTheGitLabColumns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	uid := time.Now().UnixNano()%1_000_000_000 + 9_500_000_000
	login := fmt.Sprintf("r6-gitlab-%d", uid)
	t.Cleanup(func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_identities WHERE login = $1`, login)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributor_login_history WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1)`, login)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	if err := store.UpsertContributorBatch(ctx, []model.Contributor{{
		ID: PlatformUUID(int(model.PlatformGitLab), uid), Login: login,
		Identities: []model.ContributorIdentity{{Platform: model.PlatformGitLab, UserID: uid, Login: login, Type: "User"}},
	}}); err != nil {
		t.Fatal(err)
	}
	var glID int64
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE(gl_id, 0) FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login).Scan(&glID); err != nil {
		t.Fatal(err)
	}
	if glID != uid {
		t.Errorf("gl_id = %d, want %d", glID, uid)
	}
}

// A review with a body, and a batch of review comments: each stores its
// text as a message (a table with registered write policies).
func TestAReviewBodyAndReviewCommentsAreStoredAsMessages(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	n := time.Now().UnixNano() % 1_000_000_000
	repoID := seedRepoForDeps(t, store, ctx, "r6-reviews", fmt.Sprintf("repo-%d", n))
	t.Cleanup(func() {
		c := context.Background()
		for _, q := range []string{
			`DELETE FROM aveloxis_data.pull_request_review_message_ref WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.review_comments WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.messages WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
		} {
			cleanupExecRetry(c, store, q, repoID)
		}
	})
	prID, err := store.UpsertPullRequest(ctx, &model.PullRequest{RepoID: repoID, PlatformSrcID: 9_000_000 + n, Number: 1, Title: "r6", State: "open",
		CreatedAt: time.Now(), UpdatedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRReview(ctx, &model.PullRequestReview{PRID: prID, RepoID: repoID, PlatformID: model.PlatformGitHub,
		PlatformReviewID: 9_100_000 + n, State: "COMMENTED", Body: "a review body", SubmittedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{{
		Message: model.Message{RepoID: repoID, PlatformMsgID: 9_200_000 + n, PlatformID: model.PlatformGitHub, Text: "an inline comment", Timestamp: time.Now()},
		Comment: model.ReviewComment{RepoID: repoID, PlatformSrcID: 9_200_000 + n, Path: "a.go", UpdatedAt: time.Now()},
	}}); err != nil {
		t.Fatal(err)
	}
	var messages int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.messages WHERE repo_id = $1`, repoID).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 2 {
		t.Errorf("%d messages stored for the review body and the inline comment, want 2", messages)
	}
}
