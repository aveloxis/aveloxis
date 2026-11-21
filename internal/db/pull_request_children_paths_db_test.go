// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The writers of an issue's and a pull request's child rows. Each set
// writer (assignees, reviewers, labels) upserts the members it is given
// and then removes the stored members that are no longer in the set; each
// single-row writer updates its row in place when it is written again.

var pathsOrigin = model.DataOrigin{DataSource: "zz paths"}

func TestAssigneeReviewerAndLabelSetsAreUpsertedAndReconciled(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "sets",
		"aveloxis_data.issue_assignees", "aveloxis_data.pull_request_assignees", "aveloxis_data.pull_request_reviewers",
		"aveloxis_data.pull_request_labels", "aveloxis_data.issues", "aveloxis_data.pull_requests")
	issue := pathsIssue(t, store, repoID, 1)
	otherIssue := pathsIssue(t, store, repoID, 2)
	pr := pathsPR(t, store, repoID, 1, "open")
	otherPR := pathsPR(t, store, repoID, 2, "open")
	who := seedR6Contributor(t, store, pathsTag("who"), "", nil)
	later := seedR6Contributor(t, store, pathsTag("later"), "", nil)
	t.Cleanup(func() {
		c := context.Background()
		for _, table := range []string{"issue_assignees", "pull_request_assignees", "pull_request_reviewers"} {
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID)
		}
	})
	members := func(table, parentCol, memberCol string, parent int64) string {
		return pathsText(t, store, `SELECT COALESCE(string_agg(`+memberCol+`::text || '=' || COALESCE(cntrb_id::text, '-'), ',' ORDER BY `+memberCol+`), '') FROM aveloxis_data.`+table+` WHERE `+parentCol+` = $1`, parent)
	}

	// Issue assignees.
	if err := store.UpsertIssueAssignees(ctx, otherIssue, repoID, []model.IssueAssignee{{PlatformSrcID: 9, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIssueAssignees(ctx, issue, repoID, []model.IssueAssignee{
		{PlatformSrcID: 1, PlatformNodeID: "n1", ContributorID: &who, Origin: pathsOrigin}, {PlatformSrcID: 2, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := members("issue_assignees", "issue_id", "platform_assignee_id", issue); got != "1="+who+",2=-" {
		t.Errorf("the issue's assignees: %q", got)
	}
	// Written again with 2 and 3: 1 is removed, 2 gains the contributor it
	// lacked, and 1's would not have been replaced had it stayed.
	if err := store.UpsertIssueAssignees(ctx, issue, repoID, []model.IssueAssignee{
		{PlatformSrcID: 2, ContributorID: &later, Origin: pathsOrigin}, {PlatformSrcID: 3, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := members("issue_assignees", "issue_id", "platform_assignee_id", issue); got != "2="+later+",3=-" {
		t.Errorf("the issue's assignees after the second write: %q", got)
	}
	if err := store.UpsertIssueAssignees(ctx, issue, repoID, []model.IssueAssignee{{PlatformSrcID: 2, ContributorID: &who, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := members("issue_assignees", "issue_id", "platform_assignee_id", issue); got != "2="+later {
		t.Errorf("an assignee's contributor, once set, was replaced or the set was not reconciled: %q", got)
	}
	if got := members("issue_assignees", "issue_id", "platform_assignee_id", otherIssue); got != "9=-" {
		t.Errorf("the other issue's assignee was touched: %q", got)
	}
	if got := pathsText(t, store, `SELECT data_source FROM aveloxis_data.issue_assignees WHERE issue_id = $1`, issue); got != "zz paths" {
		t.Errorf("the assignee's data source: %q", got)
	}

	// Pull request assignees and reviewers: the same contract.
	if err := store.UpsertPRAssignees(ctx, otherPR, repoID, []model.PullRequestAssignee{{PlatformSrcID: 9, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRReviewers(ctx, otherPR, repoID, []model.PullRequestReviewer{{PlatformSrcID: 9, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRAssignees(ctx, pr, repoID, []model.PullRequestAssignee{{PlatformSrcID: 1, ContributorID: &who, Origin: pathsOrigin}, {PlatformSrcID: 2, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRAssignees(ctx, pr, repoID, []model.PullRequestAssignee{{PlatformSrcID: 1, ContributorID: &later, Origin: pathsOrigin}, {PlatformSrcID: 3, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := members("pull_request_assignees", "pull_request_id", "platform_assignee_id", pr); got != "1="+who+",3=-" {
		t.Errorf("the pull request's assignees after two writes: %q", got)
	}
	if got := members("pull_request_assignees", "pull_request_id", "platform_assignee_id", otherPR); got != "9=-" {
		t.Errorf("the other pull request's assignee was touched: %q", got)
	}
	if err := store.UpsertPRReviewers(ctx, pr, repoID, []model.PullRequestReviewer{{PlatformSrcID: 1, ContributorID: &who, Origin: pathsOrigin}, {PlatformSrcID: 2, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRReviewers(ctx, pr, repoID, []model.PullRequestReviewer{{PlatformSrcID: 1, ContributorID: &later, Origin: pathsOrigin}, {PlatformSrcID: 3, Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := members("pull_request_reviewers", "pull_request_id", "platform_reviewer_id", pr); got != "1="+who+",3=-" {
		t.Errorf("the pull request's reviewers after two writes: %q", got)
	}
	if got := members("pull_request_reviewers", "pull_request_id", "platform_reviewer_id", otherPR); got != "9=-" {
		t.Errorf("the other pull request's reviewer was touched: %q", got)
	}

	// Labels: by name; a label written again takes its new description and
	// colour.
	labels := func(parent int64) string {
		return pathsText(t, store, `SELECT COALESCE(string_agg(label_name || ':' || label_description || ':' || label_color, ',' ORDER BY label_name), '') FROM aveloxis_data.pull_request_labels WHERE pull_request_id = $1`, parent)
	}
	if err := store.UpsertPRLabels(ctx, otherPR, repoID, []model.PullRequestLabel{{Name: "keep", Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRLabels(ctx, pr, repoID, []model.PullRequestLabel{{Name: "bug", Description: "d1", Color: "red", PlatformID: 5, NodeID: "L1", IsDefault: true, Origin: pathsOrigin}, {Name: "old", Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRLabels(ctx, pr, repoID, []model.PullRequestLabel{{Name: "bug", Description: "d2", Color: "blue", Origin: pathsOrigin}, {Name: "new", Origin: pathsOrigin}}); err != nil {
		t.Fatal(err)
	}
	if got := labels(pr); got != "bug:d2:blue,new::" {
		t.Errorf("the pull request's labels after two writes: %q", got)
	}
	if got := labels(otherPR); got != "keep::" {
		t.Errorf("the other pull request's label was touched: %q", got)
	}
	if got := pathsText(t, store, `SELECT platform_label_id || '|' || node_id || '|' || is_default FROM aveloxis_data.pull_request_labels WHERE pull_request_id = $1 AND label_name = 'bug'`, pr); got != "5|L1|true" {
		t.Errorf("the label's first-written identity was not kept: %q", got)
	}
}

func TestAPullRequestsCommitEventAndFileAreUpsertedInPlace(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, _, _ := pathsRepo(t, store, "prrows",
		"aveloxis_data.pull_request_commits", "aveloxis_data.pull_request_events", "aveloxis_data.pull_request_files", "aveloxis_data.pull_requests")
	pr := pathsPR(t, store, repoID, 1, "open")
	who := seedR6Contributor(t, store, pathsTag("author"), "", nil)
	t.Cleanup(func() {
		c := context.Background()
		for _, table := range []string{"pull_request_commits", "pull_request_events"} {
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID)
		}
	})
	at := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)

	commit := &model.PullRequestCommit{PRID: pr, RepoID: repoID, SHA: "abc123", NodeID: "C1", Message: "first", AuthorEmail: "a@example.org", AuthorID: &who, Timestamp: at, Origin: pathsOrigin}
	if err := store.UpsertPRCommit(ctx, commit); err != nil {
		t.Fatal(err)
	}
	// Written again with a new message and no author: the message is
	// replaced, the author kept.
	commit.Message, commit.AuthorID = "second", nil
	if err := store.UpsertPRCommit(ctx, commit); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || '|' || MIN(pr_cmt_message) || '|' || MIN(author_cntrb_id::text) || '|' || MIN(pr_cmt_author_email) || '|' || MIN(pr_cmt_node_id) FROM aveloxis_data.pull_request_commits WHERE pull_request_id = $1`, pr); got != "1|second|"+who+"|a@example.org|C1" {
		t.Errorf("the pull request's commit after two writes: %q", got)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.pull_request_commits WHERE pull_request_id = $1 AND pr_cmt_timestamp = $2`, pr, at); n != 1 {
		t.Error("the commit's time was not stored as given")
	}

	event := &model.PullRequestEvent{PRID: pr, RepoID: repoID, ContributorID: &who, PlatformID: model.PlatformGitHub, PlatformEventID: repoID*100 + 1, NodeID: "E1", Action: "labeled", ActionCommitHash: "abc123", CreatedAt: at, Origin: pathsOrigin}
	if err := store.UpsertPREvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	event.Action, event.ContributorID = "closed", nil
	if err := store.UpsertPREvent(ctx, event); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT COUNT(*) || '|' || MIN(action) || '|' || MIN(cntrb_id::text) || '|' || MIN(action_commit_hash) || '|' || MIN(platform_id) FROM aveloxis_data.pull_request_events WHERE repo_id = $1`, repoID); got != "1|closed|"+who+"|abc123|1" {
		t.Errorf("the pull request's event after two writes: %q", got)
	}

	file := &model.PullRequestFile{PRID: pr, RepoID: repoID, Path: "a/b.go", Additions: 3, Deletions: 1, Origin: pathsOrigin}
	if err := store.UpsertPRFile(ctx, file); err != nil {
		t.Fatal(err)
	}
	file.Additions, file.Deletions = 10, 7
	if err := store.UpsertPRFile(ctx, file); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertPRFile(ctx, &model.PullRequestFile{PRID: pr, RepoID: repoID, Path: "c.go", Additions: 1, Origin: pathsOrigin}); err != nil {
		t.Fatal(err)
	}
	if got := pathsText(t, store, `SELECT string_agg(pr_file_path || ':' || pr_file_additions || ':' || pr_file_deletions, ',' ORDER BY pr_file_path) FROM aveloxis_data.pull_request_files WHERE pull_request_id = $1`, pr); got != "a/b.go:10:7,c.go:1:0" {
		t.Errorf("the pull request's files after the writes: %q", got)
	}
}
