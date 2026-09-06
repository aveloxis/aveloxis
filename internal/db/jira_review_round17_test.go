// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func r17Time(t *testing.T, v string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, v)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

// TestMirrorNodeLinkIsGroupScoped (Copilot round 17 #2): a Message-ID
// is entirely sender-controlled — the host suffix authenticates
// nothing — so the resolver cross-checks the one thing the sender
// does NOT control: the resolved entity must belong to the message's
// repo GROUP (the PMC), falling back to the message's own repo when
// no group is known. A foreign project's public node ID becomes a
// clean miss; a sibling repo in the same PMC group still links (many
// repos mirror to one dev@ list).
func TestMirrorNodeLinkIsGroupScoped(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	tag := fmt.Sprintf("%d", time.Now().UnixNano())
	mk := func(owner, name string) int64 {
		name = name + "-" + tag // unique per run: UpsertRepo would otherwise reuse a residual row (and its PRs)
		id, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitHub,
			GitURL:   "https://github.com/" + owner + "/" + name,
			Owner:    owner, Name: name,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			c := context.Background()
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`, id)
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return id
	}
	groupOf := func(repoID int64) int64 {
		var g int64
		if err := store.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	setGroup := func(repoID, group int64) {
		mustExecRetry(ctx, t, store,
			`UPDATE aveloxis_data.repos SET repo_group_id = $2 WHERE repo_id = $1`, repoID, group)
	}

	listRepo := mk("_avr17", "arrow")      // the list's primary repo
	sibling := mk("_avr17", "arrow-rs")    // same PMC group
	foreign := mk("_avr17x", "kubernetes") // another project entir
	group := groupOf(listRepo)
	setGroup(sibling, group)
	// foreign goes into a DIFFERENT group (UpsertRepo's default group
	// is shared, so the foreign repo must be moved out of it or the
	// scope test is vacuous).
	var otherGroup int64
	mustQueryRowRetry(ctx, t, store,
		`INSERT INTO aveloxis_data.repo_groups (rg_name) VALUES ($1) RETURNING repo_group_id`,
		&otherGroup, "_avr17-other-"+tag)
	setGroup(foreign, otherGroup)
	t.Cleanup(func() {
		store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, otherGroup)
	})

	node := "PR_avR17scope" + tag
	var prID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, node_id)
		VALUES ($1, 97170001 + $3, 171, $2) RETURNING pull_request_id`, sibling, node, sibling).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	foreignNode := "PR_avR17foreign" + tag
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, node_id)
		VALUES ($1, 97170002 + $3, 172, $2)`, foreign, foreignNode, foreign); err != nil {
		t.Fatal(err)
	}

	// Same-group sibling: links.
	_, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, node, listRepo, &group)
	if err != nil || gotPR == nil || *gotPR != prID {
		t.Fatalf("sibling-in-group = %v err=%v, want %d — group scope must keep the multi-repo-PMC links", gotPR, err, prID)
	}
	// A FOREIGN project's public node ID pasted into a crafted
	// Message-ID: clean miss, never a link.
	gotIssue, gotPR, err := store.ResolveMirrorLinkByNodeID(ctx, foreignNode, listRepo, &group)
	if err != nil || gotIssue != nil || gotPR != nil {
		t.Fatalf("foreign node resolved (%v,%v,%v) — a sender-controlled Message-ID linked outside the message's repo group", gotIssue, gotPR, err)
	}
	// Nil group falls back to the message's own repo: the sibling's
	// PR must then MISS too (only listRepo's entities qualify).
	_, gotPR, err = store.ResolveMirrorLinkByNodeID(ctx, node, listRepo, nil)
	if err != nil || gotPR != nil {
		t.Fatalf("nil-group fallback resolved a sibling-repo PR (%v, err=%v) — the fallback scope is the message's own repo", gotPR, err)
	}

	// The body-URL sibling carries the same scope: a claimed
	// owner/repo outside the group is a clean miss.
	gotIssue, gotPR, err = store.ResolveMirrorLink(ctx, "_avr17x", "kubernetes-"+tag, "pull", 172, listRepo, &group)
	if err != nil || gotIssue != nil || gotPR != nil {
		t.Fatalf("body-URL claim outside the group resolved (%v,%v,%v)", gotIssue, gotPR, err)
	}
}

// TestUpsertJiraCommentDoesNotRecountPerComment (Copilot round 17,
// suppressed #2): the per-comment bridge used to recount the issue's
// whole ref set — recount i after comment i is quadratic in the block
// size, repeated on every later issue update. The comment upsert's
// bridge is recount-free now; the PROCESSOR recounts once per issue
// after its loop (RecountIssueComments is that closing half).
func TestUpsertJiraCommentDoesNotRecountPerComment(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round17-recount")

	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR17-9", "t", "b", "JIRA", nil,
		r17Time(t, "2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	// Sentinel count: if the upsert recounts, this gets recomputed.
	mustExecRetry(ctx, t, store,
		`UPDATE aveloxis_data.issues SET comment_count = 99 WHERE issue_id = $1`, issueID)

	if _, err := store.UpsertJiraComment(ctx, JiraAPIComment{
		RepoID: repoID, IssueID: issueID, ExternalKey: "AVR17-9",
		CommentID: 917001, Body: "hello",
		Created: r17Time(t, "2026-03-02T00:00:00Z"),
		Updated: r17Time(t, "2026-03-02T00:00:00Z"),
	}); err != nil {
		t.Fatal(err)
	}
	var cnt int
	if err := store.pool.QueryRow(ctx,
		`SELECT comment_count FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 99 {
		t.Fatalf("comment_count = %d after ONE comment upsert, want the 99 sentinel untouched — the per-comment recount is back (quadratic over the block)", cnt)
	}
	// The processor's closing half corrects it in one pass.
	if err := store.RecountIssueComments(ctx, issueID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT comment_count FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 1 {
		t.Fatalf("comment_count = %d after RecountIssueComments, want 1", cnt)
	}
}

// TestBackfillJiraIdentitiesRejectsUnknownProject (Copilot round 17,
// suppressed #1): an unknown --project used to skip everything and
// exit 0 with "backfilled 0 issues" — a typo read as success. Pinned
// at the source: the validation block sits BEFORE the registration
// loop and errors when nothing matches.
func TestBackfillJiraIdentitiesRejectsUnknownProject(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/backfill_jira_identities.go")
	validate := strings.Index(src, `matches no ENABLED Jira registration`)
	loop := strings.LastIndex(src, "for _, c := range regs {") // the MAIN registration loop (after the validation's own membership scan)
	if validate < 0 {
		t.Fatal("the unknown --project validation (\"matches no ENABLED Jira registration\") is gone — a typo'd key exits 0 as a completed backfill again")
	}
	if loop < 0 || validate > loop {
		t.Fatalf("the --project validation must run BEFORE the main registration loop (validate=%d loop=%d)", validate, loop)
	}
}
