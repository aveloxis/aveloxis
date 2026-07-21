// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 2 (v0.27.39) tests: stranded-repo reconciliation +
// child-collection set-diff reconciliation.

package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestChildUpsertsReconcileRemovals pins that all five child-upsert
// methods carry a set-diff DELETE — pre-fix they were additive-only,
// so a removed label / unassigned assignee / withdrawn review request
// stayed in the DB forever (presented as current state while actually
// a historical union).
func TestChildUpsertsReconcileRemovals(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"DELETE FROM aveloxis_data.issue_labels\n\t\t\tWHERE issue_id = $1 AND NOT (label_text = ANY($2::text[]))",
		"DELETE FROM aveloxis_data.issue_assignees\n\t\t\tWHERE issue_id = $1 AND NOT (platform_assignee_id = ANY($2::bigint[]))",
		"DELETE FROM aveloxis_data.pull_request_labels\n\t\t\tWHERE pull_request_id = $1 AND NOT (label_name = ANY($2::text[]))",
		"DELETE FROM aveloxis_data.pull_request_assignees\n\t\t\tWHERE pull_request_id = $1 AND NOT (platform_assignee_id = ANY($2::bigint[]))",
		"DELETE FROM aveloxis_data.pull_request_reviewers\n\t\t\tWHERE pull_request_id = $1 AND NOT (platform_reviewer_id = ANY($2::bigint[]))",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("missing child reconciliation delete: %.60s...", needle)
		}
	}
}

// TestPrelimKeepsQueueRowWhenArchiveFails pins the v0.27.39 prelim
// hardening: dequeuing a dead repo WITHOUT the archive succeeding
// mints a stranded row (the exact class reconcile-repos exists for).
func TestPrelimKeepsQueueRowWhenArchiveFails(t *testing.T) {
	src, err := os.ReadFile("../collector/prelim.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	idx := strings.Index(s, "failed to archive dead repo")
	if idx < 0 {
		t.Fatal("prelim must log the archive failure with the keep-queue-row semantics")
	}
	// The archive-failure branch must RETURN before DequeueRepo.
	window := s[idx:]
	retIdx := strings.Index(window, "return result, nil")
	deqIdx := strings.Index(window, "store.DequeueRepo")
	if retIdx < 0 || (deqIdx >= 0 && deqIdx < retIdx) {
		t.Error("on archive failure prelim must return WITHOUT dequeuing — dequeue-without-archive creates a stranded repo")
	}
}

// TestSchedulerRunsStrandedGauge pins the startup observation.
func TestSchedulerRunsStrandedGauge(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "CountStrandedRepos(ctx)") {
		t.Error("scheduler startup must gauge stranded repos (observation-only) and point at reconcile-repos")
	}
}

// TestListStrandedReposClassification (AVELOXIS_TEST_DB): a repo with
// no queue row lists as stranded with the right Collected flag; an
// archived queue-less repo and a queued repo never do.
func TestListStrandedReposClassification(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	mk := func(name string, archived bool) int64 {
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id, repo_archived)
			VALUES ($1, '_avstr', $2, 1, 1, $3) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avstr/%s%d", name, suffix),
			fmt.Sprintf("%s%d", name, suffix), archived).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	strandedDataless := mk("dataless", false)
	strandedCollected := mk("collected", false)
	archivedOne := mk("archived", true)
	queuedOne := mk("queued", false)
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, commit_count) VALUES ($1, 5)`, strandedCollected); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
		VALUES ($1, 'queued', 0, NOW())`, queuedOne); err != nil {
		t.Fatal(err)
	}
	all := []int64{strandedDataless, strandedCollected, archivedOne, queuedOne}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = ANY($1::bigint[])`, all)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = ANY($1::bigint[])`, all)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1::bigint[])`, all)
	})

	list, err := store.ListStrandedRepos(ctx, 100000)
	if err != nil {
		t.Fatal(err)
	}
	found := map[int64]StrandedRepo{}
	for _, sr := range list {
		found[sr.RepoID] = sr
	}
	if sr, ok := found[strandedDataless]; !ok || sr.Collected {
		t.Errorf("dataless stranded repo must list with Collected=false, got %+v ok=%v", sr, ok)
	}
	if sr, ok := found[strandedCollected]; !ok || !sr.Collected {
		t.Errorf("collected stranded repo must list with Collected=true, got %+v ok=%v", sr, ok)
	}
	if _, ok := found[archivedOne]; ok {
		t.Error("archived repos are sidelined BY DESIGN — they must not list as stranded")
	}
	if _, ok := found[queuedOne]; ok {
		t.Error("queued repos are not stranded")
	}
}

// TestChildReconciliationEndToEnd (AVELOXIS_TEST_DB): upserting a
// SMALLER complete label set removes the stale row; the surviving
// label stays.
func TestChildReconciliationEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	var repoID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, '_avlbl', $2, 1, 1) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avlbl/r%d", suffix), fmt.Sprintf("r%d", suffix)).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	var issueID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, issue_number, platform_issue_id, issue_state)
		VALUES ($1, 7, $2, 'open') RETURNING issue_id`, repoID, suffix%1000000000).Scan(&issueID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_labels WHERE issue_id = $1`, issueID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	lbl := func(name string) model.IssueLabel {
		return model.IssueLabel{Text: name, Origin: model.DataOrigin{DataSource: "test"}}
	}
	if err := store.UpsertIssueLabels(ctx, issueID, repoID, []model.IssueLabel{lbl("bug"), lbl("wontfix")}); err != nil {
		t.Fatal(err)
	}
	// Upstream removed "wontfix": the fresh COMPLETE set is just "bug".
	if err := store.UpsertIssueLabels(ctx, issueID, repoID, []model.IssueLabel{lbl("bug")}); err != nil {
		t.Fatal(err)
	}
	var labels []string
	rows, err := pool.Query(ctx, `SELECT label_text FROM aveloxis_data.issue_labels WHERE issue_id = $1 ORDER BY label_text`, issueID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			t.Fatal(err)
		}
		labels = append(labels, l)
	}
	if len(labels) != 1 || labels[0] != "bug" {
		t.Errorf("after reconciliation the label set must be exactly [bug], got %v — additive-only children present historical unions as current state", labels)
	}
	_ = context.Background
}
