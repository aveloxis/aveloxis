// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRegisterJiraProjectRaceKeepsOneInstance (Copilot round 13 on
// PR #193): the one-instance probe was check-then-act, so two
// concurrent register-jira-projects processes could both observe no
// conflicting URL and insert projects for DIFFERENT instances — after
// which the instance-blind username and comment-id collisions the
// guard exists to prevent become real. The probe+insert now run in one
// transaction under a registration advisory xact lock: two racers can
// never BOTH succeed with distinct base_urls, and whatever persists is
// single-instance. (Residue from other tests can make both racers
// refuse — that still satisfies the invariant; the pre-fix bug is the
// both-succeed arm.)
func TestRegisterJiraProjectRaceKeepsOneInstance(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	cleanup := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key LIKE '_AVR13%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	for round := 0; round < 10; round++ {
		keyA := fmt.Sprintf("_AVR13A%d", round)
		keyB := fmt.Sprintf("_AVR13B%d", round)
		urlA := fmt.Sprintf("https://race-a-%d.example.org", round)
		urlB := fmt.Sprintf("https://race-b-%d.example.org", round)
		start := make(chan struct{})
		errs := make(chan error, 2)
		go func() { <-start; errs <- store.RegisterJiraProject(ctx, keyA, urlA, nil) }()
		go func() { <-start; errs <- store.RegisterJiraProject(ctx, keyB, urlB, nil) }()
		close(start)
		errA, errB := <-errs, <-errs
		if errA == nil && errB == nil {
			t.Fatalf("round %d: BOTH registrations of distinct instances succeeded — the one-instance invariant is racy again", round)
		}
		var distinct int
		if err := store.pool.QueryRow(ctx, `
			SELECT count(DISTINCT base_url) FROM aveloxis_ops.jira_project_serve
			WHERE project_key IN ($1, $2)`, keyA, keyB).Scan(&distinct); err != nil {
			t.Fatal(err)
		}
		if distinct > 1 {
			t.Fatalf("round %d: %d distinct base_urls persisted for the racing keys", round, distinct)
		}
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key IN ($1, $2)`, keyA, keyB)
	}
}

// TestJiraProjectsWithStagingKeysetPages: the drain's project scan is
// a jps_id keyset now (round 13) — afterID excludes everything at or
// below the cursor, ordering is by id, and the tail past the cursor is
// reachable regardless of how old the head-blockers' staging is.
func TestJiraProjectsWithStagingKeysetPages(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	cleanup := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_staging WHERE issue_key LIKE '_AVR13K%'`)
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key LIKE '_AVR13K%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	var ids []int64
	for i := 0; i < 3; i++ {
		var jps int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_ops.jira_project_serve (project_key, base_url, tool_version)
			VALUES ($1, 'https://jira.example.org', $2) RETURNING jps_id`,
			fmt.Sprintf("_AVR13K%d", i), ToolVersion).Scan(&jps); err != nil {
			t.Fatal(err)
		}
		// The OLDEST staging goes on the LAST project: an oldest-first
		// scan would order 2,1,0 — the keyset must ignore that.
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_ops.jira_staging (jps_id, project_key, issue_key, issue_updated, envelope, created_at)
			VALUES ($1, $2, $3, NOW(), '{}', $4)`,
			jps, fmt.Sprintf("_AVR13K%d", i), fmt.Sprintf("_AVR13K%d-1", i),
			time.Now().Add(-time.Duration(i)*time.Hour))
		ids = append(ids, jps)
	}

	page1, err := store.JiraProjectsWithStaging(ctx, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	got := map[int64]bool{}
	for _, id := range page1 {
		got[id] = true
	}
	// The shared scratch DB may hold other staged projects; assert
	// keyset semantics on OUR ids only: paging with afterID=ids[1]
	// must exclude ids[0]+ids[1] and reach ids[2].
	page2, err := store.JiraProjectsWithStaging(ctx, ids[1], 100)
	if err != nil {
		t.Fatal(err)
	}
	sawTail := false
	for _, id := range page2 {
		if id == ids[0] || id == ids[1] {
			t.Fatalf("afterID=%d page returned %d — keyset must exclude ids at or below the cursor", ids[1], id)
		}
		if id == ids[2] {
			sawTail = true
		}
	}
	if !sawTail {
		t.Fatalf("page after %d = %v — the tail project must be reachable past the cursor", ids[1], page2)
	}
}
