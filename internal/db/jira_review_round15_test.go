// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"testing"
	"time"
)

// TestTrackerActionTieReplayCannotRegress (Copilot round 15 on
// PR #193, suppressed): Pony Mail rounds sent_at to the minute, so
// opposite actions can share a timestamp — and the row-deferral
// contract (round 2 #3) deliberately replays partially-failed rows.
// Pre-fix: Reopened@T applied, Resolved@T won the <= tie, then the
// DEFERRED Reopened row retried and the bare <= accepted it AGAIN,
// regressing the issue to open. last_mail_event_id is the persisted
// tie-breaker: at equal sent_at only an equal-or-higher
// email_message_id applies.
func TestTrackerActionTieReplayCannotRegress(t *testing.T) {
	store, ctx := sbConnect(t)
	id := tsSeedIssue(t, ctx, store, -944_150_100, "open")
	at := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	// Reopened lands first (em=100), then Resolved wins the same-minute
	// tie (em=200 — mbox ingest order preserves send order).
	if err := store.ApplyTrackerAction(ctx, id, "Reopened", at, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTrackerAction(ctx, id, "Resolved", at, 200); err != nil {
		t.Fatal(err)
	}
	if state, _ := tsState(t, ctx, store, id); state != "closed" {
		t.Fatalf("after the tie: state = %q, want closed", state)
	}

	// The deferred OLDER action replays: same minute, lower event id —
	// it must be refused (pre-fix the <= arm flip-flopped to open).
	if err := store.ApplyTrackerAction(ctx, id, "Reopened", at, 100); err != nil {
		t.Fatal(err)
	}
	state, closedAt := tsState(t, ctx, store, id)
	if state != "closed" || closedAt == nil {
		t.Fatalf("deferred older replay regressed the tie winner: state=%q closed_at=%v", state, closedAt)
	}

	// The WINNER's own replay stays idempotent (equal time AND id).
	if err := store.ApplyTrackerAction(ctx, id, "Resolved", at, 200); err != nil {
		t.Fatal(err)
	}
	if state, _ := tsState(t, ctx, store, id); state != "closed" {
		t.Fatalf("winner replay: state = %q, want closed", state)
	}
}

// TestGapCandidatesClampNegativeNativeCount (Copilot round 15,
// suppressed ×2): the drain-side activity refresh is best-effort
// (round 10), so the cached last_issues can LAG the live synthetic
// population — the unclamped subtraction went negative and made a
// zero-issue forge repo a gap candidate with a phantom gap size.
func TestGapCandidatesClampNegativeNativeCount(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round15-gap-clamp")

	// Queue row with a STALE zero cache; three live synthetics.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_collected, last_issues, last_prs)
		VALUES ($1, 'queued', NOW(), 0, 0)
		ON CONFLICT (repo_id) DO UPDATE SET last_issues = 0, last_prs = 0, last_collected = NOW()`, repoID)
	t.Cleanup(func() {
		cleanupExecRetry(t.Context(), store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
	})
	for i := range 3 {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title, issue_state, external_key, data_source)
			VALUES ($1, $2, $3, 'synthetic', 'open', $4, 'JIRA')`,
			repoID, -944_151_000-int64(i), 900+i, fmt.Sprintf("AVR15-%d", i))
	}
	// Forge metadata: ZERO issues, zero PRs — no gap exists.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, pr_count, data_collection_date)
		VALUES ($1, 0, 0, NOW())`, repoID)
	t.Cleanup(func() {
		cleanupExecRetry(t.Context(), store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
	})

	cands, err := store.GetGapHealCandidates(ctx, repoID-1, 50, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cands {
		if c.RepoID == repoID {
			t.Fatalf("clean repo became a gap candidate (gap=%d) — the lagging cache's negative native count must clamp at zero", c.Gap)
		}
	}
	// And the --all sweep must report gap size 0, never phantom
	// missing issues.
	all, err := store.GetGapHealCandidates(ctx, repoID-1, 50, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range all {
		if c.RepoID == repoID && c.Gap != 0 {
			t.Fatalf("reported gap = %d, want 0 (phantom missing issues from the unclamped subtraction)", c.Gap)
		}
	}
}
