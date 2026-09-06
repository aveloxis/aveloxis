// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// tracker_state_test.go — C1: synthetic Jira issues finally get STATE.
// All 485,892 synthetics sit permanently 'open' while the database
// holds 358,384 Resolved notifications whose parsed action was
// discarded. ApplyTrackerAction is the forward writer (explicit UPDATE
// on the LINK path — the DO UPDATE never fires for already-existing
// issues); BackfillSyntheticJiraState is the ledgered history walk.
// Pilot-measured safety: mail-derived state falsely closes 2/4,870
// (0.04%) and the event-time guard removes replay regressions.
package db

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// --- source pins -------------------------------------------------------

// TestApplyTrackerActionShape — gated to SYNTHETIC rows
// (platform_issue_id < 0; the LINK path can return a native GitHub
// issue whose state is API-owned) and event-time guarded (updated_at)
// so a replayed old month can never regress a newer state.
func TestApplyTrackerActionShape(t *testing.T) {
	src := srctest.Read(t, "internal/db/mailinglist_projection_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) ApplyTrackerAction(")
	for _, needle := range []string{
		"platform_issue_id < 0",
		"updated_at",
		"closed_at",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("ApplyTrackerAction must carry %q (synthetic gate / event-time guard / close stamp)", needle)
		}
	}
}

// TestProjectionCallersApplyTrackerAction — both keyed-projection
// callers plumb the action: the processor's drain path and
// BackfillKeyedIssueProjection (the second caller rev 1 of the plan
// missed).
func TestProjectionCallersApplyTrackerAction(t *testing.T) {
	proc := srctest.Read(t, "internal/collector/mailinglist_processor.go")
	if !strings.Contains(proc, "ApplyTrackerAction(") || !strings.Contains(proc, "TrackerActionFromSubject(") {
		t.Error("the processor's keyed path must derive the action (TrackerActionFromSubject) and apply it (ApplyTrackerAction)")
	}
	backfill := srctest.FuncBody(t, srctest.Read(t, "internal/db/mailinglist_projection_backfill.go"),
		"func (s *PostgresStore) BackfillKeyedIssueProjection(")
	if !strings.Contains(backfill, "ApplyTrackerAction(") {
		t.Error("BackfillKeyedIssueProjection must apply the tracker action too")
	}
}

// TestSyntheticJiraStateBackfillLedgered — the history walk is
// keyset-windowed over email_message ids and ledgered (pure SQL,
// bounded, unattended — the migrate-scope rules).
func TestSyntheticJiraStateBackfillLedgered(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	if !strings.Contains(src, "v0.29.0 backfill synthetic Jira issue state") {
		t.Error("migrate.go must ledger the synthetic-state backfill")
	}
	store := srctest.Read(t, "internal/db/mailinglist_projection_store.go")
	body := srctest.FuncBody(t, store, "func (s *PostgresStore) BackfillSyntheticJiraState(")
	for _, needle := range []string{"runKeysetWindows", "linked_external_key <> ''", "platform_issue_id < 0"} {
		if !strings.Contains(body, needle) {
			t.Errorf("BackfillSyntheticJiraState must carry %q", needle)
		}
	}
}

// TestGapQueriesExcludeSyntheticIssues — the poisoning sweep: a
// synthetic KAFKA-123 (issue_number=123) must not mask real GitHub
// #123 in gap detection, and permanently-open synthetics must not
// feed the open-item refresher with bogus by-number forge fetches.
func TestGapQueriesExcludeSyntheticIssues(t *testing.T) {
	src := srctest.Read(t, "internal/db/gap_store.go")
	for _, fn := range []string{
		"func (s *PostgresStore) GetCollectedIssueNumbers(",
		"func (s *PostgresStore) GetOpenIssueNumbers(",
	} {
		body := srctest.FuncBody(t, src, fn)
		if !strings.Contains(body, "platform_issue_id >= 0") {
			t.Errorf("%s must exclude synthetic (negative-id) issues", fn)
		}
	}
}

// --- behavioral tier ---------------------------------------------------

const (
	tsRepoID = int64(944_148_010)
	tsKey    = "AVTS-77"
)

func tsSeedIssue(t *testing.T, ctx context.Context, store *PostgresStore, platformIssueID int64, state string) int64 {
	t.Helper()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avts/state', '_avts', 'state', 1)
		ON CONFLICT (repo_id) DO NOTHING`, tsRepoID)
	var id int64
	err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues
		(repo_id, platform_issue_id, issue_number, issue_title, issue_state, external_key, data_source)
		VALUES ($1, $2, 77, '[AVTS-77] t', $3, $4, 'JIRA')
		ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET issue_state = EXCLUDED.issue_state, closed_at = NULL, updated_at = NULL, last_mail_event_id = NULL
		RETURNING issue_id`, tsRepoID, platformIssueID, state, tsKey).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.email_message WHERE message_id_header LIKE '<_avts-%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1 AND NOT EXISTS (
			SELECT 1 FROM aveloxis_data.issues WHERE repo_id = $1)`, tsRepoID)
	})
	return id
}

func tsState(t *testing.T, ctx context.Context, store *PostgresStore, issueID int64) (string, *time.Time) {
	t.Helper()
	var state string
	var closedAt *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT issue_state, closed_at FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&state, &closedAt); err != nil {
		t.Fatal(err)
	}
	return state, closedAt
}

// TestApplyTrackerActionLifecycle — Resolved closes with closed_at;
// an OLDER Reopened replay cannot regress; a NEWER Reopened reopens.
func TestApplyTrackerActionLifecycle(t *testing.T) {
	store, ctx := sbConnect(t)
	id := tsSeedIssue(t, ctx, store, -944_148_100, "open")
	t1 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 5, 12, 0, 0, 0, time.UTC)

	if err := store.ApplyTrackerAction(ctx, id, "Resolved", t2, 10); err != nil {
		t.Fatal(err)
	}
	state, closedAt := tsState(t, ctx, store, id)
	if state != "closed" || closedAt == nil || !closedAt.Equal(t2) {
		t.Fatalf("after Resolved: state=%q closed_at=%v", state, closedAt)
	}
	// Replayed OLDER month: a Reopened from BEFORE the close must not regress.
	if err := store.ApplyTrackerAction(ctx, id, "Reopened", t1, 11); err != nil {
		t.Fatal(err)
	}
	if state, _ := tsState(t, ctx, store, id); state != "closed" {
		t.Fatalf("older replay regressed state to %q — the event-time guard is broken", state)
	}
	// A genuinely newer Reopened reopens.
	t3 := t2.Add(48 * time.Hour)
	if err := store.ApplyTrackerAction(ctx, id, "Reopened", t3, 12); err != nil {
		t.Fatal(err)
	}
	state, closedAt = tsState(t, ctx, store, id)
	if state != "open" || closedAt != nil {
		t.Fatalf("after newer Reopened: state=%q closed_at=%v", state, closedAt)
	}
	// Non-state actions (Commented, Work logged) change nothing.
	if err := store.ApplyTrackerAction(ctx, id, "Commented", t3.Add(time.Hour), 13); err != nil {
		t.Fatal(err)
	}
	if state, _ := tsState(t, ctx, store, id); state != "open" {
		t.Fatal("a non-state action must not change issue_state")
	}
}

// TestApplyTrackerActionNeverTouchesNativeIssues — the LINK path can
// resolve to a native GitHub issue (the Jira→GitHub migration case);
// its state is API-owned and the writer must refuse it.
func TestApplyTrackerActionNeverTouchesNativeIssues(t *testing.T) {
	store, ctx := sbConnect(t)
	id := tsSeedIssue(t, ctx, store, 944_148_200, "open") // POSITIVE = native
	if err := store.ApplyTrackerAction(ctx, id, "Resolved", time.Now(), 14); err != nil {
		t.Fatal(err)
	}
	if state, _ := tsState(t, ctx, store, id); state != "open" {
		t.Fatalf("native issue state changed to %q — forge-owned fields are untouchable (C3a precedence)", state)
	}
}

// TestBackfillSyntheticJiraStateEndToEnd — the ledgered history walk:
// Created + Resolved notifications already in email_message close the
// synthetic; a second run is a no-op (event-time guard idempotence).
func TestBackfillSyntheticJiraStateEndToEnd(t *testing.T) {
	store, ctx := sbConnect(t)
	id := tsSeedIssue(t, ctx, store, -944_148_300, "open")
	created := time.Date(2026, 2, 1, 9, 0, 0, 0, time.UTC)
	resolved := time.Date(2026, 2, 20, 9, 0, 0, 0, time.UTC)
	for i, n := range []struct {
		action string
		at     time.Time
	}{{"Created", created}, {"Resolved", resolved}} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message
			(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class, linked_external_key)
			VALUES ($1, 6, $2, 'jira@apache.org', $3, $4, 'issue_event', $5)
			ON CONFLICT (message_id_header) DO UPDATE SET sent_at = EXCLUDED.sent_at`,
			tsRepoID, "<_avts-"+n.action+">", "[jira] ["+n.action+"] ("+tsKey+") t", n.at, tsKey)
		_ = i
	}
	if err := store.BackfillSyntheticJiraState(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	state, closedAt := tsState(t, ctx, store, id)
	if state != "closed" || closedAt == nil || !closedAt.Equal(resolved) {
		t.Fatalf("after backfill: state=%q closed_at=%v (want closed @ %v)", state, closedAt, resolved)
	}
	if err := store.BackfillSyntheticJiraState(ctx, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if s2, _ := tsState(t, ctx, store, id); s2 != "closed" {
		t.Fatal("second backfill run must be a no-op")
	}
}

// TestGapNumbersSyntheticMaskBehavior — behavioral half of the sweep:
// with only a synthetic issue #77 present, gap detection must NOT see
// number 77 as collected, and the open refresher must not fetch it.
func TestGapNumbersSyntheticMaskBehavior(t *testing.T) {
	store, ctx := sbConnect(t)
	tsSeedIssue(t, ctx, store, -944_148_400, "open")
	nums, err := store.GetCollectedIssueNumbers(ctx, tsRepoID)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range nums {
		if n == 77 {
			t.Fatal("synthetic issue_number 77 masks native GitHub #77 in gap detection")
		}
	}
	open, err := store.GetOpenIssueNumbers(ctx, tsRepoID)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range open {
		if n == 77 {
			t.Fatal("permanently-open synthetic feeds the open-item refresher a bogus by-number fetch")
		}
	}
}

// TestTrackerActionDBSpellingParity — internal/db cannot import
// internal/mailinglist (no sibling feature-package edges), so it
// carries its own spelling of the action extraction; this pin runs the
// shared fixtures through BOTH (the test file may import mailinglist).
func TestTrackerActionDBSpellingParity(t *testing.T) {
	for _, subject := range []string{
		"[jira] [Created] (KAFKA-123) add a thing",
		"[jira] [Resolved] (ARROW-99) fix",
		"[jira] [Work logged] (HIVE-2) hours",
		"Re: [jira] [Created] (KAFKA-123) reply",
		"[DISCUSS] design",
	} {
		if got, want := trackerActionFromSubject(subject), mailinglist.TrackerActionFromSubject(subject); got != want {
			t.Errorf("%q: db spelling = %q, mailinglist spelling = %q — one action vocabulary (SR-17)", subject, got, want)
		}
	}
}
