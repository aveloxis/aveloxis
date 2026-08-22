// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// since_boundary_test.go — v0.27.139: the blind-window fix's
// CompleteJob contract. Root cause (podman-desktop, production,
// verified to the minute): determineSince computed `now − D` and never
// read last_collected, so every incremental round skipped items whose
// final update fell between the previous round's start and now−D. The
// fix anchors last_collected at JOB START on success and freezes it on
// failure/skip; determineSince returns it verbatim.

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Source-contract pins (no DB).

func TestCompleteJobAnchorsLastCollectedAtStart(t *testing.T) {
	s := srctest.Read(t, "internal/db/queue.go")
	if !strings.Contains(s, "startedAt time.Time") {
		t.Error("CompleteJob must take the job's startedAt — completion-time stamping loses the round's own duration from the next window")
	}
	if !strings.Contains(s, "last_collected = CASE WHEN $12::timestamptz IS NOT NULL THEN $12::timestamptz ELSE last_collected END") {
		t.Error("last_collected must advance ONLY to the supplied start anchor, never to NOW(), and never on a zero anchor")
	}
	// Round-23: the failure invariant is enforced IN THE STORE — a
	// mistaken caller passing success=false with a nonzero startedAt
	// must not create a blind window (the pre-fix runjob_lifecycle_test
	// release helper did exactly that).
	body := srctest.FuncBody(t, s, "func (s *PostgresStore) CompleteJob(")
	if !strings.Contains(body, "if success {") {
		t.Error("CompleteJob must derive the anchor from success itself, not trust caller discipline")
	}
	if strings.Contains(s, "last_collected = NOW()") {
		t.Error("the pre-v0.27.139 unconditional last_collected = NOW() is back — failed/skipped passes would again stamp coverage they never collected")
	}
}

func TestFailAndSkipPassZeroStartAnchor(t *testing.T) {
	s := srctest.Read(t, "internal/scheduler/scheduler.go")
	fail := srctest.FuncBody(t, s, "func (s *Scheduler) failJob(")
	if !strings.Contains(fail, "time.Time{}") {
		t.Error("failJob must pass a zero startedAt — a failed pass never advances last_collected")
	}
	skip := srctest.FuncBody(t, s, "func (s *Scheduler) skipJob(")
	if !strings.Contains(skip, "time.Time{}") {
		t.Error("skipJob must pass a zero startedAt — a skip collects nothing and must not stamp coverage")
	}
	if !strings.Contains(s, "*job.LastCollected") {
		t.Error("determineSince must return the stored last_collected verbatim")
	}
	if strings.Contains(srctest.FuncBody(t, s, "func (s *Scheduler) determineSince("), "RecollectAfterDuration") {
		t.Error("determineSince must not derive since from the recollect window — that is the blind-window bug verbatim")
	}
}

func TestPRBreakoutBoundaryIsInclusive(t *testing.T) {
	// Both PR listings walk UPDATED_AT DESC and break out at the since
	// boundary; the break must fire only on STRICTLY-older items so an
	// item updated exactly at since is collected (matching the issues
	// connection's inclusive filterBy.since). Equality-dropping is
	// silent loss; equality re-collection is an idempotent upsert.
	gql := srctest.Read(t, "internal/platform/github/graphql_listing.go")
	if !strings.Contains(gql, "n.UpdatedAt.Before(since)") {
		t.Error("GraphQL PR breakout must use Before(since) — !After(since) drops the boundary item and everything behind it")
	}
	if strings.Contains(gql, "!n.UpdatedAt.After(since)") {
		t.Error("the exclusive !After(since) breakout is back in graphql_listing.go")
	}
	rest := srctest.Read(t, "internal/platform/github/client.go")
	if !strings.Contains(rest, "raw.UpdatedAt.Before(since)") {
		t.Error("REST PR breakout must stay Before(since) (inclusive of equality)")
	}
}

func TestCLICollectUsesQueueLastCollected(t *testing.T) {
	s := srctest.Read(t, "cmd/aveloxis/main.go")
	if !strings.Contains(s, "GetRepoLastCollected(") {
		t.Error("CLI collect must derive its incremental bound from the queue's last_collected")
	}
	if strings.Contains(s, "time.Now().AddDate(0, 0, -cfg.Collection.DaysUntilRecollect)") {
		t.Error("the CLI's now−days_until_recollect lower bound is back — the blind-window bug, CLI edition")
	}
}

// Behavioral (AVELOXIS_TEST_DB): the three CompleteJob paths.
func TestCompleteJobLastCollectedSemantics(t *testing.T) {
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

	repoID := int64(944_139_001)
	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := store.pool.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	mustExec(`INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avsince/probe', '_avsince', 'probe', 1)
		ON CONFLICT (repo_id) DO NOTHING`, repoID)
	mustExec(`INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		VALUES ($1, 100, 'queued', NOW()) ON CONFLICT (repo_id) DO UPDATE SET last_collected = NULL`, repoID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})

	readLC := func() *time.Time {
		t.Helper()
		lc, err := store.GetRepoLastCollected(ctx, repoID)
		if err != nil {
			t.Fatal(err)
		}
		return lc
	}

	// 1. FAILED first pass: last_collected stays NULL (the next round
	// stays a FULL collection — the cohort-A class killer).
	if err := store.CompleteJob(ctx, repoID, false, time.Time{}, time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "boom"); err != nil {
		t.Fatal(err)
	}
	if lc := readLC(); lc != nil {
		t.Fatalf("failed first pass must keep last_collected NULL, got %v", lc)
	}

	// 2. SUCCESS: last_collected = the supplied start anchor exactly.
	start := time.Now().Add(-45 * time.Minute).UTC().Truncate(time.Microsecond)
	if err := store.CompleteJob(ctx, repoID, true, start, time.Hour, 1, 1, 0, 0, 0, 0, 0, 100, ""); err != nil {
		t.Fatal(err)
	}
	lc := readLC()
	if lc == nil || !lc.Equal(start) {
		t.Fatalf("success must anchor last_collected at job START %v, got %v", start, lc)
	}

	// 2b. MISTAKEN CALLER (round-23): success=false with a NONZERO
	// startedAt must NOT advance the anchor — the store enforces the
	// failure invariant, not caller discipline.
	if err := store.CompleteJob(ctx, repoID, false, time.Now(), time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "mistake"); err != nil {
		t.Fatal(err)
	}
	if lcM := readLC(); lcM == nil || !lcM.Equal(start) {
		t.Fatalf("failure with nonzero startedAt must preserve the anchor %v, got %v", start, lcM)
	}

	// 3. A LATER failure preserves the successful anchor (and still
	// advances due_at for retry pacing).
	var dueBefore time.Time
	if err := store.pool.QueryRow(ctx, `SELECT due_at FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&dueBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteJob(ctx, repoID, false, time.Time{}, 2*time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "transient"); err != nil {
		t.Fatal(err)
	}
	if lc2 := readLC(); lc2 == nil || !lc2.Equal(start) {
		t.Fatalf("failure must PRESERVE the last successful anchor %v, got %v", start, lc2)
	}
	var dueAfter time.Time
	var lastErr *string
	if err := store.pool.QueryRow(ctx, `SELECT due_at, last_error FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&dueAfter, &lastErr); err != nil {
		t.Fatal(err)
	}
	if !dueAfter.After(dueBefore) {
		t.Error("failure must still advance due_at (retry pacing)")
	}
	if lastErr == nil || *lastErr != "transient" {
		t.Errorf("failure must record last_error, got %v", lastErr)
	}
}
