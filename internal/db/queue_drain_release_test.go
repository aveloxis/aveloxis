// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.29.64 (2026-09-23 operator report: "with serve not running, the
// monitor says 107 jobs are collecting"; confirmed in production as 107
// drain-parked rows, locked_by '<worker>:drain'). The startup drain parks
// its whole set as status='collecting'; a clean stop released only rows
// owned by the worker ID itself, so the parked set stayed "collecting"
// until the next start, and while serve ran it inflated the monitor's
// Collecting count past the worker count.

func seedDrainQueueRow(t *testing.T, ctx context.Context, store *PostgresStore, name, status string, lockedBy *string) int64 {
	t.Helper()
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Owner: "_avdrainrel", Name: name,
		GitURL: "https://github.com/_avdrainrel/" + name, Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("upsert repo: %v", err)
	}
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, locked_by, locked_at, last_collected)
		VALUES ($1, $2, NOW() + interval '7 days', $3, CASE WHEN $3::text IS NULL THEN NULL ELSE NOW() END, NOW() - interval '1 day')
		ON CONFLICT (repo_id) DO UPDATE SET status = EXCLUDED.status, locked_by = EXCLUDED.locked_by,
			locked_at = EXCLUDED.locked_at, due_at = EXCLUDED.due_at, last_collected = EXCLUDED.last_collected`,
		repoID, status, lockedBy)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	return repoID
}

// TestReleaseDrainLocksReleasesOnlyThisWorkersParkedSet — the shutdown
// release returns THIS process's drain-parked rows to 'queued' and touches
// nothing else: another process's parked set, this worker's own job
// locks (releaseOurLocks' job), and last_collected (a release is not a
// completed collection).
func TestReleaseDrainLocksReleasesOnlyThisWorkersParkedSet(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	me, other := "w-me-"+suffix, "w-other-"+suffix

	mine1 := seedDrainQueueRow(t, ctx, store, "mine1-"+suffix, "collecting", strptr(drainLockedBy(me)))
	mine2 := seedDrainQueueRow(t, ctx, store, "mine2-"+suffix, "collecting", strptr(drainLockedBy(me)))
	theirs := seedDrainQueueRow(t, ctx, store, "theirs-"+suffix, "collecting", strptr(drainLockedBy(other)))
	myJob := seedDrainQueueRow(t, ctx, store, "myjob-"+suffix, "collecting", strptr(me))

	n, err := store.ReleaseDrainLocks(ctx, me)
	if err != nil {
		t.Fatalf("ReleaseDrainLocks: %v", err)
	}
	if n != 2 {
		t.Errorf("released %d rows, want this worker's 2 parked rows", n)
	}
	for _, c := range []struct {
		id         int64
		wantStatus string
		wantOwner  string
	}{
		{mine1, "queued", ""},
		{mine2, "queued", ""},
		{theirs, "collecting", drainLockedBy(other)},
		{myJob, "collecting", me},
	} {
		var status, owner string
		var lastCollectedSet, dueNow bool
		if err := store.pool.QueryRow(ctx, `
			SELECT status, COALESCE(locked_by, ''), last_collected IS NOT NULL, due_at <= NOW()
			  FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, c.id).Scan(&status, &owner, &lastCollectedSet, &dueNow); err != nil {
			t.Fatal(err)
		}
		if status != c.wantStatus || owner != c.wantOwner {
			t.Errorf("repo %d: status=%q owner=%q, want %q %q", c.id, status, owner, c.wantStatus, c.wantOwner)
		}
		if !lastCollectedSet {
			t.Errorf("repo %d: last_collected was cleared — a release is not a collection", c.id)
		}
		if c.wantStatus == "queued" && !dueNow {
			t.Errorf("repo %d: a released parked repo is due now (its staging still needs the next start's drain)", c.id)
		}
	}
	if n, err := store.ReleaseDrainLocks(ctx, me); err != nil || n != 0 {
		t.Errorf("a second release is a no-op: %d %v", n, err)
	}
}

// TestQueueStatsCountsDrainParkedSeparately — the monitor's Collecting
// count is real jobs only; drain-parked rows are their own "draining"
// count, and the total still covers every row.
func TestQueueStatsCountsDrainParkedSeparately(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	before, err := store.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	seedDrainQueueRow(t, ctx, store, "q-"+suffix, "queued", nil)
	seedDrainQueueRow(t, ctx, store, "c-"+suffix, "collecting", strptr("w-"+suffix))
	seedDrainQueueRow(t, ctx, store, "d1-"+suffix, "collecting", strptr(drainLockedBy("w-"+suffix)))
	seedDrainQueueRow(t, ctx, store, "d2-"+suffix, "collecting", strptr(drainLockedBy("other-"+suffix)))
	after, err := store.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]int{"queued": 1, "collecting": 1, "draining": 2, "total": 4} {
		if got := after[key] - before[key]; got != want {
			t.Errorf("%s grew by %d, want %d", key, got, want)
		}
	}
}
