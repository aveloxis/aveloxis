// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRecoverOtherWorkerLocksLeavesALiveHealAlone pins worklist item 54:
// serve's startup reclaim took every 'collecting' row owned by another
// worker ID — including a heal-collection-gaps run still alive (its
// heartbeat keeps locked_at fresh), whose parked repositories then went back
// to 'queued' while it healed them, and routine collection could purge its
// staging. A drain owner (the ":drain" suffix) whose lock is fresher than the
// stale-lock window is left alone; a stale drain owner and any plain job
// owner (a dead serve's rows) are reclaimed as before.
func TestRecoverOtherWorkerLocksLeavesALiveHealAlone(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	me, heal, dead := "w-me-"+suffix, "heal-"+suffix, "w-dead-"+suffix
	live := seedDrainQueueRow(t, ctx, store, "live-"+suffix, "collecting", strptr(drainLockedBy(heal)))
	stale := seedDrainQueueRow(t, ctx, store, "stale-"+suffix, "collecting", strptr(drainLockedBy(heal)))
	job := seedDrainQueueRow(t, ctx, store, "job-"+suffix, "collecting", strptr(dead))
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.collection_queue SET locked_at = NOW() - interval '2 hours' WHERE repo_id = $1`, stale)

	n, err := store.RecoverOtherWorkerLocks(ctx, me, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("recovered %d rows, want 2 (the stale drain row and the dead worker's job)", n)
	}
	for _, tc := range []struct {
		repo  int64
		name  string
		owner string
	}{{live, "live heal's parked row", drainLockedBy(heal)}, {stale, "stale drain row", ""}, {job, "dead worker's job", ""}} {
		var status string
		var lockedBy *string
		if err := store.pool.QueryRow(ctx, `SELECT status, locked_by FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, tc.repo).Scan(&status, &lockedBy); err != nil {
			t.Fatal(err)
		}
		got := ""
		if lockedBy != nil {
			got = *lockedBy
		}
		if got != tc.owner || (tc.owner == "" && status != "queued") || (tc.owner != "" && status != "collecting") {
			t.Errorf("%s: status %q owner %q; want owner %q", tc.name, status, got, tc.owner)
		}
	}
}
