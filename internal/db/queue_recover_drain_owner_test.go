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
// staging. A HEAL owner (HealWorkerIDPrefix + the ":drain" suffix) whose
// lock is fresher than the stale-lock window is left alone; a stale heal
// owner and any plain job owner (a dead serve's rows) are reclaimed as
// before. Review round 1: the exemption is keyed on the heal's identity, not
// on any fresh drain owner — a serve that crashed within the hour still
// heartbeats-fresh, and its drain rows must be reclaimed so the startup
// drain pass can re-park them from staging.
func TestRecoverOtherWorkerLocksLeavesALiveHealAlone(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	me, heal, dead, crashed := "w-me-"+suffix, HealWorkerIDPrefix+"host-1-"+suffix, "w-dead-"+suffix, "w-crashed-"+suffix
	// A serve on a host NAMED "gap-heal-…" (review round 2): its ID is
	// "<host>-<HHMMSS>", spelled here with the literal, never the constant —
	// the prefix's ":" is what keeps it from matching.
	colliding := "gap-heal-host-120000"
	live := seedDrainQueueRow(t, ctx, store, "live-"+suffix, "collecting", strptr(drainLockedBy(heal)))
	stale := seedDrainQueueRow(t, ctx, store, "stale-"+suffix, "collecting", strptr(drainLockedBy(heal)))
	job := seedDrainQueueRow(t, ctx, store, "job-"+suffix, "collecting", strptr(dead))
	crashedDrain := seedDrainQueueRow(t, ctx, store, "crashed-"+suffix, "collecting", strptr(drainLockedBy(crashed)))
	collidingDrain := seedDrainQueueRow(t, ctx, store, "colliding-"+suffix, "collecting", strptr(drainLockedBy(colliding)))
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.collection_queue SET locked_at = NOW() - interval '2 hours' WHERE repo_id = $1`, stale)

	n, err := store.RecoverOtherWorkerLocks(ctx, me, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Errorf("recovered %d rows, want 4 (the stale heal row, the dead worker's job, the crashed serve's fresh drain row, the gap-heal-named host's serve)", n)
	}
	for _, tc := range []struct {
		repo  int64
		name  string
		owner string
	}{{live, "live heal's parked row", drainLockedBy(heal)}, {stale, "stale heal row", ""}, {job, "dead worker's job", ""}, {crashedDrain, "crashed serve's fresh drain row", ""}, {collidingDrain, "a serve on a gap-heal-named host", ""}} {
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
