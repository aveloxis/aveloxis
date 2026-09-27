// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestReleaseDrainLockKeepsDueForTheHeal pins worklist item 56: both drain
// releases set due_at = NOW(), so every repository heal-collection-gaps
// locked became due for recollection the moment the heal let go of it,
// whatever its place in the cycle. serve's drain wants NOW() (the drain
// processed pre-staged data; a fresh fetch reconciles it); the heal does
// not — it leaves due_at where it was. The caller says which.
func TestReleaseDrainLockKeepsDueForTheHeal(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	me := "w-keepdue-" + suffix
	// seedDrainQueueRow sets due_at = NOW() + 7 days.
	keep := seedDrainQueueRow(t, ctx, store, "keep-"+suffix, "collecting", strptr(drainLockedBy(me)))
	now := seedDrainQueueRow(t, ctx, store, "now-"+suffix, "collecting", strptr(drainLockedBy(me)))
	keepAll := seedDrainQueueRow(t, ctx, store, "keepall-"+suffix, "collecting", strptr(drainLockedBy(me)))

	if err := store.ReleaseDrainLock(ctx, keep, me, DrainReleaseKeepDue); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseDrainLock(ctx, now, me, DrainReleaseDueNow); err != nil {
		t.Fatal(err)
	}
	if n, err := store.ReleaseDrainLocks(ctx, me, DrainReleaseKeepDue); err != nil || n != 1 {
		t.Fatalf("ReleaseDrainLocks = %d, %v; want 1 (the remaining parked row)", n, err)
	}
	for _, tc := range []struct {
		repo int64
		name string
		soon bool
	}{{keep, "single release, keep", false}, {now, "single release, due now", true}, {keepAll, "bulk release, keep", false}} {
		var status string
		var hours float64
		if err := store.pool.QueryRow(ctx, `SELECT status, EXTRACT(EPOCH FROM (due_at - NOW()))/3600 FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, tc.repo).Scan(&status, &hours); err != nil {
			t.Fatal(err)
		}
		if status != "queued" {
			t.Errorf("%s: status %q, want queued", tc.name, status)
		}
		if tc.soon && hours > 1 {
			t.Errorf("%s: due in %.1f h, want now", tc.name, hours)
		}
		if !tc.soon && hours < 24*6 {
			t.Errorf("%s: due in %.1f h, want the seeded ~7 days kept", tc.name, hours)
		}
	}
}
