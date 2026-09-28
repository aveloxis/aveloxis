// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
	"time"
)

// TestRealignLeavesAFailedRowsDueDate (worklist 68, the 2026-09-28 kate log):
// a failure sets due_at = NOW() + interval and leaves last_collected at the
// last SUCCESS, so the startup realign's last_collected + interval put a
// failed row back in the past — eight repositories failed within seconds of
// each of seven starts, and the giants restarted their 6–21 h attempts. A
// row whose last attempt failed (last_error set) keeps the due date its
// failure gave it; a row whose last attempt succeeded still realigns.
func TestRealignLeavesAFailedRowsDueDate(t *testing.T) {
	store, ctx := realignConnect(t)
	t.Cleanup(store.Close) // SR-9: a deferred Close runs before the seed cleanups
	interval := 72 * time.Hour
	old := time.Now().Add(-100 * 24 * time.Hour).UTC().Truncate(time.Second)
	futureDue := time.Now().Add(interval).UTC().Truncate(time.Second)

	ok := seedRealignRepo(ctx, t, store, "ok")
	seedQueueRow(ctx, t, store, ok, "", &old, futureDue)
	failed := seedRealignRepo(ctx, t, store, "failed")
	seedQueueRow(ctx, t, store, failed, "", &old, futureDue)
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_error = 'graphql PR batch: boom' WHERE repo_id = $1`, failed); err != nil {
		t.Fatal(err)
	}

	if _, err := store.RealignDueDates(ctx, interval, 1); err != nil {
		t.Fatal(err)
	}
	if due, _, _, _ := readQueueRow(ctx, t, store, ok); !due.Equal(old.Add(interval)) {
		t.Errorf("a succeeded row realigned to %s; want last_collected + interval = %s", due, old.Add(interval))
	}
	if due, _, _, _ := readQueueRow(ctx, t, store, failed); !approxEqual(due, futureDue, time.Second) {
		t.Errorf("a failed row's due_at moved to %s; want its failure's %s kept (realigning to last_collected + interval re-runs it at every start)", due, futureDue)
	}
}
