// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"
)

// Item 83 (2026-10-06): a successful API collection whose facade failed
// hands CompleteJob success=true WITH a message. The store records the
// message in last_error (the monitor's err badge, the operator's grep) and
// still anchors last_collected — the two must not be coupled, or a
// persistent clone failure would re-walk the whole API history every
// cycle. A clean success passes "" and clears the column.
func TestCompleteJobRecordsAMessageOnASuccessfulJob(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	id := seedRepoForDeps(t, store, ctx, "_av83_facade", "fixture")
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
		VALUES ($1, 100, 'queued', NOW()) ON CONFLICT (repo_id) DO NOTHING`, id)
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
	})

	read := func() (lastErr *string, lastCollected *time.Time) {
		t.Helper()
		if err := store.pool.QueryRow(ctx,
			`SELECT last_error, last_collected FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).
			Scan(&lastErr, &lastCollected); err != nil {
			t.Fatal(err)
		}
		return lastErr, lastCollected
	}

	started := time.Now().Add(-time.Minute).Truncate(time.Second)
	const msg = "facade collection failed: git clone: mkdir /data/aveloxis-repos: permission denied"
	if err := store.CompleteJob(ctx, id, true, started, time.Hour, 3, 1, 0, 0, 0, 0, 0, 10, msg, 1); err != nil {
		t.Fatal(err)
	}
	lastErr, lastCollected := read()
	if lastErr == nil || *lastErr != msg {
		t.Fatalf("a successful job's message must be recorded in last_error, got %v", lastErr)
	}
	if lastCollected == nil || !lastCollected.Equal(started) {
		t.Fatalf("a successful job anchors last_collected at its start regardless of the message, got %v want %v", lastCollected, started)
	}

	// The next clean success clears it.
	later := started.Add(30 * time.Second)
	if err := store.CompleteJob(ctx, id, true, later, time.Hour, 3, 1, 0, 0, 0, 0, 5, 10, "", 1); err != nil {
		t.Fatal(err)
	}
	if lastErr, _ = read(); lastErr != nil {
		t.Fatalf("a clean success must clear last_error, got %q", *lastErr)
	}

	// A failed job still records its message and keeps the old anchor.
	if err := store.CompleteJob(ctx, id, false, time.Time{}, time.Hour, 0, 0, 0, 0, 0, 0, 0, 10, "rate limited", 1); err != nil {
		t.Fatal(err)
	}
	lastErr, lastCollected = read()
	if lastErr == nil || *lastErr != "rate limited" || lastCollected == nil || !lastCollected.Equal(later) {
		t.Fatalf("a failed job records its message and keeps the anchor: err=%v anchor=%v", lastErr, lastCollected)
	}
}
