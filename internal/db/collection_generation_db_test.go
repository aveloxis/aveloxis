// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

// TestCollectionGeneration — v0.29.71 (O11 option 4): the API caches
// per-repository answers under the repositories' collection generation
// (every member's queue state), so a new collection changes the key. Repositories
// with no queue row, or never collected, read as a fixed value; the order
// of the ids does not matter.
func TestCollectionGeneration(t *testing.T) {
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
	var a, b int64
	for i, id := range []*int64{&a, &b} {
		if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, 'r', '_avgen', 1) RETURNING repo_id`, "https://github.com/_avgen/r"+string(rune('a'+i))).Scan(id); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN ($1, $2)`, a, b)
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id IN ($1, $2)`, a, b)
	})
	none, err := store.CollectionGeneration(ctx, []int64{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, '2026-09-01T00:00:00Z'), ($2, NULL)`, a, b); err != nil {
		t.Fatal(err)
	}
	g1, err := store.CollectionGeneration(ctx, []int64{a, b})
	if err != nil {
		t.Fatal(err)
	}
	g1r, _ := store.CollectionGeneration(ctx, []int64{b, a})
	if g1 == none || g1 != g1r {
		t.Errorf("generation after a collection = %q (none %q, reversed ids %q): want a new value independent of order", g1, none, g1r)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = '2026-09-02T00:00:00Z' WHERE repo_id = $1`, b); err != nil {
		t.Fatal(err)
	}
	g2, _ := store.CollectionGeneration(ctx, []int64{a, b})
	if g2 == g1 {
		t.Errorf("a newer collection of one repository must change the generation (%q)", g2)
	}
	if ga, _ := store.CollectionGeneration(ctx, []int64{a}); ga == g2 {
		t.Error("the generation must follow the ids asked about")
	}
	// Review round 1 (F2): the maximum last_collected missed two changes.
	// A job that FAILS writes rows but never advances last_collected, and
	// CompleteJob stamps updated_at either way.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET updated_at = updated_at + interval '1 second' WHERE repo_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	g3, _ := store.CollectionGeneration(ctx, []int64{a, b})
	if g3 == g2 {
		t.Error("a finished job that left last_collected alone (a failure, a gap heal) must change the generation")
	}
	// Out of order: a finishes after b with an OLDER start anchor, so the
	// maximum last_collected is unchanged.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = '2026-09-01T12:00:00Z' WHERE repo_id = $1`, a); err != nil {
		t.Fatal(err)
	}
	g4, _ := store.CollectionGeneration(ctx, []int64{a, b})
	if g4 == g3 {
		t.Error("a member finishing after a newer-anchored member must change the generation")
	}
	// Review round 2 (R2-2): the writers themselves, not hand-written
	// UPDATEs. A running job's heartbeats must NOT change the key (it would
	// churn every 30 s for every repository under collection); a job that
	// ends — here a failure, which leaves last_collected alone — must.
	// a runs a job; b is drain-parked the way LockReposForDrain parks it
	// (round 3 R3-1: a plain worker id matched no row, so the drain
	// heartbeat was driven over nothing). Both locks start an hour old so
	// the heartbeats provably matched.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET status = 'collecting',
		locked_by = CASE WHEN repo_id = $1 THEN '_avgen-w' ELSE $3 END, locked_at = NOW() - interval '1 hour'
		WHERE repo_id IN ($1, $2)`, a, b, drainLockedBy("_avgen-w")); err != nil {
		t.Fatal(err)
	}
	running, _ := store.CollectionGeneration(ctx, []int64{a, b})
	if err := store.HeartbeatJob(ctx, a, "_avgen-w"); err != nil {
		t.Fatal(err)
	}
	if err := store.HeartbeatDrainLocks(ctx, "_avgen-w"); err != nil {
		t.Fatal(err)
	}
	var fresh int
	if err := store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_queue
		WHERE repo_id IN ($1, $2) AND locked_at > NOW() - interval '1 minute'`, a, b).Scan(&fresh); err != nil || fresh != 2 {
		t.Fatalf("both heartbeats must match their row (fresh locks %d, %v) — the fixture's premise", fresh, err)
	}
	if hb, _ := store.CollectionGeneration(ctx, []int64{a, b}); hb != running {
		t.Error("a heartbeat (job or drain) must not change the generation (it stamps only locked_at)")
	}
	if err := store.CompleteJob(ctx, a, false, time.Time{}, time.Hour, 0, 0, 0, 0, 0, 0, 0, 0, "probe failure", 1); err != nil {
		t.Fatal(err)
	}
	if failed, _ := store.CollectionGeneration(ctx, []int64{a, b}); failed == running {
		t.Error("CompleteJob of a FAILED job must change the generation (it stamps updated_at; last_collected stays)")
	}
}
