// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A7) — the archived-repo cadence stretch. SR-10: the knob
// is tested config-value → observable due_at, and SR-17/SR-18: ONE
// shared SQL spelling enforced inside BOTH due_at writers.

// Source pins: the shared CASE constant exists and both writers use
// it (fix-every-site made structural).
func TestArchivedStretchSharedSpelling(t *testing.T) {
	src := readSourceFile(t, "queue.go")
	if !strings.Contains(src, "const archivedStretchCaseSQL") {
		t.Fatal("archivedStretchCaseSQL shared constant missing")
	}
	// One Sprintf per writer (CompleteJob + RealignDueDates;
	// Realign's SET and idempotency predicate share its stretch var).
	if strings.Count(src, "fmt.Sprintf(archivedStretchCaseSQL,") != 2 {
		t.Error("both due_at writers (CompleteJob + RealignDueDates) must apply the shared stretch spelling — exactly 2 Sprintf sites")
	}
	// Config accessor is the single default layer (SR-10).
	cfgSrc := srctest.Read(t, "internal/config/config.go")
	if !strings.Contains(cfgSrc, "func (c *CollectionConfig) ArchivedRecollectMultiplierValue()") {
		t.Error("ArchivedRecollectMultiplierValue accessor missing (the single default layer)")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

// Config value → observable due_at, through BOTH writers.
func TestArchivedCadenceStretchEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	seedOne := func(owner string, archived bool) int64 {
		t.Helper()
		id := seedRepoForDeps(t, store, ctx, owner, "fixture")
		if archived {
			mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET repo_archived = TRUE WHERE repo_id = $1`, id)
		}
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			VALUES ($1, 100, 'queued', NOW()) ON CONFLICT (repo_id) DO NOTHING`, id)
		t.Cleanup(func() {
			cleanupExecRetry(context.Background(), store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		})
		return id
	}
	liveID := seedOne("_avarch_live", false)
	archID := seedOne("_avarch_frozen", true)

	interval := 24 * time.Hour
	const mult = 6
	for _, id := range []int64{liveID, archID} {
		if err := store.CompleteJob(ctx, id, true, time.Now(), interval,
			0, 0, 0, 0, 0, 0, 0, 0, "", mult); err != nil {
			t.Fatal(err)
		}
	}
	dueOf := func(id int64) time.Time {
		t.Helper()
		var due time.Time
		if err := store.pool.QueryRow(ctx,
			`SELECT due_at FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&due); err != nil {
			t.Fatal(err)
		}
		return due
	}
	within := func(got time.Time, want time.Duration) bool {
		diff := time.Until(got) - want
		return diff > -2*time.Minute && diff < 2*time.Minute
	}
	if got := dueOf(liveID); !within(got, interval) {
		t.Errorf("live repo due_at must be ~NOW+1d, got %v", got)
	}
	if got := dueOf(archID); !within(got, mult*interval) {
		t.Errorf("archived repo due_at must be ~NOW+%dd (the stretch), got %v", mult, got)
	}

	// RealignDueDates applies the SAME rule: realign to a new
	// interval and re-check both shapes.
	newInterval := 48 * time.Hour
	if _, err := store.RealignDueDates(ctx, newInterval, mult); err != nil {
		t.Fatal(err)
	}
	// due_at = last_collected + interval×mult; last_collected ≈ now.
	if got := dueOf(liveID); !within(got, newInterval) {
		t.Errorf("realigned live repo due_at must be ~NOW+2d, got %v", got)
	}
	if got := dueOf(archID); !within(got, mult*newInterval) {
		t.Errorf("realigned archived repo due_at must be ~NOW+%dd, got %v", 2*mult, got)
	}

	// Multiplier 1 = no stretch (the documented off switch).
	if _, err := store.RealignDueDates(ctx, newInterval, 1); err != nil {
		t.Fatal(err)
	}
	if got := dueOf(archID); !within(got, newInterval) {
		t.Errorf("multiplier 1 must disable the stretch, got %v", got)
	}
}
