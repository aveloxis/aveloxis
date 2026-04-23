package db

// Integration tests for the RealignDueDates + CompleteJob interval-baking
// contract that underpins the v0.16.6 "changed days_until_recollect takes
// effect on restart" feature.
//
// These tests exercise the actual SQL against a live Postgres. They replace
// the pre-existing source-text tests in queue_realign_test.go (which only
// verify the function is spelled correctly and contains certain keywords —
// enough to pin the shape of the code, not enough to catch a runtime
// regression in interval parsing, timezone arithmetic, or the idempotency
// guard).
//
// Set AVELOXIS_TEST_DB to a scratch Postgres connection string to run.
// The schema must already be migrated — `aveloxis migrate --config …` once
// against the scratch DB is sufficient.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
)

// seedRealignRepo creates a single repo with a unique slug and returns its
// repo_id. The caller is responsible for any queue-row seeding. Each test
// uses a slug with a nanosecond suffix so parallel / repeated runs on the
// same DB never collide on UpsertRepo's ON CONFLICT (repo_git).
func seedRealignRepo(ctx context.Context, t *testing.T, store *PostgresStore, tag string) int64 {
	t.Helper()
	slug := fmt.Sprintf("_avrealign-%s-%d", tag, time.Now().UnixNano())
	id, err := store.UpsertRepo(ctx, &model.Repo{
		Owner:    "_avrealign",
		Name:     slug,
		GitURL:   fmt.Sprintf("https://github.com/_avrealign/%s", slug),
		Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("UpsertRepo(%s): %v", slug, err)
	}
	return id
}

// seedQueueRow inserts a row into aveloxis_ops.collection_queue with the
// given fields set explicitly. status defaults to 'queued' if empty, and
// lastCollected / dueAt are passed through untouched (no NOW() magic —
// tests pin exact timestamps to assert precise arithmetic).
func seedQueueRow(ctx context.Context, t *testing.T, store *PostgresStore,
	repoID int64, status string, lastCollected *time.Time, dueAt time.Time) {
	t.Helper()
	if status == "" {
		status = "queued"
	}
	// The queue row may already exist from UpsertRepo's side effects in
	// older versions; use INSERT ... ON CONFLICT so the seeding itself is
	// idempotent.
	_, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue
			(repo_id, priority, status, due_at, last_collected)
		VALUES ($1, 100, $2, $3, $4)
		ON CONFLICT (repo_id) DO UPDATE SET
			status = EXCLUDED.status,
			due_at = EXCLUDED.due_at,
			last_collected = EXCLUDED.last_collected,
			locked_by = NULL,
			locked_at = NULL,
			updated_at = NOW()`,
		repoID, status, dueAt, lastCollected)
	if err != nil {
		t.Fatalf("seedQueueRow(%d): %v", repoID, err)
	}
}

// readQueueRow returns (dueAt, lastCollected, status, updatedAt). Callers
// assert on whichever field the test cares about.
func readQueueRow(ctx context.Context, t *testing.T, store *PostgresStore,
	repoID int64) (dueAt time.Time, lastCollected *time.Time, status string, updatedAt time.Time) {
	t.Helper()
	err := store.pool.QueryRow(ctx, `
		SELECT due_at, last_collected, status, updated_at
		FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID,
	).Scan(&dueAt, &lastCollected, &status, &updatedAt)
	if err != nil {
		t.Fatalf("readQueueRow(%d): %v", repoID, err)
	}
	return
}

// realignConnect is the shared integration-test gate. Returns nil to signal
// skip (same shape as the other AVELOXIS_TEST_DB-gated files).
func realignConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return store, ctx
}

// approxEqual returns true if a and b are within tolerance. Used to absorb
// Postgres timestamp storage precision (microseconds) and any clock skew in
// the NOW()-driven seeding paths — but NOT used where the test is asserting
// exact arithmetic (last_collected + interval); those use strict equality.
func approxEqual(a, b time.Time, tolerance time.Duration) bool {
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return d <= tolerance
}

// --------------------------------------------------------------------
// RealignDueDates — the core behavior the v0.16.6 fix is supposed to
// guarantee.
// --------------------------------------------------------------------

// TestRealignDueDates_ShiftsDueAtOnConfigChange is the scenario the user
// reported failing: change days_until_recollect from 1 to 7, restart, and
// an already-queued row whose due_at was baked at completion under the old
// 1-day setting should be rewritten to last_collected + 7 days. If this
// test passes, the SQL is correct and the observed bug is either "serve
// was not restarted" or a rendering-layer issue (not a store issue).
func TestRealignDueDates_ShiftsDueAtOnConfigChange(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "shift")

	// Simulate the state left by CompleteJob under the OLD 1-day setting:
	// the row completed 6 hours ago, and due_at was baked at
	// last_collected + 1 day — i.e., 18 hours in the future.
	lastCollected := time.Now().Add(-6 * time.Hour).UTC()
	oldDueAt := lastCollected.Add(24 * time.Hour)
	seedQueueRow(ctx, t, store, repoID, "queued", &lastCollected, oldDueAt)

	// Now the operator edits aveloxis.json: days_until_recollect 1 → 7,
	// restarts aveloxis serve, scheduler.Run calls RealignDueDates.
	newInterval := 7 * 24 * time.Hour
	n, err := store.RealignDueDates(ctx, newInterval)
	if err != nil {
		t.Fatalf("RealignDueDates: %v", err)
	}
	if n < 1 {
		t.Errorf("rows_updated = %d, want >= 1 (the seeded row should have been realigned)", n)
	}

	dueAt, _, _, _ := readQueueRow(ctx, t, store, repoID)
	wantDueAt := lastCollected.Add(newInterval)
	// Strict equality: the SQL does last_collected + $1::interval with no
	// NOW() involvement, so the arithmetic should be exact modulo Postgres
	// timestamp storage (microseconds). Tolerate 1 ms to absorb that.
	if !approxEqual(dueAt, wantDueAt, time.Millisecond) {
		t.Errorf("due_at after realign = %v, want %v (last_collected + 7d). Delta: %v",
			dueAt, wantDueAt, dueAt.Sub(wantDueAt))
	}
}

// TestRealignDueDates_Idempotent pins the WHERE due_at <> last_collected +
// interval guard. Running the realignment twice with the same interval
// must leave updated_at alone on the second call — otherwise every restart
// would churn the updated_at column across the whole fleet.
func TestRealignDueDates_Idempotent(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "idem")
	lastCollected := time.Now().Add(-2 * time.Hour).UTC()
	seedQueueRow(ctx, t, store, repoID, "queued", &lastCollected, lastCollected.Add(24*time.Hour))

	interval := 3 * 24 * time.Hour
	// First call — should update this row.
	if _, err := store.RealignDueDates(ctx, interval); err != nil {
		t.Fatalf("first RealignDueDates: %v", err)
	}
	_, _, _, updatedAfterFirst := readQueueRow(ctx, t, store, repoID)

	// Sleep a beat so a spurious update would produce a distinguishable
	// updated_at; 1.5 seconds clears Postgres's default NOW() resolution.
	time.Sleep(1500 * time.Millisecond)

	// Second call with the same interval — the row's due_at already equals
	// last_collected + 3d, so the WHERE <> guard should exclude it.
	if _, err := store.RealignDueDates(ctx, interval); err != nil {
		t.Fatalf("second RealignDueDates: %v", err)
	}
	_, _, _, updatedAfterSecond := readQueueRow(ctx, t, store, repoID)

	if !updatedAfterFirst.Equal(updatedAfterSecond) {
		t.Errorf("updated_at changed on idempotent second call: %v → %v. "+
			"The WHERE due_at <> last_collected + interval guard is not filtering correctly.",
			updatedAfterFirst, updatedAfterSecond)
	}
}

// TestRealignDueDates_SkipsCollecting verifies that a worker mid-flight is
// not disturbed. The row's due_at stays at whatever value was there when
// the worker claimed it.
func TestRealignDueDates_SkipsCollecting(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "collecting")
	lastCollected := time.Now().Add(-48 * time.Hour).UTC()
	// due_at here is intentionally misaligned vs. last_collected + any new
	// interval — if RealignDueDates touched this row, the due_at would
	// change.
	originalDueAt := lastCollected.Add(24 * time.Hour)
	seedQueueRow(ctx, t, store, repoID, "collecting", &lastCollected, originalDueAt)

	if _, err := store.RealignDueDates(ctx, 7*24*time.Hour); err != nil {
		t.Fatalf("RealignDueDates: %v", err)
	}

	dueAt, _, status, _ := readQueueRow(ctx, t, store, repoID)
	if status != "collecting" {
		t.Errorf("status = %q, want 'collecting' (seeding failed?)", status)
	}
	if !approxEqual(dueAt, originalDueAt, time.Millisecond) {
		t.Errorf("due_at changed for a 'collecting' row: seed=%v, post=%v. "+
			"RealignDueDates must not disturb in-flight jobs.",
			originalDueAt, dueAt)
	}
}

// TestRealignDueDates_SkipsNullLastCollected verifies never-collected rows
// keep their initial due_at. Otherwise a fleet that has just been added
// (all last_collected IS NULL) would either crash (NULL + interval = NULL,
// then UPDATE sets due_at=NULL which is a NOT NULL violation) or get
// silently skipped. The SQL uses last_collected IS NOT NULL to make this
// explicit.
func TestRealignDueDates_SkipsNullLastCollected(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "nevercoll")
	// last_collected = NULL, due_at = now (typical state for a newly-added
	// repo that has not yet been picked up by a worker).
	initialDueAt := time.Now().UTC()
	seedQueueRow(ctx, t, store, repoID, "queued", nil, initialDueAt)

	if _, err := store.RealignDueDates(ctx, 7*24*time.Hour); err != nil {
		t.Fatalf("RealignDueDates: %v", err)
	}

	dueAt, lastCollected, _, _ := readQueueRow(ctx, t, store, repoID)
	if lastCollected != nil {
		t.Fatalf("last_collected = %v, want NULL (seeding failed?)", *lastCollected)
	}
	if !approxEqual(dueAt, initialDueAt, time.Millisecond) {
		t.Errorf("due_at changed for a NULL-last_collected row: seed=%v, post=%v. "+
			"Never-collected repos must keep their initial due_at.",
			initialDueAt, dueAt)
	}
}

// TestRealignDueDates_VariousDurations exercises the Go time.Duration →
// Postgres ::interval path across formats that Duration.String() commonly
// produces. If any of these fail at the SQL boundary (e.g., Postgres
// chokes on a "168h0m0s" literal), we catch it here instead of in
// production.
func TestRealignDueDates_VariousDurations(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	cases := []struct {
		name     string
		interval time.Duration
	}{
		{"1 day", 24 * time.Hour},
		{"3 days", 3 * 24 * time.Hour},
		{"7 days", 7 * 24 * time.Hour},
		{"14 days", 14 * 24 * time.Hour},
		{"30 days", 30 * 24 * time.Hour},
		{"90 days", 90 * 24 * time.Hour},
		// Non-whole-day values — Duration.String() for these produces
		// mixed-unit strings like "36h30m0s" which is the format the
		// Postgres interval parser is most likely to misinterpret.
		{"36h30m", 36*time.Hour + 30*time.Minute},
		{"1h", time.Hour},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repoID := seedRealignRepo(ctx, t, store, "durs-"+tc.name)
			lastCollected := time.Now().Add(-1 * time.Hour).UTC()
			seedQueueRow(ctx, t, store, repoID, "queued", &lastCollected,
				lastCollected.Add(time.Hour))

			if _, err := store.RealignDueDates(ctx, tc.interval); err != nil {
				t.Fatalf("RealignDueDates(%v): %v — Postgres likely rejected the "+
					"interval literal produced by Go's time.Duration.String() (%q)",
					tc.interval, err, tc.interval.String())
			}

			dueAt, _, _, _ := readQueueRow(ctx, t, store, repoID)
			wantDueAt := lastCollected.Add(tc.interval)
			// Allow 1 s tolerance for non-whole-day intervals because
			// Postgres's interval type is stored as (months, days, us)
			// and the days slot is timezone-aware in some
			// configurations. The arithmetic should still be within a
			// second for these magnitudes.
			if !approxEqual(dueAt, wantDueAt, time.Second) {
				t.Errorf("due_at = %v, want ~= %v. Delta: %v. Go Duration.String() = %q.",
					dueAt, wantDueAt, dueAt.Sub(wantDueAt), tc.interval.String())
			}
		})
	}
}

// --------------------------------------------------------------------
// CompleteJob — sibling of RealignDueDates that bakes the interval at
// completion time.
// --------------------------------------------------------------------

// TestCompleteJob_BakesDueAtFromRecollectAfter verifies that a successful
// CompleteJob writes due_at = NOW() + recollectAfter. Uses strict
// arithmetic within a window (bounded by the time between the CompleteJob
// call and the post-read).
func TestCompleteJob_BakesDueAtFromRecollectAfter(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "complete")
	// Start in 'collecting' to match real life — CompleteJob is called
	// while a worker holds the lock.
	seedQueueRow(ctx, t, store, repoID, "collecting", nil, time.Now().UTC())

	recollect := 7 * 24 * time.Hour
	before := time.Now().UTC()
	if err := store.CompleteJob(ctx, repoID, true, recollect,
		0, 0, 0, 0, 0, 0, 0, 0, ""); err != nil {
		t.Fatalf("CompleteJob: %v", err)
	}
	after := time.Now().UTC()

	dueAt, lastCollected, status, _ := readQueueRow(ctx, t, store, repoID)
	if status != "queued" {
		t.Errorf("status = %q, want 'queued' (CompleteJob must re-queue)", status)
	}
	if lastCollected == nil {
		t.Fatal("last_collected = NULL, want NOW() (CompleteJob must stamp it)")
	}

	// due_at must satisfy: before + interval <= due_at <= after + interval.
	wantLo := before.Add(recollect)
	wantHi := after.Add(recollect).Add(time.Second) // +1s slop for NOW() quantization
	if dueAt.Before(wantLo) || dueAt.After(wantHi) {
		t.Errorf("due_at = %v, want within [%v, %v]. CompleteJob interval-baking "+
			"is outside the expected NOW()+%v window.", dueAt, wantLo, wantHi, recollect)
	}
}

// TestCompleteJob_ThenRealign_FullConfigChangeScenario is the end-to-end
// integration test for the user's reported scenario. It replaces the
// source-text TestSchedulerCallsRealignDueDatesOnStartup with a test that
// exercises the actual SQL state machine:
//
//	state A → CompleteJob(1d) → state B → RealignDueDates(7d) → state C
//
// State B must have due_at ≈ now + 1d. State C must have due_at =
// last_collected + 7d. If the observed bug reproduces, state C's due_at
// will still reflect the 1-day interval, and this test will fail loudly.
func TestCompleteJob_ThenRealign_FullConfigChangeScenario(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "scenario")
	seedQueueRow(ctx, t, store, repoID, "collecting", nil, time.Now().UTC())

	// Old setting: 1 day.
	oldInterval := 24 * time.Hour
	if err := store.CompleteJob(ctx, repoID, true, oldInterval,
		0, 0, 0, 0, 0, 0, 0, 0, ""); err != nil {
		t.Fatalf("CompleteJob(1d): %v", err)
	}
	dueAfterComplete, lastCollected, _, _ := readQueueRow(ctx, t, store, repoID)
	if lastCollected == nil {
		t.Fatal("last_collected is NULL after CompleteJob")
	}
	// Sanity: state B should have due_at ≈ last_collected + 1d.
	if !approxEqual(dueAfterComplete, lastCollected.Add(oldInterval), 2*time.Second) {
		t.Errorf("after CompleteJob(1d): due_at = %v, want ≈ %v",
			dueAfterComplete, lastCollected.Add(oldInterval))
	}

	// Operator edits aveloxis.json 1→7, restarts serve. Scheduler.Run
	// calls RealignDueDates with the new value.
	newInterval := 7 * 24 * time.Hour
	n, err := store.RealignDueDates(ctx, newInterval)
	if err != nil {
		t.Fatalf("RealignDueDates(7d): %v", err)
	}
	if n < 1 {
		t.Errorf("RealignDueDates(7d) rows_updated = %d, want >= 1. "+
			"The idempotency guard (due_at <> last_collected + interval) "+
			"is incorrectly excluding the row.", n)
	}

	dueAfterRealign, _, _, _ := readQueueRow(ctx, t, store, repoID)
	wantAfterRealign := lastCollected.Add(newInterval)
	if !approxEqual(dueAfterRealign, wantAfterRealign, time.Millisecond) {
		t.Errorf("after RealignDueDates(7d): due_at = %v, want %v. "+
			"Delta: %v. This is the exact symptom the user reported — a "+
			"changed days_until_recollect did not take effect on an "+
			"existing queue row.", dueAfterRealign, wantAfterRealign,
			dueAfterRealign.Sub(wantAfterRealign))
	}

	// Also verify the shift is meaningful — due_at must have moved by
	// ≈6 days (7d new - 1d old). A silent no-op would still pass the
	// previous assertion if last_collected moved, so this is the
	// explicit delta check.
	shift := dueAfterRealign.Sub(dueAfterComplete)
	wantShift := newInterval - oldInterval
	if !approxEqual(time.Time{}.Add(shift), time.Time{}.Add(wantShift), 2*time.Second) {
		t.Errorf("due_at shift = %v, want ≈ %v (new interval - old interval). "+
			"RealignDueDates should have moved due_at forward by exactly "+
			"the difference between the old and new intervals.",
			shift, wantShift)
	}
}

// TestRealignDueDates_OverdueRowGetsNewWindow covers the case where an
// existing row is already past-due (due_at < NOW()) under the old setting.
// After realign with a longer interval, the row should have due_at =
// last_collected + new interval — which may still be in the past if the
// new interval is short enough, but for a 1→30d change the row will now
// be future-due. Confirms the arithmetic is measured from last_collected
// (not from NOW()), which is the property the comment on RealignDueDates
// explicitly claims.
func TestRealignDueDates_OverdueRowGetsNewWindow(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	repoID := seedRealignRepo(ctx, t, store, "overdue")
	// Row completed 10 days ago, old 1d setting baked due_at = 9 days ago.
	lastCollected := time.Now().Add(-10 * 24 * time.Hour).UTC()
	oldDueAt := lastCollected.Add(24 * time.Hour) // 9 days in the past
	seedQueueRow(ctx, t, store, repoID, "queued", &lastCollected, oldDueAt)

	newInterval := 30 * 24 * time.Hour
	if _, err := store.RealignDueDates(ctx, newInterval); err != nil {
		t.Fatalf("RealignDueDates(30d): %v", err)
	}

	dueAt, _, _, _ := readQueueRow(ctx, t, store, repoID)
	wantDueAt := lastCollected.Add(newInterval) // 20 days in the future
	if !approxEqual(dueAt, wantDueAt, time.Millisecond) {
		t.Errorf("due_at = %v, want %v (last_collected + 30d). "+
			"A NOW()-based formula would have produced ~%v instead — check "+
			"the SQL uses last_collected + interval, not NOW() + interval.",
			dueAt, wantDueAt, time.Now().Add(newInterval).UTC())
	}
}
