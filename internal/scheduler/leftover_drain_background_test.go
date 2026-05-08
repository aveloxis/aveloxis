// Source-contract tests for v0.18.29 Fix 1: the leftover-staging drain
// runs in a background goroutine instead of blocking scheduler.Run.
//
// Background: at v0.18.28, processLeftoverStaging is invoked synchronously
// from Run() before fillWorkerSlots. On a fleet of 40K repos with backlogged
// staging, ProcessRepo for one repo can take 30+ hours, and the for-loop
// across 23 leftover repos blocked all 120 workers idle for ~3 days. The
// fix moves the drain into its own goroutine.
//
// Safety invariant: a worker must NOT claim a queue row whose staging is
// still draining, because CollectRepo's PurgeStagedForRepo would wipe the
// in-flight rows. The fix lock-parks the drain set as
// status='collecting', locked_by='<workerID>:drain' before launching the
// goroutine. fillWorkerSlots's existing WHERE status='queued' filter
// then naturally skips them, no other code change required. As each repo
// finishes draining, it's released back to 'queued' (without touching
// last_collected — see queue_drain_lock_test.go).

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestLeftoverStagingRunsInBackground pins that processLeftoverStaging is
// invoked via `go ` so Run() can proceed to fillWorkerSlots immediately.
// A future refactor that re-introduces the synchronous call (without
// removing the goroutine) would let the v0.18.26 multi-day stall back in.
func TestLeftoverStagingRunsInBackground(t *testing.T) {
	src := mustReadSchedulerSource(t)
	body := extractRunBody(src)
	if body == "" {
		t.Fatal("could not locate scheduler.Run function body")
	}

	// The expected shape: `go s.processLeftoverStagingBackground(ctx, ...)`
	// or `go func() { s.processLeftoverStaging(ctx) }()`. We accept either,
	// but the synchronous call `s.processLeftoverStaging(ctx)` on its own
	// line — without a preceding `go ` — must NOT exist after the fix.
	syncCallExists := strings.Contains(body, "\n\ts.processLeftoverStaging(ctx)\n") ||
		strings.Contains(body, "\n\t\ts.processLeftoverStaging(ctx)\n")
	if syncCallExists {
		t.Error("scheduler.Run must NOT call processLeftoverStaging synchronously — " +
			"it blocks fillWorkerSlots for hours on a backlogged fleet. " +
			"Wrap the call in a `go ` goroutine after lock-parking the drain set.")
	}

	hasGoroutine := strings.Contains(body, "go s.processLeftoverStagingBackground(") ||
		strings.Contains(body, "go func()") &&
			strings.Contains(body, "processLeftoverStaging(")
	if !hasGoroutine {
		t.Error("scheduler.Run must launch the leftover-staging drain in a goroutine " +
			"so fillWorkerSlots can immediately claim queued repos that don't need drain.")
	}
}

// TestDrainLockParkComesBeforeBackgroundLaunch enforces the safety
// invariant: the drain set must be marked status='collecting' BEFORE the
// goroutine starts, otherwise fillWorkerSlots could race the goroutine
// and CollectRepo's PurgeStagedForRepo would wipe staging rows out from
// under it.
func TestDrainLockParkComesBeforeBackgroundLaunch(t *testing.T) {
	src := mustReadSchedulerSource(t)
	body := extractRunBody(src)
	if body == "" {
		t.Fatal("could not locate scheduler.Run function body")
	}

	lockIdx := strings.Index(body, "LockReposForDrain")
	if lockIdx < 0 {
		t.Error("scheduler.Run must call store.LockReposForDrain to atomically park " +
			"the drain set as status='collecting' before launching the background drain. " +
			"Without it, fillWorkerSlots can race the goroutine and PurgeStagedForRepo " +
			"will wipe in-flight staging rows.")
		return
	}
	goIdx := strings.Index(body, "go s.processLeftoverStagingBackground")
	if goIdx < 0 {
		// Fallback: any `go ` pointing at the drain.
		goIdx = strings.Index(body, "go func()")
	}
	if goIdx < 0 {
		t.Fatal("could not find background drain launch")
	}
	if lockIdx > goIdx {
		t.Errorf("LockReposForDrain (byte %d) must come BEFORE the goroutine launch (byte %d). "+
			"Otherwise fillWorkerSlots can claim a draining repo before it's parked.",
			lockIdx, goIdx)
	}
}

// TestProcessLeftoverStagingBackgroundExists asserts that the background
// drain function is defined as a method on *Scheduler. The synchronous
// processLeftoverStaging may stay (called by tests or admin tooling),
// but a callable background variant must exist.
func TestProcessLeftoverStagingBackgroundExists(t *testing.T) {
	src := mustReadSchedulerSource(t)
	if !strings.Contains(src, "func (s *Scheduler) processLeftoverStagingBackground(") {
		t.Error("scheduler must define processLeftoverStagingBackground as the goroutine entry point. " +
			"It should iterate the drain set, call ProcessRepo per repo, and release each " +
			"repo's drain lock as draining completes.")
	}
}

// TestBackgroundDrainReleasesPerRepo asserts that the drain function
// releases the lock-park status as each repo finishes (not just at the
// end). This matters operationally: a repo that finishes draining at hour
// 2 of a 30-hour drain shouldn't have to wait for the entire drain to
// complete before becoming queue-eligible.
func TestBackgroundDrainReleasesPerRepo(t *testing.T) {
	src := mustReadSchedulerSource(t)
	idx := strings.Index(src, "func (s *Scheduler) processLeftoverStagingBackground(")
	if idx < 0 {
		t.Skip("processLeftoverStagingBackground not yet defined; covered by TestProcessLeftoverStagingBackgroundExists")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "ReleaseDrainLock") {
		t.Error("processLeftoverStagingBackground must call store.ReleaseDrainLock per repo " +
			"so finished repos rejoin the queue mid-drain. Without per-repo release, a repo " +
			"that completes draining at hour 2 has to wait for hour 30 before being collectable.")
	}
}

func mustReadSchedulerSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	return string(data)
}

func extractRunBody(src string) string {
	idx := strings.Index(src, "func (s *Scheduler) Run(")
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
