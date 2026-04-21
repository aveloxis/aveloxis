package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestSchedulerRunsPeriodicStagingCleanup — source-contract test
// pinning that scheduler.Run has a ticker that calls
// store.PurgeStagedProcessed.
//
// Background: the staging table accumulates rows with processed=true
// forever unless something deletes them. PurgeStagedProcessed is
// defined in internal/db/staging.go but was not wired to any caller
// until v0.18.16 — by v0.18.15 a single deployment had grown to
// 21.5M processed rows, at which point every staging DELETE /
// INSERT was touching a hugely bloated heap and the whole
// collection pipeline felt slow at restart.
//
// Without this test pinning the wiring, a future refactor could
// silently drop the ticker and the bloat would return.
func TestSchedulerRunsPeriodicStagingCleanup(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// A named ticker of some form must exist so the cadence is
	// discoverable from the source (not a bare time.Tick).
	if !strings.Contains(src, "stagingCleanupTicker") {
		t.Error("scheduler.go must declare stagingCleanupTicker — a named " +
			"time.Ticker that fires periodic PurgeStagedProcessed calls so " +
			"the aveloxis_ops.staging table doesn't accumulate processed-row " +
			"bloat indefinitely")
	}

	// Actual call site must exist — the ticker without the call is
	// a no-op.
	if !strings.Contains(src, "PurgeStagedProcessed") {
		t.Error("scheduler.go must call store.PurgeStagedProcessed on the " +
			"cleanup ticker; without it the ticker fires but no cleanup " +
			"happens")
	}

	// The stop / defer cleanup must be present like every other
	// ticker in Run. Missing .Stop() leaks the ticker's goroutine on
	// scheduler shutdown.
	if !strings.Contains(src, "stagingCleanupTicker.Stop()") {
		t.Error("scheduler.go must defer stagingCleanupTicker.Stop() to match " +
			"the pattern of every other ticker in Run")
	}

	// The switch-case branch wiring the ticker to the cleanup
	// handler must exist.
	if !strings.Contains(src, "<-stagingCleanupTicker.C") {
		t.Error("scheduler.go must have a `case <-stagingCleanupTicker.C:` " +
			"branch in the main select loop so the ticker actually drives " +
			"cleanup")
	}
}

// TestStagingCleanupIntervalIsReasonable — pins the cadence at
// something non-degenerate. Too short and we hammer the DB with
// DELETEs; too long and a burst of collection can regrow the bloat
// before the next fire. Hourly is the sweet spot documented in
// v0.18.16.
func TestStagingCleanupIntervalIsReasonable(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Accept any interval in the "reasonable" range [30 min, 24 h]
	// by checking for a few common Go duration literals. Tests
	// should be tight enough to catch "1 * time.Second" mistakes
	// but loose enough to allow future tuning.
	reasonable := []string{
		"30 * time.Minute",
		"1 * time.Hour",
		"time.Hour",
		"2 * time.Hour",
		"3 * time.Hour",
		"6 * time.Hour",
		"12 * time.Hour",
		"24 * time.Hour",
	}
	// The staging cleanup ticker declaration must be on a line that
	// uses one of these. Find the declaration and check the right-
	// hand side.
	idx := strings.Index(src, "stagingCleanupTicker")
	if idx < 0 {
		// TestSchedulerRunsPeriodicStagingCleanup will fail with a
		// clearer message; don't double-report.
		return
	}
	// Look at the 200-char window starting at that identifier.
	end := idx + 200
	if end > len(src) {
		end = len(src)
	}
	window := src[idx:end]

	found := false
	for _, r := range reasonable {
		if strings.Contains(window, r) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("stagingCleanupTicker interval looks unreasonable (expected one of %v "+
			"within ~200 chars of the ticker declaration).\n  window: %q",
			reasonable, window)
	}
}
