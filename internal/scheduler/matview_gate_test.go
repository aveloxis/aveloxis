package scheduler

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// TestShouldStartMatviewRebuild verifies the threshold helper. The rule is
// "start the weekly rebuild only after the scheduler has naturally drained
// below one third of its worker capacity" — chosen so the rebuild coincides
// with a quiet window rather than forcing workers to stop mid-collection.
func TestShouldStartMatviewRebuild(t *testing.T) {
	tests := []struct {
		name    string
		active  int
		total   int
		want    bool
		comment string
	}{
		{"zero-workers", 0, 0, false, "no workers configured — never start"},
		{"negative-workers", 5, -1, false, "nonsense total — treat as no workers"},
		{"all-busy", 35, 35, false, "all slots busy — wait for drain"},
		{"half-busy", 18, 35, false, "18/35 > 1/3 — still busy"},
		{"just-above-threshold", 12, 35, false, "12/35 > 1/3 — still busy"},
		{"below-real-threshold", 11, 35, true, "11/35 (~31.4%) is below 1/3 — rebuild can start"},
		{"below-threshold", 10, 35, true, "10/35 < 1/3 — rebuild can start"},
		{"exact-fraction", 4, 12, false, "4/12 = 1/3 exactly — NOT strictly below"},
		{"one-active", 1, 35, true, "only 1 worker running — safe to rebuild"},
		{"none-active", 0, 35, true, "queue drained completely — clearly safe"},
		{"small-pool-2-of-9", 2, 9, true, "3*2=6 < 9 — below threshold"},
		{"small-pool-3-of-9", 3, 9, false, "3*3=9 NOT strictly less than 9"},
		{"single-worker-busy", 1, 1, false, "sole worker busy — never rebuild"},
		{"single-worker-idle", 0, 1, true, "sole worker idle — rebuild can run"},
		{"negative-active-coerced", -5, 35, true, "negative active treated as zero"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ShouldStartMatviewRebuild(tt.active, tt.total)
			if got != tt.want {
				t.Errorf("ShouldStartMatviewRebuild(%d, %d) = %v, want %v (%s)",
					tt.active, tt.total, got, tt.want, tt.comment)
			}
		})
	}
}

// TestMatviewRebuildActiveIsAtomicBool verifies the shared flag is an
// atomic.Bool at package scope. The monitor reads it from a different
// goroutine than the scheduler writes it, so plain bool would race.
func TestMatviewRebuildActiveIsAtomicBool(t *testing.T) {
	// Compile-time check: the variable exists and has the right type.
	var _ *atomic.Bool = &MatviewRebuildActive

	// Exercise it to ensure Store/Load work and the initial state is false.
	if MatviewRebuildActive.Load() {
		t.Fatal("MatviewRebuildActive must start as false; another test left it true")
	}
	MatviewRebuildActive.Store(true)
	if !MatviewRebuildActive.Load() {
		t.Error("Store(true) did not take effect")
	}
	MatviewRebuildActive.Store(false)
	if MatviewRebuildActive.Load() {
		t.Error("Store(false) did not take effect")
	}
}

// TestRebuildMatviewsDoesNotDrainSemaphore pins the core design decision:
// rebuildMatviews must NOT block the scheduler's main goroutine by draining
// every worker slot. The drain pattern `for range s.cfg.Workers { sem <- ... }`
// is what caused the April 17 incident — a single slow parallel-mode job
// (meshery, 11K+ PRs) held the last slot for 9+ hours and froze the whole
// scheduler because the main goroutine was parked on that send.
func TestRebuildMatviewsDoesNotDrainSemaphore(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Scheduler) rebuildMatviews(")
	if idx < 0 {
		t.Fatal("cannot find rebuildMatviews in scheduler.go")
	}
	fnBody := src[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}

	// The offending pattern is a for-range over Workers that pushes into sem.
	// Any variant ("for i := 0; i < s.cfg.Workers", "for range s.cfg.Workers")
	// followed by `sem <- struct{}{}` within a few lines is the bug.
	if strings.Contains(fnBody, "sem <- struct{}{}") {
		t.Error("rebuildMatviews must not send to the worker semaphore — " +
			"this blocks the scheduler's main loop for the duration of the " +
			"longest in-flight collection (hours, in the incident case)")
	}
	if strings.Contains(fnBody, "<-sem") {
		t.Error("rebuildMatviews must not receive from the worker semaphore — " +
			"the gated-rebuild design uses the MatviewRebuildActive flag to " +
			"block new claims, not semaphore drain/refill")
	}
}

// TestRebuildMatviewsManagesActiveFlag verifies the rebuild goroutine sets
// and clears MatviewRebuildActive. Without this, fillWorkerSlots has no way
// to know collection should pause and the rebuild will race with new jobs.
func TestRebuildMatviewsManagesActiveFlag(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Scheduler) rebuildMatviews(")
	if idx < 0 {
		t.Fatal("cannot find rebuildMatviews in scheduler.go")
	}
	fnBody := src[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}

	if !strings.Contains(fnBody, "MatviewRebuildActive") {
		t.Error("rebuildMatviews must touch MatviewRebuildActive (set before " +
			"refresh, clear after) so fillWorkerSlots can honor the gate")
	}
}

// TestFillWorkerSlotsHonorsMatviewGate verifies that fillWorkerSlots short-
// circuits while a rebuild is running. Without this, new jobs would race
// against the refresh and defeat the "don't start any more jobs until it's
// done" contract.
func TestFillWorkerSlotsHonorsMatviewGate(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Scheduler) fillWorkerSlots(")
	if idx < 0 {
		t.Fatal("cannot find fillWorkerSlots in scheduler.go")
	}
	fnBody := src[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}

	if !strings.Contains(fnBody, "MatviewRebuildActive") {
		t.Error("fillWorkerSlots must consult MatviewRebuildActive so no new " +
			"jobs are claimed while the weekly rebuild runs")
	}
}

// TestMatviewCheckTickerDoesNotInlineRebuild verifies that the Saturday tick
// handler schedules the rebuild via a pending flag rather than running it
// inline. An inline call re-introduces the main-loop block that the whole
// refactor is meant to eliminate.
func TestMatviewCheckTickerDoesNotInlineRebuild(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate the matviewCheckTicker case block.
	caseIdx := strings.Index(src, "<-matviewCheckTicker.C:")
	if caseIdx < 0 {
		t.Fatal("cannot find matviewCheckTicker case in Run loop")
	}
	// Inspect the next few hundred characters — the block ends at the next
	// `case` keyword or the closing brace of the select.
	block := src[caseIdx:]
	if nextCase := strings.Index(block[1:], "case "); nextCase > 0 {
		block = block[:nextCase+1]
	}

	if strings.Contains(block, "s.rebuildMatviews(") {
		t.Error("matviewCheckTicker handler must not call rebuildMatviews " +
			"directly — it has to set a pending flag so pollTicker can start " +
			"the rebuild once active workers drop below the threshold")
	}
	if !strings.Contains(block, "matviewPending") {
		t.Error("matviewCheckTicker handler must set s.matviewPending so the " +
			"poll loop knows a rebuild is owed")
	}
}

// TestPollTickerStartsGatedRebuild verifies that pollTicker examines the
// pending flag and ShouldStartMatviewRebuild before spawning the rebuild
// goroutine. This is the hand-off point: the weekly ticker marks the
// intent, the poll loop picks the moment.
func TestPollTickerStartsGatedRebuild(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	runIdx := strings.Index(src, "func (s *Scheduler) Run(")
	if runIdx < 0 {
		t.Fatal("cannot find Run function")
	}
	runBody := src[runIdx:]

	if !strings.Contains(runBody, "ShouldStartMatviewRebuild") {
		t.Error("Run loop must call ShouldStartMatviewRebuild to decide when " +
			"to start the pending rebuild")
	}
	if !strings.Contains(runBody, "matviewPending") {
		t.Error("Run loop must consult s.matviewPending to know a rebuild is owed")
	}
}

// TestSchedulerHasMatviewPendingField verifies the scheduler struct exposes
// a matviewPending atomic.Bool so the matviewCheckTicker handler and the
// pollTicker handler can coordinate without a shared channel.
func TestSchedulerHasMatviewPendingField(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find the Scheduler struct definition.
	structIdx := strings.Index(src, "type Scheduler struct {")
	if structIdx < 0 {
		t.Fatal("cannot find Scheduler struct")
	}
	structEnd := strings.Index(src[structIdx:], "\n}")
	if structEnd < 0 {
		t.Fatal("cannot find end of Scheduler struct")
	}
	structBody := src[structIdx : structIdx+structEnd]

	if !strings.Contains(structBody, "matviewPending") {
		t.Error("Scheduler struct must declare matviewPending field so the " +
			"weekly tick can defer the rebuild to the poll loop")
	}
	if !strings.Contains(structBody, "atomic.Bool") {
		t.Error("matviewPending must be atomic.Bool — it's written by one " +
			"goroutine (the rebuild) and read by another (pollTicker)")
	}
}
