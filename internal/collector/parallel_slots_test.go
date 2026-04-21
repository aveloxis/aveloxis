package collector

import (
	"os"
	"strings"
	"testing"
)

// TestParallelSlotsForWorkersSmallFleet pins the legacy claim of 3 slots
// for worker counts at or below the scale-up threshold. collectParallel
// only forks 3 goroutines (issues, PRs, events), so 3 is both the
// minimum and the truthful count of extra concurrent work on small
// fleets. Claiming more would reserve phantom capacity a small fleet
// can't spare.
func TestParallelSlotsForWorkersSmallFleet(t *testing.T) {
	cases := []struct{ workers, want int }{
		{0, 3},
		{1, 3},
		{10, 3},
		{20, 3},
		{29, 3},
		{30, 3},
	}
	for _, c := range cases {
		got := parallelSlotsForWorkers(c.workers)
		if got != c.want {
			t.Errorf("parallelSlotsForWorkers(%d) = %d, want %d", c.workers, got, c.want)
		}
	}
}

// TestParallelSlotsForWorkersLargeFleet verifies the claim scales to
// ~10% of the worker pool once the pool grows past the small-fleet
// band. This is what makes the scheduler's throttling rule at
// fillWorkerSlots actually trigger — a fixed claim of 3 on an 80-worker
// pool throttles only at 77 active workers, effectively never. At
// workers=80, returning 8 makes fillWorkerSlots stop claiming new jobs
// once 72 are busy, reserving bandwidth for the large repo's parallel
// goroutines to actually make progress.
func TestParallelSlotsForWorkersLargeFleet(t *testing.T) {
	cases := []struct{ workers, want int }{
		{40, 4},
		{50, 5},
		{60, 6},
		{80, 8},
		{100, 10},
		{200, 20},
	}
	for _, c := range cases {
		got := parallelSlotsForWorkers(c.workers)
		if got != c.want {
			t.Errorf("parallelSlotsForWorkers(%d) = %d, want %d", c.workers, got, c.want)
		}
	}
}

// TestParallelSlotsForWorkersMonotonic verifies the slot count never
// decreases as worker count grows. A regression that, e.g., clamped
// the claim to min(3, workers/10) for small fleets would break this
// invariant and silently under-reserve for the 31–40 band.
func TestParallelSlotsForWorkersMonotonic(t *testing.T) {
	prev := 0
	for w := 0; w <= 300; w++ {
		got := parallelSlotsForWorkers(w)
		if got < prev {
			t.Fatalf("parallelSlotsForWorkers(%d) = %d, was %d at %d workers (not monotonic)",
				w, got, prev, w-1)
		}
		prev = got
	}
}

// TestParallelSlotsForWorkersNeverBelowThree verifies the function
// never returns fewer than 3. collectParallel unconditionally forks
// 3 goroutines; returning fewer would under-represent actual in-flight
// work and the scheduler's throttle rule would leak real capacity.
func TestParallelSlotsForWorkersNeverBelowThree(t *testing.T) {
	for w := -5; w <= 500; w++ {
		if got := parallelSlotsForWorkers(w); got < 3 {
			t.Fatalf("parallelSlotsForWorkers(%d) = %d, must be >= 3", w, got)
		}
	}
}

// TestCollectParallelUsesScaledSlotHelper is a source-contract test
// pinning that collectParallel uses parallelSlotsForWorkers rather
// than a literal ParallelSlots.Add(3). A regression back to the
// hardcoded constant would silently revert the v0.18.11 fix on
// large worker pools.
func TestCollectParallelUsesScaledSlotHelper(t *testing.T) {
	data, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "parallelSlotsForWorkers") {
		t.Error("staged.go must call parallelSlotsForWorkers to scale the ParallelSlots " +
			"claim with the scheduler worker count")
	}
	if strings.Contains(src, "ParallelSlots.Add(3)") {
		t.Error("staged.go still has a literal ParallelSlots.Add(3) — replace it with " +
			"parallelSlotsForWorkers(sc.workers) so the claim scales with the pool")
	}
}

// TestStagedCollectorCarriesWorkerCount pins that StagedCollector has
// a workers field so collectParallel's slot scaling has a real number
// to work with. A regression that dropped the field would make the
// helper always fall back to the small-fleet constant.
func TestStagedCollectorCarriesWorkerCount(t *testing.T) {
	data, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The struct must declare a workers field.
	if !strings.Contains(src, "workers") {
		t.Error("staged.go StagedCollector must carry a workers field so " +
			"parallelSlotsForWorkers has the current scheduler worker count")
	}
}

// TestSchedulerPassesWorkerCountToCollector pins that the scheduler
// actually propagates its configured Workers into StagedCollector —
// otherwise the plumbing exists but the field stays at zero and the
// scaling helper silently uses the small-fleet fallback forever.
func TestSchedulerPassesWorkerCountToCollector(t *testing.T) {
	data, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The scheduler must reference s.cfg.Workers when constructing
	// or configuring a StagedCollector. Accept either a new
	// constructor signature or a setter/builder call.
	if !strings.Contains(src, "s.cfg.Workers") {
		t.Error("scheduler.go must pass s.cfg.Workers into StagedCollector so the " +
			"parallel-slot claim scales with the pool")
	}
	// Belt-and-suspenders: the scheduler file must show Workers
	// flowing into the staged collector construction path.
	if !strings.Contains(src, "WithWorkers") && !strings.Contains(src, "Workers:") &&
		!strings.Contains(src, ", s.cfg.Workers") {
		t.Error("scheduler.go must wire s.cfg.Workers into the StagedCollector — either via " +
			"a WithWorkers builder, a Workers struct field, or a constructor argument")
	}
}
