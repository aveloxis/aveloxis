// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package costtest

import (
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"
)

var sink int

func linearOp(scale int) func(n int) func() {
	return func(n int) func() {
		return func() {
			for i := range scale * n {
				sink += i
			}
		}
	}
}

func cubicOp(n int) func() {
	return func() {
		for i := range n {
			for j := range n {
				for k := range n {
					sink += i ^ j ^ k
				}
			}
		}
	}
}

// TestCheckSeparatesLinearFromSuperlinear, on the real clock: linear work
// passes, and clearly superlinear work fails. The superlinear case is CUBIC
// (64x the time at 4x the input in theory, about 50x measured with loop
// overhead — six times the limit): the quadratic it
// replaced (16x, twice the limit) passed the check on a shared CI runner
// whenever one of the three attempts was noisy — the check passes on ANY
// attempt, by design, so linear code is not failed by noise (PR #226 CI;
// old problem O16). The boundary itself (12x fails, a mostly-linear
// quadratic step, the dilution of PR #218 review F1) is pinned on exact
// durations in TestDecideRules, where noise cannot reach it.
func TestCheckSeparatesLinearFromSuperlinear(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	// Short enough (about 1 ms at 4n) that macOS keeps the thread on a
	// performance core: a 16 ms loop was moved to an efficiency core for
	// whole measurements and read as 17–20x (the wall clock's
	// efficiency-core case, not the process-CPU-clock one).
	if ok, report := Check(linearOp(50), 20000, 0); !ok {
		t.Errorf("linear work failed the check: %s", report)
	}
	if ok, _ := Check(cubicOp, 60, 0); ok {
		t.Error("cubic work passed the check")
	}
}

// TestDecideRules pins the verdict on exact durations: the limit sits
// between linear (4x) and quadratic (16x); a mostly-linear operation with a
// quadratic step eight times its linear part (12x) fails (PR #218 review F1:
// the dilution a wider comparison let through); a pass on any attempt
// passes; a runaway first attempt stops without re-measuring; slack widens
// the bound.
func TestDecideRules(t *testing.T) {
	const ms = time.Millisecond
	seq := func(pairs ...[2]time.Duration) (func() (time.Duration, time.Duration), *int) {
		calls := 0
		return func() (time.Duration, time.Duration) {
			p := pairs[calls%len(pairs)]
			calls++
			return p[0], p[1]
		}, &calls
	}
	for _, c := range []struct {
		name      string
		pairs     [][2]time.Duration
		slack     time.Duration
		wantOK    bool
		wantCalls int
	}{
		{"linear 4x", [][2]time.Duration{{10 * ms, 40 * ms}}, 0, true, 1},
		{"at the limit 8x", [][2]time.Duration{{10 * ms, 80 * ms}}, 0, true, 1},
		{"quadratic 16x", [][2]time.Duration{{10 * ms, 160 * ms}}, 0, false, attempts},
		{"mostly linear with a quadratic step 12x", [][2]time.Duration{{10 * ms, 120 * ms}}, 0, false, attempts},
		{"one quiet attempt passes", [][2]time.Duration{{10 * ms, 90 * ms}, {10 * ms, 90 * ms}, {10 * ms, 70 * ms}}, 0, true, 3},
		{"runaway stops at once", [][2]time.Duration{{10 * ms, 330 * ms}}, 0, false, 1},
		{"slack widens the bound", [][2]time.Duration{{1 * ms, 9 * ms}}, 2 * ms, true, 1},
	} {
		measure, calls := seq(c.pairs...)
		ok, report := decide(measure, c.slack)
		if ok != c.wantOK || *calls != c.wantCalls {
			t.Errorf("%s: ok=%v after %d measurement(s), want ok=%v after %d (%s)", c.name, ok, *calls, c.wantOK, c.wantCalls, report)
		}
		if !ok && report == "" {
			t.Errorf("%s: a failure must say what it measured", c.name)
		}
	}
}

type node struct {
	next *node
	data []byte
}

var keep *node

// TestCheckPassesAllocatingLinearWork (PR #218 review of the CPU clock,
// critical): the process CPU clock counts the collector's background mark
// workers on every P, so linear work that allocates — the larger input
// triggering more collections — read as 8–11x and failed (measured on an
// idle machine with TestOperandTextsCostIsLinear). Collection is
// off while a measurement runs; linear allocating work must pass.
func TestCheckPassesAllocatingLinearWork(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	alloc := func(n int) func() {
		return func() {
			keep = nil
			for range n {
				keep = &node{next: keep, data: make([]byte, 48)}
			}
		}
	}
	for range 3 {
		if ok, report := Check(alloc, 30000, 0); !ok {
			t.Fatalf("linear allocating work failed the check: %s", report)
		}
	}
}

// TestNoCollectionRunsWhileMeasuring pins the fix deterministically: a
// measurement of allocating work sees no collection of its own — only the
// one forced before the warm-up, and a cycle already running at entry. (The allocating self-test above reproduces
// the flake only a few times in ten; TestOperandTextsCostIsLinear in
// internal/spdx failed 9 of 10 runs with collection on.)
func TestNoCollectionRunsWhileMeasuring(t *testing.T) {
	op := func() {
		keep = nil
		for range 200000 { // ~20 MB: several collections at the default GOGC
			keep = &node{next: keep, data: make([]byte, 48)}
		}
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fastest(op)
	runtime.ReadMemStats(&after)
	// One forced before the warm-up, and one more when a cycle was already
	// running as the test began (runtime.GC waits it out and counts it).
	// Restoring the settings starts none. With the collector on during the
	// timed runs this op saw 14.
	if n := after.NumGC - before.NumGC; n > 2 {
		t.Errorf("%d collections during a measurement; want the one forced before it and at most one already running at entry", n)
	}
}

// TestConcurrentMeasurementsRestoreTheCollector (PR #218 review F3): the
// collector settings fastest turns off are process-wide. Measurement B
// starts while A is running and ends after it; without serialization B
// saves A's "off" and restores it last, leaving the collector off for the
// rest of the process.
func TestConcurrentMeasurementsRestoreTheCollector(t *testing.T) {
	orig := debug.SetGCPercent(100)
	debug.SetGCPercent(orig)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		fastest(func() { time.Sleep(2 * time.Millisecond) }) // A: about 12 ms
	}()
	time.Sleep(4 * time.Millisecond) // B starts inside A
	go func() {
		defer wg.Done()
		fastest(func() { time.Sleep(6 * time.Millisecond) }) // B: about 36 ms
	}()
	wg.Wait()
	now := debug.SetGCPercent(orig)
	if now != orig {
		t.Errorf("GC percent is %d after two concurrent measurements; want %d restored", now, orig)
	}
}

// recordingTB records Errorf; everything else is the embedded TB's (unused).
type recordingTB struct {
	testing.TB
	failed bool
}

func (r *recordingTB) Helper()               {}
func (r *recordingTB) Errorf(string, ...any) { r.failed = true }
func (r *recordingTB) Fatalf(string, ...any) { r.failed = true }

// TestLinearReportsAFailure: Linear, the wrapper the cost tests call, turns
// a failed check into a test failure.
func TestLinearReportsAFailure(t *testing.T) {
	if testing.Short() || raceBuild {
		t.Skip("timing comparison (not under -short or the race detector)")
	}
	// Cubic, not quadratic: about six times the limit, so noise on a shared
	// runner cannot pass it (TestCheckSeparatesLinearFromSuperlinear).
	rec := &recordingTB{}
	Linear(rec, "cubic", cubicOp, 60, 0)
	if !rec.failed {
		t.Error("Linear did not report a cubic operation")
	}
}
