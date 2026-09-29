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

func quadraticOp(n int) func() {
	return func() {
		for i := range n {
			for j := range n {
				sink += i ^ j
			}
		}
	}
}

// TestCheckSeparatesLinearFromQuadratic: linear work passes; pure quadratic
// work fails; and so does work that is mostly linear with a quadratic step —
// the dilution a wider comparison let through (PR #218 review F1). The mixed
// case puts the quadratic at eight times the linear part at the larger size
// (expected ratio 12 against the limit of 8): the check's boundary is about
// twice, and a self-test near it would measure noise, not the check.
func TestCheckSeparatesLinearFromQuadratic(t *testing.T) {
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
	if ok, _ := Check(quadraticOp, 1000, 0); ok {
		t.Error("quadratic work passed the check")
	}
	// At the larger size (4n) the quadratic part is (4n)² = 16n² loop steps
	// and the linear part scale·4n: scale = n/2 makes the quadratic eight
	// times the linear part there. Small: a/4 + 8a/16 = 0.75a; large: 9a.
	const n = 1200
	mixed := func(m int) func() {
		lin, quad := linearOp(n/2)(m), quadraticOp(m)
		return func() { lin(); quad() }
	}
	if ok, _ := Check(mixed, n, 0); ok {
		t.Error("mostly-linear work with a quadratic step eight times its linear part passed the check")
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
	rec := &recordingTB{}
	Linear(rec, "quadratic", quadraticOp, 1000, 0)
	if !rec.failed {
		t.Error("Linear did not report a quadratic operation")
	}
}
