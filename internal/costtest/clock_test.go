// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package costtest

import "testing"

// TestClockChoiceFallbackIsSticky (PR #218 review B7): the measurement clock
// is decided by one probe at first use and kept for the whole process. A
// failed thread-clock probe selects the wall clock for every later reading;
// a probe that would now succeed does not switch clocks mid-process, so no
// measurement ever subtracts a wall reading from a thread-CPU reading.
func TestClockChoiceFallbackIsSticky(t *testing.T) {
	probes := 0
	works := false
	c := &clockChoice{probe: func() bool { probes++; return works }}
	if c.useThread() {
		t.Fatal("a failed thread-clock probe chose the thread clock")
	}
	works = true
	for range 3 {
		if c.useThread() {
			t.Fatal("the clock choice changed after the first probe")
		}
	}
	if probes != 1 {
		t.Fatalf("the thread clock was probed %d times, want once", probes)
	}
}

// TestClockChoiceThreadIsSticky: the other direction — a working probe
// keeps the thread clock, probed once.
func TestClockChoiceThreadIsSticky(t *testing.T) {
	probes := 0
	c := &clockChoice{probe: func() bool { probes++; return probes == 1 }}
	for range 3 {
		if !c.useThread() {
			t.Fatal("a working thread-clock probe did not keep the thread clock")
		}
	}
	if probes != 1 {
		t.Fatalf("the thread clock was probed %d times, want once", probes)
	}
}
