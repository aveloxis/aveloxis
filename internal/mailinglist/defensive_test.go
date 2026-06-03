// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"testing"
	"time"
)

func TestPacerStartsAtMinAndBacksOff(t *testing.T) {
	p := NewPacer(1*time.Second, 16*time.Second)
	if p.Delay() != 1*time.Second {
		t.Fatalf("start delay = %v, want 1s", p.Delay())
	}
	// Back off fast: 1 → 2 → 4 → 8 → 16 (cap).
	want := []time.Duration{2, 4, 8, 16, 16}
	for i, w := range want {
		p.OnStrain()
		if p.Delay() != w*time.Second {
			t.Errorf("after %d strains: %v, want %vs", i+1, p.Delay(), w)
		}
	}
}

func TestPacerDecaysTowardMinOnSuccess(t *testing.T) {
	p := NewPacer(1*time.Second, 60*time.Second)
	for i := 0; i < 6; i++ {
		p.OnStrain() // climb up
	}
	high := p.Delay()
	if high <= 1*time.Second {
		t.Fatalf("expected elevated delay, got %v", high)
	}
	// Recover slowly; many successes should return to the floor.
	for i := 0; i < 100; i++ {
		p.OnSuccess()
	}
	if p.Delay() != 1*time.Second {
		t.Errorf("after sustained success, delay = %v, want floor 1s", p.Delay())
	}
}

func TestBreakerTripsAtThresholdAndRecovers(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	b := NewBreaker(3, 1*time.Hour)
	b.now = func() time.Time { return now }

	if b.IsOpen() {
		t.Fatal("breaker should start closed")
	}
	// Two failures: still closed.
	if tripped := b.RecordTransientFailure(); tripped {
		t.Error("should not trip on first failure")
	}
	b.RecordTransientFailure()
	if b.IsOpen() {
		t.Fatal("breaker should be closed below threshold")
	}
	// Third failure trips it.
	if tripped := b.RecordTransientFailure(); !tripped {
		t.Error("third failure should trip the breaker")
	}
	if !b.IsOpen() || b.Healthy() {
		t.Fatal("breaker should be open after reaching threshold")
	}
	// Still open before the pause elapses.
	now = now.Add(59 * time.Minute)
	if !b.IsOpen() {
		t.Error("breaker should still be open before pause elapses")
	}
	// After the pause it allows a probe (closed).
	now = now.Add(2 * time.Minute)
	if b.IsOpen() {
		t.Error("breaker should reopen for a probe after the pause elapses")
	}
}

func TestBreakerSuccessResets(t *testing.T) {
	b := NewBreaker(3, time.Hour)
	b.RecordTransientFailure()
	b.RecordTransientFailure()
	b.RecordSuccess() // clean probe/result resets the counter
	// Two more failures should NOT trip (counter was reset).
	b.RecordTransientFailure()
	if tripped := b.RecordTransientFailure(); tripped {
		t.Error("counter must reset on success — 2 failures after reset must not trip a threshold-3 breaker")
	}
	if b.IsOpen() {
		t.Error("breaker should be closed")
	}
}
