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

// The breaker's own behaviour is tested in internal/hostguard. What is this
// package's is the defaults NewBreaker applies.
func TestNewBreakerAppliesTheMailingListDefaults(t *testing.T) {
	b := NewBreaker(0, 0)
	for i := 1; i < DefaultCircuitThreshold; i++ {
		if b.RecordTransientFailure() {
			t.Fatalf("tripped at failure %d, below the default threshold %d", i, DefaultCircuitThreshold)
		}
	}
	if !b.RecordTransientFailure() {
		t.Fatalf("failure %d must trip a breaker built with the default threshold", DefaultCircuitThreshold)
	}
	if !b.IsOpen() {
		t.Error("a breaker built with the default pause must be open right after it trips")
	}
}

// A caller's own values are kept.
func TestNewBreakerKeepsTheCallersValues(t *testing.T) {
	b := NewBreaker(2, time.Hour)
	b.RecordTransientFailure()
	if !b.RecordTransientFailure() {
		t.Error("a threshold of 2 must trip at the second failure")
	}
}
