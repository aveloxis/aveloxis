// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostguard

import (
	"sync"
	"testing"
	"time"
)

// These are the breaker's tests from internal/mailinglist, moved with it: what
// it did there is what it does here.

func TestBreakerTripsAtThresholdAndRecovers(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	b := NewBreaker(3, 1*time.Hour, func() time.Time { return now })

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
	b := NewBreaker(3, time.Hour, time.Now)
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

// A failure while the breaker is open extends the pause and is not reported
// as a second trip: the caller logs the opening once.
func TestBreakerFailureWhileOpenExtendsThePauseWithoutTrippingAgain(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	b := NewBreaker(2, time.Hour, func() time.Time { return now })
	b.RecordTransientFailure()
	if !b.RecordTransientFailure() {
		t.Fatal("the second failure must trip a threshold-2 breaker")
	}
	now = now.Add(30 * time.Minute)
	if b.RecordTransientFailure() {
		t.Error("a failure while open must not report a second trip")
	}
	// The pause now runs from the later failure: still open 45 minutes on,
	// which is 75 minutes after the first trip.
	now = now.Add(45 * time.Minute)
	if !b.IsOpen() {
		t.Error("a failure while open must extend the pause from that failure")
	}
	now = now.Add(16 * time.Minute)
	if b.IsOpen() {
		t.Error("the breaker must let a probe through once the extended pause has passed")
	}
}

// The package has no defaults; a caller's mistake is loud, at construction.
func TestNewBreakerRefusesValuesItCannotUse(t *testing.T) {
	cases := []struct {
		name      string
		threshold int
		pause     time.Duration
		now       func() time.Time
	}{
		{"zero threshold", 0, time.Hour, time.Now},
		{"negative threshold", -1, time.Hour, time.Now},
		{"zero pause", 3, 0, time.Now},
		{"negative pause", 3, -time.Second, time.Now},
		{"nil clock", 3, time.Hour, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("NewBreaker(%d, %v, clock) must panic", c.threshold, c.pause)
				}
			}()
			NewBreaker(c.threshold, c.pause, c.now)
		})
	}
}

func TestBreakerConcurrentAccess(t *testing.T) {
	b := NewBreaker(10, 50*time.Millisecond, time.Now)
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				if (g+i)%3 == 0 {
					b.RecordTransientFailure()
				} else {
					b.RecordSuccess()
				}
				_ = b.IsOpen()
				_ = b.Healthy()
			}
		}(g)
	}
	wg.Wait()
}
