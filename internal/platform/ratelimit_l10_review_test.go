// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// L10 pass on the 2026-09-12 review-round fixes. The "usable keys only"
// reserve line (finding 7) opened a hole its own review did not see: with
// ZERO usable keys the line is 0, `0 <= 0` blocks, and the reserve branch's
// wake ignores every secondary/quarantine expiry — an all-resting pool is
// reported as "paced by the foreground reserve" and a non-fast-fail sweep
// parks until the next WINDOW reset instead of the ~60 s Retry-After. The
// same wake blindness applies when a resting key's expiry would lift both
// the usable count and the total. And `usableLocked` was a third inline
// spelling of "usable" that ignored secondaryUntil.

package platform

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestAllRestingPoolIsNotReserveBlocked: every key holds a full window but
// rests on a secondary limit. That is budget-blocked (wake = the rest
// expiry), never reserve-blocked (wake = the window reset). Arm 1 pins the
// verdict through the log; arm 2 pins the wake by admitting once the rests
// expire. Red-first: the draft logged "foreground reserve … wait=49m59s"
// and never admitted arm 2.
func TestAllRestingPoolIsNotReserveBlocked(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	kp := NewKeyPool([]string{"a", "b"}, logger)
	kp.SetAdmission(0, 0, 25)
	for _, k := range kp.keys {
		k.secondaryUntil = time.Now().Add(time.Minute)
		k.GraphQLResetAt = time.Now().Add(50 * time.Minute)
	}
	ctx, cancel := context.WithTimeout(WithGraphQLBackgroundBudget(context.Background()), 200*time.Millisecond)
	if _, _, err := kp.Acquire(ctx, ResourceGraphQL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("all keys resting: err = %v, want the ctx deadline (nothing is admissible)", err)
	}
	cancel()
	if strings.Contains(buf.String(), "foreground reserve") {
		t.Errorf("an all-resting pool was reported as reserve-paced — with zero usable keys the reserve has nothing to say:\n%s", buf.String())
	}
	// Every key HOLDS a full window; what is wrong with them is the rest.
	// The ops-grepped "exhausted" line means "every key is dry" and must
	// not fire here (L10 pass 2: it did, with wait=59s beside 5,000 points).
	if strings.Contains(buf.String(), "exhausted for GraphQL") {
		t.Errorf("an all-resting pool logged the pool-EXHAUSTION line while every key held a full window:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "unavailable (rate-limited or quarantined)") {
		t.Errorf("an all-resting pool must log the rate-limited/quarantined line:\n%s", buf.String())
	}

	// Arm 2: rests expire in 300 ms; the sweep must wake on THAT, not on
	// a window reset it does not know (no reset stamped → the 5-minute
	// probe under the reserve branch's wake).
	kp = NewKeyPool([]string{"a", "b"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25)
	for _, k := range kp.keys {
		k.secondaryUntil = time.Now().Add(300 * time.Millisecond)
	}
	ctx, cancel = context.WithTimeout(WithGraphQLBackgroundBudget(context.Background()), 8*time.Second)
	defer cancel()
	start := time.Now()
	_, release, err := kp.Acquire(ctx, ResourceGraphQL)
	if err != nil {
		t.Fatalf("background Acquire after the rests expired: %v after %v — the wake must consider the rest expiry", err, time.Since(start))
	}
	release()
}

// TestReserveWakeConsidersRestExpiry: key a is usable at the line (1,200 of
// one key's 5,000 → 25% line 1,250); key b rests 300 ms with a full window.
// When b's rest ends the usable set is two keys and the total 6,200 clears
// the 2,500 line, so the sweep must wake then — not at the +50 min window
// reset the draft used as the only wake source.
func TestReserveWakeConsidersRestExpiry(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25)
	far := time.Now().Add(50 * time.Minute)
	kp.keys[0].GraphQLRemaining = 1200
	kp.keys[0].GraphQLResetAt = far
	kp.keys[1].secondaryUntil = time.Now().Add(300 * time.Millisecond)
	kp.keys[1].GraphQLResetAt = far
	ctx, cancel := context.WithTimeout(WithGraphQLBackgroundBudget(context.Background()), 8*time.Second)
	defer cancel()
	start := time.Now()
	_, release, err := kp.Acquire(ctx, ResourceGraphQL)
	if err != nil {
		t.Fatalf("background Acquire once the resting key returned: %v after %v — the reserve wake must be min(window reset, rest expiry)", err, time.Since(start))
	}
	release()
}

// TestUsableCountExcludesSecondaryResting: the 401-quarantine log's
// `usable_keys` and the reserve's usable set must agree — one spelling of
// "usable" (SR-17). Red-first: usableLocked ignored secondaryUntil.
func TestUsableCountExcludesSecondaryResting(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c"}, rlTestLogger())
	now := time.Now()
	kp.keys[0].secondaryUntil = now.Add(time.Minute)
	kp.keys[1].quarantineUntil = now.Add(time.Minute)
	kp.mu.Lock()
	got := kp.usableLocked(now)
	kp.mu.Unlock()
	if got != 1 {
		t.Errorf("usableLocked = %d, want 1 — a key resting on a secondary limit is not usable, exactly as the reserve and selection already treat it", got)
	}
}

// TestReserveBlockStampsProbeOnZeroResetKeys: one usable key at 1,000
// points (above the buffer, at/below the 1,250 line) whose reset is
// UNKNOWN — the round-22 shape (a Remaining-only header) one branch up.
// Selection would admit such a key, so it never stamps it; the reserve
// branch blocks it, and if it does not stamp the probe either, nothing
// ever refills the key and the sweep re-logs every five minutes until a
// foreground response happens to carry a reset (L10 pass 2, finding 1).
func TestReserveBlockStampsProbeOnZeroResetKeys(t *testing.T) {
	kp := NewKeyPool([]string{"a"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25)
	kp.keys[0].GraphQLRemaining = 1000
	kp.keys[0].GraphQLResetAt = time.Time{}
	ctx, cancel := context.WithTimeout(WithGraphQLBackgroundBudget(context.Background()), 200*time.Millisecond)
	defer cancel()
	if _, _, err := kp.Acquire(ctx, ResourceGraphQL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reserve-blocked sweep: err = %v, want the ctx deadline", err)
	}
	kp.mu.Lock()
	stamped := kp.keys[0].GraphQLResetAt
	kp.mu.Unlock()
	if stamped.IsZero() {
		t.Fatal("a reserve block over a key with no known reset must stamp the probe window on the key, or refillLocked can never refill it")
	}
	if until := time.Until(stamped); until <= 0 || until > graphQLDepletedProbe+time.Second {
		t.Errorf("probe stamp = %v from now, want within the %v probe window", until, graphQLDepletedProbe)
	}
}
