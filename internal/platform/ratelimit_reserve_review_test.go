// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Fresh-context review round on the 2026-09-12 admission change. Three
// behavioral pins on the pool-level foreground reserve, each red against
// the first draft:
//
//   - a passed window reset is authoritative for EVERY key, not only the
//     ones at or below the buffer — otherwise a reserve block over keys
//     that each still hold some budget never clears (the sweep parks in
//     Acquire until a foreground response happens to refresh a header);
//   - the reserve line is measured over the keys background could USE —
//     a resting (secondary-limited or quarantined) key's budget must not
//     admit a sweep that then drains the usable keys past the line;
//   - a reserve block says so in its own words, so ops grep for pool
//     exhaustion does not read reserve pacing as "every key is dry".

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

// TestPassedResetRefillsKeysAboveBuffer: four keys at 1,200 points each
// (4,800 total, at or below the 25% line of 4 x 5,000 = 5,000) whose
// hourly windows all reset a minute ago. The reset means GitHub has
// already refilled them; the pool must agree and admit the background
// caller. Red-first: the draft refilled only keys <= buffer (15), so the
// total stayed 4,800 forever and fast-fail returned budget-exhausted.
func TestPassedResetRefillsKeysAboveBuffer(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b", "c", "d"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25)
	past := time.Now().Add(-time.Minute)
	for _, k := range kp.keys {
		k.GraphQLRemaining = 1200
		k.GraphQLResetAt = past
	}
	bg := WithGraphQLFastFail(WithGraphQLBackgroundBudget(context.Background()))
	key, release, err := kp.Acquire(bg, ResourceGraphQL)
	if err != nil {
		t.Fatalf("background Acquire after every key's window reset: %v — a passed reset must refill the key whatever its remaining balance", err)
	}
	release()
	if key.GraphQLRemaining < graphQLPointsPerHour-1 {
		t.Errorf("admitted key holds %d points after a passed reset, want a full window (%d) minus the checkout reservation", key.GraphQLRemaining, graphQLPointsPerHour)
	}
	if !key.GraphQLResetAt.IsZero() {
		t.Errorf("a refilled key must drop its stale reset stamp so the next header refresh is accepted as a new window, got %v", key.GraphQLResetAt)
	}
}

// TestReserveLineExcludesRestingKeys: key a is full but resting on a
// secondary limit; key b holds 1,200. Background may not spend b below
// the line, and a's budget — which background cannot touch — must not
// lift the total over it. Red-first: the draft summed every alive key
// (6,200 > 2,500) and admitted the sweep onto b.
func TestReserveLineExcludesRestingKeys(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, rlTestLogger())
	kp.SetAdmission(0, 0, 25)
	kp.keys[0].secondaryUntil = time.Now().Add(time.Hour)
	kp.keys[1].GraphQLRemaining = 1200 // 1,200 <= 25% of the ONE usable key's 5,000
	bg := WithGraphQLFastFail(WithGraphQLBackgroundBudget(context.Background()))
	if _, _, err := kp.Acquire(bg, ResourceGraphQL); !errors.Is(err, ErrGraphQLBudgetExhausted) {
		t.Fatalf("background with the only usable key at the reserve line: err = %v, want ErrGraphQLBudgetExhausted — a resting key's budget is not spendable and must not count toward the line", err)
	}
	// Foreground is never gated by the reserve.
	_, release := heldAcquire(t, kp, WithGraphQLFastFail(context.Background()), ResourceGraphQL)
	release()
}

// TestReserveBlockLogsItsOwnReason: a background caller blocked ONLY by
// the reserve (every key holds budget) must not log the pool-exhaustion
// line — that message is grepped by ops as "every key is dry".
func TestReserveBlockLogsItsOwnReason(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	kp := NewKeyPool([]string{"a", "b"}, logger)
	kp.SetAdmission(0, 0, 25)
	kp.keys[0].GraphQLRemaining = 1200
	kp.keys[1].GraphQLRemaining = 1300 // total 2,500 == the line; both keys above the buffer
	ctx, cancel := context.WithTimeout(WithGraphQLBackgroundBudget(context.Background()), 200*time.Millisecond)
	defer cancel()
	if _, _, err := kp.Acquire(ctx, ResourceGraphQL); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("non-fast-fail background at the reserve line must wait until ctx expires, got %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "all API keys exhausted for GraphQL") {
		t.Errorf("a reserve block logged the pool-exhaustion line while every key held budget:\n%s", out)
	}
	if !strings.Contains(out, "foreground reserve") {
		t.Errorf("a reserve block must name the reserve as the reason:\n%s", out)
	}
}
