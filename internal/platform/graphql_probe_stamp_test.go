// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestGetGraphQLKeyStampsProbeWhenResetUnknown (Copilot round 22 on
// PR #193): when a key is exhausted (Remaining at/below budget) but its
// successful responses carried NO Reset, GraphQLResetAt stays zero. The
// refill guard skips zero-reset keys, checkout skips below-budget keys,
// and the earliestWake scan skips a zero wake — so without a probe stamp
// the key is stuck forever and non-fast-fail callers poll the 30s
// fallback indefinitely. GetGraphQLKey must stamp the same bounded probe
// deadline MarkGraphQLExhausted uses.
func TestGetGraphQLKeyStampsProbeWhenResetUnknown(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	kp := NewKeyPool([]string{"t"}, quiet)
	kp.keys[0].GraphQLRemaining = 0         // exhausted
	kp.keys[0].GraphQLResetAt = time.Time{} // Remaining known, Reset unknown

	before := time.Now()
	_, err := kp.GetGraphQLKey(WithGraphQLFastFail(context.Background()))
	if !errors.Is(err, ErrGraphQLBudgetExhausted) {
		t.Fatalf("fast-fail on an exhausted pool must return ErrGraphQLBudgetExhausted, got %v", err)
	}

	got := kp.keys[0].GraphQLResetAt
	if got.IsZero() {
		t.Fatal("GetGraphQLKey must stamp a bounded probe deadline on an exhausted key with no known reset — else the refill guard never fires and callers poll forever (round 22)")
	}
	if got.Before(before.Add(graphQLDepletedProbe-time.Second)) ||
		got.After(time.Now().Add(graphQLDepletedProbe+time.Second)) {
		t.Errorf("probe deadline = %v, want ~now+%v", got, graphQLDepletedProbe)
	}
}
