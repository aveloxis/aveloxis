// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

// TestBeltWithoutUsableResetBenchesForTheProbe (review round 2 on v0.29.55):
// a refusal whose reset header is present but already past (a refusal at
// the window's end, or a local clock ahead of GitHub's) or unparseable took
// MarkBudgetExhausted's no-reset branch, which zeroed the balance and kept the
// key's tracked LATER window — benching graphql for up to an hour (probe P5:
// reset −2 s on a +55 m window → 55 m). Rule 2 of the key pool contract says
// such a refusal lasts the probe window, as it already does on REST core.
// Without a usable reset the balance may be zeroed only when no future
// window is tracked; the refusal is the bench.
func TestBeltWithoutUsableResetBenchesForTheProbe(t *testing.T) {
	past := strconv.FormatInt(time.Now().Add(-2*time.Second).Unix(), 10)
	for name, reset := range map[string]string{"past": past, "unparseable": "soon", "absent": ""} {
		t.Run(name, func(t *testing.T) {
			kp := NewKeyPool([]string{"k"}, testLogger())
			key := kp.keys[0]
			kp.UpdateFromResponse(key, windowResp("graphql", "4999", time.Now().Add(55*time.Minute).Unix()))
			h := http.Header{}
			h.Set("X-RateLimit-Resource", "graphql")
			h.Set("X-RateLimit-Remaining", "0")
			if reset != "" {
				h.Set("X-RateLimit-Reset", reset)
			}
			before := time.Now()
			kp.MarkBudgetExhausted(key, ResourceGraphQL, &http.Response{StatusCode: http.StatusOK, Header: h})
			lo, hi := before.Add(graphQLDepletedProbe), time.Now().Add(graphQLDepletedProbe)
			if key.graphQLRefusedUntil.Before(lo) || key.graphQLRefusedUntil.After(hi) {
				t.Fatalf("graphQLRefusedUntil = %v, want the probe window [%v, %v]", key.graphQLRefusedUntil, lo, hi)
			}
			kp.mu.Lock()
			key.graphQLRefusedUntil = time.Now().Add(-time.Second)
			ok := kp.spendable(key, ResourceGraphQL, time.Now())
			kp.mu.Unlock()
			if !ok {
				t.Fatalf("after the probe the key is still benched (GraphQLRemaining=%d, GraphQLResetAt=%v) — the zero outlived the refusal", key.GraphQLRemaining, key.GraphQLResetAt)
			}
		})
	}
}

// TestBeltZeroNeverOutlivesTheRefusal: on a key with no tracked future
// window the belt also zeroes the balance; that zero's reset must be the
// refusal's end, or a +60 s refusal would leave a zero standing for the
// 5-minute probe and bench the key four minutes past its refusal.
func TestBeltZeroNeverOutlivesTheRefusal(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, testLogger())
	key := kp.keys[0]
	h := http.Header{}
	h.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(60*time.Second).Unix(), 10))
	kp.MarkBudgetExhausted(key, ResourceGraphQL, &http.Response{StatusCode: http.StatusOK, Header: h})
	if key.GraphQLRemaining != 0 {
		t.Fatalf("GraphQLRemaining = %d, want the zero on a key with no tracked window", key.GraphQLRemaining)
	}
	if !key.GraphQLResetAt.Equal(key.graphQLRefusedUntil) {
		t.Fatalf("zero resets at %v but the refusal ends at %v", key.GraphQLResetAt, key.graphQLRefusedUntil)
	}
}
