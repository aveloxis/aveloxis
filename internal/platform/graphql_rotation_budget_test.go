// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Copilot round 24 (PR #193): an in-body rate-limit ROTATION swaps to a
// fresh key — it is not a transport retry and must not spend the fixed
// transport budget (maxRetries=10). A pool with MORE keys than the budget
// must still consider every key before the error escapes. Here 11 keys are
// rate-limited and the 12th succeeds: pre-fix the loop gives up at attempt
// 10 (k10 and the live key never tried); post-fix rotations are bounded by
// the key count so the 12th request lands on the live key.
func TestGraphQLRotationsDoNotSpendTransportBudget(t *testing.T) {
	var mu sync.Mutex
	var reqs int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		reqs++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Resource", "graphql")
		if strings.Contains(auth, "live-key") {
			w.Header().Set("X-RateLimit-Remaining", "4999")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded for user."}]}`))
	}))
	defer server.Close()

	// 11 dead keys, then the live key LAST in round-robin order — so it is
	// only reachable on the 12th request, past the maxRetries=10 budget.
	tokens := []string{
		"dead00", "dead01", "dead02", "dead03", "dead04", "dead05",
		"dead06", "dead07", "dead08", "dead09", "dead10", "live-key",
	}
	keys := NewKeyPool(tokens, rlTestLogger())
	if keys.Len() != len(tokens) {
		t.Fatalf("Len() = %d, want %d", keys.Len(), len(tokens))
	}
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitHub)

	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL must rotate through every key past the transport budget, got error: %v", err)
	}
	if got.Hello != "world" {
		t.Errorf("got.Hello = %q, want \"world\"", got.Hello)
	}
	mu.Lock()
	defer mu.Unlock()
	// 11 rate-limited requests + 1 success. If rotations spent the transport
	// budget, the loop would have stopped at 10 and never reached the live key.
	if reqs < 12 {
		t.Errorf("expected >= 12 requests (11 rate-limited then live), got %d — rotations spent the transport budget", reqs)
	}
}
