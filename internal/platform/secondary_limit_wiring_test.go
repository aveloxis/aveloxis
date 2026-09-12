// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// 2026-09-12, Bug C of the chaoss.tv analysis: a 403 + Retry-After (or a
// 429) changed NO pool state — only the calling goroutine slept, and
// every other caller kept being handed the throttled key, each earning
// its own rejection and its own 60-second sleep (179 rejections in one
// second). These are the WIRING pins for the fix: the two forge clients
// must rest the key in the pool, so the very next checkout — from any
// caller — lands on a different key. MarkSecondaryLimited's own
// contract is pinned in ratelimit_admission_test.go; without these two,
// a client that forgot to call it would leave the pool green.

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// throttleOnce serves `status` + Retry-After to the FIRST request it
// sees and 200 to every later one, recording the bearer/token each
// carried. retryAfter is the header value: the GraphQL client's sleep
// is seamed (SetGraphQLSleepForTest) so any value works there; the REST
// client sleeps inline, so its tests pass "0" — the pool still rests
// the key (MarkSecondaryLimited floors a zero at one second), which is
// exactly the contract under test.
func throttleOnce(t *testing.T, status int, retryAfter, body string) (*httptest.Server, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var tokens []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n := len(tokens)
		tok := r.Header.Get("Authorization")
		if tok == "" {
			tok = r.Header.Get("PRIVATE-TOKEN")
		}
		tokens = append(tokens, tok)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if n == 0 {
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(status)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), tokens...)
	}
}

func restingKeys(kp *KeyPool) map[string]bool {
	kp.mu.Lock()
	defer kp.mu.Unlock()
	out := map[string]bool{}
	for _, k := range kp.keys {
		if time.Now().Before(k.secondaryUntil) {
			out[k.Token] = true
		}
	}
	return out
}

// TestGraphQLSecondaryLimitRestsTheKeyInThePool: after the 403 the
// throttled key must be resting in the POOL (not just in this
// goroutine's sleep), and the retry — routed through Acquire — must
// land on the other key.
func TestGraphQLSecondaryLimitRestsTheKeyInThePool(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	srv, seen := throttleOnce(t, http.StatusForbidden, "30", `{"data":{"hello":"world"}}`)

	keys := NewKeyPool([]string{"first", "second"}, rlTestLogger())
	c := NewHTTPClient(srv.URL, keys, rlTestLogger(), AuthGitHub)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL: %v", err)
	}
	tokens := seen()
	if len(tokens) != 2 {
		t.Fatalf("requests = %v, want the throttled attempt then one retry", tokens)
	}
	if tokens[0] == tokens[1] {
		t.Fatalf("retry reused the throttled key %q — the pool was never told to rest it (Bug C)", tokens[0])
	}
	throttled := tokens[0][len("bearer "):]
	if !restingKeys(keys)[throttled] {
		t.Fatalf("key %q is not resting in the pool after a 403 + Retry-After", throttled)
	}
}

// TestRESTSecondaryLimitRestsTheKeyInThePool — the same contract on the
// REST client, for both throttle shapes GitHub uses.
func TestRESTSecondaryLimitRestsTheKeyInThePool(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv, seen := throttleOnce(t, status, "0", `{"ok":true}`)
			keys := NewKeyPool([]string{"first", "second"}, rlTestLogger())
			c := NewHTTPClient(srv.URL, keys, rlTestLogger(), AuthGitHub)
			resp, err := c.Get(context.Background(), "/x")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			resp.Body.Close()
			tokens := seen()
			if len(tokens) != 2 || tokens[0] == tokens[1] {
				t.Fatalf("requests = %v, want a throttled attempt then a retry on the OTHER key", tokens)
			}
			throttled := tokens[0][len("token "):]
			if !restingKeys(keys)[throttled] {
				t.Fatalf("key %q is not resting in the pool after a %d + Retry-After", throttled, status)
			}
		})
	}
}
