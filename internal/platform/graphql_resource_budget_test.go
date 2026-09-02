// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The 2026-09-01 chaoss.tv diagnostic (pytorch/pytorch stuck since June):
// the key pool tracked ONLY the core rate-limit bucket — UpdateFromResponse
// discarded every response whose X-RateLimit-Resource was not "core", so a
// key with a full core budget and ZERO graphql points looked perfect to
// GetKey. 36,164 "API rate limit already exceeded for user ID …" errors in
// 4 days: GraphQL requests kept landing on graphql-dead keys, and monster
// PR-batch jobs (pytorch 86h43m, vscode, winget-pkgs, home-assistant/core)
// died on single unretried hits. These tests pin the per-resource budget.

func rlTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func gqlHeaderResp(remaining string, resetAt time.Time) *http.Response {
	h := http.Header{}
	h.Set("X-RateLimit-Resource", "graphql")
	h.Set("X-RateLimit-Remaining", remaining)
	if !resetAt.IsZero() {
		h.Set("X-RateLimit-Reset", strconv.FormatInt(resetAt.Unix(), 10))
	}
	return &http.Response{StatusCode: http.StatusOK, Header: h}
}

// TestUpdateFromResponseRoutesGraphQLResource: a graphql-resource response
// must update the key's GRAPHQL budget and leave the core counter alone —
// and vice versa. Pre-fix, the graphql response was DISCARDED outright.
func TestUpdateFromResponseRoutesGraphQLResource(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, rlTestLogger())
	key := kp.keys[0]
	reset := time.Now().Add(30 * time.Minute).Truncate(time.Second)

	kp.UpdateFromResponse(key, gqlHeaderResp("0", reset))
	if key.GraphQLRemaining != 0 {
		t.Errorf("GraphQLRemaining = %d after graphql-resource response with Remaining: 0, want 0 (the pool was graphql-blind — the pytorch root cause)", key.GraphQLRemaining)
	}
	if !key.GraphQLResetAt.Equal(reset) {
		t.Errorf("GraphQLResetAt = %v, want %v", key.GraphQLResetAt, reset)
	}
	if key.Remaining != 5000 {
		t.Errorf("core Remaining = %d — a graphql response must NEVER touch the core bucket (the documented starvation hazard)", key.Remaining)
	}

	// Core response leaves the graphql bucket alone.
	key2 := kp.keys[0]
	key2.GraphQLRemaining = 4000
	h := http.Header{}
	h.Set("X-RateLimit-Resource", "core")
	h.Set("X-RateLimit-Remaining", "17")
	kp.UpdateFromResponse(key2, &http.Response{StatusCode: 200, Header: h})
	if key2.Remaining != 17 {
		t.Errorf("core Remaining = %d, want 17", key2.Remaining)
	}
	if key2.GraphQLRemaining != 4000 {
		t.Errorf("GraphQLRemaining = %d — a core response must not touch the graphql bucket", key2.GraphQLRemaining)
	}
}

// TestGetGraphQLKeySkipsGraphQLDepletedKeys: checkout for GraphQL work must
// gate on the GRAPHQL budget. A key with a dead graphql bucket and a full
// core bucket is unusable for GraphQL (and still fine for REST).
func TestGetGraphQLKeySkipsGraphQLDepletedKeys(t *testing.T) {
	kp := NewKeyPool([]string{"dead-gql", "alive"}, rlTestLogger())
	kp.keys[0].GraphQLRemaining = 0
	kp.keys[0].GraphQLResetAt = time.Now().Add(time.Hour)

	for i := 0; i < 4; i++ {
		k, err := kp.GetGraphQLKey(context.Background())
		if err != nil {
			t.Fatalf("GetGraphQLKey: %v", err)
		}
		if k.Token != "alive" {
			t.Fatalf("checkout %d returned the graphql-dead key — GetGraphQLKey must skip keys with no graphql budget", i)
		}
	}
	// The graphql-dead key is still perfectly fine for REST (core budget full).
	k, err := kp.GetKey(context.Background())
	if err != nil {
		t.Fatalf("GetKey: %v", err)
	}
	if k == nil {
		t.Fatal("GetKey returned nil")
	}
}

// TestGetGraphQLKeyRefillsAfterReset: once a key's graphql window resets,
// the budget refills (5,000 points/hr) and the key is usable again.
func TestGetGraphQLKeyRefillsAfterReset(t *testing.T) {
	kp := NewKeyPool([]string{"only"}, rlTestLogger())
	kp.keys[0].GraphQLRemaining = 0
	kp.keys[0].GraphQLResetAt = time.Now().Add(50 * time.Millisecond)

	start := time.Now()
	k, err := kp.GetGraphQLKey(context.Background())
	if err != nil {
		t.Fatalf("GetGraphQLKey: %v", err)
	}
	if k.GraphQLRemaining <= kp.buffer {
		t.Errorf("GraphQLRemaining = %d after reset elapsed — window refill missing", k.GraphQLRemaining)
	}
	if time.Since(start) > 30*time.Second {
		t.Errorf("waited %v — should have woken at the 50ms graphql reset", time.Since(start))
	}
}

// TestGetGraphQLKeyFastFailReturnsInsteadOfWaiting: fast-fail callers
// (subdividing batches, the deferred-retry tickers) must get a typed
// rate-limit error immediately instead of blocking until a window reset —
// their claim/subdivision machinery IS the retry strategy.
func TestGetGraphQLKeyFastFailReturnsInsteadOfWaiting(t *testing.T) {
	kp := NewKeyPool([]string{"a", "b"}, rlTestLogger())
	for _, k := range kp.keys {
		k.GraphQLRemaining = 0
		k.GraphQLResetAt = time.Now().Add(time.Hour)
	}
	start := time.Now()
	_, err := kp.GetGraphQLKey(WithGraphQLFastFail(context.Background()))
	if err == nil {
		t.Fatal("expected a typed error when every graphql budget is exhausted under fast-fail")
	}
	if ClassifyError(err) != ClassRateLimit {
		t.Errorf("ClassifyError = %v, want ClassRateLimit so subdivision/deferral machinery dispatches correctly", ClassifyError(err))
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("fast-fail checkout blocked %v — must return immediately", time.Since(start))
	}
}

// TestGraphQLBackgroundReserveKeepsHeadroom: background sweeps (activity
// history, classification) must refuse keys whose graphql budget is below
// the reserve, leaving that headroom for foreground collection. Foreground
// checkout still accepts the same key.
func TestGraphQLBackgroundReserveKeepsHeadroom(t *testing.T) {
	kp := NewKeyPool([]string{"low", "high"}, rlTestLogger())
	// AT the reserve exactly: selection is strict (> minBudget), so a key
	// holding precisely the reserve is refused for background work too
	// (review F6 — the boundary the first test never probed).
	kp.keys[0].GraphQLRemaining = GraphQLBackgroundReserve
	kp.keys[1].GraphQLRemaining = GraphQLBackgroundReserve + 100

	bg := WithGraphQLBackgroundBudget(context.Background())
	for i := 0; i < 4; i++ {
		k, err := kp.GetGraphQLKey(bg)
		if err != nil {
			t.Fatalf("background GetGraphQLKey: %v", err)
		}
		if k.Token != "high" {
			t.Fatalf("background checkout %d got the below-reserve key — the reserve exists so sweeps can't starve collection", i)
		}
	}
	// Foreground checkout may use the low key (it's above the plain buffer).
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		k, err := kp.GetGraphQLKey(context.Background())
		if err != nil {
			t.Fatalf("foreground GetGraphQLKey: %v", err)
		}
		seen[k.Token] = true
	}
	if !seen["low"] {
		t.Error("foreground checkout never used the below-reserve key — the reserve must apply to BACKGROUND callers only")
	}
}

// TestClassifyGraphQLErrorsRecognizesRateLimitVariants: production
// (2026-09-01, pytorch shard 41) received type "RATE_LIMIT" — not the
// documented "RATE_LIMITED" — and the classifier fell to the generic
// ClassFatal arm, so subdivision AND the size-1 REST fallback never
// engaged and an 86h43m job died on one hit. Every rate-limit spelling
// must classify ClassRateLimit.
func TestClassifyGraphQLErrorsRecognizesRateLimitVariants(t *testing.T) {
	cases := []struct {
		name string
		errs []graphqlError
	}{
		{"documented RATE_LIMITED", []graphqlError{{Type: "RATE_LIMITED", Message: "API rate limit exceeded"}}},
		{"observed RATE_LIMIT", []graphqlError{{Type: "RATE_LIMIT", Message: "API rate limit already exceeded for user ID 172139126."}}},
		{"typeless with rate-limit message", []graphqlError{{Message: "API rate limit already exceeded for user ID 172139126."}}},
		{"dominates other errors", []graphqlError{{Type: "NOT_FOUND", Message: "x"}, {Type: "RATE_LIMIT", Message: "API rate limit already exceeded"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyGraphQLErrors(tc.errs)
			if ClassifyError(err) != ClassRateLimit {
				t.Errorf("ClassifyError = %v for %v, want ClassRateLimit", ClassifyError(err), tc.errs)
			}
		})
	}
}

// TestGraphQLRotatesOnInBodyRateLimit is the end-to-end pin for the
// production failure: an in-body RATE_LIMITED on key 1 must mark that
// key's graphql budget dead, rotate to a fresh key, and SUCCEED — not
// return the error to the caller (which is what killed pytorch's 86h run).
func TestGraphQLRotatesOnInBodyRateLimit(t *testing.T) {
	var mu sync.Mutex
	var tokens []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		tokens = append(tokens, auth)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-RateLimit-Resource", "graphql")
		if strings.Contains(auth, "dead-key") {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":[{"type":"RATE_LIMIT","message":"API rate limit already exceeded for user ID 172139126."}]}`))
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "4999")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"dead-key", "live-key"}, rlTestLogger())
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitHub)

	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL must rotate past an in-body rate limit, got error: %v", err)
	}
	if got.Hello != "world" {
		t.Errorf("got.Hello = %q, want \"world\"", got.Hello)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) < 2 {
		t.Fatalf("expected >= 2 requests (rate-limited then rotated), got %d", len(tokens))
	}
	if tokens[0] == tokens[len(tokens)-1] {
		t.Errorf("final request used the SAME token as the rate-limited one — rotation did not happen")
	}
	// And the pool must have LEARNED: the dead key's graphql budget is 0.
	for _, k := range keys.keys {
		if k.Token == "dead-key" && k.GraphQLRemaining > 0 {
			t.Errorf("dead key's GraphQLRemaining = %d, want 0 (marked from the in-body rate limit)", k.GraphQLRemaining)
		}
	}
}

// TestPerPathRateLimitErrorsAreGlobal (review F1 — MEDIUM): a rate-limit
// error carrying a "path" must be hoisted to a whole-query failure like
// RESOURCE_LIMITS_EXCEEDED is (v0.27.79: GitHub reported the global
// condition as per-path entries and 216K contributors were mark-stamped
// dataless from empty-but-successful results). Without the hoist, the
// per-path arm WARNs, returns nil, and none of the mark/rotate machinery
// engages.
func TestPerPathRateLimitErrorsAreGlobal(t *testing.T) {
	body := []byte(`{"data":{"u0":null},"errors":[{"type":"RATE_LIMIT","path":["u0"],"message":"API rate limit already exceeded for user ID 172139126."}]}`)
	var dst map[string]any
	err := parseGraphQLResponse(body, &dst, rlTestLogger())
	if err == nil {
		t.Fatal("a per-path rate-limit error must fail the query — an empty-but-successful result is the v0.27.79 incident shape")
	}
	if ClassifyError(err) != ClassRateLimit {
		t.Errorf("ClassifyError = %v, want ClassRateLimit", ClassifyError(err))
	}
}

// TestGitLabGraphQLChecksOutByUnifiedBudget (review F2): GitLab has ONE
// unified rate limit (no X-RateLimit-Resource header — its RateLimit-*
// headers route to the CORE bucket), so gating GitLab GraphQL checkout on
// the never-updated graphql bucket would hand out core-exhausted keys.
// A GitLab-auth client must check out by the core budget.
func TestGitLabGraphQLChecksOutByUnifiedBudget(t *testing.T) {
	var mu sync.Mutex
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		// GraphQL sends "Authorization: bearer <key>" for BOTH forges.
		tokens = append(tokens, strings.TrimPrefix(r.Header.Get("Authorization"), "bearer "))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"core-dead", "core-alive"}, rlTestLogger())
	keys.keys[0].Remaining = 0 // unified budget spent
	keys.keys[0].ResetAt = time.Now().Add(time.Hour)
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitLab)

	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, tok := range tokens {
		if tok == "core-dead" {
			t.Errorf("GitLab GraphQL checkout used a key whose UNIFIED budget is spent — GitLab must gate on the core bucket")
		}
	}
}

// TestMixedExhaustionClassifiesTransient (review F3): one in-body rate
// limit followed by 5xxs to exhaustion must NOT wear the rate-limit
// class — the stale cause would make deferring callers skip their
// subdivision. Only a rate limit on the FINAL attempt wins the wrap.
func TestMixedExhaustionClassifiesTransient(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()

	var mu sync.Mutex
	n := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		n++
		first := n == 1
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if first {
			w.Header().Set("X-RateLimit-Resource", "graphql")
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`))
			return
		}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	// Two keys so the post-rate-limit rotation finds a fresh one and the
	// remaining attempts burn on 502s.
	keys := NewKeyPool([]string{"k1", "k2"}, rlTestLogger())
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitHub)

	var dst map[string]any
	err := c.GraphQL(WithGraphQLFastFail(context.Background()), "{ x }", nil, &dst)
	if err == nil {
		t.Fatal("expected exhaustion error")
	}
	if got := ClassifyError(err); got != ClassTransient {
		t.Errorf("mixed exhaustion (rate limit on attempt 1, 5xxs after) classified %v, want ClassTransient — the stale rate-limit cause must not win the wrap (err=%v)", got, err)
	}
}

// TestGitLabInBodyRateLimitExhaustsCoreBudget (Copilot round 2 on
// PR #193, suppressed #2): GitLab GraphQL checks out via GetKey (the
// unified/core budget — no X-RateLimit-Resource header exists there),
// so an in-body rate limit that only zeroes GraphQLRemaining would be
// decorative: the next retry's GetKey reads Remaining, sees it
// healthy, and re-serves the SAME exhausted token through the whole
// retry budget. The in-body branch must mark the budget the checkout
// dimension actually reads: core for AuthGitLab, graphql for GitHub.
func TestGitLabInBodyRateLimitExhaustsCoreBudget(t *testing.T) {
	var mu sync.Mutex
	var tokens []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "bearer ")
		mu.Lock()
		tokens = append(tokens, tok)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if tok == "gl-dead" {
			// GitLab-shaped in-body rate limit: HTTP 200 + errors array,
			// NO usable rate headers on the response.
			_, _ = w.Write([]byte(`{"errors":[{"message":"API rate limit exceeded"}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"gl-dead", "gl-alive"}, rlTestLogger())
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitLab)

	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL must rotate past a GitLab in-body rate limit, got: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) < 2 || tokens[len(tokens)-1] == "gl-dead" {
		t.Fatalf("tokens = %v — the retry must rotate off the exhausted GitLab key", tokens)
	}
	// The pool must have learned in the dimension GitLab CHECKS OUT by:
	// the dead key's CORE budget, not (only) its graphql bucket.
	for _, k := range keys.keys {
		if k.Token == "gl-dead" && k.Remaining > 0 {
			t.Errorf("gl-dead key's core Remaining = %d, want 0 — GetKey would re-serve this exhausted token", k.Remaining)
		}
	}
}

// TestMarkCoreExhaustedZeroesAndSetsProbeWindow — the unit contract of
// the GitLab arm's marker: core Remaining goes to 0, and when no reset
// is known a short probe window is installed so the key re-checks in
// minutes rather than being consulted immediately.
func TestMarkCoreExhaustedZeroesAndSetsProbeWindow(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, rlTestLogger())
	key := kp.keys[0]
	key.Remaining = 4999
	key.ResetAt = time.Time{}
	kp.MarkCoreExhausted(key)
	if key.Remaining != 0 {
		t.Fatalf("Remaining = %d, want 0", key.Remaining)
	}
	if key.ResetAt.Before(time.Now()) || key.ResetAt.After(time.Now().Add(10*time.Minute)) {
		t.Fatalf("ResetAt = %v, want a short future probe window", key.ResetAt)
	}
}

// TestHTTPRateLimitExhaustionClassifiesRateLimit (Copilot round 6 on
// PR #193, suppressed #3): a retry budget exhausted by PERSISTENT
// HTTP throttling (429, or 403 + Retry-After) must classify
// ClassRateLimit like an in-body RATE_LIMITED exhaustion does —
// downstream deferral/subdivision keys on the class, and the
// pre-fix return wore ErrTransient because only the in-body branch
// recorded attempt state.
func TestHTTPRateLimitExhaustionClassifiesRateLimit(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()

	for _, tc := range []struct {
		name  string
		serve func(w http.ResponseWriter)
	}{
		{"429", func(w http.ResponseWriter) {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
		}},
		{"403-retry-after", func(w http.ResponseWriter) {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusForbidden)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.serve(w)
			}))
			defer server.Close()

			keys := NewKeyPool([]string{"k"}, rlTestLogger())
			c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitHub)
			var got map[string]any
			err := c.GraphQL(WithGraphQLFastFail(context.Background()), "{ hello }", nil, &got)
			if err == nil {
				t.Fatal("persistent throttling must exhaust to an error")
			}
			if ClassifyError(err) != ClassRateLimit {
				t.Fatalf("ClassifyError = %v (%v), want ClassRateLimit — deferral/subdivision keys on the class", ClassifyError(err), err)
			}
		})
	}
}

// TestHeaderlessRateLimit403MarksTheCheckoutBudget (Copilot round 7
// on PR #193): a 403 with X-RateLimit-Remaining: 0 but NO
// X-RateLimit-Resource header (the older GitHub shape) routes the
// zero into the CORE bucket, while GitHub's GraphQL checkout reads
// GraphQLRemaining — pre-fix the exhausted key stayed eligible and
// every retry reused it. The branch must mark the budget the
// client's checkout dimension actually reads, so under fast-fail a
// single-key pool stops after ONE request instead of burning the
// whole budget on the same dead key.
func TestHeaderlessRateLimit403MarksTheCheckoutBudget(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()

	var mu sync.Mutex
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		// Deliberately NO X-RateLimit-Resource header.
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	keys := NewKeyPool([]string{"only-key"}, rlTestLogger())
	c := NewHTTPClient(server.URL, keys, rlTestLogger(), AuthGitHub)
	var got map[string]any
	err := c.GraphQL(WithGraphQLFastFail(context.Background()), "{ hello }", nil, &got)
	if err == nil {
		t.Fatal("an exhausted single-key pool must surface an error")
	}
	if ClassifyError(err) != ClassRateLimit {
		t.Fatalf("ClassifyError = %v (%v), want ClassRateLimit", ClassifyError(err), err)
	}
	for _, k := range keys.keys {
		if k.GraphQLRemaining > 0 {
			t.Fatalf("GraphQLRemaining = %d, want 0 — header omission must not leave the key eligible for the graphql checkout", k.GraphQLRemaining)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("requests = %d, want 1 — the marked key must not be re-served (pre-fix the whole retry budget burned on it)", requests)
	}
}

// TestBackgroundReserveActuallyBinds (Copilot round 8 on PR #193,
// active): checkout must RESERVE the query's cost — pre-fix it only
// gated on the pre-request balance, so concurrent background windows
// all saw the same number and collectively spent through
// GraphQLBackgroundReserve. With the optimistic 1-point spend, a key
// holding reserve+3 admits exactly 3 background checkouts before the
// gate closes (the response headers later overwrite with the
// authoritative absolute, so an underestimate reconciles itself).
func TestBackgroundReserveActuallyBinds(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, rlTestLogger())
	kp.keys[0].GraphQLRemaining = GraphQLBackgroundReserve + 3
	ctx := WithGraphQLFastFail(WithGraphQLBackgroundBudget(context.Background()))

	got := 0
	for i := 0; i < 10; i++ {
		if _, err := kp.GetGraphQLKey(ctx); err != nil {
			if !errors.Is(err, ErrGraphQLBudgetExhausted) {
				t.Fatalf("checkout %d: %v", i, err)
			}
			break
		}
		got++
	}
	if got != 3 {
		t.Fatalf("background checkouts admitted = %d, want exactly 3 — the reserve must BIND, not just gate on a never-decremented balance", got)
	}
}

// TestBackgroundReserveBindsUnderConcurrency — the same contract with
// 32 goroutines racing the checkout (run under -race): total
// admissions never exceed the excess above the reserve.
func TestBackgroundReserveBindsUnderConcurrency(t *testing.T) {
	kp := NewKeyPool([]string{"k"}, rlTestLogger())
	kp.keys[0].GraphQLRemaining = GraphQLBackgroundReserve + 5
	ctx := WithGraphQLFastFail(WithGraphQLBackgroundBudget(context.Background()))

	var mu sync.Mutex
	admitted := 0
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := kp.GetGraphQLKey(ctx); err == nil {
				mu.Lock()
				admitted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if admitted != 5 {
		t.Fatalf("admitted = %d, want exactly 5 — concurrent checkouts must not spend through the reserve", admitted)
	}
}
