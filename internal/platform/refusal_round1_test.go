// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Round 1 of the v0.29.55 fresh-context review (SR-20).

// TestIsDefinitiveAnswer (R1-F1): a failed lookup may stamp a cooldown only
// when the forge ANSWERED about the item — not found, gone, not visible, or
// a request it rejects. Everything else (rate limit, transient, auth,
// shutdown, an empty pool, a cut-off body, an unknown error) says nothing
// about the item. The round-1 probe: a GitLab-only serve has an EMPTY GitHub
// pool, Acquire's plain "no API keys configured" error classified Fatal, and
// the enrichment predicate — which listed the NON-definitive classes —
// stamped every login in the batch.
func TestIsDefinitiveAnswer(t *testing.T) {
	_, _, emptyPoolErr := NewKeyPool(nil, testLogger()).Acquire(context.Background(), ResourceCore)
	if emptyPoolErr == nil {
		t.Fatal("precondition: an empty pool must refuse Acquire")
	}
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"404", fmt.Errorf("%w: u", ErrNotFound), true},
		{"410", fmt.Errorf("%w: u", ErrGone), true},
		{"403 not a rate limit", fmt.Errorf("%w: u", ErrForbidden), true},
		{"400", fmt.Errorf("bad request: u: %w", ErrRequestRejected), true},
		{"422 pagination cap", fmt.Errorf("%w: u", ErrPaginationLimitExceeded), true},
		{"empty pool", fmt.Errorf("getting API key: %w", emptyPoolErr), false},
		{"retries exhausted", fmt.Errorf("exhausted 10 retries for u: %w", ErrTransient), false},
		{"graphql budget", ErrGraphQLBudgetExhausted, false},
		{"all keys invalidated", ErrAllKeysInvalidated, false},
		{"deadline", context.DeadlineExceeded, false},
		{"shutdown", context.Canceled, false},
		{"cut-off body", fmt.Errorf("decode: %w", io.ErrUnexpectedEOF), false},
		{"unknown", errors.New("boom"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsDefinitiveAnswer(tc.err); got != tc.want {
				t.Fatalf("IsDefinitiveAnswer(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestRejectedRequestsWrapErrRequestRejected: the 400 and non-cap 422 arms
// are the forge rejecting the request itself — the only Fatal-class answers
// IsDefinitiveAnswer accepts — so they must carry the sentinel.
func TestRejectedRequestsWrapErrRequestRejected(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"message":"Validation Failed"}`))
		}))
		c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, testLogger()), testLogger(), AuthGitHub)
		_, err := c.Get(context.Background(), "/users/x")
		srv.Close()
		if !errors.Is(err, ErrRequestRejected) || ClassifyError(err) != ClassFatal {
			t.Errorf("%d: err=%v class=%v, want ErrRequestRejected and ClassFatal (unchanged)", status, err, ClassifyError(err))
		}
	}
}

// graphqlRefusalServer serves a GraphQL refusal to "dead" and data to
// "live". hdr is applied to the refusal.
func graphqlRefusalServer(t *testing.T, status int, hdr map[string]string, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") == "bearer dead" {
			for k, v := range hdr {
				w.Header().Set(k, v)
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"hello":"world"}}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func near(t *testing.T, name string, got, want time.Time) {
	t.Helper()
	if d := got.Sub(want); d < -2*time.Second || d > 2*time.Second {
		t.Errorf("%s = %v, want ~%v", name, got, want)
	}
}

// TestGraphQLRefusalBenchesUntilTheResponseReset (R1-F2): the GraphQL belt
// must bench until the REFUSAL's reset, not the key's tracked window, and
// one response is one refusal. Round-1 probes: a graphql 403 with reset
// +3m on a key tracked on a +55m window stayed benched 55m and counted 2;
// a header-less 403 with reset +4m benched graphql for the 5m probe.
func TestGraphQLRefusalBenchesUntilTheResponseReset(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	now := time.Now()
	for _, tc := range []struct {
		name     string
		status   int
		hdr      map[string]string
		body     string
		wantCore bool
	}{
		{"403 graphql resource", http.StatusForbidden,
			map[string]string{"X-RateLimit-Resource": "graphql", "X-RateLimit-Remaining": "0"}, `{}`, false},
		{"403 without resource header", http.StatusForbidden,
			map[string]string{"X-RateLimit-Remaining": "0"}, `{}`, true},
		{"in-body RATE_LIMITED", http.StatusOK,
			map[string]string{"X-RateLimit-Resource": "graphql", "X-RateLimit-Remaining": "0"},
			`{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset := now.Add(3 * time.Minute)
			hdr := map[string]string{"X-RateLimit-Reset": strconv.FormatInt(reset.Unix(), 10)}
			for k, v := range tc.hdr {
				hdr[k] = v
			}
			srv := graphqlRefusalServer(t, tc.status, hdr, tc.body)
			keys := NewKeyPool([]string{"dead", "live"}, testLogger())
			later := now.Add(55 * time.Minute).Unix()
			keys.UpdateFromResponse(keys.keys[0], windowResp("graphql", "4999", later))
			keys.UpdateFromResponse(keys.keys[1], windowResp("graphql", "4000", later))
			c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitHub)
			var got map[string]any
			if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
				t.Fatalf("GraphQL: %v", err)
			}
			snap, _ := keys.Snapshot()
			near(t, "GraphQLRefusedUntil", snap[0].GraphQLRefusedUntil, reset)
			if tc.wantCore {
				near(t, "CoreRefusedUntil", snap[0].CoreRefusedUntil, reset)
			}
			if snap[0].Refusals != 1 {
				t.Errorf("Refusals = %d, want 1 — one response is one refusal", snap[0].Refusals)
			}
			// The refusal is the bench: once it ends the key serves again,
			// without waiting for the later tracked window.
			keys.mu.Lock()
			keys.keys[0].graphQLRefusedUntil = time.Now().Add(-time.Second)
			spendable := keys.spendable(keys.keys[0], ResourceGraphQL, time.Now())
			keys.mu.Unlock()
			if !spendable {
				t.Errorf("after the refusal's reset the key is still benched (GraphQLRemaining=%d) — the belt zeroed a balance the headers own", snap[0].GraphQL)
			}
		})
	}
}

// TestSearchRefusalRotatesToAnotherKey (R1-F3): search has its own
// per-user budget. Pre-fix the pool had no search bucket, the REST client
// kept its plain retry, and the round-1 probe measured 10 hits on one key
// in ~1 ms. A search refusal must bench that key for SEARCH only and the
// request must go straight to another key.
func TestSearchRefusalRotatesToAnotherKey(t *testing.T) {
	reset := strconv.FormatInt(time.Now().Add(50*time.Second).Unix(), 10)
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		mu.Lock()
		hits[auth]++
		n := hits[auth]
		mu.Unlock()
		w.Header().Set("X-RateLimit-Resource", "search")
		w.Header().Set("X-RateLimit-Reset", reset)
		if auth == "token a" && n == 1 {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "20")
		_, _ = w.Write([]byte(`{"total_count":0,"items":[]}`))
	}))
	defer srv.Close()
	keys := NewKeyPool([]string{"a", "b"}, testLogger())
	c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitHub)
	// Rotation is immediate: a client that waited out the 50-second reset
	// instead (and then happened to land on b) must fail here.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Get(ctx, "/search/users?q=x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
	mu.Lock()
	if hits["token a"] != 1 || hits["token b"] != 1 {
		t.Fatalf("hits = %v, want one refused request on a and one on b", hits)
	}
	mu.Unlock()
	snap, _ := keys.Snapshot()
	if snap[0].SearchRefusedUntil.IsZero() || !snap[0].CoreRefusedUntil.IsZero() {
		t.Fatalf("snapshot a = %+v, want search benched and core untouched", snap[0])
	}
	// Core work may still use the search-refused key.
	keys.UpdateFromResponse(keys.keys[1], windowResp("core", "10", time.Now().Add(30*time.Minute).Unix()))
	k, release, err := keys.Acquire(context.Background(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if k != keys.keys[0] {
		t.Fatalf("core Acquire = %q, want a — a search refusal must not bench core", k.Token)
	}
}

// TestUnbenchedRefusalWaitsForItsReset (R1-F3): a refusal on a bucket the
// pool does not bench for this request (a resource label it does not track)
// cannot be rotated around; the client must wait for that reset, as GitHub
// instructs, not re-send on the same key at once.
func TestUnbenchedRefusalWaitsForItsReset(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.Header().Set("X-RateLimit-Resource", "code_search")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(30*time.Second).Unix(), 10))
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"only"}, testLogger()), testLogger(), AuthGitHub)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := c.Get(ctx, "/search/code?q=x")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the wait for the reset to run into the deadline", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("hits = %d, want 1 — the client re-sent before the refusal's reset", hits)
	}
}
