// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review round 2 on PR #203 (review 5187405891, 2026-09-12):
// response-derived key state that a waiter's selection reads must be
// in the pool BEFORE the lease is released. Round 1 moved the header
// state (primary budget, secondary-limit rest) under the lease; three
// marks were still applied after release():
//
//   - the 401 auth strike (REST handleResponse, GraphQL 401 arm) — at
//     the quarantine threshold, waiters woken by release could lease
//     the key that this very response was about to quarantine;
//   - GraphQL's in-body RATE_LIMITED (HTTP 200) mark, which needs the
//     body; the round-1 site note called the gap "at most one wasted
//     request", but release() Broadcasts, so every parked waiter can
//     select the key in that gap;
//   - GraphQL's 403 + Remaining: 0 belt for the resource-header-less
//     shape, whose zero UpdateFromResponse routes into CORE.
//
// These tests observe the pool at the instant of each release, so they
// pin the ORDER as behavior rather than as source shape.

package platform

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// releaseObs is one key's pool state captured inside releaseFunc, under
// kp.mu, before the in-flight counters drop and the Broadcast fires.
type releaseObs struct {
	key              *APIKey
	authStrikes      int
	resting          bool
	coreRemaining    int
	graphqlRemaining int
}

func observeLeaseReleases(t *testing.T) func() []releaseObs {
	t.Helper()
	if leaseReleaseObserver != nil {
		t.Fatal("leaseReleaseObserver must default to nil (a test seam, never set in production)")
	}
	var mu sync.Mutex
	var got []releaseObs
	leaseReleaseObserver = func(k *APIKey) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, releaseObs{
			key: k, authStrikes: k.authStrikes, resting: k.restingAt(time.Now()),
			coreRemaining: k.Remaining, graphqlRemaining: k.GraphQLRemaining,
		})
	}
	t.Cleanup(func() { leaseReleaseObserver = nil })
	return func() []releaseObs {
		mu.Lock()
		defer mu.Unlock()
		return append([]releaseObs(nil), got...)
	}
}

// scriptedServer answers the first request with first and every later
// request with rest.
func scriptedServer(t *testing.T, first, rest http.HandlerFunc) *httptest.Server {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			first(w, r)
			return
		}
		rest(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) }
}

func graphqlData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"data":{"x":1}}`)
}

type wireCall func(ctx context.Context, c *HTTPClient) error

var restGet wireCall = func(ctx context.Context, c *HTTPClient) error {
	resp, err := c.Get(ctx, "/x")
	if resp != nil {
		resp.Body.Close()
	}
	return err
}

var graphqlCall wireCall = func(ctx context.Context, c *HTTPClient) error {
	var dest struct {
		X int `json:"x"`
	}
	return c.GraphQL(ctx, "query{x}", nil, &dest)
}

func TestAuthStrikeRecordedBeforeLeaseRelease(t *testing.T) {
	t.Cleanup(SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil }))
	for _, tc := range []struct {
		name     string
		call     wireCall
		terminal http.HandlerFunc // a non-2xx that ends the call without resetting strikes
	}{
		{"REST Get", restGet, status(http.StatusNotFound)},
		{"GraphQL", graphqlCall, status(http.StatusForbidden)},
	} {
		t.Run(tc.name+"/threshold 401 quarantines under the lease", func(t *testing.T) {
			releases := observeLeaseReleases(t)
			srv := scriptedServer(t, status(http.StatusUnauthorized), tc.terminal)
			kp := NewKeyPool([]string{"tok-a", "tok-b"}, rlTestLogger())
			kp.mu.Lock()
			for _, k := range kp.keys {
				k.authStrikes = maxAuthStrikes - 1 // whichever key serves the 401 hits the threshold
			}
			kp.mu.Unlock()
			_ = tc.call(context.Background(), NewHTTPClient(srv.URL, kp, rlTestLogger(), AuthGitHub))

			obs := releases()
			if len(obs) < 1 {
				t.Fatal("no lease was released")
			}
			if !obs[0].resting {
				t.Errorf("at the release after the threshold 401 the key was not yet quarantined (strikes=%d) — a waiter woken by that release can lease the key this response quarantines", obs[0].authStrikes)
			}
		})
		t.Run(tc.name+"/one 401 is one strike", func(t *testing.T) {
			releases := observeLeaseReleases(t)
			srv := scriptedServer(t, status(http.StatusUnauthorized), tc.terminal)
			kp := NewKeyPool([]string{"tok-a", "tok-b"}, rlTestLogger())
			_ = tc.call(context.Background(), NewHTTPClient(srv.URL, kp, rlTestLogger(), AuthGitHub))

			obs := releases()
			if len(obs) < 1 {
				t.Fatal("no lease was released")
			}
			if obs[0].authStrikes != 1 {
				t.Errorf("strikes on the 401 key at its release = %d, want 1 (recorded under the lease)", obs[0].authStrikes)
			}
			kp.mu.Lock()
			after := obs[0].key.authStrikes
			kp.mu.Unlock()
			if after != 1 {
				t.Errorf("strikes on the 401 key after the call = %d, want exactly 1 — a second, client-side strike double-counts and quarantines valid keys after two transient 401s instead of three", after)
			}
		})
	}
}

func TestGraphQLBudgetMarksAppliedBeforeLeaseRelease(t *testing.T) {
	t.Cleanup(SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil }))
	inBodyRateLimited := func(w http.ResponseWriter, r *http.Request) {
		// No rate-limit headers: the mark must not depend on the header
		// belt (MarkGraphQLExhausted's own contract).
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`)
	}
	forbiddenRemainingZero := func(w http.ResponseWriter, r *http.Request) {
		// The older shape: Remaining: 0 with no X-RateLimit-Resource, which
		// UpdateFromResponse routes into the CORE bucket.
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.WriteHeader(http.StatusForbidden)
	}
	for _, tc := range []struct {
		name  string
		style AuthStyle
		first http.HandlerFunc
		check func(o releaseObs) bool
		want  string
	}{
		{"in-body RATE_LIMITED (GitHub)", AuthGitHub, inBodyRateLimited,
			func(o releaseObs) bool { return o.graphqlRemaining == 0 }, "graphql budget zeroed"},
		{"in-body RATE_LIMITED (GitLab, unified core bucket)", AuthGitLab, inBodyRateLimited,
			func(o releaseObs) bool { return o.coreRemaining == 0 }, "core budget zeroed"},
		{"403 + Remaining: 0 without resource header (GitHub)", AuthGitHub, forbiddenRemainingZero,
			func(o releaseObs) bool { return o.graphqlRemaining == 0 }, "graphql budget zeroed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releases := observeLeaseReleases(t)
			srv := scriptedServer(t, tc.first, graphqlData)
			kp := NewKeyPool([]string{"tok-a", "tok-b"}, rlTestLogger())
			if err := graphqlCall(context.Background(), NewHTTPClient(srv.URL, kp, rlTestLogger(), tc.style)); err != nil {
				t.Fatalf("the call should rotate to the healthy key and succeed: %v", err)
			}
			obs := releases()
			if len(obs) < 2 {
				t.Fatalf("expected a release per attempt (rate-limited then rotated), got %d", len(obs))
			}
			if !tc.check(obs[0]) {
				t.Errorf("at the release after the rate-limited response: want %s, got core=%d graphql=%d — a waiter woken by the release can select the exhausted key", tc.want, obs[0].coreRemaining, obs[0].graphqlRemaining)
			}
			if obs[1].key == obs[0].key {
				t.Error("the retry must rotate off the exhausted key")
			}
		})
	}
}

// TestDoErrorReleasesBeforeRetrySleep — the Do-error arm releases the
// lease AT ONCE, before its retry wait, on both wire paths. Round 1's
// shape pin counted that release; its behavioral replacement covered
// only the success path, and lease_every_exit_test.go's do-fails case
// checks the pool after the call RETURNS (a ctx deadline, no retry
// wait) — so a refactor that held the slot through the 2-20 s
// transport backoff, releasing after the sleep, passed every test
// (fresh-context review, mutation-proven). Here the connection is
// dropped while ctx stays alive, and the pool is read INSIDE the sleep.
func TestDoErrorReleasesBeforeRetrySleep(t *testing.T) {
	drop := func(t *testing.T) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server does not support hijacking")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			_ = conn.Close() // no response: Do fails with a transport error
		}
	}

	for _, tc := range []struct {
		name     string
		call     wireCall
		setSleep func(f func(context.Context, time.Duration) error) (restore func())
	}{
		{"REST Get", restGet, func(f func(context.Context, time.Duration) error) func() {
			old := restTransportRetrySleep
			restTransportRetrySleep = f
			return func() { restTransportRetrySleep = old }
		}},
		{"GraphQL", graphqlCall, SetGraphQLSleepForTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dropThenOK := scriptedServer(t, drop(t), graphqlData) // per subtest: the FIRST request drops
			kp := NewKeyPool([]string{"tok-a"}, rlTestLogger())
			slept := 0
			t.Cleanup(tc.setSleep(func(ctx context.Context, d time.Duration) error {
				slept++
				_, inflight := kp.Snapshot()
				kp.mu.Lock()
				perKey := kp.keys[0].inflight
				kp.mu.Unlock()
				if inflight != 0 || perKey != 0 {
					t.Errorf("during the retry wait after a transport error: pool inflight=%d key inflight=%d, want 0/0 — the lease must be released before the wait, not after it", inflight, perKey)
				}
				return nil
			}))
			_ = tc.call(context.Background(), NewHTTPClient(dropThenOK.URL, kp, rlTestLogger(), AuthGitHub))
			if slept == 0 {
				t.Fatal("the transport-error retry wait never ran — the dropped connection did not reach the Do-error arm, so this test proved nothing")
			}
		})
	}
}
