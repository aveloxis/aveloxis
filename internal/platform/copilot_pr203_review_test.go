// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review on PR #203 (2026-09-12), the three platform findings
// the operator marked: (1) LendTokens handed scorecard keys the pool
// itself refuses to Acquire (quarantined / secondary-limited); (2+3)
// both wire paths released the lease BEFORE the response's rate-limit
// state reached the pool, so a waiter could reacquire a key the
// response had just exhausted or throttled.

package platform

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestLendTokensSkipsRestingKeys(t *testing.T) {
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c", "tok-d"}, rlTestLogger())
	kp.mu.Lock()
	quarantined, throttled := kp.keys[1], kp.keys[2]
	quarantined.quarantineUntil = time.Now().Add(time.Hour)
	kp.mu.Unlock()
	kp.MarkSecondaryLimited(throttled, time.Hour)

	got, release := kp.LendTokens(0)
	defer release()
	if len(got) != 2 || got[0] != "tok-a" || got[1] != "tok-d" {
		t.Fatalf("LendTokens = %v, want [tok-a tok-d] — a resting key (401 quarantine or secondary limit) must not be lent to a subprocess that Acquire would refuse it to", got)
	}
	// The exclusion is by rest state, not permanent: once the rest
	// expires the key is lendable again.
	kp.mu.Lock()
	quarantined.quarantineUntil = time.Time{}
	throttled.secondaryUntil = time.Time{}
	kp.mu.Unlock()
	got2, release2 := kp.LendTokens(0)
	defer release2()
	if len(got2) != 4 {
		t.Fatalf("after the rests expire LendTokens = %v, want all four", got2)
	}
}

func TestUpdateFromResponseRestsASecondaryLimitedKey(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		retryAfter string
		wantRest   bool
	}{
		{"403 with Retry-After", http.StatusForbidden, "30", true},
		{"429 with Retry-After", http.StatusTooManyRequests, "30", true},
		{"403 without Retry-After (permission error)", http.StatusForbidden, "", false},
		{"200", http.StatusOK, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kp := NewKeyPool([]string{"tok-a"}, rlTestLogger())
			kp.mu.Lock()
			key := kp.keys[0]
			kp.mu.Unlock()
			resp := &http.Response{StatusCode: tc.status, Header: http.Header{}}
			if tc.retryAfter != "" {
				resp.Header.Set("Retry-After", tc.retryAfter)
			}
			kp.UpdateFromResponse(key, resp)
			kp.mu.Lock()
			resting := key.restingAt(time.Now())
			hits := key.secondaryHits
			kp.mu.Unlock()
			if resting != tc.wantRest {
				t.Fatalf("resting = %v, want %v — the secondary-limit state must be applied by UpdateFromResponse, i.e. while the lease is still held", resting, tc.wantRest)
			}
			if tc.wantRest && hits != 1 {
				t.Fatalf("secondary hits = %d, want exactly 1 (one response, one hit — no double counting with a branch-level mark)", hits)
			}
		})
	}
}

// TestLeaseReleasedAfterResponseStateApplied pins the ORDER on both
// wire paths as BEHAVIOR: the pool is observed at the instant of the
// release (leaseReleaseObserver), and the key a secondary-limit response
// throttled must already be resting there. Until round 2 this was a
// source-shape pin ("the line after UpdateFromResponse must be
// release()"); round 2's GraphQL marks legitimately sit between the two
// and a comment mentioning release() tripped its token count, so the
// shape pin was replaced by the observation it stood in for. The
// Do-error arm's release-before-retry-wait, the auth-strike and the
// GraphQL budget marks are observed in copilot_pr203_round2_test.go
// (TestDoErrorReleasesBeforeRetrySleep, TestAuthStrikeRecordedBeforeLeaseRelease,
// TestGraphQLBudgetMarksAppliedBeforeLeaseRelease); lease_every_exit_test.go
// proves the pool is idle after every exit returns.
func TestLeaseReleasedAfterResponseStateApplied(t *testing.T) {
	t.Cleanup(SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil }))
	throttled := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}
	restOK := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":{"x":1}}`)
	}
	for _, tc := range []struct {
		name string
		call wireCall
	}{
		{"REST Get", restGet},
		{"GraphQL", graphqlCall},
	} {
		t.Run(tc.name, func(t *testing.T) {
			releases := observeLeaseReleases(t)
			srv := scriptedServer(t, throttled, restOK)
			kp := NewKeyPool([]string{"tok-a", "tok-b"}, rlTestLogger())
			if err := tc.call(context.Background(), NewHTTPClient(srv.URL, kp, rlTestLogger(), AuthGitHub)); err != nil {
				t.Fatalf("the call should rotate to the healthy key and succeed: %v", err)
			}
			obs := releases()
			if len(obs) < 2 {
				t.Fatalf("expected a release per attempt, got %d", len(obs))
			}
			if !obs[0].resting {
				t.Error("at the release after the 429 the key was not yet resting — a waiter woken by that release can select the key the response just throttled")
			}
			if obs[1].key == obs[0].key {
				t.Error("the retry must be served by the other key while the throttled one rests")
			}
		})
	}
}
