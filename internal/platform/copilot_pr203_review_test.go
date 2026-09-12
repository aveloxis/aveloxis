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
	"net/http"
	"os"
	"strings"
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
// wire paths: after Do, the success path applies UpdateFromResponse
// BEFORE release(); the Do-error arm still releases at once. A
// behavioral driver for the gap itself would need a waiter racing a
// throttled response inside a few microseconds; the order is the
// contract and it is pinned here, while lease_every_exit_test.go keeps
// proving the release still happens on every exit.
func TestLeaseReleasedAfterResponseStateApplied(t *testing.T) {
	for _, tc := range []struct{ file, fn string }{
		{"httpclient.go", "func (c *HTTPClient) Get("},
		{"graphql.go", "func (c *HTTPClient) GraphQLAt("},
	} {
		src, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		body := string(src)
		i := strings.Index(body, tc.fn)
		if i < 0 {
			t.Fatalf("%s: %s missing", tc.file, tc.fn)
		}
		body = body[i:]
		do := strings.Index(body, "resp, err := c.inner.Do(req)")
		upd := strings.Index(body, "c.keys.UpdateFromResponse(key, resp)")
		if do < 0 || upd < 0 || upd < do {
			t.Fatalf("%s: expected Do then UpdateFromResponse in %s", tc.file, tc.fn)
		}
		between := body[do:upd]
		// Exactly one release between Do and UpdateFromResponse, and it
		// lives inside the Do-error arm.
		if n := strings.Count(between, "release()"); n != 1 {
			t.Errorf("%s: %d release() calls between Do and UpdateFromResponse, want exactly 1 (the Do-error arm)", tc.file, n)
		}
		errArm := strings.Index(between, "if err != nil {")
		rel := strings.Index(between, "release()")
		if errArm < 0 || rel < errArm {
			t.Errorf("%s: the release between Do and UpdateFromResponse must be inside the `if err != nil` arm, not before it", tc.file)
		}
		// And the success path releases right after the state is applied.
		after := body[upd:]
		nl := strings.Index(after, "\n")
		next := strings.TrimSpace(after[nl+1:])
		if !strings.HasPrefix(next, "release()") {
			t.Errorf("%s: the line after UpdateFromResponse must be release() (state applied under the lease, released before any retry sleep); got %q", tc.file, strings.SplitN(next, "\n", 2)[0])
		}
	}
}
