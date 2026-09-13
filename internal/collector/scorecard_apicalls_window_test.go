// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review round 2 on PR #203: GitHub's /rate_limit `used` counter
// resets when the rate-limit window rolls over, so a run whose before and
// after probes straddle a reset produced a NEGATIVE (or silently
// understated) api_calls_used. The window is identified by the resource's
// `reset` epoch; a changed epoch makes the delta unknown (-1), never a
// number.

package collector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitDeltaIsUnknownAcrossAWindowReset(t *testing.T) {
	snap := func(coreUsed, coreReset, gqlUsed, gqlReset int64) rateLimitSnapshot {
		return rateLimitSnapshot{coreUsed: coreUsed, coreReset: coreReset,
			graphqlUsed: gqlUsed, graphqlReset: gqlReset, ok: true}
	}
	const w1, w2 = 1_800_000_000, 1_800_003_600
	for _, tc := range []struct {
		name          string
		before, after rateLimitSnapshot
		want          int64
	}{
		{"same windows", snap(100, w1, 10, w1), snap(132, w1, 18, w1), 40},
		{"core window rolled over (used reset to a smaller number)", snap(4900, w1, 10, w1), snap(25, w2, 18, w1), -1},
		{"core window rolled over (used grew past the old value)", snap(10, w1, 10, w1), snap(900, w2, 18, w1), -1},
		{"graphql window rolled over", snap(100, w1, 4000, w1), snap(132, w1, 3, w2), -1},
		// An idle resource has no window yet: GitHub reports used 0 and a
		// reset that floats with the clock, so the epoch moving is not a
		// rollover — every call counted after is inside the one window
		// the run opened, as long as that window's reset is less than an
		// hour past the floating value (the bound cases below).
		{"core idle before the run (floating reset)", snap(0, w1, 10, w1), snap(37, w1+900, 18, w1), 45},
		{"graphql idle before the run (floating reset)", snap(100, w1, 0, w1+5), snap(132, w1, 8, w1+700), 40},
		// scorecard_timeout_minutes has no upper clamp, so a run CAN
		// outlive a window. An idle reset floats at probe time + 1 h, so
		// the window a run opens resets less than an hour after the
		// first probe's floating value, and any LATER window at least an
		// hour after it: the second means window 1's calls were lost.
		{"core idle, run outlived the window it opened", snap(0, w1, 10, w1), snap(12, w1+3600+300, 18, w1), -1},
		{"graphql idle, run outlived the window it opened", snap(100, w1, 0, w1), snap(132, w1, 5, w2), -1},
		{"core idle, window opened just inside the hour", snap(0, w1, 10, w1), snap(37, w1+3599, 18, w1), 45},
		{"negative delta inside one window (never a number)", snap(132, w1, 10, w1), snap(100, w1, 18, w1), -1},
		{"before probe failed", rateLimitSnapshot{}, snap(1, w1, 1, w1), -1},
		{"after probe failed", snap(1, w1, 1, w1), rateLimitSnapshot{}, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rateLimitDelta(tc.before, tc.after); got != tc.want {
				t.Errorf("rateLimitDelta = %d, want %d", got, tc.want)
			}
		})
	}
}

// End to end through RunScorecard: the probe must parse `reset`, or the
// window check above has nothing to compare.
func TestScorecardInstrumentationWindowResetIsUnknown(t *testing.T) {
	_, _ = installFakeScorecard(t,
		`case "$1" in --repo) printf '%s' '`+fakeScorecardJSON+`';; esac`)
	store := &fakeScorecardStore{}

	var hits atomic.Int64
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			fmt.Fprint(w, `{"resources":{"core":{"used":4990,"reset":1800000000},"graphql":{"used":10,"reset":1800000000}}}`)
		} else {
			fmt.Fprint(w, `{"resources":{"core":{"used":12,"reset":1800003600},"graphql":{"used":18,"reset":1800000000}}}`)
		}
	}))
	defer rl.Close()

	res, err := RunScorecard(context.Background(), store, 42, ScorecardOptions{
		RepoURL:         "https://github.com/augurlabs/augur",
		RemotePrimary:   true,
		Timeout:         time.Minute,
		GithubToken:     "tok1",
		InstrumentToken: "tok1",
		RateLimitURL:    rl.URL,
	}, quietLogger())
	if err != nil {
		t.Fatalf("RunScorecard: %v", err)
	}
	if res.APICalls != -1 {
		t.Errorf("APICalls = %d, want -1 — the core window reset between the probes (pre-fix: (12-4990)+(18-10) = -4970)", res.APICalls)
	}
}

// v0.29.12 (redirect sweep): the /rate_limit probe sends a pool token and must
// not follow a redirect. Go's default client drops Authorization only when a
// redirect leaves the original domain and its subdomains — it keeps it for a
// subdomain and on an https→http downgrade — and the probe only ever wants
// the endpoint's own 200: a 3xx is an unknown sample (-1), and the redirect
// target receives nothing.
func TestRateLimitProbeDoesNotFollowRedirects(t *testing.T) {
	var foreignHits atomic.Int64
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		foreignHits.Add(1)
		fmt.Fprint(w, `{"resources":{"core":{"used":1,"reset":1800000000},"graphql":{"used":1,"reset":1800000000}}}`)
	}))
	defer foreign.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, foreign.URL+"/rate_limit", http.StatusFound)
	}))
	defer redirector.Close()

	snap := fetchRateLimitSnapshot(context.Background(), redirector.URL, "tok1", quietLogger())
	if snap.ok {
		t.Error("a redirected /rate_limit probe must be an unknown sample, not a reading from the redirect target")
	}
	if n := foreignHits.Load(); n != 0 {
		t.Errorf("the redirect target received %d probe request(s) carrying the token", n)
	}
}
