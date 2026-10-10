// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.84 (operator, 2026-10-09): a signed-in session is not rate limited
// (0.29.82), so a scraper that copies a browser's session token reads
// without any allowance. Step 1 is observation only: each user's session
// requests are counted per hour, and an hour that reaches what an issued API
// token may make (the API-token default allowance, the operator's own
// number for a programmatic client) is logged, again at each doubling.
// Nothing is refused.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func sessionVolumeChain(t *testing.T, budget func() (int, bool)) (http.Handler, *rateLimiter, *lockedBuffer, *time.Time) {
	t.Helper()
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"sess": true},
		apiValid: map[string]db.APITokenIdentity{db.APITokenPrefix + "tok": {TokenID: 3, UserID: 7, RateLimitPerHour: 1_000_000}}}
	// Exempt the loopback: a session is counted whatever its address.
	h, rl := tokenChain(t, store, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 1_000_000, ExemptCIDRs: []string{"127.0.0.0/8"}})
	logs := &lockedBuffer{}
	rl.logger = slog.New(slog.NewTextHandler(logs, nil))
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	rl.now = func() time.Time { return now }
	rl.sessionBudget = budget
	return h, rl, logs, &now
}

func TestSessionVolumeIsLoggedNeverLimited(t *testing.T) {
	h, _, logs, now := sessionVolumeChain(t, func() (int, bool) { return 10, true })
	for i := 0; i < 39; i++ {
		addr := "198.51.100." + strconv.Itoa(i%3+1) + ":1"
		if i%2 == 0 {
			addr = "127.0.0.1:1" // an exempt address still counts
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq(addr, "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("session request %d = %d: observation must never refuse", i+1, w.Code)
		}
		if i == 8 && strings.Contains(logs.String(), "signed-in session") {
			t.Fatalf("logged below the threshold:\n%s", logs.String())
		}
	}
	out := logs.String()
	// 39 requests: lines at 10 and 20 (each doubling), none at 39.
	if n := strings.Count(out, "signed-in session"); n != 2 {
		t.Fatalf("39 requests against a threshold of 10 logged %d lines, want 2 (at 10 and 20):\n%s", n, out)
	}
	for _, want := range []string{"user_id=7", "requests_this_hour=10", "requests_this_hour=20", "threshold=10"} {
		if !strings.Contains(out, want) {
			t.Fatalf("the line must carry %s:\n%s", want, out)
		}
	}
	if strings.Contains(out, "sess ") || strings.Contains(out, "=sess") {
		t.Fatalf("a log line carries the token:\n%s", out)
	}
	// A new hour starts a new count.
	*now = now.Add(time.Hour)
	for i := 0; i < 10; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.1:1", "sess"))
	}
	var lines []string
	for _, l := range strings.Split(logs.String(), "\n") {
		if strings.Contains(l, "signed-in session") {
			lines = append(lines, l)
		}
	}
	if len(lines) != 3 || !strings.Contains(lines[2], "requests_this_hour=10 ") {
		t.Fatalf("a new hour must start its count over and log at its own 10th request:\n%s", strings.Join(lines, "\n"))
	}
}

// Only valid SESSIONS are counted: API tokens have their own allowance and
// anonymous callers the per-IP limit.
func TestSessionVolumeCountsOnlySessions(t *testing.T) {
	h, rl, logs, _ := sessionVolumeChain(t, func() (int, bool) { return 2, true })
	for i := 0; i < 5; i++ {
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.9:1", db.APITokenPrefix+"tok"))
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("127.0.0.1:1", ""))
		h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.9:1", "junk"))
	}
	if strings.Contains(logs.String(), "signed-in session") || len(rl.sessionWindows) != 0 {
		t.Fatalf("API-token, anonymous and invalid-token requests were counted as a session:\n%s", logs.String())
	}
}

// The threshold is read, not assumed: when it cannot be read nothing is
// logged (an unreadable setting is not a number, SR-5) and every request is
// still served.
func TestSessionVolumeWithAnUnreadableThresholdLogsNothing(t *testing.T) {
	h, _, logs, _ := sessionVolumeChain(t, func() (int, bool) { return 0, false })
	for i := 0; i < 50; i++ {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, tokenReq("198.51.100.1:1", "sess"))
		if w.Code != http.StatusOK {
			t.Fatalf("request %d = %d", i+1, w.Code)
		}
	}
	if strings.Contains(logs.String(), "signed-in session") {
		t.Fatalf("logged with no readable threshold:\n%s", logs.String())
	}
}

// The per-user map is bounded like the others.
func TestSessionVolumeMapIsBounded(t *testing.T) {
	rl := &rateLimiter{now: time.Now}
	for u := 0; u < maxTrackedIPs+20; u++ {
		rl.observeSession(u, 1_000_000, true)
	}
	if got := len(rl.sessionWindows); got > maxTrackedIPs {
		t.Fatalf("session map holds %d windows, bound is %d", got, maxTrackedIPs)
	}
}

// The threshold is the API-token default allowance, re-read once per
// authCacheTTL; a failed read is logged and keeps the last value; with no
// value ever read it reports unknown.
func TestSessionThresholdReadsTheTokenDefault(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	reads := 0
	answer := db.APITokenSettings{DefaultRateLimitPerHour: 5000}
	var fail error
	logs := &lockedBuffer{}
	th := &sessionThreshold{
		read: func(context.Context) (db.APITokenSettings, error) {
			reads++
			return answer, fail
		},
		now:    func() time.Time { return now },
		logger: slog.New(slog.NewTextHandler(logs, nil)),
		spawn:  func(f func()) { f() }, // inline: the read completes before get answers
	}
	if v, ok := th.get(); !ok || v != 5000 {
		t.Fatalf("first read = %d, %v; want 5000, true", v, ok)
	}
	answer.DefaultRateLimitPerHour = 7
	if v, _ := th.get(); v != 5000 || reads != 1 {
		t.Fatalf("within the TTL: %d after %d reads, want the cached 5000 after 1", v, reads)
	}
	now = now.Add(authCacheTTL)
	if v, _ := th.get(); v != 7 {
		t.Fatalf("after the TTL the edited default must apply: got %d", v)
	}
	fail = errors.New("store down")
	now = now.Add(authCacheTTL)
	if v, ok := th.get(); !ok || v != 7 {
		t.Fatalf("a failed read must keep the last value: %d, %v", v, ok)
	}
	if !strings.Contains(logs.String(), "could not read the API-token default allowance") {
		t.Fatalf("a failed read must be logged:\n%s", logs.String())
	}
	fresh := &sessionThreshold{read: func(context.Context) (db.APITokenSettings, error) { return db.APITokenSettings{}, fail }, now: time.Now, spawn: func(f func()) { f() }}
	if _, ok := fresh.get(); ok {
		t.Fatal("with no value ever read the threshold is unknown")
	}
}

// The read never runs on the request path: while it is in flight, get
// answers the last value at once and starts no second read.
func TestSessionThresholdNeverWaitsOnTheStore(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	th := &sessionThreshold{
		read: func(context.Context) (db.APITokenSettings, error) {
			started <- struct{}{}
			<-release
			return db.APITokenSettings{DefaultRateLimitPerHour: 5000}, nil
		},
		now: time.Now,
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			if _, ok := th.get(); ok {
				t.Error("no value can be known before the first read returns")
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("get waited on a store read in flight")
	}
	<-started
	if len(started) != 0 {
		t.Fatal("a second read started while one was in flight")
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if v, ok := th.get(); ok && v == 5000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the value read never reached get")
		}
		time.Sleep(time.Millisecond)
	}
}

// L10 round 1 on 0.29.85 (LOW-MEDIUM): the request nginx forwards after an
// admitted authz subrequest (rl.uncounted) was counted again — a session
// hour reached its log line at about half the stated threshold, and an API
// token was charged twice per cache miss since 0.29.82. The authz request
// paid; the forwarded one is neither observed nor charged, and an API token
// still sees its current numbers.
func TestForwardedRequestIsNeitherObservedNorCharged(t *testing.T) {
	h, rl, _, _ := sessionVolumeChain(t, func() (int, bool) { return 1000, true })
	rl.uncounted = func(r *http.Request) bool { return r.Header.Get("X-Test-Forwarded") == "1" }
	fwd := func(token string) *http.Request {
		r := tokenReq("198.51.100.80:1", token)
		r.Header.Set("X-Test-Forwarded", "1")
		return r
	}
	// Session: one authz-style request, three forwarded ones.
	h.ServeHTTP(httptest.NewRecorder(), tokenReq("198.51.100.80:1", "sess"))
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), fwd("sess"))
	}
	if got := rl.sessionWindows[7].count; got != 1 {
		t.Fatalf("session counted %d requests, want 1 (forwarded requests already paid)", got)
	}
	// API token: charged once, the forwarded request reports without charging.
	tok := db.APITokenPrefix + "tok"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.80:1", tok))
	first := w.Header().Get("X-RateLimit-Remaining")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, fwd(tok))
	if w.Code != http.StatusOK || w.Header().Get("X-RateLimit-Remaining") != first || w.Header().Get("X-RateLimit-Limit") == "" {
		t.Fatalf("forwarded API-token request: code %d remaining %q (after the charged one: %q); want 200, same remaining, headers present",
			w.Code, w.Header().Get("X-RateLimit-Remaining"), first)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("198.51.100.80:1", tok))
	if w.Header().Get("X-RateLimit-Remaining") == first {
		t.Fatal("a direct request after the forwarded one must still be charged")
	}
}

// L10 round 1 on 0.29.85: the next log line follows the threshold IN FORCE.
// Raised mid-hour → no false "more than an API token" line; lowered → the
// observation is not missed; first known after an unknown stretch → one
// line, not one per request.
func TestSessionLogFollowsTheThresholdInForce(t *testing.T) {
	cases := []struct {
		name      string
		phases    [][2]int // {threshold (0 = unknown), requests}
		wantLines int
	}{
		{"raised mid-hour", [][2]int{{10, 20}, {5000, 30}}, 2},  // 10, 20; none under 5000
		{"lowered mid-hour", [][2]int{{5000, 100}, {10, 1}}, 1}, // 101 >= 10: once
		{"unknown then known", [][2]int{{0, 1000}, {10, 7}}, 1}, // once at 1001, next at 1280
		{"steady doubling", [][2]int{{10, 80}}, 4},              // 10, 20, 40, 80
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedBuffer{}
			rl := &rateLimiter{now: time.Now, logger: slog.New(slog.NewTextHandler(logs, nil))}
			for _, p := range tc.phases {
				for i := 0; i < p[1]; i++ {
					rl.observeSession(7, p[0], p[0] > 0)
				}
			}
			if n := strings.Count(logs.String(), "signed-in session"); n != tc.wantLines {
				t.Fatalf("%d lines, want %d:\n%s", n, tc.wantLines, logs.String())
			}
			if strings.Contains(logs.String(), "threshold=5000") && tc.name == "raised mid-hour" {
				t.Fatalf("a line claims the raised threshold was passed:\n%s", logs.String())
			}
		})
	}
}
