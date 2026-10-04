// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A front end that checks GET /api/v1/authz/repos/{id} before forwarding a
// cached route's request (which the authorization request already counted
// against the visitor) marks the forwarded request; the limiter does not
// count it a second time (review round 1, nginx finding 1). The mark is the
// shared api.front_end_secret, never a fixed value: round 2 found that a
// constant "1" forwarded by any proxy that did not strip it (the plain
// /api/ block, the web process's proxy, a /repos/+1/ path that missed the
// cached location) bypassed the limit with no authorization request at all.
func TestFrontEndAuthorizedRequestsAreCountedOnce(t *testing.T) {
	const proxy, visitor = "192.0.2.10", "203.0.113.5"
	const secret = "0123456789abcdef0123456789abcdef"
	build := func(secret string) (http.Handler, *int) {
		s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
			Options{RateLimitRPS: 0.001, RateLimitBurst: 1, RateLimitDaily: 1000, TrustedProxy: proxy, FrontEndSecret: secret})
		if err != nil {
			t.Fatal(err)
		}
		reached := 0
		return s.limiter.middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached++ })), &reached
	}
	send := func(h http.Handler, peer, path, mark string) int {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.RemoteAddr = peer + ":40000"
		r.Header.Set("X-Forwarded-For", visitor)
		if mark != "" {
			r.Header.Set(frontEndAuthorizedHeader, mark)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}

	h, reached := build(secret)
	// The authorization request takes the visitor's one token…
	if code := send(h, proxy, "/api/v1/authz/repos/7", ""); code != 200 {
		t.Fatalf("the authorization request: %d", code)
	}
	// …and the forwarded data requests carrying the secret cost nothing.
	for i := 0; i < 3; i++ {
		if code := send(h, proxy, "/api/v1/repos/7/licenses?scope=runtime", secret); code != 200 {
			t.Errorf("data request %d with the secret, from the trusted proxy, on a cached route: %d, want it uncounted", i, code)
		}
	}
	// Anything else is counted (the bucket is empty: 429).
	for _, c := range []struct{ name, peer, path, mark string }{
		{"a constant 1 (the round-2 bypass)", proxy, "/api/v1/repos/7/licenses", "1"},
		{"a wrong secret", proxy, "/api/v1/repos/7/licenses", secret[:31] + "0"},
		{"the secret on an uncached route", proxy, "/api/v1/repos/7/stats", secret},
		{"the secret on the authorization route", proxy, "/api/v1/authz/repos/7", secret},
		{"the secret from a direct client", visitor, "/api/v1/repos/7/licenses", secret},
	} {
		if code := send(h, c.peer, c.path, c.mark); code != http.StatusTooManyRequests {
			t.Errorf("%s: answered %d, want the limiter's 429", c.name, code)
		}
	}
	if *reached != 4 {
		t.Errorf("reached the handler %d times, want 4", *reached)
	}

	// No secret configured: nothing is ever uncounted, whatever is sent.
	h, _ = build("")
	send(h, proxy, "/api/v1/authz/repos/7", "")
	for _, mark := range []string{"", "1"} {
		if code := send(h, proxy, "/api/v1/repos/7/licenses", mark); code != http.StatusTooManyRequests {
			t.Errorf("without a secret, mark %q: %d, want 429", mark, code)
		}
	}
}

// Every answer says it varies by Origin (review round 1, nginx finding 3): a
// shared cache in front of the API stored an answer to a request without
// an Origin — no Access-Control-Allow-Origin — and served it to a
// cross-origin caller, whose browser then refused it. With Vary: Origin a
// cache keeps one copy per Origin value.
func TestEveryAnswerVariesByOrigin(t *testing.T) {
	for name, origins := range map[string][]string{"allowlist": {"https://gui.example"}, "open": nil} {
		s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{CORSOrigins: origins, ExemptCIDRs: DefaultExemptCIDRs})
		if err != nil {
			t.Fatal(err)
		}
		for _, origin := range []string{"", "https://gui.example", "https://other.example"} {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
			r.RemoteAddr = "127.0.0.1:1"
			if origin != "" {
				r.Header.Set("Origin", origin)
			}
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if vary := w.Result().Header.Values("Vary"); len(vary) != 1 || vary[0] != "Origin" {
				t.Errorf("%s mode, Origin %q: Vary = %v, want exactly [Origin]", name, origin, vary)
			}
		}
	}
}

// PR #226 review 5403516185: a cross-origin browser sending If-None-Match
// (the ETag the API exposes) must pass its preflight, in both CORS modes.
func TestCORSPreflightAllowsConditionalRequests(t *testing.T) {
	for name, origins := range map[string][]string{"allowlist": {"https://gui.example"}, "open": nil} {
		s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{CORSOrigins: origins, ExemptCIDRs: DefaultExemptCIDRs})
		if err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodOptions, "/api/v1/repos/7/licenses", nil)
		r.RemoteAddr = "127.0.0.1:1"
		r.Header.Set("Origin", "https://gui.example")
		r.Header.Set("Access-Control-Request-Method", "GET")
		r.Header.Set("Access-Control-Request-Headers", "authorization, if-none-match")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if allow := w.Result().Header.Get("Access-Control-Allow-Headers"); !strings.Contains(strings.ToLower(allow), "if-none-match") {
			t.Errorf("%s mode: preflight Allow-Headers %q must include If-None-Match", name, allow)
		}
	}
}
