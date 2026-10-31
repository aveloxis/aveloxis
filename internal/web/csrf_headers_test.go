// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// csrf gives a test request the double-submit pair the guard requires
// (0.29.102, stage 2f): the cookie and, for a form route, the matching
// value in the X-CSRF-Token header (a server-rendered form carries it as
// the csrf_token field; the header is for fetches and tests).
func csrf(r *http.Request) *http.Request {
	// Both spellings of the form-token cookie (0.29.113): the guard reads
	// the __Host- name on a production server and the bare name under
	// dev_mode, and this helper serves tests of both kinds.
	r.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "test-csrf-token"})
	r.AddCookie(&http.Cookie{Name: "__Host-" + csrfCookieName, Value: "test-csrf-token"})
	r.Header.Set("X-CSRF-Token", "test-csrf-token")
	return r
}

// TestCSRFGuardOnEveryPostRoute (B1/D-10): every POST route Handler
// registers refuses, BEFORE its handler, a request whose Origin is another
// site, a request a browser marks cross-site, and (for the HTML form
// routes) a signed-in request without the session's form token; the same
// request with the token reaches the handler. The two routes aveloxis-gui
// calls from its own origin accept that origin and need no form token.
func TestCSRFGuardOnEveryPostRoute(t *testing.T) {
	s := New(nil, config.WebConfig{Addr: ":0", APIInternalURL: "http://127.0.0.1:1", DevMode: true, SPAURL: "https://gui.example.org"},
		"", slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := s.Handler()
	posts := 0
	for _, rt := range registeredRoutes(t) {
		method, _, _ := strings.Cut(rt.pattern, " ")
		if method != http.MethodPost {
			continue
		}
		posts++
		session := func(r *http.Request) *http.Request {
			token := plantSession(t, s, "csrf-"+rt.pattern, &Session{UserID: 1, LoginName: "c", IsAdmin: true, ExpiresAt: time.Now().Add(time.Hour)})
			r.AddCookie(&http.Cookie{Name: s.cookieName("aveloxis_session"), Value: token})
			return r
		}
		r := session(requestFor(rt.pattern))
		r.Header.Set("Origin", "https://evil.example")
		if code, _, reached := serve(h, r); reached || code != http.StatusForbidden {
			t.Errorf("%s: a cross-site Origin got %d (reached=%v); want 403 before the handler", rt.pattern, code, reached)
		}
		r = session(requestFor(rt.pattern))
		r.Header.Set("Sec-Fetch-Site", "cross-site")
		if code, _, reached := serve(h, r); reached || code != http.StatusForbidden {
			t.Errorf("%s: Sec-Fetch-Site: cross-site got %d (reached=%v); want 403 before the handler", rt.pattern, code, reached)
		}
		exempt := csrfExempt[strings.TrimPrefix(rt.pattern, "POST ")]
		r = session(requestFor(rt.pattern))
		r.Header.Set("Origin", "http://example.com")
		r.Host = "example.com"
		code, _, reached := serve(h, r)
		if !exempt && (reached || code != http.StatusForbidden) {
			t.Errorf("%s: a same-origin POST without the form token got %d (reached=%v); want 403 before the handler", rt.pattern, code, reached)
		}
		if exempt && !reached && code == http.StatusForbidden {
			t.Errorf("%s: an exempt route refused a same-origin POST without a form token", rt.pattern)
		}
		r = csrf(session(requestFor(rt.pattern)))
		r.Header.Set("Origin", "http://example.com")
		r.Host = "example.com"
		if code, _, reached := serve(h, r); !reached && code == http.StatusForbidden {
			t.Errorf("%s: a same-origin POST WITH the form token was refused (%d)", rt.pattern, code)
		}
		if !exempt {
			// The pair must MATCH: a cookie with another value, and a value
			// without the cookie, are both refused (review A2 — a guard that
			// accepted any non-empty token passed the suite).
			r = session(requestFor(rt.pattern))
			r.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "cookie-value"})
			r.Header.Set("X-CSRF-Token", "another-value")
			if code, _, reached := serve(h, r); reached || code != http.StatusForbidden {
				t.Errorf("%s: a form token that does not match its cookie got %d (reached=%v); want 403", rt.pattern, code, reached)
			}
			r = session(requestFor(rt.pattern))
			r.Header.Set("X-CSRF-Token", "orphan-value")
			if code, _, reached := serve(h, r); reached || code != http.StatusForbidden {
				t.Errorf("%s: a form token without its cookie got %d (reached=%v); want 403", rt.pattern, code, reached)
			}
		}
	}
	if posts < 8 {
		t.Fatalf("only %d POST routes examined", posts)
	}
	// The SPA's origin: its two calls carry credentials but no form token.
	for _, path := range []string{"/auth/token", "/logout"} {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		r.Header.Set("Origin", "https://gui.example.org")
		r.AddCookie(&http.Cookie{Name: s.cookieName("aveloxis_session"), Value: plantSession(t, s, "spa"+path, &Session{UserID: 1, LoginName: "c", ExpiresAt: time.Now().Add(time.Hour)})})
		if code, _, _ := serve(h, r); code == http.StatusForbidden {
			t.Errorf("POST %s from the configured SPA origin was refused", path)
		}
	}
	// No session, no token: the gate's answer, not the guard's (an
	// anonymous request forges nothing; the route matrix pins the 302).
	r := httptest.NewRequest(http.MethodPost, "/groups/new", nil)
	if code, loc, _ := serve(h, r); code != http.StatusFound || loc != "/login" {
		t.Errorf("an anonymous POST /groups/new got %d %q; want the gate's 302 /login", code, loc)
	}
}

// TestSecurityHeadersOnEveryResponse (D-11): every answer of the web
// process carries the Content-Security-Policy (one script source, no
// inline script, never framed), nosniff, the referrer policy and the
// frame refusal — the pages, the assets, the redirects and the 404s.
func TestSecurityHeadersOnEveryResponse(t *testing.T) {
	s := New(nil, config.WebConfig{Addr: ":0", APIInternalURL: "http://127.0.0.1:1", DevMode: true},
		"", slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := s.Handler()
	for _, path := range []string{"/login", "/", "/dashboard", "/static/app.css", "/static/nope.js", "/groups/abc", "/icon.png"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		csp := rec.Header().Get("Content-Security-Policy")
		for _, want := range []string{"default-src 'self'", "script-src 'self'", "frame-ancestors 'none'", "base-uri 'none'", "object-src 'none'"} {
			if !strings.Contains(csp, want) {
				t.Errorf("GET %s (%d): CSP %q lacks %s", path, rec.Code, csp, want)
			}
		}
		if strings.Contains(csp, "unsafe-inline'") && !strings.Contains(csp, "style-src 'self' 'unsafe-inline'") {
			t.Errorf("GET %s: CSP allows inline beyond styles: %q", path, csp)
		}
		scriptSrc := strings.SplitN(strings.SplitN(csp+";", "script-src", 2)[1], ";", 2)[0]
		if strings.Contains(scriptSrc, "unsafe") || strings.Contains(scriptSrc, "http") {
			t.Errorf("GET %s: script-src allows unsafe or remote sources: %q", path, scriptSrc)
		}
		if (path == "/login" || path == "/") && !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html; charset=utf-8") {
			t.Errorf("GET %s: a rendered page states its type, got %q", path, rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("X-Frame-Options") != "DENY" || rec.Header().Get("Referrer-Policy") == "" {
			t.Errorf("GET %s: nosniff=%q frame=%q referrer=%q", path, rec.Header().Get("X-Content-Type-Options"), rec.Header().Get("X-Frame-Options"), rec.Header().Get("Referrer-Policy"))
		}
	}
	// Images: https (the forges' avatars) in production; http too under
	// dev_mode only (an http:// self-hosted GitLab in development).
	for _, dev := range []bool{false, true} {
		s := New(nil, config.WebConfig{Addr: ":0", APIInternalURL: "http://127.0.0.1:1", DevMode: dev},
			"", slog.New(slog.NewTextHandler(io.Discard, nil)))
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/login", nil))
		img := strings.SplitN(strings.SplitN(rec.Header().Get("Content-Security-Policy")+";", "img-src", 2)[1], ";", 2)[0]
		if strings.Contains(img, "http:") != dev || !strings.Contains(img, "https:") {
			t.Errorf("dev_mode=%v: img-src%s; want https: always and http: only under dev_mode", dev, img)
		}
	}
}
