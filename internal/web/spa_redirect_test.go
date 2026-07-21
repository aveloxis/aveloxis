// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

// v0.27.4 — post-login SPA redirect. Operator report: signing in from
// the SPA (localhost:8000) dumped the browser at the server-rendered
// :8082/dashboard instead of returning to the SPA. The login flow now
// carries ?next= through OAuth via the oauth_next cookie, honored ONLY
// for relative paths or the configured web.spa_url origin.

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

func redirectServer(spaURL string) *Server {
	return &Server{cfg: config.WebConfig{SPAURL: spaURL}, logger: discardLogger()}
}

func withNextCookie(next string) *http.Request {
	r := httptest.NewRequest("GET", "/auth/github/callback", nil)
	if next != "" {
		r.AddCookie(&http.Cookie{Name: "oauth_next", Value: next})
	}
	return r
}

func TestPostLoginRedirectValidation(t *testing.T) {
	// The matrix drives safeNextTarget directly: Go's own cookie
	// transport strips backslashes (RFC 6265 excludes 0x5C), so the
	// /\ bypass inputs cannot be delivered through AddCookie — the
	// pure function is the testable seam for the full matrix, and the
	// wiring test below proves postLoginRedirect consumes it.
	const spa = "http://localhost:8000"
	cases := []struct {
		next, want string // want "" = untrusted → blocked
	}{
		{"", ""},                     // empty → untrusted
		{"/groups/5", "/groups/5"},   // relative path ok
		{"//evil.example/phish", ""}, // scheme-relative open redirect BLOCKED
		// Browsers normalize backslash to slash, so /\host is ALSO a
		// protocol-relative absolute URL (CodeQL go/bad-redirect-check,
		// fixed v0.27.10).
		{`/\evil.example/phish`, ""},                                             // backslash protocol-relative BLOCKED
		{"/groups/5/repos/9", "/groups/5/repos/9"},                               // deeper relative path still ok
		{"http://localhost:8000/login.html", "http://localhost:8000/login.html"}, // configured SPA origin ok
		{"http://localhost:8000", "http://localhost:8000"},                       // bare origin ok
		{"http://localhost:80001/x", ""},                                         // prefix trick (extra digit) BLOCKED
		{"https://evil.example/login.html", ""},                                  // arbitrary origin BLOCKED
	}
	for _, c := range cases {
		if got := safeNextTarget(c.next, spa); got != c.want {
			t.Errorf("safeNextTarget(%q) = %q, want %q", c.next, got, c.want)
		}
	}

	// Wiring: postLoginRedirect must route through the validator and
	// fall back to /dashboard on untrusted input.
	s := redirectServer(spa)
	if got := s.postLoginRedirect(withNextCookie("/groups/5")); got != "/groups/5" {
		t.Errorf("postLoginRedirect trusted path → %q, want /groups/5", got)
	}
	if got := s.postLoginRedirect(withNextCookie("//evil.example/phish")); got != "/dashboard" {
		t.Errorf("postLoginRedirect untrusted path → %q, want /dashboard", got)
	}
}

func TestPostLoginRedirectWithoutSPAURL(t *testing.T) {
	s := redirectServer("") // spa_url unset → only relative paths pass
	if got := s.postLoginRedirect(withNextCookie("http://localhost:8000/login.html")); got != "/dashboard" {
		t.Errorf("absolute next must be rejected when spa_url is unconfigured, got %q", got)
	}
	if got := s.postLoginRedirect(withNextCookie("/dashboard")); got != "/dashboard" {
		t.Errorf("relative next must still work, got %q", got)
	}
}

func TestOAuthFlowCarriesNext(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if strings.Count(s, "s.stashNext(w, r)") < 2 {
		t.Error("both handleGitHubAuth and handleGitLabAuth must stash the ?next= destination")
	}
	// v0.27.42: both callbacks route through completeOAuthLogin, the
	// single home of the postLoginRedirect call — one call site + two
	// handoffs is the deduplicated equivalent of the old two-site pin.
	if strings.Count(s, "s.postLoginRedirect(r)") < 1 {
		t.Error("completeOAuthLogin must resolve the redirect via postLoginRedirect")
	}
	if strings.Count(s, "s.completeOAuthLogin(w, r,") < 2 {
		t.Error("both OAuth callbacks must hand off to completeOAuthLogin")
	}
}

// TestOAuthNextCookiesCarryHouseAttributes pins the house cookie rule
// (HttpOnly always; Secure unless dev_mode) on BOTH oauth_next
// Set-Cookie emissions — the stash AND the expiry. CodeQL flagged the
// pre-v0.27.10 clearNext for missing both attributes; deletion cookies
// must carry the same attributes as the original.
func TestOAuthNextCookiesCarryHouseAttributes(t *testing.T) {
	s := redirectServer("http://localhost:8000") // DevMode=false → Secure expected

	// stashNext
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/auth/github?next=/groups/5", nil)
	s.stashNext(w, r)
	stash := findCookie(t, w, "oauth_next")
	if !stash.HttpOnly || !stash.Secure {
		t.Errorf("stashNext cookie must be HttpOnly+Secure, got HttpOnly=%v Secure=%v", stash.HttpOnly, stash.Secure)
	}

	// clearNext
	w = httptest.NewRecorder()
	s.clearNext(w)
	clear := findCookie(t, w, "oauth_next")
	if !clear.HttpOnly || !clear.Secure {
		t.Errorf("clearNext expiry cookie must be HttpOnly+Secure, got HttpOnly=%v Secure=%v", clear.HttpOnly, clear.Secure)
	}
	if clear.MaxAge >= 0 {
		t.Errorf("clearNext must expire the cookie (MaxAge<0), got %d", clear.MaxAge)
	}
}

func findCookie(t *testing.T, w *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range w.Result().Cookies() {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s cookie in response", name)
	return nil
}
