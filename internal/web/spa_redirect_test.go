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
	s := redirectServer("http://localhost:8000")
	cases := []struct {
		next, want string
	}{
		{"", "/dashboard"},                     // no next → legacy behavior
		{"/groups/5", "/groups/5"},             // relative path ok
		{"//evil.example/phish", "/dashboard"}, // scheme-relative open redirect BLOCKED
		{"http://localhost:8000/login.html", "http://localhost:8000/login.html"}, // configured SPA origin ok
		{"http://localhost:8000", "http://localhost:8000"},                       // bare origin ok
		{"http://localhost:80001/x", "/dashboard"},                               // prefix trick (extra digit) BLOCKED
		{"https://evil.example/login.html", "/dashboard"},                        // arbitrary origin BLOCKED
	}
	for _, c := range cases {
		if got := s.postLoginRedirect(withNextCookie(c.next)); got != c.want {
			t.Errorf("next=%q → %q, want %q", c.next, got, c.want)
		}
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
	if strings.Count(s, "s.postLoginRedirect(r)") < 2 {
		t.Error("both OAuth callbacks must resolve their redirect via postLoginRedirect")
	}
}
