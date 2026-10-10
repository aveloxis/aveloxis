// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.89: a sign-up the per-address quota refused gets a kind page —
// the limit, the hardware constraint, tomorrow, whom to write — as a 429
// with Retry-After until 00:00 UTC, never stored, the message escaped.
func TestSignupRefusalPage(t *testing.T) {
	reset := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, 1)
	ex := &capacity.Exceeded{Kind: capacity.KindRate, Quota: capacity.QuotaSignupsPerAddressPerDay, Window: capacity.UTCDay,
		Used: 3, Allowed: 3, ResetAt: reset, Contact: "a<b>@example.org"}
	w := httptest.NewRecorder()
	writeSignupRefusal(w, ex)
	body := w.Body.String()
	retry, _ := strconv.Atoi(w.Header().Get("Retry-After"))
	if w.Code != http.StatusTooManyRequests || retry < 1 || retry > 86400 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("= %d Retry-After %q Cache-Control %q", w.Code, w.Header().Get("Retry-After"), w.Header().Get("Cache-Control"))
	}
	for _, want := range []string{"limited hardware and infrastructure", "try again tomorrow", "a&lt;b&gt;@example.org"} {
		if !strings.Contains(body, want) {
			t.Errorf("the page lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "a<b>") {
		t.Error("the message must be escaped")
	}
	// ASVS review (V3.4): a static page that loads nothing and is never framed.
	if csp := w.Header().Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors 'none'") ||
		w.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("security headers = CSP %q, nosniff %q", csp, w.Header().Get("X-Content-Type-Options"))
	}
}

// Both OAuth callbacks hand the browser's address to the sign-in through
// signupAddr (web.trusted_proxy, the one rule httpserver.ClientIP), and the
// shared tail answers the quota's refusal with the page, not the 500.
func TestOAuthCallbackCountsTheBrowsersAddress(t *testing.T) {
	src := srctest.Read(t, "internal/web/server.go")
	for _, cb := range []string{"func (s *Server) handleGitHubCallback(", "func (s *Server) handleGitLabCallback("} {
		if body := srctest.StripGoComments(srctest.FuncBody(t, src, cb)); !strings.Contains(body, "SignupAddr: s.signupAddr(r)") {
			t.Errorf("%s does not pass the browser's address", cb)
		}
	}
	if body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Server) signupAddr(")); !strings.Contains(body, "httpserver.ClientIP(r, s.cfg.TrustedProxy)") {
		t.Error("signupAddr must read the address through web.trusted_proxy")
	}
	tail := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *Server) completeOAuthLogin("))
	if !strings.Contains(tail, "writeSignupRefusal(w, ex)") ||
		strings.Index(tail, "capacity.AsExceeded(err)") > strings.Index(tail, `s.logger.Error("failed to upsert OAuth user"`) {
		t.Error("completeOAuthLogin must answer the quota's refusal before the generic failure")
	}
}

// ASVS review I1/I3: nginx in front without web.trusted_proxy (a loopback
// peer sending X-Forwarded-For) is logged once; a configured proxy, a
// direct client or a request without the header is not.
func TestMissingTrustedProxyIsLoggedOnce(t *testing.T) {
	logs := &strings.Builder{}
	s := &Server{logger: slog.New(slog.NewTextHandler(logs, nil))}
	req := func(peer, xff string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/auth/github/callback", nil)
		r.RemoteAddr = peer
		if xff != "" {
			r.Header.Set("X-Forwarded-For", xff)
		}
		return r
	}
	s.signupAddr(req("203.0.113.5:1", "198.51.100.1")) // direct client: no proxy
	s.signupAddr(req("127.0.0.1:1", ""))               // local, no header
	if logs.Len() != 0 {
		t.Fatalf("logged without the misconfiguration:\n%s", logs.String())
	}
	s.signupAddr(req("127.0.0.1:1", "198.51.100.1"))
	s.signupAddr(req("[::1]:1", "198.51.100.2"))
	if n := strings.Count(logs.String(), "web.trusted_proxy is empty"); n != 1 {
		t.Fatalf("logged %d times, want once:\n%s", n, logs.String())
	}
	configured := &Server{logger: slog.New(slog.NewTextHandler(logs, nil))}
	configured.cfg.TrustedProxy = "127.0.0.1"
	logs.Reset()
	configured.signupAddr(req("127.0.0.1:1", "198.51.100.1"))
	if logs.Len() != 0 {
		t.Fatalf("logged with the proxy configured:\n%s", logs.String())
	}
}
