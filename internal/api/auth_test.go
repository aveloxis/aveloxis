// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.1 — Bearer auth + per-user repo scope (plan §2/§2b).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type fakeSessionStore struct {
	userID    int
	admin     bool
	scope     []int64
	valid     map[string]bool
	validates atomic.Int64
}

func (f *fakeSessionStore) ValidateSessionToken(_ context.Context, token string) (int, error) {
	f.validates.Add(1)
	if f.valid[token] {
		return f.userID, nil
	}
	return 0, contextErr("invalid")
}
func (f *fakeSessionStore) IsUserAdmin(context.Context, int) (bool, error) { return f.admin, nil }
func (f *fakeSessionStore) GetUserRepoScope(context.Context, int) ([]int64, error) {
	return f.scope, nil
}

type contextErr string

func (e contextErr) Error() string { return string(e) }

func authedChain(t *testing.T, store sessionStore, opts Options, next http.Handler) http.Handler {
	t.Helper()
	rl, err := newRateLimiter(opts)
	if err != nil {
		t.Fatal(err)
	}
	a := newAuthenticator(store, opts.RequireAuth)
	return a.middleware(rl, next)
}

func TestAuthRequiredRejectsMissingAndBadTokens(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"good": true}}
	h := authedChain(t, store, Options{RequireAuth: true}, okHandler())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
	req.RemoteAddr = "203.0.113.5:1"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no bearer token must 401, got %d", rec.Code)
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "json") {
		t.Error("auth errors must be JSON so the SPA can render them")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
	req.RemoteAddr = "203.0.113.5:1"
	req.Header.Set("Authorization", "Bearer nope")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("invalid token must 401, got %d", rec.Code)
	}
}

func TestAuthRequiredPassesValidTokenAndCaches(t *testing.T) {
	store := &fakeSessionStore{userID: 7, scope: []int64{42}, valid: map[string]bool{"good": true}}
	h := authedChain(t, store, Options{RequireAuth: true, RateLimitRPS: 100, RateLimitBurst: 100}, okHandler())

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/repos/42/stats", nil)
		req.RemoteAddr = "203.0.113.5:1"
		req.Header.Set("Authorization", "Bearer good")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("valid token request %d must pass, got %d", i, rec.Code)
		}
	}
	if got := store.validates.Load(); got != 1 {
		t.Errorf("token must be cached after first validation (60s TTL), store hit %d times", got)
	}
}

func TestAuthHealthAndExemptBypass(t *testing.T) {
	store := &fakeSessionStore{valid: map[string]bool{}}
	h := authedChain(t, store, Options{RequireAuth: true, ExemptCIDRs: []string{"10.0.0.0/8"}}, okHandler())

	// /health stays public even with auth required.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	req.RemoteAddr = "203.0.113.5:1"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("/health must stay public, got %d", rec.Code)
	}

	// LAN clients bypass auth (operator tooling keeps working).
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
	req.RemoteAddr = "10.1.2.3:1"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("exempt-CIDR client must bypass auth, got %d", rec.Code)
	}
}

func TestAuthOffIsOpen(t *testing.T) {
	store := &fakeSessionStore{valid: map[string]bool{}}
	h := authedChain(t, store, Options{RequireAuth: false}, okHandler())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
	req.RemoteAddr = "203.0.113.5:1"
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("require_auth=false must leave endpoints open (rollout default), got %d", rec.Code)
	}
}

func TestAuthorizeRepoScopeEnforcement(t *testing.T) {
	s := &Server{}
	base := httptest.NewRequest("GET", "/api/v1/repos/42/stats", nil)

	// No auth context (auth off / exempt LAN): allowed.
	rec := httptest.NewRecorder()
	if !s.authorizeRepo(rec, base, 42) {
		t.Error("unauthenticated context must be allowed (auth off / LAN)")
	}

	// Scoped user, repo in scope.
	ctx := context.WithValue(base.Context(), authCtxKey{}, authInfo{UserID: 7, Scope: map[int64]bool{42: true}})
	rec = httptest.NewRecorder()
	if !s.authorizeRepo(rec, base.WithContext(ctx), 42) {
		t.Error("in-scope repo must be allowed")
	}

	// Scoped user, repo OUT of scope: structured 403.
	rec = httptest.NewRecorder()
	if s.authorizeRepo(rec, base.WithContext(ctx), 99) {
		t.Error("out-of-scope repo must be denied")
	}
	if rec.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "repo_out_of_scope") || !strings.Contains(body, "99") {
		t.Errorf("403 must be STRUCTURED (error id + repo_id) so the GUI renders the "+
			"ask-for-access affordance, got %q", body)
	}

	// Admin: unscoped.
	ctx = context.WithValue(base.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true})
	rec = httptest.NewRecorder()
	if !s.authorizeRepo(rec, base.WithContext(ctx), 99) {
		t.Error("admins are unscoped (§2b)")
	}
}

func TestRepoHandlersEnforceScope(t *testing.T) {
	// Every repo-scoped handler must call authorizeRepo right after
	// parsing repoID.
	for _, f := range []struct {
		file string
		min  int
	}{
		{"server.go", 6},
		{"contributions.go", 3},
		{"metrics.go", 20},
	} {
		src := mustReadFile(t, f.file)
		if n := strings.Count(src, "s.authorizeRepo(w, r, repoID)"); n < f.min {
			t.Errorf("%s: expected ≥%d authorizeRepo call sites, found %d — a repo "+
				"endpoint without the scope check leaks data to any logged-in user", f.file, f.min, n)
		}
	}
	// The web process must expose the token-mint endpoint.
	web := mustReadFile(t, "../web/server.go")
	if !strings.Contains(web, `"/auth/token"`) || !strings.Contains(web, "CreateSessionToken(") {
		t.Error("web process must expose /auth/token minting DB-backed session tokens for the SPA")
	}
}
