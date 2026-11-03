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

	"github.com/aveloxis/aveloxis/internal/srctest"

	"github.com/aveloxis/aveloxis/internal/db"
)

type fakeSessionStore struct {
	userID int
	admin  bool
	// The store's three lookups can each fail as a STORE (a lost connection),
	// which is not "invalid token" / "not an admin" / "no scope" (worklist
	// follow-up 6, SR-5): the fake honours that boundary like the real store
	// — an unknown token is db.ErrInvalidSessionToken, anything else an error.
	validateErr error
	adminErr    error
	scopeErr    error
	scope       []int64
	valid       map[string]bool
	apiValid    map[string]db.APITokenIdentity // v0.29.82: operator-issued API tokens
	validates   atomic.Int64
	actives     atomic.Int64 // APITokenActive rechecks (ASVS review G4)
	identities  atomic.Int64 // IsUserAdmin + GetUserRepoScope reads: the cached part (0.29.102 A1)
	onValidate  func()       // runs inside ValidateSessionToken (worklist follow-up 7: a bust racing a resolve)
}

func (f *fakeSessionStore) ValidateSessionToken(_ context.Context, token string) (int, error) {
	f.validates.Add(1)
	if f.onValidate != nil {
		f.onValidate()
	}
	if f.validateErr != nil {
		return 0, f.validateErr
	}
	if f.valid[token] {
		return f.userID, nil
	}
	return 0, db.ErrInvalidSessionToken
}
func (f *fakeSessionStore) ValidateAPIToken(_ context.Context, token string) (db.APITokenIdentity, error) {
	f.validates.Add(1)
	if f.validateErr != nil {
		return db.APITokenIdentity{}, f.validateErr
	}
	if id, ok := f.apiValid[token]; ok {
		return id, nil
	}
	return db.APITokenIdentity{}, db.ErrInvalidAPIToken
}
func (f *fakeSessionStore) APITokenActive(_ context.Context, token string) (bool, error) {
	f.actives.Add(1)
	if f.validateErr != nil {
		return false, f.validateErr
	}
	_, ok := f.apiValid[token]
	return ok, nil
}
func (f *fakeSessionStore) IsUserAdmin(context.Context, int) (bool, error) {
	f.identities.Add(1)
	return f.admin, f.adminErr
}
func (f *fakeSessionStore) GetUserRepoScope(context.Context, int) ([]int64, error) {
	f.identities.Add(1)
	return f.scope, f.scopeErr
}

type contextErr string

func (e contextErr) Error() string { return string(e) }

func authedChain(t *testing.T, store sessionStore, opts Options, next http.Handler) http.Handler {
	t.Helper()
	rl, err := newRateLimiter(opts)
	if err != nil {
		t.Fatal(err)
	}
	a := newAuthenticator(store, opts.RequireAuth, nil)
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
	// The identity (role, scope) is cached for authCacheTTL: two reads for
	// three requests. The token itself is rechecked against its row on
	// every request (0.29.102 review A1: a web sign-out deletes it, and
	// the next request must be the 401), so validates counts every call.
	if got := store.identities.Load(); got != 2 {
		t.Errorf("identity must be cached after the first resolution (60s TTL), role/scope read %d times for 3 requests", got)
	}
	if got := store.validates.Load(); got != 3 {
		t.Errorf("a session token is rechecked on every request, validated %d times for 3 requests", got)
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

	// Scoped user, repo OUT of scope, NO sharedWithMe seam (bare
	// Server): fail closed to the structured 403. Since v0.27.82 an
	// out-of-scope repo on a fully-wired Server auto-adds to the
	// user's "Shared with Me" group instead — see
	// shared_with_me_authz_test.go — so this case now specifically
	// pins the nil-seam fail-closed contract.
	rec = httptest.NewRecorder()
	if s.authorizeRepo(rec, base.WithContext(ctx), 99) {
		t.Error("out-of-scope repo with no auto-add seam must be denied (fail closed)")
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
	// Every repository route decides scope through the prelude
	// (repoFromPath → authorizeRepo): TestRepositoryRoutesReachThePrelude
	// walks the registrations (stage 1b replaced the per-file count of
	// `s.authorizeRepo(w, r, repoID)`, which a route could pass out of
	// order). The web process must expose the token-mint endpoint.
	routes := srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler(")
	mint := srctest.PackageFunc(t, "internal/web", "func (s *Server) handleAuthToken(")
	if !strings.Contains(routes, `"POST /auth/token"`) || !strings.Contains(mint, "MintTokenForWebSession(") {
		t.Error("web process must expose /auth/token minting DB-backed session tokens for the SPA")
	}
}

// TestACachedSessionTokenIsRecheckedThroughTheChain (0.29.102 review round
// 2): identify answered a cached session token from the cache alone, so
// the per-call recheck lived only in resolveToken's uncached path and a
// web sign-out left the GUI's token working for authCacheTTL. Through the
// real chain: the second request after the row is gone is the 401.
func TestACachedSessionTokenIsRecheckedThroughTheChain(t *testing.T) {
	store := &fakeSessionStore{userID: 7, scope: []int64{42}, valid: map[string]bool{"good": true}}
	h := authedChain(t, store, Options{RequireAuth: true, RateLimitRPS: 100, RateLimitBurst: 100}, okHandler())
	get := func() int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/repos/42/stats", nil)
		req.RemoteAddr = "203.0.113.5:1"
		req.Header.Set("Authorization", "Bearer good")
		h.ServeHTTP(rec, req)
		return rec.Code
	}
	if code := get(); code != http.StatusOK {
		t.Fatalf("first request = %d", code)
	}
	store.valid["good"] = false // the web sign-out deleted the row
	if code := get(); code != http.StatusUnauthorized {
		t.Errorf("the request after the row was deleted = %d; want 401 at once, not after authCacheTTL", code)
	}
}

// ASVS 5.0 review of 0.29.125 (V7.4.1/V8.2.1, C-2): a demotion made in the
// built-in web console reaches no api process's cache (the web has no
// hooks), so the demoted account's live SPA token kept administrator
// privilege for authCacheTTL. A cached ADMIN session is now re-read for its
// role on every call, as the row itself already is; a plain session pays
// nothing more.
func TestADemotedAdministratorsCachedSessionLosesPrivilegeAtOnce(t *testing.T) {
	store := &fakeSessionStore{userID: 7, admin: true, valid: map[string]bool{"good": true}}
	a := newAuthenticator(store, true, nil)
	ctx := context.Background()
	info, err := a.resolveToken(ctx, "good")
	if err != nil || !info.IsAdmin {
		t.Fatalf("premise: an administrator's token resolves as one: %+v %v", info, err)
	}
	store.admin = false // the web console demoted the account
	info, err = a.resolveToken(ctx, "good")
	if err != nil {
		t.Fatal(err)
	}
	if info.IsAdmin {
		t.Error("a demoted administrator's cached session still carried the privilege (the demotion came from the web process, which cannot reach this cache)")
	}
	// The recheck's error arm (SR-5/SR-16; L10 round 2): a store failure is
	// an error to the caller, never the cached privilege.
	store.admin = true
	a.forget("good")
	if info, err := a.resolveToken(ctx, "good"); err != nil || !info.IsAdmin {
		t.Fatalf("premise: cached again as an administrator: %+v %v", info, err)
	}
	store.adminErr = contextErr("connection reset")
	if info, err := a.resolveToken(ctx, "good"); err == nil {
		t.Errorf("a failed administrator recheck answered the cached privilege (IsAdmin=%v) instead of an error", info.IsAdmin)
	}
}
