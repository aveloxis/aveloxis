// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.3 — gating tests for the portal/admin endpoints. The
// security contract: these ALWAYS require a Bearer identity (even
// with api.require_auth=false / from exempt LAN addresses), and
// /admin/* additionally requires the admin flag.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func portalServer(t *testing.T, store sessionStore) *Server {
	t.Helper()
	s := &Server{}
	s.auth = newAuthenticator(store, false) // require_auth OFF — portal must gate anyway
	return s
}

func TestPortalAlwaysRequiresIdentity(t *testing.T) {
	store := &fakeSessionStore{valid: map[string]bool{}}
	s := portalServer(t, store)

	for _, h := range []http.HandlerFunc{
		s.handleMe, s.handleGroupsList, s.handleGroupCreate,
		s.handleGroupPendingAdds, // v0.27.20
		s.handleGroupOrgs,        // 2026-07-21 tracked-orgs listing
		s.handleAdminUsers, s.handleAdminPendingGroups,
		s.handleAdminMonitorStats, s.handleAdminMonitorQueue,
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/x", nil)
		req.RemoteAddr = "10.0.0.5:1" // exempt LAN — must NOT bypass portal auth
		h(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("portal handler without identity must 401 even from LAN with auth off, got %d", rec.Code)
		}
	}
}

func TestAdminRoutesRejectNonAdmins(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"tok": true}} // not admin
	s := portalServer(t, store)

	for _, h := range []http.HandlerFunc{
		s.handleAdminUsers, s.handleAdminPendingGroups,
		s.handleAdminGroupDecision, s.handleAdminSetUserAdmin,
		s.handleAdminMonitorStats, s.handleAdminMonitorQueue,
		s.handleAdminPrioritizeRepo,
		s.handleAdminAddRequests, s.handleAdminAddRequestDecision, // v0.27.20
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/api/v1/admin/x", nil)
		req.Header.Set("Authorization", "Bearer tok")
		h(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("admin handler with non-admin identity must 403, got %d", rec.Code)
		}
	}
}

func TestAdminSelfDemotionRefused(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/admin/users/9/admin", strings.NewReader(`{"admin":false}`))
	req.SetPathValue("userID", "9")
	ctx := context.WithValue(req.Context(), authCtxKey{}, authInfo{UserID: 9, IsAdmin: true})
	s.handleAdminSetUserAdmin(rec, req.WithContext(ctx))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("self-demotion must be refused (last-admin guard), got %d", rec.Code)
	}
}

// TestAdminMutationsBustAuthCache pins the v0.27.3 cache-invalidation
// fix: promoting a user (or approving a group) must drop the 60s
// token-validation cache, so the affected user's very next /me
// reflects the change instead of the stale cached role/scope. Found
// live during GUI wiring: a promoted user's nav stayed non-admin for
// up to authCacheTTL.
func TestAdminMutationsBustAuthCache(t *testing.T) {
	store := &fakeSessionStore{userID: 7, valid: map[string]bool{"tok": true}}
	auth := newAuthenticator(store, false)

	// Prime the cache with the non-admin role.
	if info, ok := auth.resolveToken(context.Background(), "tok"); !ok || info.IsAdmin {
		t.Fatalf("prime: want cached non-admin, got ok=%v admin=%v", ok, info.IsAdmin)
	}

	// Role flips in the store; the cache still answers stale.
	store.admin = true
	if info, _ := auth.resolveToken(context.Background(), "tok"); info.IsAdmin {
		t.Fatal("precondition: cache should still be serving the stale non-admin entry")
	}

	// The admin-mutation handlers call invalidateAll — after it, the
	// next resolve must see the new role immediately.
	auth.invalidateAll()
	if info, ok := auth.resolveToken(context.Background(), "tok"); !ok || !info.IsAdmin {
		t.Errorf("after invalidateAll, resolveToken must re-validate and see admin=true, got ok=%v admin=%v", ok, info.IsAdmin)
	}
}

func TestAdminMutationHandlersInvalidateCache(t *testing.T) {
	src := mustReadFile(t, "portal.go")
	setAdmin := extractFuncBody(t, src, "handleAdminSetUserAdmin")
	if !strings.Contains(setAdmin, "invalidateAll()") {
		t.Error("handleAdminSetUserAdmin must call s.auth.invalidateAll() after SetUserAdmin — else the promoted user's own /me stays stale for up to authCacheTTL")
	}
	decision := extractFuncBody(t, src, "handleAdminGroupDecision")
	if !strings.Contains(decision, "invalidateAll()") {
		t.Error("handleAdminGroupDecision must call s.auth.invalidateAll() after approve/reject — else the requester's cached scope stays empty for up to authCacheTTL")
	}
}

func extractFuncBody(t *testing.T, src, name string) string {
	t.Helper()
	start := strings.Index(src, "func (s *Server) "+name+"(")
	if start < 0 {
		t.Fatalf("function %s not found", name)
	}
	rest := src[start:]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		end = len(rest)
	}
	return rest[:end]
}

func TestPortalRoutesRegistered(t *testing.T) {
	src := mustReadFile(t, "server.go")
	for _, route := range []string{
		`"GET /api/v1/me"`,
		`"GET /api/v1/groups"`,
		`"POST /api/v1/groups"`,
		`"GET /api/v1/groups/{groupID}/repos"`,
		`"POST /api/v1/groups/{groupID}/repos"`,
		`"GET /api/v1/groups/{groupID}/orgs"`, // 2026-07-21 tracked-orgs listing
		`"GET /api/v1/admin/users"`,
		`"POST /api/v1/admin/users/{userID}/admin"`,
		`"GET /api/v1/admin/groups/pending"`,
		`"POST /api/v1/admin/groups/{groupID}/{decision}"`,
		`"GET /api/v1/admin/monitor/stats"`,
		`"GET /api/v1/admin/monitor/queue"`,
	} {
		if !strings.Contains(src, route) {
			t.Errorf("server.go must register %s", route)
		}
	}
}
