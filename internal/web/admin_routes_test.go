// Source-contract tests for v0.19.0 admin web routes.
//
// New routes:
//   - GET  /admin/groups/pending       — list of pending groups awaiting review
//   - POST /admin/groups/{id}/approve  — admin approves a group
//   - POST /admin/groups/{id}/reject   — admin rejects a group
//   - GET  /admin/users                — list of all users
//   - POST /admin/users/{id}/admin     — toggle admin role
//
// All gated by a new requireAdmin middleware: auth + admin=TRUE.
// Non-admin users see a 403; unauthenticated users get redirected to
// /login (existing requireAuth behavior).

package web

import (
	"os"
	"strings"
	"testing"
)

// TestRequireAdminMiddlewareExists pins the admin gate.
func TestRequireAdminMiddlewareExists(t *testing.T) {
	src := mustReadServerSource(t)
	if !strings.Contains(src, "func (s *Server) requireAdmin(") {
		t.Error("server.go must define requireAdmin middleware that gates routes on session.IsAdmin")
	}
}

// TestSessionStructHasIsAdmin pins that the in-memory session carries
// the admin flag so requireAdmin doesn't need a DB roundtrip per request.
func TestSessionStructHasIsAdmin(t *testing.T) {
	src := mustReadServerSource(t)
	idx := strings.Index(src, "type Session struct {")
	if idx < 0 {
		t.Fatal("could not locate Session struct")
	}
	end := strings.Index(src[idx:], "\n}")
	if end < 0 {
		t.Fatal("could not find end of Session struct")
	}
	decl := src[idx : idx+end]
	if !strings.Contains(decl, "IsAdmin") {
		t.Error("Session struct must hold IsAdmin so requireAdmin can gate without a DB lookup. " +
			"Set it from the OAuth callback's UpsertOAuthUser return.")
	}
}

// TestAdminRoutesRegistered pins that all admin endpoints are wired
// through requireAdmin.
func TestAdminRoutesRegistered(t *testing.T) {
	src := mustReadServerSource(t)

	for _, route := range []string{
		"/admin/groups/pending",
		"/admin/groups/",
		"/admin/users",
	} {
		if !strings.Contains(src, route) {
			t.Errorf("server.go must register the %q route", route)
		}
	}

	// Each admin route handler must be wrapped in requireAdmin, not just requireAuth.
	if !strings.Contains(src, "requireAdmin(s.handleAdminPendingGroups") &&
		!strings.Contains(src, "requireAdmin(s.handlePendingGroups") {
		t.Error("the pending-groups admin page must be wrapped in requireAdmin (not requireAuth) — non-admins must NOT see other users' pending submissions")
	}
}

// TestApproveGroupHandlerExists pins the POST handler.
func TestApproveGroupHandlerExists(t *testing.T) {
	src := mustReadServerSource(t)
	if !strings.Contains(src, "handleApproveGroup") {
		t.Error("server.go must define handleApproveGroup — the POST /admin/groups/{id}/approve handler")
	}
}

// TestRejectGroupHandlerExists pins the rejection POST handler.
func TestRejectGroupHandlerExists(t *testing.T) {
	src := mustReadServerSource(t)
	if !strings.Contains(src, "handleRejectGroup") {
		t.Error("server.go must define handleRejectGroup — the POST /admin/groups/{id}/reject handler")
	}
}

// TestSetAdminHandlerExists pins the admin-role toggle handler.
func TestSetAdminHandlerExists(t *testing.T) {
	src := mustReadServerSource(t)
	if !strings.Contains(src, "handleSetUserAdmin") || !strings.Contains(src, "SetUserAdmin") {
		t.Error("server.go must define handleSetUserAdmin and call s.store.SetUserAdmin to toggle the admin role")
	}
}

func mustReadServerSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	return string(data)
}
