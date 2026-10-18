// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRequireAdminMiddlewareExists pins the admin gate.
func TestRequireAdminMiddlewareExists(t *testing.T) {
	// PackageFunc is fatal when the middleware is gone (v0.29.98, P-2).
	body := srctest.PackageFunc(t, "internal/web", "func (s *Server) requireAdmin(")
	if !strings.Contains(body, "IsAdmin") {
		t.Error("requireAdmin must gate on the session's IsAdmin")
	}
}

// TestSessionStructHasIsAdmin pins that the in-memory session carries
// the admin flag so requireAdmin doesn't need a DB roundtrip per request.
func TestSessionStructHasIsAdmin(t *testing.T) {
	// Session stays in server.go with Server, New and Handler (P-2).
	decl := srctest.TypeBody(t, srctest.Read(t, "internal/web/server.go"), "Session")
	if !strings.Contains(decl, "IsAdmin") {
		t.Error("Session struct must hold IsAdmin so requireAdmin can gate without a DB lookup. " +
			"Set it from the OAuth callback's UpsertOAuthUser return.")
	}
}

// TestAdminRoutesRegistered pins that all admin endpoints are wired
// through requireAdmin.
func TestAdminRoutesRegistered(t *testing.T) {
	src := srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler(")

	for _, route := range []string{
		"/admin/groups/pending",
		"/admin/groups/",
		"/admin/users",
	} {
		if !strings.Contains(src, route) {
			t.Errorf("Handler must register the %q route", route)
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
	if !strings.Contains(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("), "s.requireAdmin(s.handleApproveGroup)") {
		t.Error("Handler must register handleApproveGroup — the POST /admin/groups/{id}/approve handler — behind requireAdmin")
	}
	srctest.PackageFunc(t, "internal/web", "func (s *Server) handleApproveGroup(")
}

// TestRejectGroupHandlerExists pins the rejection POST handler.
func TestRejectGroupHandlerExists(t *testing.T) {
	if !strings.Contains(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("), "s.requireAdmin(s.handleRejectGroup)") {
		t.Error("Handler must register handleRejectGroup — the POST /admin/groups/{id}/reject handler — behind requireAdmin")
	}
	srctest.PackageFunc(t, "internal/web", "func (s *Server) handleRejectGroup(")
}

// TestSetAdminHandlerExists pins the admin-role toggle handler.
func TestSetAdminHandlerExists(t *testing.T) {
	if !strings.Contains(srctest.PackageFunc(t, "internal/web", "func (s *Server) handleSetUserAdmin("), "SetUserAdmin(") {
		t.Error("handleSetUserAdmin must call s.store.SetUserAdmin to toggle the admin role")
	}
}
