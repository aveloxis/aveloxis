// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.14 — admin "Boost" on the SPA monitor page:
// POST /api/v1/admin/monitor/queue/{repoID}/prioritize → pure reuse
// of store.PrioritizeRepo (the same call the legacy :5555 monitor's
// /api/prioritize/{repoID} makes).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrioritizeRouteRegistered(t *testing.T) {
	src := mustReadFile(t, "server.go")
	if !strings.Contains(src, `"POST /api/v1/admin/monitor/queue/{repoID}/prioritize"`) {
		t.Error("server.go must register POST /api/v1/admin/monitor/queue/{repoID}/prioritize")
	}
	portal := mustReadFile(t, "portal.go")
	if !strings.Contains(portal, "PrioritizeRepo(") {
		t.Error("the prioritize handler must reuse store.PrioritizeRepo — no parallel queue-mutation path")
	}
}

func TestPrioritizeRequiresAdmin(t *testing.T) {
	// No identity → 401 (even from exempt LAN with require_auth off).
	store := &fakeSessionStore{valid: map[string]bool{}}
	s := portalServer(t, store)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/admin/monitor/queue/1/prioritize", nil)
	req.RemoteAddr = "10.0.0.5:1"
	s.handleAdminPrioritizeRepo(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("prioritize without identity must 401, got %d", rec.Code)
	}

	// Non-admin identity → 403.
	store = &fakeSessionStore{userID: 7, valid: map[string]bool{"tok": true}}
	s = portalServer(t, store)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/admin/monitor/queue/1/prioritize", nil)
	req.Header.Set("Authorization", "Bearer tok")
	s.handleAdminPrioritizeRepo(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Errorf("prioritize with non-admin identity must 403, got %d", rec.Code)
	}
}

func TestPrioritizeInvalidID400(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/admin/monitor/queue/x/prioritize", nil)
	req.SetPathValue("repoID", "x")
	ctx := context.WithValue(req.Context(), authCtxKey{}, authInfo{UserID: 1, IsAdmin: true})
	s.handleAdminPrioritizeRepo(rec, req.WithContext(ctx))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("invalid repo id must 400, got %d", rec.Code)
	}
}
