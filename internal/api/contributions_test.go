// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// ============================================================
// Source-contract: routes are registered + handlers exist
// ============================================================

// TestContributionsRoutesRegistered pins that the two v0.23.10 routes
// flow through the same mux as the existing endpoints. If a future
// refactor moves them to a different package or drops the registration,
// this test catches it.
func TestContributionsRoutesRegistered(t *testing.T) {
	body, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	src := string(body)
	for _, needle := range []string{
		`/api/v1/repos/{repoID}/contributions/identities`,
		`/api/v1/repos/{repoID}/contributions/affiliations`,
		`s.handleRepoContributors`,
		`s.handleRepoAffiliations`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("server.go missing route registration token %q", needle)
		}
	}
}

// ============================================================
// Parameter validation (nil store — routing-layer only)
// ============================================================

func TestRepoContributors_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/contributions/identities", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for invalid repo_id", w.Code)
	}
}

func TestRepoAffiliations_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/contributions/affiliations", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for invalid repo_id", w.Code)
	}
}

// TestRepoContributors_SinceAfterUntil pins that an invalid window
// surfaces as 400 instead of being silently flipped to a default
// (which would mask the operator's mistake).
func TestRepoContributors_SinceAfterUntil(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET",
		"/api/v1/repos/1/contributions/identities?since=2026-01-01&until=2025-01-01", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for since >= until", w.Code)
	}
}

func TestRepoAffiliations_SinceAfterUntil(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET",
		"/api/v1/repos/1/contributions/affiliations?since=2026-01-01&until=2025-01-01", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for since >= until", w.Code)
	}
}

// ============================================================
// parseWindow unit tests
// ============================================================

// TestParseWindowDefaults pins the 2-year default for ?since with no
// upper bound (zero until). Matches the operator's "last 2 years" ask
// from the original query request.
func TestParseWindowDefaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/repos/1/contributions/identities", nil)
	since, until, ok := parseWindow(r)
	if !ok {
		t.Fatal("default window should be ok")
	}
	// since should be ~2 years before now; until should be zero.
	if since.IsZero() {
		t.Error("default since should not be zero")
	}
	if !until.IsZero() {
		t.Error("default until should be zero (unbounded)")
	}
}

// TestParseWindowInclusiveUntil pins the "until is inclusive" contract:
// a YYYY-MM-DD until param is shifted by +1 day before going to the
// store, so the store's exclusive-upper comparison includes the
// requested final calendar day.
func TestParseWindowInclusiveUntil(t *testing.T) {
	r := httptest.NewRequest("GET",
		"/api/v1/repos/1/contributions/identities?since=2024-01-01&until=2024-12-31", nil)
	since, until, ok := parseWindow(r)
	if !ok {
		t.Fatal("window should be ok")
	}
	if since.Format("2006-01-02") != "2024-01-01" {
		t.Errorf("since = %v, want 2024-01-01", since)
	}
	// until passed as 2024-12-31 should land as 2025-01-01 after the
	// inclusive-end shift.
	if until.Format("2006-01-02") != "2025-01-01" {
		t.Errorf("until = %v, want 2025-01-01 (one day past 2024-12-31)", until)
	}
}

// TestParseWindowInvalidSinceFallsBackToDefault pins the
// "malformed input doesn't error" behavior — matches the existing
// /timeseries endpoint's resilience.
func TestParseWindowInvalidSinceFallsBackToDefault(t *testing.T) {
	r := httptest.NewRequest("GET",
		"/api/v1/repos/1/contributions/identities?since=garbage", nil)
	since, _, ok := parseWindow(r)
	if !ok {
		t.Fatal("garbage since should fall back, not error")
	}
	if since.IsZero() {
		t.Error("since should fall back to 2-year default, not zero")
	}
}
