// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// v0.29.60: the supply-chain endpoints' request contract, with a bare
// Server (no store): a session is required before anything else, and the
// sort key and group id are validated against the allowlist and shape.
func TestSupplyChainEndpointsRequireASession(t *testing.T) {
	srv := newTestServer()
	for _, path := range []string{"/api/v1/supply-chain/packages", "/api/v1/supply-chain/packages/npm/minimatch", "/api/v1/supply-chain/packages/npm/@scope/name"} {
		rec := httptest.NewRecorder()
		srv.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s without a token: %d, want 401", path, rec.Code)
		}
	}
}

func TestSupplyChainScopedNameRouteMatches(t *testing.T) {
	// The {name...} wildcard keeps a scoped npm name's slash: the route must
	// match (401 from the session gate, not 404 from the mux).
	srv := newTestServer()
	rec := httptest.NewRecorder()
	srv.mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/supply-chain/packages/npm/@jest/transform", nil))
	if rec.Code == http.StatusNotFound {
		t.Fatal("a scoped package name must route to the package handler")
	}
}
