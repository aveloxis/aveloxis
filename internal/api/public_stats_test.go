// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// public_stats_test.go — TDD suite for the v0.27.59 public repo count
// (the landing page's bullseye-target number). Contracts:
//   - /api/v1/public/stats bypasses require_auth via an EXPLICIT
//     publicPaths allowlist (the fail-closed boundary stays tiny and
//     named — no prefix matching, no pattern magic);
//   - a path NOT on the allowlist still 401s, even under /public/;
//   - the count is served through a 60s single-value cache with
//     stale-on-error semantics (the landing page never renders an
//     error for a vanity number; a DB hiccup serves the last value).

import (
	"errors"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPublicStatsBypassesRequireAuth(t *testing.T) {
	store := &fakeSessionStore{valid: map[string]bool{}}
	h := authedChain(t, store, Options{RequireAuth: true}, okHandler())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/public/stats", nil)
	req.RemoteAddr = "203.0.113.5:1" // NOT exempt
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Errorf("/api/v1/public/stats must bypass require_auth (publicPaths allowlist), got %d", rec.Code)
	}
}

func TestPublicNamespaceIsNotBlanketPublic(t *testing.T) {
	store := &fakeSessionStore{valid: map[string]bool{}}
	h := authedChain(t, store, Options{RequireAuth: true}, okHandler())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/public/other", nil)
	req.RemoteAddr = "203.0.113.5:1"
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Errorf("only EXACT allowlisted paths bypass — /api/v1/public/other must 401, got %d (prefix matching would silently widen the fail-closed boundary)", rec.Code)
	}
}

func TestPublicStatsCacheSingleLoadWithinTTL(t *testing.T) {
	loads := 0
	c := newPublicStatsCache(time.Minute, func() (int, error) {
		loads++
		return 12483, nil
	})
	for i := 0; i < 5; i++ {
		n, err := c.get()
		if err != nil || n != 12483 {
			t.Fatalf("get %d: n=%d err=%v", i, n, err)
		}
	}
	if loads != 1 {
		t.Errorf("5 gets inside the TTL must load once, loaded %d times", loads)
	}
}

func TestPublicStatsCacheServesStaleOnError(t *testing.T) {
	loads := 0
	c := newPublicStatsCache(0, func() (int, error) { // TTL 0 → reload every get
		loads++
		if loads == 1 {
			return 12483, nil
		}
		return 0, errors.New("db down")
	})
	if n, _ := c.get(); n != 12483 {
		t.Fatal("first load")
	}
	n, err := c.get() // loader errors now
	if err != nil || n != 12483 {
		t.Errorf("a failed refresh must serve the stale value (n=%d err=%v) — the landing page never renders an error for the count", n, err)
	}
	// Never-loaded + error → the error DOES surface (nothing to be stale with).
	c2 := newPublicStatsCache(time.Minute, func() (int, error) { return 0, errors.New("down") })
	if _, err := c2.get(); err == nil {
		t.Error("no cached value and a failed load must surface the error")
	}
}
