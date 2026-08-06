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

	"github.com/aveloxis/aveloxis/internal/db"
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
	want := db.PublicFleetStats{Repos: 12483, Commits: 500_000_000, Issues: 9_000_000, PRs: 8_000_000, Contributors: 1_700_000}
	c := newPublicStatsCache(time.Minute, func() (db.PublicFleetStats, error) {
		loads++
		return want, nil
	})
	for i := 0; i < 5; i++ {
		v, err := c.get()
		if err != nil || v != want {
			t.Fatalf("get %d: v=%+v err=%v", i, v, err)
		}
	}
	if loads != 1 {
		t.Errorf("5 gets inside the TTL must load once, loaded %d times", loads)
	}
}

func TestPublicStatsCacheServesStaleOnError(t *testing.T) {
	loads := 0
	want := db.PublicFleetStats{Repos: 12483}
	c := newPublicStatsCache(0, func() (db.PublicFleetStats, error) { // TTL 0 → reload every get
		loads++
		if loads == 1 {
			return want, nil
		}
		return db.PublicFleetStats{}, errors.New("db down")
	})
	if v, _ := c.get(); v != want {
		t.Fatal("first load")
	}
	v, err := c.get() // loader errors now
	if err != nil || v != want {
		t.Errorf("a failed refresh must serve the stale value (v=%+v err=%v) — the landing page never renders an error for the numbers", v, err)
	}
	// Never-loaded + error → the error DOES surface (nothing to be stale with).
	c2 := newPublicStatsCache(time.Minute, func() (db.PublicFleetStats, error) {
		return db.PublicFleetStats{}, errors.New("down")
	})
	if _, err := c2.get(); err == nil {
		t.Error("no cached value and a failed load must surface the error")
	}
}
