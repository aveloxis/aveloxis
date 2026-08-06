// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// public_stats.go — the anonymous landing-page stats. v0.27.59
// introduced the single repo count; v0.27.77 grows the payload to the
// fleet-scale numbers {repos, commits, issues, prs, contributors}
// (growth-plan phase 3: the landing page's mock "LIVE ANALYTICS
// PREVIEW" numbers become real). The endpoint is on the publicPaths
// allowlist (auth.go) so an anonymous landing page can read it even
// with require_auth on; the per-IP rate limiter still applies. The
// numbers are vanity stats for engagement, so the cache serves STALE
// on a failed refresh — the landing page never renders an error for
// them.

// publicStatsCache is a single-value TTL cache with stale-on-error
// semantics (the QueueStatsCache posture applied to one payload).
type publicStatsCache struct {
	mu        sync.Mutex
	ttl       time.Duration
	load      func() (db.PublicFleetStats, error)
	value     db.PublicFleetStats
	haveValue bool
	loadedAt  time.Time
}

func newPublicStatsCache(ttl time.Duration, load func() (db.PublicFleetStats, error)) *publicStatsCache {
	return &publicStatsCache{ttl: ttl, load: load}
}

func (c *publicStatsCache) get() (db.PublicFleetStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.haveValue && time.Since(c.loadedAt) < c.ttl {
		return c.value, nil
	}
	v, err := c.load()
	if err != nil {
		if c.haveValue {
			return c.value, nil // stale beats an error for vanity numbers
		}
		return db.PublicFleetStats{}, err
	}
	c.value, c.haveValue, c.loadedAt = v, true, time.Now()
	return v, nil
}

// handlePublicStats serves GET /api/v1/public/stats →
// {"repos": N, "commits": N, "issues": N, "prs": N, "contributors": N}.
func (s *Server) handlePublicStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.publicStats.get()
	if err != nil {
		http.Error(w, "stats unavailable", http.StatusServiceUnavailable)
		return
	}
	jsonResponse(w, stats)
}
