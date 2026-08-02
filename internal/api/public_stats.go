// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"sync"
	"time"
)

// public_stats.go — the v0.27.59 public repo count backing the landing
// page's bullseye target ("N repositories under analysis"). The
// endpoint is on the publicPaths allowlist (auth.go) so an anonymous
// landing page can read it even with require_auth on; the per-IP rate
// limiter still applies. The count is a vanity number for engagement,
// so the cache serves STALE on a failed refresh — the landing page
// never renders an error for it.

// publicStatsCache is a single-value TTL cache with stale-on-error
// semantics (the QueueStatsCache posture applied to one integer).
type publicStatsCache struct {
	mu        sync.Mutex
	ttl       time.Duration
	load      func() (int, error)
	value     int
	haveValue bool
	loadedAt  time.Time
}

func newPublicStatsCache(ttl time.Duration, load func() (int, error)) *publicStatsCache {
	return &publicStatsCache{ttl: ttl, load: load}
}

func (c *publicStatsCache) get() (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.haveValue && time.Since(c.loadedAt) < c.ttl {
		return c.value, nil
	}
	n, err := c.load()
	if err != nil {
		if c.haveValue {
			return c.value, nil // stale beats an error for a vanity count
		}
		return 0, err
	}
	c.value, c.haveValue, c.loadedAt = n, true, time.Now()
	return n, nil
}

// handlePublicStats serves GET /api/v1/public/stats → {"repos": N}.
func (s *Server) handlePublicStats(w http.ResponseWriter, r *http.Request) {
	n, err := s.publicStats.get()
	if err != nil {
		http.Error(w, "count unavailable", http.StatusServiceUnavailable)
		return
	}
	jsonResponse(w, map[string]int{"repos": n})
}
