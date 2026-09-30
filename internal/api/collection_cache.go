// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// collectionCache caches per-repository answers under the repositories'
// collection generation (db.CollectionGeneration: a digest of every
// member's queue row, updated_at and last_collected), O11 option 4
// (v0.29.71; measured on kate: summary/43 §2 item 9). A job that ends over
// any member changes the key, so an answer is not served past it; the TTL bounds what changes outside a
// collection — contributor names and logins filled by the enrichment sweep —
// and is that sweep's interval (collection.enrich_interval_minutes). The
// 60 s body caches it replaces missed on every page view more than a minute
// apart.
type collectionCache struct {
	mu  sync.Mutex
	m   map[string]collectionCacheEntry
	ttl time.Duration
	now func() time.Time
}

type collectionCacheEntry struct {
	val     any
	expires time.Time
}

// collectionCacheMaxEntries is compareCache's bound: past it the map starts
// over (bodies are small; the hot set refills in one page view each).
const collectionCacheMaxEntries = 1000

// newCollectionCache returns a cache with the given TTL; zero (a Server
// built by New, as tests do) keeps the 60 s compareCacheTTL.
func newCollectionCache(ttl time.Duration) *collectionCache {
	if ttl <= 0 {
		ttl = compareCacheTTL
	}
	return &collectionCache{m: map[string]collectionCacheEntry{}, ttl: ttl, now: time.Now}
}

// collectionKey joins a request key and a collection generation.
func collectionKey(key, gen string) string { return key + "|gen=" + gen }

func (c *collectionCache) get(key string) (any, bool) {
	if c == nil { // a bare test Server
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[key]
	if !ok || c.now().After(e.expires) {
		return nil, false
	}
	return e.val, true
}

func (c *collectionCache) put(key string, v any) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.m) >= collectionCacheMaxEntries {
		c.m = map[string]collectionCacheEntry{}
	}
	c.m[key] = collectionCacheEntry{val: v, expires: c.now().Add(c.ttl)}
}

// generationKey returns the request key joined with the repositories'
// collection generation. ok is false when the generation could not be read:
// the caller computes without the cache (never serves a possibly stale
// answer) and the failure is logged here.
func (s *Server) generationKey(ctx context.Context, r *http.Request, where, key string, repoIDs []int64) (string, bool) {
	gen, err := s.store.CollectionGeneration(ctx, repoIDs)
	if err != nil {
		httpserver.LogFailure(r.Context(), s.logger, slog.LevelWarn, err, "collection generation unreadable — answering without the cache",
			"handler", where, "error", err)
		return "", false
	}
	return collectionKey(key, gen), true
}
