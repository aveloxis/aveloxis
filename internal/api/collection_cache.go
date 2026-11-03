// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"

	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// collectionCache caches /compare's per-entity series under the
// repositories' collection generation (db.CollectionGeneration: a digest of
// every member's queue row, updated_at and last_collected, and its
// data_changed_at), O11 option 4 (v0.29.71; measured on kate: summary/43 §2
// item 9). A job that ends over any member, or a writer outside a job that
// stamps one, changes the key, so an answer is not served past it. It has no
// age limit (D6, 0.29.120, summary/63: durable until recollection; the
// enrichment-interval TTL it had made every chart recompute every 17 minutes
// on kate).
type collectionCache struct {
	mu sync.Mutex
	m  map[string]collectionCacheEntry
	// What it did since start (the administrator's view, D4, 0.29.123):
	// resets counts the whole-map clears at collectionCacheMaxEntries, the
	// figure D5 left the organization path's eviction rule to.
	hits, misses, resets int64
}

type collectionCacheEntry struct {
	val any
}

// collectionCacheMaxEntries is compareCache's bound: past it the map starts
// over (bodies are small; the hot set refills in one page view each).
const collectionCacheMaxEntries = 1000

func newCollectionCache() *collectionCache {
	return &collectionCache{m: map[string]collectionCacheEntry{}}
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
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
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
		c.resets++
	}
	c.m[key] = collectionCacheEntry{val: v}
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
