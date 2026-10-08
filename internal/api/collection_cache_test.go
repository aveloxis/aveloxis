// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestCollectionCacheIsKeyedByGenerationAndBounded — O11 option 4
// (v0.29.71): an answer is reused while the repositories' collection
// generation is unchanged and its age is under the TTL (the contributor
// enrichment cadence), recomputed when either moves. The old 60 s body
// caches missed on every page view more than a minute apart.
func TestCollectionCacheIsKeyedByGenerationAndBounded(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	c := newCollectionCache(30 * time.Minute)
	c.now = func() time.Time { return now }
	c.put(collectionKey("topcontrib|7", "gen-1"), []byte("A"))

	now = now.Add(10 * time.Minute) // well past the old 60 s TTL
	if v, ok := c.get(collectionKey("topcontrib|7", "gen-1")); !ok || string(v.([]byte)) != "A" {
		t.Errorf("same generation within the TTL must hit, got %v %v", v, ok)
	}
	if _, ok := c.get(collectionKey("topcontrib|7", "gen-2")); ok {
		t.Error("a new collection generation must miss")
	}
	now = now.Add(21 * time.Minute)
	if _, ok := c.get(collectionKey("topcontrib|7", "gen-1")); ok {
		t.Error("an entry older than the TTL must miss")
	}
	if newCollectionCache(0).ttl != compareCacheTTL {
		t.Error("a zero TTL (a Server built by New) keeps the old 60 s behavior")
	}
}

// TestServerCacheUsesTheConfiguredMaxAge — the option reaches the cache.
func TestServerCacheUsesTheConfiguredMaxAge(t *testing.T) {
	var buf bytes.Buffer
	s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(&buf, nil)), Options{ResponseCacheMaxAge: 45 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	// The compare series and the repository page (v0.29.73) are separate
	// caches (a compare flood cannot evict the repository-page answers),
	// both aged by the same enrichment interval.
	if s.seriesCache == nil || s.seriesCache.ttl != 45*time.Minute {
		t.Errorf("seriesCache = %+v, want a 45 min TTL", s.seriesCache)
	}
	if s.pageCache == nil || s.pageCache.ttl != 45*time.Minute {
		t.Errorf("pageCache = %+v, want a 45 min enriched TTL", s.pageCache)
	}
	// SR-10: the EFFECTIVE value is logged at startup.
	if got := buf.String(); !strings.Contains(got, "collection cache") || !strings.Contains(got, "ttl=45m0s") {
		t.Errorf("the API must log the cache TTL in effect; log:\n%s", got)
	}
	// A nil logger (tests) must not panic.
	if _, err := NewWithOptions(nil, nil, Options{}); err != nil {
		t.Fatal(err)
	}
}
