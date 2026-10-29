// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
)

// TestCollectionCacheIsKeyedByGenerationAndBounded — O11 option 4
// (v0.29.71): an answer is reused while the repositories' collection
// generation is unchanged and recomputed when it moves. Since 0.29.120 (D6,
// summary/63) it has no age limit: durable until a member is collected again
// or a stamping writer moves its repository (the generation digests
// data_changed_at).
func TestCollectionCacheIsKeyedByGenerationAndBounded(t *testing.T) {
	c := newCollectionCache()
	c.put(collectionKey("series|7", "gen-1"), []byte("A"))
	// No clock: nothing in the cache reads one, so an entry's age cannot
	// end it.
	if v, ok := c.get(collectionKey("series|7", "gen-1")); !ok || string(v.([]byte)) != "A" {
		t.Errorf("the same generation must hit however old the entry is, got %v %v", v, ok)
	}
	if _, ok := c.get(collectionKey("series|7", "gen-2")); ok {
		t.Error("a new collection generation must miss")
	}
}

// TestServerCachesHaveNoAgeLimit — D6 (0.29.120, summary/63): neither the
// compare series cache nor the repository-page cache ages its answers, so the
// api takes no maximum age (the enrichment interval it used to receive is
// removed, not left unread: a negative tripwire), and the start-up lines
// report no TTL.
func TestServerCachesHaveNoAgeLimit(t *testing.T) {
	if _, ok := reflect.TypeOf(Options{}).FieldByName("ResponseCacheMaxAge"); ok {
		t.Error("api.Options carries ResponseCacheMaxAge again: the caches are durable until recollection (D6), an age option would be dead or would bring the timer back")
	}
	var buf lockedBuffer
	s, err := NewWithOptions(nil, slog.New(slog.NewTextHandler(&buf, nil)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	// The compare series and the repository page (v0.29.73) are separate
	// caches (a flood of organization comparisons cannot evict the
	// repository-page answers; a repository entity's series lives in the
	// page cache since 0.29.121).
	if s.seriesCache == nil || s.pageCache == nil {
		t.Fatalf("both caches must exist: series %v page %v", s.seriesCache, s.pageCache)
	}
	got := buf.String()
	if !strings.Contains(got, "API repository page cache") || !strings.Contains(got, "API collection cache") {
		t.Fatalf("the start-up lines are missing; log:\n%s", got)
	}
	if strings.Contains(got, "ttl=") {
		t.Errorf("a start-up line reports a TTL, which no cache has; log:\n%s", got)
	}
	// A nil logger (tests) must not panic.
	if _, err := NewWithOptions(nil, nil, Options{}); err != nil {
		t.Fatal(err)
	}
}
