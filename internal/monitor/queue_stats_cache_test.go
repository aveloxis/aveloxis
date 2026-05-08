// Tests for the in-memory QueueStats cache (v0.18.30 monitor perf fix #2).
//
// At v0.18.29, every dashboard render and every /api/stats poll fired a
// full GROUP BY scan of aveloxis_ops.collection_queue. With the
// dashboard meta-refreshing every 10 seconds across multiple browser
// tabs, this triggered a recurring scan even when nothing changed.
//
// The fix: a 60-second in-memory cache. The user explicitly confirmed
// they're OK with periodic freshness ("freshness CAN be periodic") and
// asked for "last updated" / "next update" timestamps at the top of the
// page. The cache exposes both timestamps to drive that header.

package monitor

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestQueueStatsCacheTypeExists pins the existence of the type so other
// tests can construct it.
func TestQueueStatsCacheTypeExists(t *testing.T) {
	src := mustReadFileMon(t, "queue_stats_cache.go")
	if !strings.Contains(src, "type QueueStatsCache struct") {
		t.Error("queue_stats_cache.go must define type QueueStatsCache for the in-memory monitor cache")
	}
}

// TestQueueStatsCacheServesFromCacheWithinTTL is the headline behavioral
// test: a second call within the TTL must return the cached value
// without invoking the loader function.
func TestQueueStatsCacheServesFromCacheWithinTTL(t *testing.T) {
	cache := NewQueueStatsCache(100 * time.Millisecond)

	calls := 0
	var mu sync.Mutex
	loader := func(ctx context.Context) (map[string]int, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return map[string]int{"queued": 5, "collecting": 2, "total": 7}, nil
	}

	ctx := context.Background()

	stats1, _, _, err := cache.Get(ctx, loader)
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	if stats1["queued"] != 5 {
		t.Errorf("first Get: queued = %d, want 5", stats1["queued"])
	}

	// Second call within TTL — should be a cache hit.
	stats2, _, _, err := cache.Get(ctx, loader)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if stats2["queued"] != 5 {
		t.Errorf("second Get: queued = %d, want 5", stats2["queued"])
	}

	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Errorf("loader invoked %d times within TTL window, want exactly 1", got)
	}
}

// TestQueueStatsCacheReloadsAfterTTL pins the staleness behavior:
// after the TTL elapses, the loader must be re-invoked.
func TestQueueStatsCacheReloadsAfterTTL(t *testing.T) {
	cache := NewQueueStatsCache(20 * time.Millisecond)

	calls := 0
	var mu sync.Mutex
	loader := func(ctx context.Context) (map[string]int, error) {
		mu.Lock()
		calls++
		current := calls
		mu.Unlock()
		return map[string]int{"queued": current}, nil
	}

	ctx := context.Background()
	if _, _, _, err := cache.Get(ctx, loader); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Wait past the TTL.
	time.Sleep(40 * time.Millisecond)

	stats, _, _, err := cache.Get(ctx, loader)
	if err != nil {
		t.Fatalf("post-TTL Get: %v", err)
	}
	if stats["queued"] != 2 {
		t.Errorf("post-TTL Get: queued = %d, want 2 (loader re-invoked)", stats["queued"])
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 2 {
		t.Errorf("loader invoked %d times across TTL boundary, want 2", got)
	}
}

// TestQueueStatsCacheReturnsTimestamps pins the LastRefreshedAt and
// NextRefreshAt outputs that drive the dashboard header. Without them,
// the user can't see when stats are stale or when they'll refresh.
func TestQueueStatsCacheReturnsTimestamps(t *testing.T) {
	ttl := 90 * time.Second
	cache := NewQueueStatsCache(ttl)
	ctx := context.Background()

	loader := func(ctx context.Context) (map[string]int, error) {
		return map[string]int{"queued": 1, "collecting": 0, "total": 1}, nil
	}

	before := time.Now()
	_, lastRefreshed, nextRefresh, err := cache.Get(ctx, loader)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	after := time.Now()

	if lastRefreshed.Before(before) || lastRefreshed.After(after) {
		t.Errorf("lastRefreshed %v must be in [%v, %v]", lastRefreshed, before, after)
	}
	want := lastRefreshed.Add(ttl)
	if !nextRefresh.Equal(want) {
		t.Errorf("nextRefresh = %v, want lastRefreshed + ttl = %v", nextRefresh, want)
	}
}

// TestQueueStatsCacheStaleOnLoaderError pins the failure path: if the
// loader errors AND we have a previously-cached value, return the
// stale value with the cached timestamps. Better stale than empty —
// the dashboard stays usable when the DB hiccups briefly.
func TestQueueStatsCacheStaleOnLoaderError(t *testing.T) {
	cache := NewQueueStatsCache(20 * time.Millisecond)
	ctx := context.Background()

	calls := 0
	loader := func(ctx context.Context) (map[string]int, error) {
		calls++
		if calls == 1 {
			return map[string]int{"queued": 42, "total": 42}, nil
		}
		return nil, errSimulatedDBFailure
	}
	if _, _, _, err := cache.Get(ctx, loader); err != nil {
		t.Fatalf("first Get: %v", err)
	}

	// Wait past TTL so loader fires again.
	time.Sleep(40 * time.Millisecond)

	stats, _, _, err := cache.Get(ctx, loader)
	if err != nil {
		t.Errorf("Get returned err on stale fallback: %v — should fall back to cached value", err)
	}
	if stats == nil || stats["queued"] != 42 {
		t.Errorf("Get returned %v on loader failure, want stale cached map", stats)
	}
}

var errSimulatedDBFailure = newErr("simulated DB failure")

func newErr(s string) error { return &simErr{s: s} }

type simErr struct{ s string }

func (e *simErr) Error() string { return e.s }

func mustReadFileMon(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}
