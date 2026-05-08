// Package monitor — queue_stats_cache.go provides an in-memory cache
// for QueueStats so the GROUP BY scan over aveloxis_ops.collection_queue
// runs at most once per TTL window instead of once per dashboard render.
//
// Background: at v0.18.29 the dashboard meta-refreshed every 10 seconds
// and every render triggered s.store.QueueStats(ctx) — a `SELECT
// status, COUNT(*) … GROUP BY status` against a 100K-row queue table
// per browser tab per 10 seconds. The user explicitly confirmed
// "freshness CAN be periodic" and asked for "last updated" / "next
// update" timestamps in the header. This cache delivers both.

package monitor

import (
	"context"
	"sync"
	"time"
)

// queueStatsLoader is the function signature the cache invokes to
// re-populate stale data. The dashboard wires this to
// s.store.QueueStats so the cache stays decoupled from the store
// (helps tests stub the loader).
type queueStatsLoader func(ctx context.Context) (map[string]int, error)

// QueueStatsCache is a small in-memory cache keyed by nothing (single
// global QueueStats result). Returns the cached value plus the
// timestamp it was refreshed at and the time the next refresh is due,
// so the dashboard header can show the operator how stale the data is.
//
// Stale-on-error semantics: if the loader fails AND we have a
// previously cached value, we serve the stale value and treat the
// refresh as "skipped this tick" — the dashboard stays usable when the
// DB hiccups briefly. The error is silently swallowed; callers that
// need to surface it can wrap the loader.
type QueueStatsCache struct {
	ttl time.Duration

	mu       sync.Mutex
	cached   map[string]int
	filledAt time.Time
}

// NewQueueStatsCache returns a cache with the given TTL. v0.18.30
// uses 60 seconds in the dashboard wire-up — long enough to soak the
// per-tab meta-refresh, short enough that operators won't notice the
// staleness.
func NewQueueStatsCache(ttl time.Duration) *QueueStatsCache {
	return &QueueStatsCache{ttl: ttl}
}

// Get returns the cached stats, refreshing via loader if the cached
// entry is older than ttl. Returns the stats, the timestamp of the
// latest successful refresh, and the timestamp of the next due
// refresh. Errors from a re-load fall through to the previously
// cached value when one is available — see the type doc.
func (c *QueueStatsCache) Get(ctx context.Context, loader queueStatsLoader) (map[string]int, time.Time, time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if c.cached != nil && now.Sub(c.filledAt) < c.ttl {
		return c.cached, c.filledAt, c.filledAt.Add(c.ttl), nil
	}

	stats, err := loader(ctx)
	if err != nil {
		// Stale-on-error: if we have a previous value, return it.
		if c.cached != nil {
			return c.cached, c.filledAt, c.filledAt.Add(c.ttl), nil
		}
		return nil, time.Time{}, time.Time{}, err
	}

	c.cached = stats
	c.filledAt = now
	return c.cached, c.filledAt, c.filledAt.Add(c.ttl), nil
}
