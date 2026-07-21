// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.40 (summary/18 Phase 3): -race coverage for the dashboard
// stats cache under concurrent Get (its mutex had never been observed
// under contention).

package monitor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueStatsCacheConcurrentAccess(t *testing.T) {
	c := NewQueueStatsCache(10 * time.Millisecond)
	var loads atomic.Int32
	loader := func(ctx context.Context) (map[string]int, error) {
		loads.Add(1)
		return map[string]int{"queued": int(loads.Load())}, nil
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				stats, _, _, err := c.Get(ctx, loader)
				if err != nil {
					t.Error(err)
					return
				}
				// Read the returned map — a concurrent in-place mutation
				// by the cache would trip the race detector here.
				_ = stats["queued"]
			}
		}()
	}
	wg.Wait()
}
