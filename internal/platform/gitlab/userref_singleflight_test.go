// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// userref_singleflight_test.go — v0.27.141 (Copilot round 22): the
// v0.27.122 username cache prevented REPEAT lookups but not the cold
// thundering herd — the load-fetch-store sequence let every concurrent
// first encounter of a username issue its own /users request. Cold
// misses now coalesce onto one flight; errors are still never cached.

package gitlab

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLookupGLUserRefColdMissesCoalesce(t *testing.T) {
	var hits atomic.Int64
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-release // hold the flight open so every goroutine is a COLD caller
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id": 4242, "username": "acme", "name": "Acme"}]`))
	})
	client, _ := newTestClientWithCapture(t, handler)

	const callers = 8
	var wg sync.WaitGroup
	results := make([]bool, callers)
	started := make(chan struct{}, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			started <- struct{}{}
			_, ok := client.lookupGLUserRef(context.Background(), "acme")
			results[i] = ok
		}()
	}
	for range callers {
		<-started
	}
	close(release)
	wg.Wait()

	if got := hits.Load(); got != 1 {
		t.Errorf("8 concurrent cold lookups issued %d HTTP requests — cold misses must coalesce to ONE flight", got)
	}
	for i, ok := range results {
		if !ok {
			t.Errorf("caller %d did not receive the coalesced result", i)
		}
	}
}

func TestLookupGLUserRefFailedFlightIsRetriable(t *testing.T) {
	var hits atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusNotFound) // classifies as a clean miss? no — 404 on /users list...
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id": 7, "username": "retry", "name": "R"}]`))
	})
	client, _ := newTestClientWithCapture(t, handler)

	// First flight fails (transient) — must NOT be cached.
	if _, ok := client.lookupGLUserRef(context.Background(), "retry"); ok {
		t.Fatal("failed flight must report no ref")
	}
	// Second call retries fresh and succeeds.
	ref, ok := client.lookupGLUserRef(context.Background(), "retry")
	if !ok || ref.PlatformID != 7 {
		t.Errorf("post-failure retry must run a fresh flight, got ok=%v ref=%+v", ok, ref)
	}
	if hits.Load() < 2 {
		t.Error("the failed flight was cached — errors must never cache")
	}
}
