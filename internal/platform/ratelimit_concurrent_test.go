// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.36 concurrency stress test for KeyPool. The pool is shared by
// every scheduler worker (up to 80+ concurrent Acquire callers on the
// production fleet), but before this test nothing exercised it
// concurrently — so `-race` in CI certified nothing about it (the race
// detector only reports on interleavings that actually run). This test
// exists to give the detector something to observe; run it with
// `go test -race`. Since 2026-09-12 every acquire is a LEASE under the
// default ceilings (40 global / 4 per key), so 32 workers over 4 keys
// also contend on the condition variable here — the wait/broadcast path
// runs under the detector too.

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestKeyPoolConcurrentAccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	kp := NewKeyPool([]string{"k1", "k2", "k3", "k4"}, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mkResp := func(remaining string) *http.Response {
		h := http.Header{}
		h.Set("X-RateLimit-Remaining", remaining)
		h.Set("X-RateLimit-Limit", "5000")
		return &http.Response{StatusCode: 200, Header: h}
	}

	var wg sync.WaitGroup
	for worker := range 32 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 200 {
				key, release, err := kp.Acquire(ctx, ResourceCore)
				if err != nil {
					t.Errorf("worker %d iter %d: Acquire: %v", w, i, err)
					return
				}
				switch i % 5 {
				case 0, 1, 2:
					kp.UpdateFromResponse(key, mkResp("4000"))
				case 3:
					// Auth failure + success cycle — exercises the
					// v0.25.31 quarantine counters. ONLY worker 0
					// records strikes: with many concurrent
					// strike-writers, three unpaired failures from
					// different workers can land on one key
					// back-to-back and legitimately quarantine it
					// (strikes count CONSECUTIVE 401s; success clears
					// strikes but never lifts an active quarantine).
					// On a slow -race CI runner that occasionally
					// quarantined ALL keys at once and starved GetKey
					// past its 30s ctx — a test-design flake, not a
					// pool bug (observed 2026-07-14). A single
					// strike-writer keeps the "never reaches
					// maxAuthStrikes" invariant true while these
					// methods still race against 31 other workers.
					if w == 0 {
						kp.RecordAuthFailure(key)
						kp.RecordAuthSuccess(key)
					} else {
						kp.UpdateFromResponse(key, mkResp("4000"))
					}
				case 4:
					// Read-side methods raced against the writers.
					_ = kp.AliveCount()
					_ = kp.TotalRemaining()
					_ = kp.IsEmpty()
					_, _ = kp.Snapshot()
				}
				release()
			}
		}(worker)
	}
	wg.Wait()

	if kp.AliveCount() != 4 {
		t.Errorf("all 4 keys should remain alive after the stress run, got %d", kp.AliveCount())
	}
	// Every lease was released: the counters must be back at zero, or a
	// leak would make the ceilings admit fewer callers forever.
	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.inflight != 0 {
		t.Errorf("pool inflight = %d after every release, want 0", kp.inflight)
	}
	for _, k := range kp.keys {
		if k.inflight != 0 {
			t.Errorf("key %q inflight = %d after every release, want 0", k.Token, k.inflight)
		}
	}
}
