// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.36 concurrency stress test for KeyPool. The pool is shared by
// every scheduler worker (up to 80+ concurrent GetKey callers on the
// production fleet), but before this test nothing exercised it
// concurrently — so `-race` in CI certified nothing about it (the race
// detector only reports on interleavings that actually run). This test
// exists to give the detector something to observe; run it with
// `go test -race`.

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
				key, err := kp.GetKey(ctx)
				if err != nil {
					t.Errorf("worker %d iter %d: GetKey: %v", w, i, err)
					return
				}
				switch i % 5 {
				case 0, 1, 2:
					kp.UpdateFromResponse(key, mkResp("4000"))
				case 3:
					// Auth failure + success cycle — exercises the
					// v0.25.31 quarantine counters without ever
					// reaching maxAuthStrikes (success resets).
					kp.RecordAuthFailure(key)
					kp.RecordAuthSuccess(key)
				case 4:
					// Read-side methods raced against the writers.
					_ = kp.AliveCount()
					_ = kp.TotalRemaining()
					_ = kp.IsEmpty()
				}
			}
		}(worker)
	}
	wg.Wait()

	if kp.AliveCount() != 4 {
		t.Errorf("all 4 keys should remain alive after the stress run, got %d", kp.AliveCount())
	}
}
