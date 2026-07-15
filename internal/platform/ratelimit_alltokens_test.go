// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 — KeyPool.AllTokens feeds scorecard's comma-separated
// multi-token GITHUB_TOKEN. Behavioral tests: pool order preserved,
// invalidated keys excluded, no checkout side effects.

package platform

import (
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestAllTokensReturnsAllValidTokensInPoolOrder(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	got := kp.AllTokens()
	want := []string{"tok-a", "tok-b", "tok-c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AllTokens() = %v, want %v (pool order preserved)", got, want)
	}
}

func TestAllTokensSkipsInvalidatedKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	// Invalidate the middle key via the public path.
	kp.mu.Lock()
	middle := kp.keys[1]
	kp.mu.Unlock()
	kp.InvalidateKey(middle)

	got := kp.AllTokens()
	want := []string{"tok-a", "tok-c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AllTokens() after invalidating tok-b = %v, want %v", got, want)
	}
}

func TestAllTokensEmptyPool(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool(nil, logger)
	if got := kp.AllTokens(); len(got) != 0 {
		t.Errorf("AllTokens() on empty pool = %v, want empty", got)
	}
}

// TestAllTokensDoesNotCheckOutKeys pins the no-side-effect contract:
// AllTokens must not advance the round-robin index or touch Remaining —
// it is a read-only snapshot, not a checkout. (The pre-v0.27.5 scorecard
// phase checked a key out via GetKey and MarkDepleted'd it; the whole
// point of AllTokens is that no checkout happens.)
func TestAllTokensDoesNotCheckOutKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b"}, logger)

	kp.mu.Lock()
	rrBefore := kp.rrIndex
	remBefore := kp.keys[0].Remaining
	kp.mu.Unlock()

	_ = kp.AllTokens()

	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.rrIndex != rrBefore {
		t.Errorf("AllTokens advanced rrIndex %d → %d; must be read-only", rrBefore, kp.rrIndex)
	}
	if kp.keys[0].Remaining != remBefore {
		t.Errorf("AllTokens changed Remaining %d → %d; must be read-only", remBefore, kp.keys[0].Remaining)
	}
}

// TestAllTokensConcurrentAccess drives AllTokens alongside key mutation
// so the race detector observes the mutex guard.
func TestAllTokensConcurrentAccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = kp.AllTokens()
				_ = kp.AliveCount()
			}
		}()
	}
	wg.Wait()
}
