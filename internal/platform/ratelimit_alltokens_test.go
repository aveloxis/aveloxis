// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 introduced KeyPool.AllTokens to feed scorecard's comma-separated
// multi-token GITHUB_TOKEN; 2026-09-12 replaced it with LendTokens — the
// same tokens, but ACCOUNTED (the pool records who borrowed what) instead
// of handed out invisibly. Behavioral tests: pool order preserved when
// nothing distinguishes the keys, invalidated keys excluded, no checkout
// side effects on the budget or the round-robin cursor.

package platform

import (
	"log/slog"
	"os"
	"reflect"
	"sync"
	"testing"
)

func TestLendTokensReturnsAllValidTokensInPoolOrder(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	got, release := kp.LendTokens(0)
	defer release()
	want := []string{"tok-a", "tok-b", "tok-c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LendTokens(0) = %v, want %v (pool order preserved when nothing distinguishes the keys)", got, want)
	}
}

func TestLendTokensSkipsInvalidatedKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	// Invalidate the middle key via the public path.
	kp.mu.Lock()
	middle := kp.keys[1]
	kp.mu.Unlock()
	kp.InvalidateKey(middle)

	got, release := kp.LendTokens(0)
	defer release()
	want := []string{"tok-a", "tok-c"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LendTokens(0) after invalidating tok-b = %v, want %v", got, want)
	}
}

func TestLendTokensEmptyPool(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool(nil, logger)
	got, release := kp.LendTokens(0)
	release()
	if len(got) != 0 {
		t.Errorf("LendTokens(0) on empty pool = %v, want empty", got)
	}
}

// TestLendTokensDoesNotCheckOutKeys pins the no-budget-side-effect
// contract: lending must not advance the round-robin cursors or touch
// Remaining — a subprocess's ~40 calls are accounted by the lent counter,
// not by pretending to be one in-flight request.
func TestLendTokensDoesNotCheckOutKeys(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b"}, logger)

	kp.mu.Lock()
	rrBefore, rrGQLBefore := kp.rrIndex, kp.rrIndexGQL
	remBefore := kp.keys[0].Remaining
	inflightBefore := kp.inflight
	kp.mu.Unlock()

	_, release := kp.LendTokens(0)
	defer release()

	kp.mu.Lock()
	defer kp.mu.Unlock()
	if kp.rrIndex != rrBefore || kp.rrIndexGQL != rrGQLBefore {
		t.Errorf("LendTokens advanced a cursor (%d/%d → %d/%d); must not touch selection state", rrBefore, rrGQLBefore, kp.rrIndex, kp.rrIndexGQL)
	}
	if kp.keys[0].Remaining != remBefore {
		t.Errorf("LendTokens changed Remaining %d → %d; must not spend budget", remBefore, kp.keys[0].Remaining)
	}
	if kp.inflight != inflightBefore {
		t.Errorf("LendTokens changed pool inflight %d → %d; a lend is not a lease", inflightBefore, kp.inflight)
	}
}

// TestLendTokensConcurrentAccess drives LendTokens alongside key mutation
// so the race detector observes the mutex guard.
func TestLendTokensConcurrentAccess(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	kp := NewKeyPool([]string{"tok-a", "tok-b", "tok-c"}, logger)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, release := kp.LendTokens(2)
				_ = kp.AliveCount()
				release()
			}
		}()
	}
	wg.Wait()
	kp.mu.Lock()
	defer kp.mu.Unlock()
	for _, k := range kp.keys {
		if k.lent != 0 {
			t.Errorf("key %q lent=%d after every release, want 0", k.Token, k.lent)
		}
	}
}
