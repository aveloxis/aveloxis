// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"testing"
)

// TestHasUsableKey pins the one "is GitHub reachable" predicate in its own
// package (review round 3 of items 40/21): the nil receiver is the reason
// it exists — the web server calls it on a pool that may be nil (its keys
// are optional) — and the scheduler's transitive test would not see a
// dropped nil check from `go test ./internal/platform/` alone.
func TestHasUsableKey(t *testing.T) {
	lg := slog.New(slog.NewTextHandler(io.Discard, nil))
	var nilPool *KeyPool
	if nilPool.HasUsableKey() {
		t.Error("a nil pool reports a usable key")
	}
	if NewKeyPool(nil, lg).HasUsableKey() {
		t.Error("an empty pool (GitLab-only keys) reports a usable key")
	}
	pool := NewKeyPool([]string{"ghp_usable_probe"}, lg)
	if !pool.HasUsableKey() {
		t.Error("a pool with one key reports none usable")
	}
	key, release, err := pool.Acquire(t.Context(), ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	release()
	pool.InvalidateKey(key)
	if pool.HasUsableKey() {
		t.Error("a pool whose only key is invalidated reports a usable key")
	}
}
