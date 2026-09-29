// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
)

// TestAffiliationLoadFailureIsNotLoaded (AVELOXIS_TEST_DB) — old problem O1:
// loadAll swallowed the query error, skipped unscannable rows and never
// checked rows.Err(), then marked the cache loaded — so a failed or
// truncated load served a partial affiliation map for the process's
// lifetime, silently. A failed load is logged, is NOT marked loaded, and is
// retried after affiliationRetryInterval.
func TestAffiliationLoadFailureIsNotLoaded(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	logs := &syncLog{}
	store, err := NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close() // every query now fails
	r := NewAffiliationResolver(store)
	if got := r.Resolve(context.Background(), "a@example.com"); got != "" {
		t.Errorf("a failed load resolved %q", got)
	}
	r.mu.RLock()
	loaded, failedAt := r.loaded, r.failedAt
	r.mu.RUnlock()
	if loaded || failedAt.IsZero() {
		t.Errorf("a failed load must stay unloaded and record the failure (loaded=%v failedAt=%v)", loaded, failedAt)
	}
	if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "affiliation") {
		t.Errorf("a failed load must be logged:\n%s", logs.String())
	}
	// Within the retry interval no second query (no second WARN).
	before := strings.Count(logs.String(), "level=WARN")
	r.Resolve(context.Background(), "b@example.com")
	if strings.Count(logs.String(), "level=WARN") != before {
		t.Error("a failed load must not be retried on every lookup")
	}
	// After the interval it is retried.
	r.mu.Lock()
	r.failedAt = time.Now().Add(-2 * affiliationRetryInterval)
	r.mu.Unlock()
	r.Resolve(context.Background(), "c@example.com")
	if strings.Count(logs.String(), "level=WARN") == before {
		t.Error("a failed load must be retried after affiliationRetryInterval")
	}
}
