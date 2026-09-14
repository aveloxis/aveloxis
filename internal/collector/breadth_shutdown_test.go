// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Copilot round 8 on PR #191: a shutdown observed by the contributor-event
// INSERT must abort the run, not merely skip that contributor.
//
// The pre-fix arm was a bare `continue`, so it never set abortErr. When
// the canceled insert belonged to the LAST outcome the loop simply ended,
// abortErr stayed nil, and the worker logged "contributor breadth
// complete" and returned (result, nil) — a `stop serve` reported as a
// clean cycle. Aborting also cancels the remaining in-flight fetches
// instead of letting them run against a dead context.
//
// Driven with concurrency 1 so the first outcome is deterministic.
func TestBreadthAbortsWhenInsertObservesShutdown(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") != "1" {
			fmt.Fprint(w, "[]")
			return
		}
		login := strings.Split(strings.TrimPrefix(r.URL.Path, "/users/"), "/")[0]
		fmt.Fprintf(w, `[{"id": "9%03d", "type": "PushEvent",
			"repo": {"id": 7, "name": "a/b", "url": "https://api.github.com/repos/a/b"},
			"created_at": "2026-01-15T10:30:00Z"}]`, len(login))
	})

	store := &fakeBreadthStore{contributors: breadthFixture(3)}
	// Every insert observes the shutdown, so the FIRST outcome aborts.
	store.insertCanceledFor = store.contributors[0].ID
	store.contributors[1].ID = store.contributors[0].ID
	store.contributors[2].ID = store.contributors[0].ID

	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(1)

	_, err := worker.Run(context.Background(), 3, time.Hour)
	if err == nil {
		t.Fatal("Run returned nil after a shutdown-canceled insert: the cycle " +
			"reports success and logs \"contributor breadth complete\" on every " +
			"`stop serve` that lands mid-insert")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled — the abort must carry the cause", err)
	}

	// The contributor whose insert was canceled must stay UNMARKED so the
	// next cycle retries it (the v0.27.8 ordering contract).
	store.mu.Lock()
	defer store.mu.Unlock()
	for _, id := range store.attempted {
		if id == store.contributors[0].ID {
			t.Error("a contributor whose insert was canceled by shutdown was marked attempted — " +
				"it will sit behind the cooldown window without its events")
		}
	}
}
