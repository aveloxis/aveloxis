// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
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

// lockedBuffer is a log sink safe for the worker's concurrent writers.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newBreadthLoggingWorker is newBreadthTestWorker with its log kept.
func newBreadthLoggingWorker(t *testing.T, store breadthStore, handler http.Handler) (*BreadthWorker, *lockedBuffer) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	logs := &lockedBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	httpClient := platform.NewHTTPClient(server.URL, platform.NewKeyPool([]string{"t"}, logger), logger, platform.AuthGitHub)
	return NewBreadthWorkerWithHTTP(store, httpClient, logger), logs
}

// Copilot on PR #210 (review 5254485811): a `stop serve` that lands after
// earlier contributors failed logged the run's error tally as a WARN
// ("breadth: contributors not collected") on the way out. Shutdown is not
// a failure; the run must end quietly with the cancellation as its error.
func TestBreadthShutdownAfterEarlierErrorsIsQuiet(t *testing.T) {
	store := &fakeBreadthStore{contributors: breadthFixture(3)}
	gone := fmt.Errorf("events: %w", platform.ErrNotFound)
	store.getNewestErrFor = map[string]error{}
	for _, c := range store.contributors {
		store.getNewestErrFor[c.ID] = gone
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	last := store.contributors[2].ID
	store.onGetNewest = func(id string) {
		if id == last {
			cancel() // `stop serve` while the last contributor is fetched
		}
	}
	worker, logs := newBreadthLoggingWorker(t, store, http.NotFoundHandler())
	worker = worker.WithFetchConcurrency(1)

	_, err := worker.Run(ctx, 3, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled", err)
	}
	if out := logs.String(); strings.Contains(out, "contributors not collected") || strings.Contains(out, "contributor breadth complete") {
		t.Errorf("a shutdown must end the run quietly, got:\n%s", out)
	}
}

// Found while checking the review above: the fetchers stop sending once
// the context is cancelled, so a shutdown that lands after the last
// outcome was handled — or while every fetcher is choosing between
// sending and stopping — ends the drain with nothing left to observe it.
// The run then logged "contributor breadth complete" and returned nil on a
// `stop serve`, the Copilot round 8 defect on another path.
func TestBreadthReturnsCanceledWhenShutdownLandsAfterTheLastOutcome(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") != "1" {
			fmt.Fprint(w, "[]")
			return
		}
		fmt.Fprint(w, `[{"id": "9001", "type": "PushEvent",
			"repo": {"id": 7, "name": "a/b", "url": "https://api.github.com/repos/a/b"},
			"created_at": "2026-01-15T10:30:00Z"}]`)
	})
	store := &fakeBreadthStore{contributors: breadthFixture(3)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	last := store.contributors[2].ID
	store.onInsert = func(rows []*db.ContributorRepoRow) {
		for _, r := range rows {
			if r.CntrbID == last {
				cancel() // the last insert succeeded; then `stop serve`
			}
		}
	}
	worker, logs := newBreadthLoggingWorker(t, store, handler)
	worker = worker.WithFetchConcurrency(1)

	_, err := worker.Run(ctx, 3, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run error = %v, want context.Canceled — a shutdown reported as a clean cycle", err)
	}
	if out := logs.String(); strings.Contains(out, "contributor breadth complete") {
		t.Errorf("a shutdown was logged as a completed run:\n%s", out)
	}
}
