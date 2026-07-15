// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL tests for the v0.27.8 concurrent breadth worker. The
// pre-v0.27.8 Run loop was strictly sequential (one contributor, one
// HTTP request in flight, one single-row UPDATE per contributor);
// v0.27.8 splits fetch (concurrent pool) from persist (single
// coordinator, batched writes). These tests drive the REAL Run path
// through httptest + the fake store and must be run under -race — the
// fetcher pool, breaker counters, and coordinator handoff are exactly
// the state the race detector needs to observe.
//
// Preserved-semantics matrix covered here:
//   - fetches actually overlap (barrier server proves >1 in flight)
//   - mark happens only AFTER that contributor's events are durably
//     inserted; an insert failure leaves the contributor UNMARKED
//   - marks are batched (chunked UPDATE), not per-row
//   - circuit trip under concurrency: exactly threshold-1 pre-trip
//     failures are marked; the tripping outcome and everything after
//     it stays unmarked; fetchers stop promptly
//   - 404 rename detection (lookup-by-id + RenameContributorGhLogin +
//     one retry) works from inside the fetcher pool

package collector

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestBreadthFetchesRunConcurrently proves the fetch pool actually
// overlaps requests. The handler blocks every /users/*/events request
// on a barrier that opens only once `want` requests are in flight
// simultaneously — a sequential worker deadlocks here (fails via the
// watchdog timeout), a concurrent one sails through.
func TestBreadthFetchesRunConcurrently(t *testing.T) {
	const want = 4

	var mu sync.Mutex
	inFlight := 0
	barrier := make(chan struct{})
	opened := false

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inFlight++
		if inFlight >= want && !opened {
			opened = true
			close(barrier)
		}
		mu.Unlock()

		select {
		case <-barrier:
		case <-time.After(5 * time.Second):
			// Sequential shape: only 1 request ever in flight — the
			// barrier never opens. Answer anyway so the test fails on
			// the `opened` assertion instead of hanging for minutes.
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "[]")
	})

	store := &fakeBreadthStore{contributors: breadthFixture(want)}
	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(want)

	result, err := worker.Run(context.Background(), want, time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	mu.Lock()
	sawConcurrency := opened
	mu.Unlock()
	if !sawConcurrency {
		t.Fatalf("never observed %d concurrent /users/{login}/events requests — "+
			"the v0.27.8 fetch pool is not actually running fetches concurrently", want)
	}
	if result.ContributorsProcessed != want {
		t.Errorf("processed = %d, want %d", result.ContributorsProcessed, want)
	}
	if len(store.attempted) != want {
		t.Errorf("attempted = %d, want %d — every contributor must still be marked "+
			"(the v0.20.17 unconditional stamp survives the concurrency restructure)",
			len(store.attempted), want)
	}
}

// TestBreadthMarksOnlyAfterEventsInserted pins the v0.27.8 ORDERING
// CONTRACT end to end: for a contributor WITH events, the
// InsertContributorRepoBatch call must be recorded strictly before the
// mark for that contributor; and when the insert FAILS, the
// contributor must never be marked (so a retry happens next cycle).
func TestBreadthMarksOnlyAfterEventsInserted(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") != "1" {
			fmt.Fprint(w, "[]")
			return
		}
		// One event per contributor; unique event id derived from login.
		login := strings.Split(strings.TrimPrefix(r.URL.Path, "/users/"), "/")[0]
		fmt.Fprintf(w, `[{"id": "9%03d", "type": "PushEvent",
			"repo": {"id": 7, "name": "a/b", "url": "https://api.github.com/repos/a/b"},
			"created_at": "2026-01-15T10:30:00Z"}]`, len(login))
	})

	store := &fakeBreadthStore{contributors: breadthFixture(6)}
	// Fail inserts for contributor #2's batches.
	store.insertErrFor = store.contributors[2].ID
	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(3)

	result, err := worker.Run(context.Background(), 6, time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	// (a) Ordering: every marked contributor's insert op precedes its mark op.
	pos := map[string]int{}
	for i, op := range store.ops {
		if _, seen := pos[op]; !seen {
			pos[op] = i
		}
	}
	for _, c := range store.contributors {
		markAt, marked := pos["mark:"+c.ID]
		insertAt, inserted := pos["insert:"+c.ID]
		if marked && inserted && insertAt > markAt {
			t.Errorf("contributor %s was marked (op %d) BEFORE its events were "+
				"inserted (op %d) — violates the v0.27.8 ordering contract: a crash "+
				"between fetch and insert must leave the contributor unmarked",
				c.Login, markAt, insertAt)
		}
	}

	// (b) Insert failure ⇒ unmarked.
	failed := store.contributors[2].ID
	for _, id := range store.attempted {
		if id == failed {
			t.Errorf("contributor with FAILED event insert was marked attempted — "+
				"they must stay unmarked so the cooldown queue retries them "+
				"(events for them are not durable). attempted=%v", store.attempted)
		}
	}
	if len(store.attempted) != 5 {
		t.Errorf("attempted = %d, want 5 (6 contributors, 1 insert failure)", len(store.attempted))
	}
	if result.Errors != 1 {
		t.Errorf("Errors = %d, want 1 (the synthetic insert failure)", result.Errors)
	}
}

// TestBreadthMarksAreBatched pins that mark-attempted goes through the
// chunked batch API — one statement per flush — instead of the
// pre-v0.27.8 shape of one single-row UPDATE per contributor (18,000
// per cycle at the operator's fleet batch size).
func TestBreadthMarksAreBatched(t *testing.T) {
	const n = 40
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "[]")
	})
	store := &fakeBreadthStore{contributors: breadthFixture(n)}
	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(8)

	if _, err := worker.Run(context.Background(), n, time.Hour); err != nil {
		t.Fatalf("Run: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.attempted) != n {
		t.Fatalf("attempted = %d, want %d", len(store.attempted), n)
	}
	// n < breadthMarkFlushSize, so the whole cycle must flush as ONE batch.
	if store.markBatchCalls != 1 {
		t.Errorf("markBatchCalls = %d, want 1 — %d contributors under the flush "+
			"size (%d) must be stamped by a single batched UPDATE, not per-row calls",
			store.markBatchCalls, n, breadthMarkFlushSize)
	}
}

// TestBreadthCircuitTripUnderConcurrency drives a full transient storm
// through the REAL concurrent Run path (failures synthesized at the
// GetNewestContributorRepoEvent seam so no HTTP retry backoff is paid)
// and pins the preserved v0.22.12 trip semantics:
//   - the coordinator counts outcomes serially, so EXACTLY
//     threshold-1 pre-trip failures get marked attempted;
//   - the tripping contributor and everything after stays UNMARKED
//     (they must re-enter the queue once GitHub recovers);
//   - the breaker deadline is set and the next Run short-circuits.
func TestBreadthCircuitTripUnderConcurrency(t *testing.T) {
	const n = breadthCircuitBreakerThreshold * 3
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "[]")
	})
	store := &fakeBreadthStore{
		contributors: breadthFixture(n),
		getNewestErr: fmt.Errorf("synthetic storm: %w", platform.ErrTransient),
	}
	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(8)

	result, err := worker.Run(context.Background(), n, time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !result.CircuitBreakerTripped {
		t.Fatal("a sustained transient storm must trip the circuit breaker")
	}

	store.mu.Lock()
	marked := len(store.attempted)
	store.mu.Unlock()
	// Sequential semantics preserved: failures 1..threshold-1 are marked
	// (per-user errors count as attempted); the tripping outcome and all
	// post-trip outcomes are NOT. The coordinator serializes outcome
	// processing, so this is exact, not approximate.
	if marked != breadthCircuitBreakerThreshold-1 {
		t.Errorf("marked = %d, want exactly %d — pre-trip failures are marked, the "+
			"tripping contributor and everything after must stay unmarked so they "+
			"re-enter the queue once GitHub recovers", marked, breadthCircuitBreakerThreshold-1)
	}

	// The pause must hold: a second Run short-circuits before the store.
	store.mu.Lock()
	store.queried = false
	store.mu.Unlock()
	again, err := worker.Run(context.Background(), n, time.Hour)
	if err != nil {
		t.Fatalf("Run while open: %v", err)
	}
	if !again.CircuitBreakerTripped {
		t.Error("Run during the open window must report CircuitBreakerTripped")
	}
	store.mu.Lock()
	requeried := store.queried
	store.mu.Unlock()
	if requeried {
		t.Error("Run during the open window must not query the store")
	}
}

// TestBreadthRenameDetectionUnderConcurrency drives the v0.22.12
// 404-rename flow through the concurrent pool: /users/{old}/events
// 404s, /user/{id} resolves the new login, the rename persists via
// RenameContributorGhLogin, and the retry fetch lands events that the
// coordinator inserts before marking.
func TestBreadthRenameDetectionUnderConcurrency(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/users/olduser/"):
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"message": "Not Found"}`)
		case r.URL.Path == "/user/777":
			fmt.Fprint(w, `{"login": "newuser"}`)
		case strings.HasPrefix(r.URL.Path, "/users/newuser/"):
			if r.URL.Query().Get("page") == "1" {
				fmt.Fprint(w, `[{"id": "4242", "type": "PushEvent",
					"repo": {"id": 7, "name": "a/b", "url": "https://api.github.com/repos/a/b"},
					"created_at": "2026-01-15T10:30:00Z"}]`)
			} else {
				fmt.Fprint(w, "[]")
			}
		default:
			// Filler contributors: empty event streams.
			fmt.Fprint(w, "[]")
		}
	})

	contribs := breadthFixture(5)
	contribs[2].Login = "olduser"
	contribs[2].GHUserID = 777
	store := &fakeBreadthStore{contributors: contribs}
	worker := newBreadthTestWorker(t, store, handler).WithFetchConcurrency(4)

	result, err := worker.Run(context.Background(), len(contribs), time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.renames) != 1 || !strings.HasSuffix(store.renames[0], "→newuser") {
		t.Errorf("renames = %v, want exactly one rename to newuser via "+
			"RenameContributorGhLogin (the v0.20.2 merge machinery)", store.renames)
	}
	if result.Renames != 1 {
		t.Errorf("result.Renames = %d, want 1", result.Renames)
	}
	if store.inserted != 1 {
		t.Errorf("inserted events = %d, want 1 (the post-rename retry's event)", store.inserted)
	}
	if len(store.attempted) != len(contribs) {
		t.Errorf("attempted = %d, want %d — the renamed contributor must still be "+
			"marked after their retried fetch", len(store.attempted), len(contribs))
	}
	if result.ContributorsProcessed != len(contribs) {
		t.Errorf("processed = %d, want %d", result.ContributorsProcessed, len(contribs))
	}
}

// TestBreadthThroughputLogFields pins that the cycle-complete log line
// carries the v0.27.8 operator-visibility fields — contributors/sec and
// the effective fetch concurrency — so config changes are measurable
// across cycles. (Source pin: the slog keys must be present at the
// "contributor breadth complete" call site.)
func TestBreadthThroughputLogFields(t *testing.T) {
	body := breadthRunBody(t)
	idx := strings.Index(body, `"contributor breadth complete"`)
	if idx < 0 {
		t.Fatal("Run must keep the \"contributor breadth complete\" cycle log line")
	}
	// Window rather than paren-matching: the call's arguments contain
	// nested parens (elapsed.Round(...)) so "first )" would truncate.
	logCall := body[idx:min(idx+600, len(body))]
	for _, key := range []string{`"contributors_per_sec"`, `"fetch_concurrency"`} {
		if !strings.Contains(logCall, key) {
			t.Errorf("the cycle-complete log must carry the %s slog key (v0.27.8 "+
				"throughput visibility — the operator measures the improvement "+
				"across breadth_fetch_concurrency changes with it)", key)
		}
	}
}
