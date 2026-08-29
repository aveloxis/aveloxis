// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// BEHAVIORAL tests for the breadth circuit breaker (v0.25.38, tech-debt
// Action 2). The breaker was built in v0.22.12 in response to a real
// production incident (1,429 WARNs during a GitHub 500-storm) but until
// now was covered only by source-contract substring pins — an
// off-by-one on the threshold or an inverted deadline comparison would
// have shipped green. These tests actually trip it.

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// fakeBreadthStore satisfies breadthStore without a database.
//
// v0.27.8: implements the batch-shaped interface (MarkBreadthAttemptedBatch,
// InsertContributorRepoBatch). `attempted` accumulates the individual
// cntrb_ids across batch calls so assertions stay per-contributor;
// `markBatchCalls` counts the batch statements so tests can pin that
// marking is actually batched (one UPDATE per chunk, not per row).
// `ops` records the interleaving of inserts and marks for the
// mark-only-after-durable-insert ordering pin.
type fakeBreadthStore struct {
	mu             sync.Mutex
	contributors   []db.BreadthContributor
	attempted      []string
	inserted       int
	insertedRows   []*db.ContributorRepoRow
	markBatchCalls int
	queried        bool
	renames        []string // "cntrbID→newLogin"
	ops            []string // "insert:<cntrbID>" / "mark:<cntrbID>" in call order

	// getNewestErr, when set, is returned by GetNewestContributorRepoEvent —
	// a fast way to synthesize per-contributor fetch failures (a real
	// HTTP 5xx exhausts the client's full retry backoff per call).
	getNewestErr error
	// insertErrFor makes InsertContributorRepoBatch fail for batches
	// containing this cntrb_id (ordering-contract tests).
	insertErrFor string
	// insertCanceledFor makes InsertContributorRepoBatch return
	// context.Canceled for batches containing this cntrb_id — the
	// shutdown-mid-insert shape (Copilot round 8).
	insertCanceledFor string
}

func (f *fakeBreadthStore) GetContributorsForBreadth(ctx context.Context, limit int, cooldown time.Duration) ([]db.BreadthContributor, error) {
	f.mu.Lock()
	f.queried = true
	f.mu.Unlock()
	if limit > len(f.contributors) {
		limit = len(f.contributors)
	}
	return f.contributors[:limit], nil
}

func (f *fakeBreadthStore) MarkBreadthAttemptedBatch(ctx context.Context, cntrbIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markBatchCalls++
	f.attempted = append(f.attempted, cntrbIDs...)
	for _, id := range cntrbIDs {
		f.ops = append(f.ops, "mark:"+id)
	}
	return nil
}

func (f *fakeBreadthStore) RenameContributorGhLogin(ctx context.Context, cntrbID, newLogin string, ghUserID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renames = append(f.renames, cntrbID+"→"+newLogin)
	return nil
}

func (f *fakeBreadthStore) GetNewestContributorRepoEvent(ctx context.Context, cntrbID string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getNewestErr != nil {
		return time.Time{}, f.getNewestErr
	}
	return time.Time{}, nil
}

func (f *fakeBreadthStore) InsertContributorRepoBatch(ctx context.Context, rows []*db.ContributorRepoRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErrFor != "" {
		for _, r := range rows {
			if r.CntrbID == f.insertErrFor {
				return fmt.Errorf("synthetic insert failure for %s", f.insertErrFor)
			}
		}
	}
	if f.insertCanceledFor != "" {
		for _, r := range rows {
			if r.CntrbID == f.insertCanceledFor {
				return context.Canceled
			}
		}
	}
	f.inserted += len(rows)
	f.insertedRows = append(f.insertedRows, rows...)
	for _, r := range rows {
		f.ops = append(f.ops, "insert:"+r.CntrbID)
	}
	return nil
}

func breadthFixture(n int) []db.BreadthContributor {
	out := make([]db.BreadthContributor, n)
	for i := range out {
		out[i] = db.BreadthContributor{
			ID:       fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
			Login:    fmt.Sprintf("bhv-user-%d", i),
			GHUserID: 0, // no rename-detection fallback — a plain 500 path
		}
	}
	return out
}

func newBreadthTestWorker(t *testing.T, store breadthStore, handler http.Handler) *BreadthWorker {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	httpClient := platform.NewHTTPClient(server.URL, platform.NewKeyPool([]string{"t"}, logger), logger, platform.AuthGitHub)
	return NewBreadthWorkerWithHTTP(store, httpClient, logger)
}

// A sustained transient storm must trip the breaker at EXACTLY the
// threshold — and the next Run must return immediately without touching
// the store or the API. Driven through the noteContributorOutcome seam
// because a real 5xx exhausts the HTTP client's full retry backoff per
// call (hours of test wall-clock for a 20-failure storm).
func TestBreadthCircuitBreakerTripsAtThreshold(t *testing.T) {
	store := &fakeBreadthStore{}
	worker := newBreadthTestWorker(t, store, http.NotFoundHandler())

	transient := fmt.Errorf("events fetch: %w", platform.ErrTransient)
	for i := 1; i < breadthCircuitBreakerThreshold; i++ {
		if worker.noteContributorOutcome(transient, "u") {
			t.Fatalf("breaker tripped early at %d consecutive failures (threshold %d)",
				i, breadthCircuitBreakerThreshold)
		}
	}
	if !worker.noteContributorOutcome(transient, "u") {
		t.Fatalf("breaker must trip at exactly %d consecutive failures", breadthCircuitBreakerThreshold)
	}
	worker.mu.Lock()
	openUntil := worker.circuitOpenUntil
	worker.mu.Unlock()
	if !openUntil.After(time.Now()) {
		t.Fatal("circuitOpenUntil must be set in the future after a trip")
	}

	// While open, Run must short-circuit before the store is consulted.
	result, err := worker.Run(context.Background(), 10, time.Hour)
	if err != nil {
		t.Fatalf("Run while open: %v", err)
	}
	if !result.CircuitBreakerTripped {
		t.Error("Run during the open window must report CircuitBreakerTripped")
	}
	if store.queried {
		t.Error("Run during the open window must not query the store")
	}
}

// A success anywhere in the storm resets the counter — isolated
// transient failures can never accumulate into a trip across a healthy
// run.
func TestBreadthCounterResetsOnSuccess(t *testing.T) {
	store := &fakeBreadthStore{}
	worker := newBreadthTestWorker(t, store, http.NotFoundHandler())

	transient := fmt.Errorf("events fetch: %w", platform.ErrTransient)
	for range breadthCircuitBreakerThreshold - 1 {
		worker.noteContributorOutcome(transient, "u")
	}
	worker.noteContributorOutcome(nil, "u") // success resets
	worker.mu.Lock()
	consecutive := worker.consecutive5xx
	worker.mu.Unlock()
	if consecutive != 0 {
		t.Fatalf("success must reset the counter, got %d", consecutive)
	}
	for i := range breadthCircuitBreakerThreshold - 1 {
		if worker.noteContributorOutcome(transient, "u") {
			t.Fatalf("tripped at %d after reset — counter did not actually reset", i+1)
		}
	}
	worker.mu.Lock()
	stillClosed := worker.circuitOpenUntil.IsZero()
	worker.mu.Unlock()
	if !stillClosed {
		t.Fatal("breaker must still be closed")
	}
}

// Healthy responses must never trip the breaker, every contributor must
// be marked attempted (success or empty — the v0.20.17 unconditional
// stamp), and the counter must reset on success so isolated failures
// can't accumulate across a healthy run.
func TestBreadthHealthyRunDoesNotTrip(t *testing.T) {
	store := &fakeBreadthStore{contributors: breadthFixture(30)}
	worker := newBreadthTestWorker(t, store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, "[]") // valid, empty event stream
	}))

	result, err := worker.Run(context.Background(), 30, time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.CircuitBreakerTripped {
		t.Fatal("healthy run must not trip the breaker")
	}
	if len(store.attempted) != 30 {
		t.Errorf("every contributor must be marked attempted (got %d of 30) — the "+
			"unconditional stamp is what drains the cooldown queue", len(store.attempted))
	}
}

// Per-contributor 404s are legitimate per-user conditions, not an
// incident signal: they must not advance the breaker counter.
func TestBreadth404sDoNotTripBreaker(t *testing.T) {
	store := &fakeBreadthStore{contributors: breadthFixture(breadthCircuitBreakerThreshold + 5)}
	worker := newBreadthTestWorker(t, store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	result, err := worker.Run(context.Background(), breadthCircuitBreakerThreshold+5, time.Hour)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.CircuitBreakerTripped {
		t.Fatal("404s are per-contributor conditions and must NOT trip the breaker")
	}
}
