// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/depsdev"
	"github.com/aveloxis/aveloxis/internal/platform/ecosystems"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// v0.25.0 — scan completeness + dispatcher pause tests.
//
// These tests cover the contract pieces that protect the 10-repo
// cohort that trips the ecosyste.ms circuit breaker:
//   - Scan returns complete=false when an external source had a
//     transient error or was skipped due to an open breaker.
//   - Worker passes complete to MarkDistributionComplete.
//   - Dispatcher pauses when scanner.Healthy() returns false.
//
// The combination ensures that during a 1-hour ecosyste.ms outage
// (a) the breaker-tripping cohort still scans but gets marked
// incomplete so it re-collects on the next dispatch cycle, and
// (b) no further repos get dispatched during the pause — so they
// don't get permanent "complete scan" stamps with missing data.

// ---- source-contract tests -------------------------------------------

func TestScannerInterfaceRequiresHealthy(t *testing.T) {
	// Source-contract pin: the Scanner interface in worker.go must
	// declare a Healthy() method so the dispatcher can pause when
	// an external source is unavailable.
	body := readFile(t, "worker.go")
	if !strings.Contains(body, "Healthy() bool") {
		t.Fatal("Scanner interface must declare Healthy() bool — dispatcher pause depends on it")
	}
}

func TestScannerInterfaceScanReturnsComplete(t *testing.T) {
	// Pin the 4-return signature of Scan. The third return (before
	// the error) is the complete bool.
	body := readFile(t, "worker.go")
	if !strings.Contains(body, "complete bool") {
		t.Error("Scanner.Scan must return a complete bool (third return value) so partial scans can be distinguished from full scans for the distribution_scan_complete column")
	}
}

func TestCompositeScannerImplementsHealthy(t *testing.T) {
	body := readFile(t, "scanner.go")
	// Function definition (not just the doc).
	if !strings.Contains(body, "func (s *CompositeScanner) Healthy() bool") {
		t.Error("CompositeScanner must implement Healthy() bool")
	}
	// Must check ecosyste.ms breaker state.
	if !strings.Contains(body, "IsCircuitOpen()") {
		t.Error("CompositeScanner.Healthy() must consult ecosystems.IsCircuitOpen()")
	}
}

func TestScannerTracksIncompleteOnTransientExternalError(t *testing.T) {
	body := readFile(t, "scanner.go")
	// Pin the tracker variable.
	if !strings.Contains(body, "scanIncomplete") {
		t.Error("Scan must declare a scanIncomplete tracker so transient external errors mark the scan partial")
	}
	// Pin that the tracker gets set on ClassTransient/ClassRateLimit.
	if !strings.Contains(body, "ClassTransient") || !strings.Contains(body, "ClassRateLimit") {
		t.Error("scanIncomplete must be set when external source returns ClassTransient or ClassRateLimit")
	}
}

func TestScannerHandlesErrCircuitOpenAsIncompleteSkip(t *testing.T) {
	body := readFile(t, "scanner.go")
	// Pin: ErrCircuitOpen is identified specifically and sets
	// scanIncomplete WITHOUT incrementing erroredSources.
	if !strings.Contains(body, "ecosystems.ErrCircuitOpen") {
		t.Error("Scan must check errors.Is(err, ecosystems.ErrCircuitOpen) so circuit-open skips mark the scan incomplete WITHOUT incrementing erroredSources (otherwise the failure-counter path would fire on every repo during an outage)")
	}
}

func TestWorkerDispatcherPausesWhenUnhealthy(t *testing.T) {
	body := readFile(t, "worker.go")
	// Pin: the dispatcher calls scanner.Healthy().
	if !strings.Contains(body, "scanner.Healthy()") && !strings.Contains(body, "w.scanner.Healthy()") {
		t.Error("dispatcher must call w.scanner.Healthy() before each claim — without this, repos dispatched during an ecosyste.ms outage get permanent 'complete scan' stamps with missing data")
	}
	// Pin: there's an unhealthy-pause log message.
	if !strings.Contains(body, "scanner unhealthy") {
		t.Error("dispatcher must log when entering the unhealthy-pause state — operator visibility")
	}
}

func TestWorkerProcessJobPassesScanCompleteToStore(t *testing.T) {
	body := readFile(t, "worker.go")
	// Pin: scanComplete is captured from Scan and passed through.
	if !strings.Contains(body, "scanComplete") {
		t.Error("processJob must capture scanComplete from Scan's return and pass it to MarkDistributionComplete")
	}
	if !strings.Contains(body, "MarkDistributionComplete(completionCtx, job, distributions, manifests, scanComplete)") {
		t.Error("MarkDistributionComplete call must thread scanComplete through")
	}
}

// ---- behavioral tests ------------------------------------------------

// TestCompositeScannerReturnsIncompleteOnEcosystemsTransient pins
// the headline v0.25.0 partial-scan behavior. When ecosyste.ms 500s
// (the dominant chaoss.tv failure mode), the scanner returns
// complete=false so the store stamps distribution_scan_complete=
// FALSE and the row re-claims on the next dispatch cycle.
func TestCompositeScannerReturnsIncompleteOnEcosystemsTransient(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true)

	_, _, complete, err := scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")
	if err != nil {
		t.Fatalf("scan must NOT fail when only ecosyste.ms 500s and other sources are clean — got err=%v", err)
	}
	if complete {
		t.Error("scan with ecosyste.ms 500 must return complete=false so the row gets re-collected when ecosyste.ms recovers — without this, the partial-scan rows would be stamped with the cadence gate and locked out for 180 days")
	}
}

// TestCompositeScannerReturnsCompleteOnCleanScan pins the success
// path: every external source completed cleanly → complete=true,
// row gets normal cadence treatment.
func TestCompositeScannerReturnsCompleteOnCleanScan(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)

	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ecoServer.Close)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, ghServer.URL, true)

	_, _, complete, err := scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")
	if err != nil {
		t.Fatalf("scan must not fail on clean responses: %v", err)
	}
	if !complete {
		t.Error("scan with every source returning clean empty must return complete=true — operator wants the cadence gate to apply normally for genuinely-no-evidence repos")
	}
}

// TestCompositeScannerReturnsIncompleteOnEcosystemsCircuitOpen pins
// the breaker-open path: when ecosyste.ms's circuit is already open
// (LookupPackages short-circuits with ErrCircuitOpen), the scan
// gets marked incomplete WITHOUT erroredSources incrementing.
func TestCompositeScannerReturnsIncompleteOnEcosystemsCircuitOpen(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)

	// Build an ecosystems client pointed at a local always-500 server
	// and trip its breaker with real HTTP failures — deterministic on
	// any network (the previous "http://does-not-matter" approach
	// relied on DNS lookup failure, which NXDOMAIN-hijacking resolvers
	// on public Wi-Fi break: the name resolves, the calls don't
	// classify as transient, the breaker never opens).
	ecoClient := trippedEcosystemsClient(t)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ghServer.Close)

	logger := slog.Default()
	depsClient := depsdev.New(depsdev.Options{BaseURL: depsServer.URL})
	keys := platform.NewKeyPool([]string{"test"}, logger)
	ghClient := github.New(ghServer.URL, keys, logger)

	scanner := &CompositeScanner{
		DepsDev:           depsClient,
		Ecosystems:        ecoClient,
		GitHub:            ghClient,
		Logger:            logger,
		CrossCheckSources: true,
	}

	_, _, complete, err := scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")
	if err != nil {
		t.Fatalf("circuit-open path must not produce an error: %v", err)
	}
	if complete {
		t.Error("scan with ecosyste.ms circuit open must return complete=false — the source was not consulted; row must re-collect when breaker closes")
	}
	if !scanner.Healthy() {
		// Healthy() should also report false while the breaker is open.
		// (Asserted separately, but confirm here for completeness.)
	}
	if scanner.Healthy() {
		t.Error("Healthy() must return false while ecosyste.ms breaker is open")
	}
}

// trippedEcosystemsClient returns an ecosystems client whose circuit
// breaker is OPEN, tripped by real HTTP 500 responses from a local
// httptest server. Deterministic on any network — never depends on
// DNS behavior (public-Wi-Fi resolvers that hijack NXDOMAIN broke the
// earlier does-not-resolve approach on 2026-07-10).
func trippedEcosystemsClient(t *testing.T) *ecosystems.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	c := ecosystems.New(ecosystems.Options{BaseURL: server.URL})
	for i := 0; i < ecosystems.CircuitBreakerThreshold; i++ {
		_, _ = c.LookupPackages(context.Background(), "https://github.com/x/y")
	}
	if !c.IsCircuitOpen() {
		t.Fatal("expected breaker to be open after threshold 500s; check ecosystems CircuitBreakerThreshold + noteTransientFailure")
	}
	return c
}

// TestWorkerDispatcherPausesAndResumes confirms the dispatcher
// actually halts when scanner.Healthy()=false and resumes when it
// flips back to true. Uses the fakeScanner.unhealthy atomic.
func TestWorkerDispatcherPausesAndResumes(t *testing.T) {
	scanner := &fakeScanner{
		results: map[int64]scanResult{},
	}
	// Mark unhealthy before any claim fires.
	scanner.unhealthy.Store(true)

	// Three jobs in the queue.
	store := &fakeStore{
		queue: []*db.DistributionJob{
			{RepoID: 1, RepoOwner: "x", RepoName: "y", RepoGit: "https://github.com/x/y"},
			{RepoID: 2, RepoOwner: "x", RepoName: "z", RepoGit: "https://github.com/x/z"},
			{RepoID: 3, RepoOwner: "x", RepoName: "w", RepoGit: "https://github.com/x/w"},
		},
	}

	worker := NewWorker(WorkerOptions{
		Store:         store,
		Scanner:       scanner,
		Workers:       1,
		StartInterval: 0,
		Cadence:       180 * 24 * time.Hour,
		Logger:        testLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(doneCh)
	}()

	// Sleep briefly to confirm no claims fire while unhealthy. The
	// dispatcher's healthCheckInterval is 60s; in test-time this
	// just means it sleeps in its first health-check branch.
	time.Sleep(200 * time.Millisecond)
	marks, _ := store.snapshot()
	if len(marks) != 0 {
		t.Errorf("dispatcher must NOT process any jobs while scanner is unhealthy — got %d marks", len(marks))
	}

	// Flip healthy. Dispatcher should pick up after the next
	// healthCheckInterval. To avoid waiting the full 60s in the
	// test, we patch worker.healthCheckInterval... but we can't,
	// it's a package constant. So we use the worker_test.go
	// pattern of canceling ctx and starting fresh.
	cancel()
	<-doneCh

	// Restart with scanner healthy and verify jobs flow.
	scanner.unhealthy.Store(false)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	doneCh2 := make(chan struct{})
	go func() {
		worker.Run(ctx2)
		close(doneCh2)
	}()
	// Give it time to process all three.
	time.Sleep(500 * time.Millisecond)
	cancel2()
	<-doneCh2

	marks, _ = store.snapshot()
	if len(marks) != 3 {
		t.Errorf("expected 3 marks once scanner became healthy, got %d", len(marks))
	}
}

// TestWorkerProcessJobPassesIncompleteScanToStore confirms that a
// scanner returning complete=false routes through to the store as
// scanComplete=false on MarkDistributionComplete.
func TestWorkerProcessJobPassesIncompleteScanToStore(t *testing.T) {
	scanner := &fakeScanner{
		results: map[int64]scanResult{
			42: {partial: true}, // → Scan returns complete=false
		},
	}
	store := &fakeStore{
		queue: []*db.DistributionJob{
			{RepoID: 42, RepoOwner: "x", RepoName: "y", RepoGit: "https://github.com/x/y"},
		},
	}
	worker := NewWorker(WorkerOptions{
		Store:   store,
		Scanner: scanner,
		Workers: 1,
		Logger:  testLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	doneCh := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(doneCh)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-doneCh

	marks, _ := store.snapshot()
	if len(marks) != 1 {
		t.Fatalf("expected 1 mark, got %d", len(marks))
	}
	if marks[0].scanComplete {
		t.Errorf("partial scan must propagate as scanComplete=false to MarkDistributionComplete — without this the distribution_scan_complete column stays TRUE and the row gets locked out for 180 days")
	}
}

// silence unused import warnings if any util doesn't fire in this file
var _ = errors.New
var _ = fmt.Sprintf
var _ = atomic.AddInt32
var _ = os.Stat
