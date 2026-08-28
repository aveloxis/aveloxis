// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// v0.24.0 — DistributionWorker tests.
//
// The worker is a stateless dispatcher that pulls jobs from
// db.PostgresStore.ClaimNextDistributionRepo at a paced cadence
// (minimum-gap, v0.21.3 design), hands each job to a runner
// goroutine, and routes the outcome to MarkComplete or
// RecordFailure. The actual fetching is delegated to an injected
// Scanner so tests can substitute a fake.

// fakeStore implements the small subset of PostgresStore that
// Worker needs. Tracks claim/mark/record call counts.
type fakeStore struct {
	mu             sync.Mutex
	queue          []*db.DistributionJob
	marks          []markCall
	failures       []int64
	claimErr       error
	releasedJobIDs []int64
}

type markCall struct {
	repoID        int64
	distributions []model.PackageDistribution
	manifests     []model.DistributionManifest
	scanComplete  bool
}

// ClaimNextDistributionRepo satisfies the v0.25.3 Store interface
// signature. The fake ignores the immediatePartialReclaim flag —
// behavioral verification of the flag's SQL effect lives in
// internal/db/v025_3_distribution_repair_test.go's integration
// test against a live Postgres.
func (f *fakeStore) ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration, immediatePartialReclaim bool) (*db.DistributionJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	if len(f.queue) == 0 {
		return nil, nil
	}
	job := f.queue[0]
	f.queue = f.queue[1:]
	return job, nil
}

func (f *fakeStore) MarkDistributionComplete(ctx context.Context, job *db.DistributionJob,
	distributions []model.PackageDistribution, manifests []model.DistributionManifest, scanComplete bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, markCall{job.RepoID, distributions, manifests, scanComplete})
	f.releasedJobIDs = append(f.releasedJobIDs, job.RepoID)
	return nil
}

func (f *fakeStore) RecordDistributionFailure(ctx context.Context, job *db.DistributionJob) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, job.RepoID)
	f.releasedJobIDs = append(f.releasedJobIDs, job.RepoID)
	return nil
}

func (f *fakeStore) snapshot() (marks []markCall, failures []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]markCall(nil), f.marks...), append([]int64(nil), f.failures...)
}

// fakeScanner returns canned data and/or canned errors.
type fakeScanner struct {
	mu        sync.Mutex
	calls     int32
	results   map[int64]scanResult
	scanErr   error
	scanWait  time.Duration
	unhealthy atomic.Bool // v0.25.0: when true, Healthy() returns false
}

type scanResult struct {
	distributions []model.PackageDistribution
	manifests     []model.DistributionManifest
	// partial=true → Scan returns complete=false (the v0.25.0
	// partial-scan path). Default false preserves pre-v0.25.0
	// "scan returns complete=true" semantics for legacy tests.
	partial bool
}

func (f *fakeScanner) Scan(ctx context.Context, repoID int64, owner, repo, repoGit string) ([]model.PackageDistribution, []model.DistributionManifest, bool, error) {
	atomic.AddInt32(&f.calls, 1)
	if f.scanWait > 0 {
		select {
		case <-time.After(f.scanWait):
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		}
	}
	if f.scanErr != nil {
		return nil, nil, false, f.scanErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.results[repoID]
	if !ok {
		// No canned result → empty-but-complete success.
		return nil, nil, true, nil
	}
	// scanResult.complete defaults to false (zero value of bool).
	// Pre-v0.25.0 tests didn't set this field; flip the default so
	// they continue to assert "complete success" without churn.
	// Tests that want to exercise partial-scan behavior set
	// complete:false explicitly when constructing scanResult.
	return r.distributions, r.manifests, !r.partial, nil
}

// Healthy implements distribution.Scanner.
func (f *fakeScanner) Healthy() bool {
	return !f.unhealthy.Load()
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ---- source-contract tests --------------------------------------

func TestWorkerSourceFileExists(t *testing.T) {
	if _, err := os.Stat("worker.go"); err != nil {
		t.Fatalf("internal/collector/distribution/worker.go must exist (Phase F deliverable): %v", err)
	}
}

func TestWorkerUsesMinimumGapPacing(t *testing.T) {
	// v0.21.3 fix: dispatcher must NOT use time.NewTicker as the
	// primary throughput gate (that throttles fleet-wide throughput
	// even when workers are idle). It must use a minimum-gap
	// deadline variable stamped AFTER each successful claim.
	data, err := os.ReadFile("worker.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Required: a "next start allowed" deadline-style variable.
	deadlineNames := []string{"nextStartAllowed", "nextStart", "startGate"}
	hasDeadline := false
	for _, name := range deadlineNames {
		if strings.Contains(src, name) {
			hasDeadline = true
			break
		}
	}
	if !hasDeadline {
		t.Errorf("worker.go must declare a deadline variable for minimum-gap pacing (one of %v) — v0.21.3 lesson", deadlineNames)
	}

	// Negative pin: the dispatcher must NOT use time.NewTicker as
	// its primary throughput gate. (A ticker for a periodic
	// status-log heartbeat is fine, so this isn't a fatal global
	// ban — but the pre-v0.21.3 throttle pattern is.)
	if strings.Contains(src, "time.NewTicker(w.startInterval") {
		t.Error("worker.go must not gate primary throughput on a startInterval ticker — that's the v0.21.3 regression (caps fleet rate regardless of worker availability). Use a per-start deadline variable instead.")
	}
}

// ---- behavioral tests -------------------------------------------

func TestWorkerProcessesQueuedJobMarksComplete(t *testing.T) {
	store := &fakeStore{
		queue: []*db.DistributionJob{
			{RepoID: 42, RepoOwner: "x", RepoName: "y", RepoGit: "https://github.com/x/y"},
		},
	}
	scanner := &fakeScanner{
		results: map[int64]scanResult{
			42: {
				distributions: []model.PackageDistribution{{Ecosystem: "npm", PackageName: "y", Source: "deps.dev"}},
				manifests:     []model.DistributionManifest{{ManifestPath: "package.json", ManifestType: "npm"}},
			},
		},
	}
	w := NewWorker(WorkerOptions{
		Store:         store,
		Scanner:       scanner,
		Workers:       1,
		StartInterval: 0, // no pacing in tests
		Cadence:       180 * 24 * time.Hour,
		Logger:        testLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Run(ctx)

	marks, failures := store.snapshot()
	if len(failures) != 0 {
		t.Errorf("got %d failures, want 0", len(failures))
	}
	if len(marks) != 1 {
		t.Fatalf("got %d marks, want 1", len(marks))
	}
	if marks[0].repoID != 42 {
		t.Errorf("marked repo = %d, want 42", marks[0].repoID)
	}
	if len(marks[0].distributions) != 1 || marks[0].distributions[0].Ecosystem != "npm" {
		t.Errorf("distributions = %+v", marks[0].distributions)
	}
	if len(marks[0].manifests) != 1 {
		t.Errorf("manifests = %+v", marks[0].manifests)
	}
}

func TestWorkerRecordsFailureOnScannerError(t *testing.T) {
	store := &fakeStore{
		queue: []*db.DistributionJob{
			{RepoID: 99, RepoOwner: "x", RepoName: "y"},
		},
	}
	scanner := &fakeScanner{scanErr: errors.New("network exploded")}

	w := NewWorker(WorkerOptions{
		Store:   store,
		Scanner: scanner,
		Workers: 1,
		Logger:  testLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w.Run(ctx)

	marks, failures := store.snapshot()
	if len(marks) != 0 {
		t.Errorf("got %d marks, want 0", len(marks))
	}
	if len(failures) != 1 || failures[0] != 99 {
		t.Errorf("failures = %v, want [99]", failures)
	}
}

func TestWorkerShutsDownOnCtxCancel(t *testing.T) {
	// Long-running queue + slow scanner. Cancel ctx; worker must
	// return promptly without processing every queued job.
	queue := make([]*db.DistributionJob, 50)
	for i := range queue {
		queue[i] = &db.DistributionJob{RepoID: int64(i + 1)}
	}
	store := &fakeStore{queue: queue}
	scanner := &fakeScanner{scanWait: 100 * time.Millisecond}

	w := NewWorker(WorkerOptions{
		Store:   store,
		Scanner: scanner,
		Workers: 1,
		Logger:  testLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not shut down within 2s of ctx cancel")
	}

	// At least some jobs should have been picked up but NOT all 50.
	marks, failures := store.snapshot()
	processed := len(marks) + len(failures)
	if processed >= len(queue) {
		t.Errorf("worker processed %d of %d jobs — ctx cancel should have interrupted before draining", processed, len(queue))
	}
}

func TestWorkerHandlesEmptyQueueGracefully(t *testing.T) {
	store := &fakeStore{} // empty queue
	scanner := &fakeScanner{}

	w := NewWorker(WorkerOptions{
		Store:   store,
		Scanner: scanner,
		Workers: 1,
		Logger:  testLogger(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	// Should not panic, should not spin tightly. Returns when ctx expires.
	w.Run(ctx)

	if atomic.LoadInt32(&scanner.calls) != 0 {
		t.Errorf("scanner called %d times on empty queue, want 0", scanner.calls)
	}
}

func (f *fakeStore) ReleaseDistributionClaim(context.Context, *db.DistributionJob) error { return nil }
