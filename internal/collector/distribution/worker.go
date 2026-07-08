// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"context"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// v0.24.0 — DistributionWorker pool.
//
// Mirrors the v0.21.0/v0.21.3 ScancodeWorker design with these
// differences:
//
//   - No subprocesses → no (pid, boot_id) lock state, no orphan
//     recovery. The claim transaction held open from
//     ClaimNextDistributionRepo through Mark/Record is the
//     atomic recovery primitive.
//   - HTTP-only work → much shorter per-job duration (seconds
//     instead of minutes-to-hours), so the default cadence is 6
//     months instead of subprocess-tuned numbers.
//   - Dispatcher uses v0.21.3 minimum-gap pacing (deadline-
//     based) rather than the pre-v0.21.3 ticker throttle, so
//     when all workers are idle they pick up new work as fast as
//     the StartInterval allows.

// Store is the slice of PostgresStore the worker depends on.
// Interface (not a *db.PostgresStore directly) so tests can
// substitute a fake without standing up postgres.
type Store interface {
	// v0.25.3: immediatePartialReclaim flag controls whether
	// distribution_scan_complete = FALSE rows bypass the cadence
	// gate. Plumbed through from
	// CollectionConfig.DistributionTrackingImmediatePartialReclaim
	// (default true preserves v0.25.0 behavior).
	ClaimNextDistributionRepo(ctx context.Context, cadence time.Duration, immediatePartialReclaim bool) (*db.DistributionJob, error)
	MarkDistributionComplete(ctx context.Context, job *db.DistributionJob,
		distributions []model.PackageDistribution, manifests []model.DistributionManifest, scanComplete bool) error
	RecordDistributionFailure(ctx context.Context, job *db.DistributionJob) error
}

// Scanner produces the distribution evidence for one claimed repo.
// In production this is a composition of depsdev.Client +
// ecosystems.Client + the three github.Client distribution methods
// + the manifest content fetch + Phase D parser. Tests substitute
// a fake.
//
// Conservative contract: a successful Scan returns whatever
// evidence was found (which may be empty). An error means the
// scan as a whole failed and the row should record a failure;
// individual fetcher failures inside the scan are aggregated
// best-effort by the implementation, not bubbled here. The worker
// does not interpret the error beyond "success vs. failure".
//
// v0.25.0: the third return value `complete bool` indicates
// whether all enabled external sources were consulted successfully.
// FALSE means at least one external source had a transient error
// (or was skipped due to an open circuit breaker) so the scan
// data is partial — the store records this in
// distribution_scan_complete so the claim query treats the row as
// immediately re-eligible (bypassing the cadence gate).
//
// v0.25.0: Healthy() returns false when a critical external source
// is unavailable (currently: ecosyste.ms circuit breaker is open).
// The Worker dispatcher checks this before each claim and pauses
// when unhealthy, so we don't dispatch new repos into a known-bad
// upstream and end up stamping their cadence with partial scans.
type Scanner interface {
	Scan(ctx context.Context, repoID int64, owner, repo, repoGit string) (distributions []model.PackageDistribution, manifests []model.DistributionManifest, complete bool, err error)
	Healthy() bool
}

// WorkerOptions configures NewWorker.
type WorkerOptions struct {
	Store         Store
	Scanner       Scanner
	Workers       int           // number of runner goroutines (default 1)
	StartInterval time.Duration // minimum gap between successful starts (default 0 in tests)
	Cadence       time.Duration // per-repo cooldown (default 180 days)
	Logger        *slog.Logger
	// ImmediatePartialReclaim, when true (the default), preserves
	// the v0.25.0 behavior: partial-scan repos (distribution_scan_complete
	// = FALSE) bypass the cadence gate and re-collect on the next
	// dispatcher tick. When false, partial scans wait for normal
	// cadence — operator-level escape hatch for post-transition
	// fleets where the immediate-reclaim becomes operational churn.
	// Threaded through to ClaimNextDistributionRepo's WHERE-clause
	// branching. v0.25.3.
	ImmediatePartialReclaim bool
}

// Worker is a goroutine-based DistributionWorker pool.
type Worker struct {
	store                   Store
	scanner                 Scanner
	workers                 int
	startInterval           time.Duration
	cadence                 time.Duration
	logger                  *slog.Logger
	immediatePartialReclaim bool // v0.25.3
}

// NewWorker constructs a Worker. Workers <= 0 falls back to 1.
func NewWorker(opts WorkerOptions) *Worker {
	w := &Worker{
		store:                   opts.Store,
		scanner:                 opts.Scanner,
		workers:                 opts.Workers,
		startInterval:           opts.StartInterval,
		cadence:                 opts.Cadence,
		logger:                  opts.Logger,
		immediatePartialReclaim: opts.ImmediatePartialReclaim,
	}
	if w.workers <= 0 {
		w.workers = 1
	}
	if w.cadence <= 0 {
		w.cadence = 180 * 24 * time.Hour
	}
	if w.logger == nil {
		w.logger = slog.Default()
	}
	return w
}

// Run starts the dispatcher + N runner goroutines and blocks until
// ctx is canceled. Safe to call exactly once per Worker instance.
func (w *Worker) Run(ctx context.Context) {
	// Unbuffered channel: dispatcher's `jobs <- job` blocks until a
	// runner reads. This is how we naturally throttle to N concurrent
	// scans without an explicit semaphore — runners do the gating by
	// not consuming.
	jobs := make(chan *db.DistributionJob)
	done := make(chan struct{})

	// Spawn N runners.
	runnersDone := make(chan struct{}, w.workers)
	for i := 0; i < w.workers; i++ {
		safego.Go(w.logger, "distribution-runner", func() { w.runner(ctx, jobs, runnersDone) })
	}

	// Dispatcher: claim loop with minimum-gap pacing (v0.21.3).
	go func() {
		defer safego.Recover(w.logger, "distribution-dispatcher")
		defer close(jobs)
		defer close(done)
		w.dispatcher(ctx, jobs)
	}()

	// Wait for dispatcher to exit (jobs closed), then drain runners.
	<-done
	for i := 0; i < w.workers; i++ {
		<-runnersDone
	}
	w.logger.Info("distribution worker stopped")
}

// healthCheckInterval is the dispatcher's sleep between unhealthy
// re-checks. 60 seconds is small enough to resume promptly when the
// breaker (1-hour pause) closes, large enough to avoid spinning the
// CPU. v0.25.0.
const healthCheckInterval = 60 * time.Second

// dispatcher polls the store for new claims and forwards them to
// the runners via the jobs channel. Minimum-gap pacing means
// nextStartAllowed is stamped AFTER each successful start, so the
// dispatcher loops as fast as the runtime allows when jobs are
// available; the gate only fires to prevent claim bursts on
// fresh startup.
//
// v0.25.0: also checks scanner.Healthy() before each claim and
// pauses entirely when the scanner reports unhealthy (currently:
// ecosyste.ms circuit breaker open). This prevents dispatching new
// repos into a known-bad upstream — without this, every repo
// dispatched during the breaker's 1-hour pause would get a
// "complete" scan stamped with no ecosyste.ms data, losing
// ecosyste.ms coverage for 180 days.
//
// Three exit conditions:
//   - ctx canceled (graceful shutdown)
//   - empty queue (sleeps briefly and re-polls)
//   - store error (logs and continues; transient DB issues shouldn't
//     stop the worker)
func (w *Worker) dispatcher(ctx context.Context, jobs chan<- *db.DistributionJob) {
	// nextStartAllowed gates the minimum gap between successful
	// starts. Initialized to time.Now() so the first claim fires
	// immediately.
	nextStartAllowed := time.Now()
	// unhealthyLogged ensures we emit the "scanner unhealthy" WARN
	// once per outage rather than once per check (1-hour outage at
	// 60s checks would otherwise produce 60 identical log lines).
	unhealthyLogged := false

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// v0.25.0 health gate. Re-checked on every loop iteration so
		// recovery is observed within healthCheckInterval of the
		// breaker reopening.
		if !w.scanner.Healthy() {
			if !unhealthyLogged {
				w.logger.Warn("distribution dispatcher: scanner unhealthy — pausing dispatch until source recovers",
					"check_interval", healthCheckInterval)
				unhealthyLogged = true
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(healthCheckInterval):
			}
			continue
		}
		if unhealthyLogged {
			w.logger.Info("distribution dispatcher: scanner healthy again — resuming dispatch")
			unhealthyLogged = false
		}

		// Wait until we're past nextStartAllowed.
		now := time.Now()
		if now.Before(nextStartAllowed) {
			wait := nextStartAllowed.Sub(now)
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
		}

		job, err := w.store.ClaimNextDistributionRepo(ctx, w.cadence, w.immediatePartialReclaim)
		if err != nil {
			w.logger.Warn("distribution dispatcher: claim failed", "error", err)
			// Back off briefly to avoid spinning on a broken DB.
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		if job == nil {
			// Queue empty: sleep before next poll. Conservative 30s
			// keeps the polling rate gentle on the DB when no work
			// is available.
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
			continue
		}

		// Hand the job to a runner.
		select {
		case <-ctx.Done():
			// ctx canceled while waiting for a runner: release the
			// claim so the row becomes immediately re-claimable.
			_ = w.store.RecordDistributionFailure(context.Background(), job)
			return
		case jobs <- job:
			// Stamp the next-start deadline AFTER the handoff so
			// the gate enforces minimum gap between successful
			// starts, not between poll attempts.
			if w.startInterval > 0 {
				nextStartAllowed = time.Now().Add(w.startInterval)
			}
		}
	}
}

// runner reads jobs from the channel, runs the scanner, and routes
// the result to MarkComplete or RecordFailure. Loops until the
// jobs channel closes.
func (w *Worker) runner(ctx context.Context, jobs <-chan *db.DistributionJob, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	for job := range jobs {
		w.processJob(ctx, job)
	}
}

// processJob runs the scanner against one claimed job and routes
// the outcome to the store. Uses a separate context for the
// completion call so we still persist results even if the parent
// ctx was canceled mid-scan.
func (w *Worker) processJob(ctx context.Context, job *db.DistributionJob) {
	distributions, manifests, scanComplete, scanErr := w.scanner.Scan(ctx, job.RepoID, job.RepoOwner, job.RepoName, job.RepoGit)

	// completionCtx: short window for the DB write to finish even
	// if the parent ctx is canceling. Without this, a shutdown
	// during the scan would lose the result entirely.
	completionCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if scanErr != nil {
		w.logger.Warn("distribution scan failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", scanErr)
		if err := w.store.RecordDistributionFailure(completionCtx, job); err != nil {
			w.logger.Error("distribution: record failure also failed",
				"repo_id", job.RepoID, "error", err)
		}
		return
	}

	// v0.25.0: scanComplete is stamped into distribution_scan_complete
	// so the claim query can treat partial scans as immediately
	// re-eligible. Partial-scan rows still rotate to history on the
	// next full re-scan via the snapshot-replace path.
	if !scanComplete {
		w.logger.Info("distribution scan partial — will be re-collected once source recovers",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"distributions", len(distributions), "manifests", len(manifests))
	}

	if err := w.store.MarkDistributionComplete(completionCtx, job, distributions, manifests, scanComplete); err != nil {
		w.logger.Error("distribution: mark complete failed",
			"repo_id", job.RepoID, "error", err)
		return
	}
	w.logger.Info("distribution scan complete",
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"distributions", len(distributions), "manifests", len(manifests))
}
