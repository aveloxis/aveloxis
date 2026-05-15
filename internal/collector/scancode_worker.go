// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — scancode_worker.go runs ScanCode Toolkit in a
// dedicated worker pool, decoupled from the per-repo collection
// pipeline.
//
// # Why a dedicated worker (v0.21.0)
//
// Pre-v0.21.0, scancode ran inline in AnalysisCollector.AnalyzeRepo
// gated by a 2-slot package-level semaphore. The 2026-05-14 production
// incident on a 180-worker fleet showed 177 worker goroutines parked
// at the semaphore for 7+ hours — effectively the whole pool stalled
// behind two slow scancode runs. The fix moves scancode out of the
// per-job pipeline entirely. The new worker:
//
//   - Claims eligible repos via Postgres FOR UPDATE SKIP LOCKED so
//     multiple aveloxis processes (or future shard splits) can
//     coordinate without coordination overhead.
//   - Re-clones each repo shallowly (git clone --depth 1) so it
//     doesn't depend on the facade's bare clone — the two paths
//     can run independently without lock-on-bare-clone hazards.
//   - Persists the OS PID + kernel boot_id immediately after the
//     scancode subprocess starts, so a crash (kill -9, OOM, host
//     reboot) leaves recoverable state for the next startup.
//   - Honors a configurable 6-month default cadence (was 30 days
//     hardcoded) because per-file license + copyright data
//     changes rarely on the timescale we care about.
//
// See docs/architecture/scancode.md for the full architectural
// write-up, the four-state recovery table, and the force-rerun
// cookbook.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// scancodeStderrTailBytes is the cap on how much subprocess output
// we keep in memory and log on failure. Big enough to surface a
// meaningful traceback or error message, small enough that 100+
// concurrent failures don't pressure the heap.
const scancodeStderrTailBytes = 4096

// bootIDPath is the kernel-generated UUID that changes on every
// boot. Reading this file at lock-record time (paired with the
// scancode subprocess PID) makes the recovery liveness check
// unambiguous across kernel reboots — without it a stored PID of
// 12345 from before a reboot could match an unrelated process that
// now happens to hold that PID. Linux-only; on macOS dev machines
// the file is absent and we fall back to a process-stable
// substitute that still detects "this is a new process".
const bootIDPath = "/proc/sys/kernel/random/boot_id"

// ScancodeWorker is the v0.21.0 decoupled scancode runner.
//
// Lifecycle:
//
//  1. Run() probes for the scancode binary on PATH; if absent,
//     logs once at INFO and returns (no goroutines spawned).
//  2. recoverOrphans() runs once — examines every row with a
//     non-null scancode_locked_at and applies the four-state
//     recovery (reboot survivor / live orphan / recoverable corpse
//     / lost run).
//  3. dispatcher() ticks every startInterval seconds, attempts a
//     claim, and feeds a free runner slot. The ticker (not a tight
//     loop) is what paces clone-bandwidth bursts on operator
//     restart.
//  4. runner() goroutines (workerCount of them) consume jobs and
//     call runOne(). Each runOne does: shallow clone, scancode
//     subprocess, parse JSON output, ingest to the existing
//     scancode_scans + scancode_file_results tables, clear lock.
//  5. On ctx.Done(), the dispatcher exits immediately. Runners
//     keep going until shutdownGrace elapses, at which point
//     pending subprocesses are killed and Run() returns.
type ScancodeWorker struct {
	store         *db.PostgresStore
	logger        *slog.Logger
	workerCount   int
	startInterval time.Duration
	cadence       time.Duration
	cloneDir      string
	shutdownGrace time.Duration
}

// NewScancodeWorker constructs a worker. All time-based fields fall
// back to documented defaults when zero is passed, so the caller
// can do `NewScancodeWorker(store, logger, 0, 0, 0, "", 0)` to get
// an entirely default-configured worker.
func NewScancodeWorker(store *db.PostgresStore, logger *slog.Logger,
	workerCount int, startInterval, cadence time.Duration,
	cloneDir string, shutdownGrace time.Duration) *ScancodeWorker {
	if workerCount <= 0 {
		workerCount = 2
	}
	if startInterval <= 0 {
		startInterval = 90 * time.Second
	}
	if cadence <= 0 {
		cadence = 180 * 24 * time.Hour
	}
	if cloneDir == "" {
		cloneDir = "/tmp/aveloxis-scancode"
	}
	if shutdownGrace <= 0 {
		shutdownGrace = 30 * time.Minute
	}
	return &ScancodeWorker{
		store:         store,
		logger:        logger,
		workerCount:   workerCount,
		startInterval: startInterval,
		cadence:       cadence,
		cloneDir:      cloneDir,
		shutdownGrace: shutdownGrace,
	}
}

// Run starts the worker pool and blocks until ctx is done. After
// ctx cancellation, waits up to shutdownGrace for in-flight runners
// before forcing them to exit and returning.
//
// Probes for the scancode binary at startup; if not installed, logs
// once and returns without spawning goroutines (matches the pre-
// v0.21.0 silent-skip behavior of the inline analysis-phase scan).
func (w *ScancodeWorker) Run(ctx context.Context) {
	if _, err := exec.LookPath("scancode"); err != nil {
		w.logger.Info("scancode binary not installed; ScancodeWorker disabled",
			"install_hint", "pipx install scancode-toolkit")
		return
	}

	if err := os.MkdirAll(w.cloneDir, 0o755); err != nil {
		w.logger.Warn("failed to create scancode clone directory; ScancodeWorker disabled",
			"clone_dir", w.cloneDir, "error", err)
		return
	}

	w.logger.Info("scancode worker started",
		"workers", w.workerCount,
		"start_interval", w.startInterval.String(),
		"cadence", w.cadence.String(),
		"clone_dir", w.cloneDir,
		"shutdown_grace", w.shutdownGrace.String())

	// One-shot recovery pass before the dispatcher starts claiming
	// new work. This is what makes graceful shutdown + kill -9
	// survivable: any orphaned subprocess from a prior run is
	// either adopted (monitor goroutine spawned) or its lock
	// cleared, so the dispatcher's claim query sees a clean state.
	w.recoverOrphans(ctx)

	jobs := make(chan db.ScancodeJob)
	var wg sync.WaitGroup

	// Runner pool — each runner consumes one job at a time.
	for i := 0; i < w.workerCount; i++ {
		wg.Add(1)
		go w.runner(ctx, jobs, &wg)
	}

	// Dispatcher claims jobs and feeds the channel. When ctx is
	// canceled, dispatcher exits, closes the channel, and the
	// runners drain naturally.
	dispatcherDone := make(chan struct{})
	go func() {
		defer close(dispatcherDone)
		w.dispatcher(ctx, jobs)
		close(jobs)
	}()

	<-dispatcherDone

	// Graceful shutdown: wait up to shutdownGrace for runners to
	// finish. If they don't, the runners' ctx-derived cmd.Start
	// will have its subprocess killed via syscall.Kill in the
	// graceful-shutdown defer chain on each runOne.
	runnersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(runnersDone)
	}()

	select {
	case <-runnersDone:
		w.logger.Info("scancode worker stopped cleanly — all runners completed within grace window")
	case <-time.After(w.shutdownGrace):
		w.logger.Warn("scancode worker shutdown grace expired — outstanding scancode subprocesses will be killed on next syscall",
			"grace", w.shutdownGrace.String())
	}
}

// dispatcher claims eligible repos and feeds them to runners.
//
// v0.21.3 design: minimum-gap pacing, not throughput cap. The
// dispatcher uses a `nextStartAllowed` deadline variable to enforce
// at least `startInterval` between consecutive successful claims,
// preserving the original burst-protection intent. Between gates
// the dispatcher runs as fast as workers free up.
//
// Pre-v0.21.3 the dispatcher was driven by
// time.NewTicker(startInterval) — one claim attempt per tick,
// regardless of how many workers were idle. At 90 s/tick × 7
// workers × ~3-min average scan time, the dispatcher capped fleet
// throughput at 40 claims/hour while runners had capacity for
// ~140. On a 40K-repo fleet this produced ~42-day first-pass
// estimates when actual capacity was ~12 days. See CLAUDE.md
// v0.21.3 entry.
//
// Why an UNBUFFERED jobs channel keeps the design correct: the
// dispatcher's send blocks until a runner is ready to receive.
// When all N workers are busy, the dispatcher's send blocks —
// claims naturally pause until a runner frees up. The
// `nextStartAllowed` gate only kicks in BETWEEN successful starts
// when workers have spare capacity; in the busy case the send-
// blocks-naturally semantics provide the back-pressure.
//
// Idle-queue behavior: when ClaimNextScancodeRepo returns (nil, nil)
// — no eligible repos — the dispatcher sleeps for startInterval
// before re-polling. Avoids hot-looping the DB when the fleet is
// fully scanned and waiting for cadence windows to elapse.
func (w *ScancodeWorker) dispatcher(ctx context.Context, jobs chan<- db.ScancodeJob) {
	// Zero value (Time{}) means "no gap required yet" — the first
	// claim on startup happens immediately. Subsequent claims wait
	// until time.Now() >= nextStartAllowed.
	var nextStartAllowed time.Time

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		// Rate-limit gate. Sleeps until the minimum-gap window
		// elapses (or ctx cancels). On startup or after an idle
		// queue, the gate is in the past and this is a no-op.
		if delay := time.Until(nextStartAllowed); delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		}

		job, err := w.store.ClaimNextScancodeRepo(ctx, w.cadence)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Warn("scancode worker claim failed", "error", err)
			// Brief backoff before retrying the failing claim
			// so a transient DB issue doesn't pin a CPU core.
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
			continue
		}
		if job == nil {
			// Queue empty — no eligible repo. Sleep for one
			// startInterval before re-polling so we don't hot-
			// loop the DB. When new repos become eligible
			// (cadence expires) the next poll picks them up.
			select {
			case <-time.After(w.startInterval):
			case <-ctx.Done():
				return
			}
			continue
		}

		// Send to a runner. UNBUFFERED channel — this BLOCKS
		// until a runner is ready to receive. When all N
		// workers are busy, the dispatcher pauses here
		// naturally; no over-claiming.
		select {
		case jobs <- *job:
			// Successful start. Stamp the next-start window so
			// we wait at least startInterval before the next
			// claim — even if a runner frees up immediately.
			nextStartAllowed = time.Now().Add(w.startInterval)
		case <-ctx.Done():
			// We claimed but ctx canceled before any runner
			// could accept. Best-effort release of the lock so
			// the next aveloxis startup's recoverOrphans pass
			// doesn't have to deal with a phantom claim.
			_ = w.store.ClearScancodeLock(context.Background(), job.RepoID)
			return
		}
	}
}

// runner consumes jobs and runs them serially. When jobs is closed
// (dispatcher exit), the loop falls through and the goroutine
// returns, decrementing the WaitGroup.
func (w *ScancodeWorker) runner(ctx context.Context, jobs <-chan db.ScancodeJob, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		w.runOne(ctx, job)
	}
}

// runOne is the actual scan pipeline for one repo. Always clears
// the scancode_locked_* columns on exit (success or failure) so the
// row becomes eligible for the next claim cycle without waiting for
// the 12-hour stale-lock fallback.
//
// Steps:
//  1. mkdir <cloneDir>/repo_<id>_<unix_ts>
//  2. git clone --depth 1 <repo_git> <tempDir>
//  3. exec.CommandContext(ctx, "scancode", ...) with --json
//     pointing at <tempDir>/results.json
//  4. cmd.Start() — non-blocking; we need the PID NOW.
//  5. store.RecordScancodeLockState(repoID, pid, bootID, outputPath)
//     so the next aveloxis startup can decide what to do with this
//     row if we crash before cmd.Wait() returns.
//  6. cmd.Wait() — blocks until subprocess exits.
//  7. Parse JSON output, call existing RunScanCode-equivalent
//     ingest logic to write scancode_scans + scancode_file_results.
//  8. store.MarkScancodeComplete on success, ClearScancodeLock on
//     failure.
//  9. defer os.RemoveAll(tempDir).
//
// Errors at any step are logged (with repo identity) and route to
// the ClearScancodeLock path. We never crash the runner on a
// single repo's failure — the worker stays up.
func (w *ScancodeWorker) runOne(ctx context.Context, job db.ScancodeJob) {
	tempDir := filepath.Join(w.cloneDir,
		fmt.Sprintf("repo_%d_%d", job.RepoID, time.Now().UnixNano()))
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			w.logger.Warn("scancode worker failed to remove temp clone",
				"repo_id", job.RepoID, "temp_dir", tempDir, "error", err)
		}
	}()

	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		w.logger.Warn("scancode runOne mkdir failed",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	// Shallow clone — scancode only needs current file state, not
	// history. --depth 1 dramatically reduces bandwidth for big
	// repos (Linux kernel: full history ~3 GB, shallow ~500 MB).
	//
	// GIT_LFS_SKIP_SMUDGE=1 (v0.21.4): suppresses the LFS smudge
	// filter so pointer files come down as ~130-byte text blobs
	// instead of triggering a fetch from the LFS endpoint. Scancode
	// scans source-license + copyright headers — LFS payloads
	// (tarballs, binary assets, compiled artifacts) aren't useful
	// for that work. Critical for repos whose LFS budget is
	// exhausted (2026-05-14 diagnostic: 214 clone failures on the
	// chaoss.tv fleet, all "exceeded its LFS budget"); the LFS
	// fetch fails, which fails the smudge filter, which fails the
	// checkout, which fails the whole clone. Skipping smudge bypasses
	// the entire chain. Also speeds up clones on every LFS-using
	// repo (no payload fetch).
	cloneCmd := exec.CommandContext(ctx, "git", "clone", "--depth", "1", "--", job.RepoGit, tempDir)
	cloneCmd.Env = append(os.Environ(), "GIT_LFS_SKIP_SMUDGE=1")
	if out, err := cloneCmd.CombinedOutput(); err != nil {
		w.logger.Warn("scancode runOne git clone failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err, "git_output", string(out))
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	scancodePath, err := exec.LookPath("scancode")
	if err != nil {
		// Should never happen — Run() already checked. Defensive.
		w.logger.Warn("scancode runOne: binary not on PATH at run time",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	outputPath := filepath.Join(tempDir, "results.json")
	procs := 2

	// Build the subprocess. Mirrors the legacy RunScanCode flag set
	// so the JSON output format is identical and the existing
	// parser + ingest path (reused below) keeps working.
	cmd := exec.CommandContext(ctx, scancodePath,
		"-clpi",
		"--only-findings",
		"--json", outputPath,
		"--quiet",
		"--timeout", "300",
		"--processes", strconv.Itoa(procs),
		"--max-in-memory", "5000",
		tempDir,
	)

	// Capture stdout + stderr to bounded ring buffers so we can
	// log the tail on failure. Pre-v0.21.4, both streams were nil
	// and Go's exec default discarded the subprocess output —
	// producing log lines that said only "exit status 1" with no
	// diagnostic information whatsoever. The 2026-05-14 production
	// log had 87 such failures across 8 repos and we had no idea
	// why scancode was crashing. The buffers are capped via
	// tailBuffer so concurrent large failures can't pressure the
	// heap (worst case: workerCount × 2 × scancodeStderrTailBytes
	// ≈ a few hundred KB on a 7-worker pool).
	stderrBuf := &tailBuffer{cap: scancodeStderrTailBytes}
	stdoutBuf := &tailBuffer{cap: scancodeStderrTailBytes}
	cmd.Stderr = stderrBuf
	cmd.Stdout = stdoutBuf

	// cmd.Start() is the key change from the legacy synchronous
	// invocation: we need the OS PID BEFORE the subprocess
	// finishes running, so we can persist it to scancode_locked_pid
	// for crash recovery. A blocking exec call would only return
	// the PID after the subprocess had already exited — useful
	// for post-mortem reaping but no help to the recovery pass.
	if err := cmd.Start(); err != nil {
		w.logger.Warn("scancode runOne: cmd.Start() failed",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	pid := cmd.Process.Pid
	bootID := readBootID()
	if err := w.store.RecordScancodeLockState(ctx, job.RepoID, pid, bootID, outputPath); err != nil {
		w.logger.Warn("scancode runOne: failed to record lock state — proceeding anyway",
			"repo_id", job.RepoID, "pid", pid, "error", err)
		// Don't abort the scan — the recovery path can still
		// handle a lost lock state (it'll treat the row as
		// "locked_at present, pid 0" which falls into the
		// "lost run" bucket on next startup).
	}

	w.logger.Info("running ScanCode",
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"path", tempDir, "pid", pid)

	if err := cmd.Wait(); err != nil {
		w.logger.Warn("scancode runOne: scancode subprocess failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err, "pid", pid,
			"stderr_tail", stderrBuf.String(),
			"stdout_tail", stdoutBuf.String())
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	// Parse + ingest. The legacy parser in scancode.go takes a
	// localPath and re-invokes the binary; here we already have
	// the output file, so we parse it directly via the helper.
	version, err := ingestScancodeOutput(ctx, w.store, job.RepoID, outputPath, w.logger)
	if err != nil {
		w.logger.Warn("scancode runOne: ingest failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	if err := w.store.MarkScancodeComplete(ctx, job.RepoID, version); err != nil {
		w.logger.Warn("scancode runOne: MarkScancodeComplete failed",
			"repo_id", job.RepoID, "error", err)
		// The scan succeeded and was ingested; only the
		// completion-stamp write failed. The next claim cycle's
		// cadence gate will see the recent scancode_scans row
		// via the v0.21.0 backfill semantics on the next migrate
		// run, OR re-run scancode (no harm — ingest is
		// idempotent via RotateScancodeToHistory).
		return
	}

	w.logger.Info("scancode worker complete",
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"version", version, "pid", pid)
}

// clearLockBestEffort attempts to clear the lock and logs if it
// fails. Uses a background context so a canceled ctx (graceful
// shutdown) doesn't prevent the cleanup write.
//
// As of v0.21.4 this is used ONLY by the dispatcher's
// ctx-canceled-after-claim cleanup — that's a clean release of a
// row we claimed but never dispatched to a runner, not a failure
// event. All repo-specific failure paths in runOne use
// recordFailureBestEffort instead so the failure-counter +
// last_failed_at columns get updated and the backoff gate applies.
func (w *ScancodeWorker) clearLockBestEffort(ctx context.Context, repoID int64) {
	// Try the live ctx first; fall back to background if canceled.
	useCtx := ctx
	if ctx.Err() != nil {
		useCtx = context.Background()
	}
	if err := w.store.ClearScancodeLock(useCtx, repoID); err != nil {
		w.logger.Warn("scancode runOne: ClearScancodeLock failed",
			"repo_id", repoID, "error", err)
	}
}

// recordFailureBestEffort (v0.21.4) is the failure-path counterpart
// to clearLockBestEffort. It calls store.RecordScancodeFailure which
// increments scancode_failed_attempts, stamps scancode_last_failed_at,
// and conditionally stamps scancode_last_run when the failure count
// reaches ScancodeMaxFailures.
//
// Falls back to a background context if the live ctx is canceled,
// matching clearLockBestEffort's contract so graceful shutdown
// doesn't lose the failure record.
func (w *ScancodeWorker) recordFailureBestEffort(ctx context.Context, repoID int64) {
	useCtx := ctx
	if ctx.Err() != nil {
		useCtx = context.Background()
	}
	if err := w.store.RecordScancodeFailure(useCtx, repoID); err != nil {
		w.logger.Warn("scancode runOne: RecordScancodeFailure failed",
			"repo_id", repoID, "error", err)
	}
}

// recoverOrphans is the worker's one-shot startup pass. For each
// row with a non-null scancode_locked_at it applies the four-state
// decision documented in docs/architecture/scancode.md:
//
//  1. Reboot survivor — stored boot_id ≠ current boot_id. The
//     scancode subprocess is definitively dead. Clear lock.
//
//  2. Live orphan — stored boot_id == current AND kill(-0, pid)
//     succeeds. The scancode subprocess survived an aveloxis
//     crash and is now an orphan of init. Spawn a monitor
//     goroutine to wait for it to exit, then ingest if output is
//     present.
//
//  3. Recoverable corpse — boot_id matches, PID is dead, output
//     file exists and parses. Ingest then clear.
//
//  4. Lost run — boot_id matches, PID dead, no usable output.
//     Clear lock.
//
// Runs synchronously: the dispatcher does NOT start until this
// returns. Live orphans (case 2) get a monitor goroutine that
// outlives recoverOrphans, but those goroutines hold their lock
// rows so the dispatcher's claim query naturally skips them.
func (w *ScancodeWorker) recoverOrphans(ctx context.Context) {
	rows, err := w.store.ListLockedScancodeRows(ctx)
	if err != nil {
		w.logger.Warn("scancode recoverOrphans: ListLockedScancodeRows failed — proceeding without recovery",
			"error", err)
		return
	}
	if len(rows) == 0 {
		return
	}

	currentBootID := readBootID()
	w.logger.Info("scancode recoverOrphans: examining locked rows",
		"count", len(rows), "current_boot_id", currentBootID)

	for _, r := range rows {
		// State 1: reboot survivor.
		if r.LockedBootID != "" && currentBootID != "" && r.LockedBootID != currentBootID {
			w.logger.Info("scancode recover: reboot survivor — clearing lock",
				"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
				"stored_boot_id", r.LockedBootID, "current_boot_id", currentBootID)
			w.clearLockBestEffort(ctx, r.RepoID)
			continue
		}

		// State 2: live orphan — subprocess survived our crash.
		if r.LockedPID > 0 && pidAlive(r.LockedPID) {
			w.logger.Info("scancode recover: live orphan detected — spawning monitor",
				"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
				"pid", r.LockedPID, "output_path", r.OutputPath)
			go w.monitorOrphan(ctx, r)
			continue
		}

		// State 3: recoverable corpse — PID dead, output file
		// looks ingestible. Try to ingest.
		if r.OutputPath != "" && fileExistsAndNonEmpty(r.OutputPath) {
			version, err := ingestScancodeOutput(ctx, w.store, r.RepoID, r.OutputPath, w.logger)
			if err == nil {
				w.logger.Info("scancode recover: ingested orphaned scancode result",
					"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
					"version", version)
				if err := w.store.MarkScancodeComplete(ctx, r.RepoID, version); err != nil {
					w.logger.Warn("scancode recover: MarkScancodeComplete failed after corpse ingest",
						"repo_id", r.RepoID, "error", err)
				}
				continue
			}
			w.logger.Info("scancode recover: corpse output unparseable — clearing lock",
				"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
				"output_path", r.OutputPath, "error", err)
		}

		// State 4: lost run. Clear and move on.
		w.logger.Info("scancode recover: lost run — clearing lock",
			"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
			"pid", r.LockedPID)
		w.clearLockBestEffort(ctx, r.RepoID)
	}
}

// monitorOrphan polls a live-orphan scancode subprocess until it
// exits, then ingests its output if present. Polling interval is
// 30 seconds — orphaned scancode runs typically run for many
// minutes to hours, so a tight poll would just burn CPU.
func (w *ScancodeWorker) monitorOrphan(ctx context.Context, row db.ScancodeLockedRow) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Worker is shutting down. Leave the orphan alone —
			// it'll be picked up by the next aveloxis startup's
			// recovery pass.
			return
		case <-ticker.C:
			if pidAlive(row.LockedPID) {
				continue
			}
			// PID went away. Try to ingest the output.
			if row.OutputPath != "" && fileExistsAndNonEmpty(row.OutputPath) {
				version, err := ingestScancodeOutput(ctx, w.store, row.RepoID, row.OutputPath, w.logger)
				if err == nil {
					if mErr := w.store.MarkScancodeComplete(ctx, row.RepoID, version); mErr != nil {
						w.logger.Warn("scancode orphan monitor: MarkScancodeComplete failed",
							"repo_id", row.RepoID, "error", mErr)
					} else {
						w.logger.Info("scancode orphan monitor: orphan finished, output ingested",
							"repo_id", row.RepoID, "owner", row.RepoOwner, "repo", row.RepoName,
							"version", version)
					}
					return
				}
				w.logger.Info("scancode orphan monitor: orphan finished but output unparseable — clearing lock",
					"repo_id", row.RepoID, "error", err)
			} else {
				w.logger.Info("scancode orphan monitor: orphan finished with no usable output — clearing lock",
					"repo_id", row.RepoID)
			}
			w.clearLockBestEffort(ctx, row.RepoID)
			return
		}
	}
}

// readBootID returns the kernel boot UUID from /proc on Linux. On
// other systems (macOS dev machines), returns an empty string —
// the recovery logic treats empty boot_ids as "unknown, can't make
// a reboot decision" and falls through to the PID-liveness check.
func readBootID() string {
	data, err := os.ReadFile(bootIDPath)
	if err != nil {
		return ""
	}
	return string(bytesTrimSpace(data))
}

// bytesTrimSpace is a small allocation-free wrapper around the
// common os.ReadFile + strings.TrimSpace pattern.
func bytesTrimSpace(b []byte) []byte {
	start, end := 0, len(b)
	for start < end && isSpace(b[start]) {
		start++
	}
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// pidAlive returns true if the given PID is a process we could
// signal — i.e., it exists and we have permission to signal it.
// Uses syscall.Kill(pid, 0) which only checks existence/permission
// without actually delivering a signal. Works on Linux + Darwin.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	// nil = process exists and we can signal it.
	// ESRCH = no such process.
	// EPERM = process exists but we can't signal it (still alive
	// from a liveness POV).
	if err == nil {
		return true
	}
	if errno, ok := err.(syscall.Errno); ok {
		return errno == syscall.EPERM
	}
	return false
}

// fileExistsAndNonEmpty returns true if path refers to a regular
// file with at least one byte. Used to decide whether a scancode
// output file is worth attempting to parse.
func fileExistsAndNonEmpty(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir() && info.Size() > 0
}
