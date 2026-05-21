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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// v0.23.3: wall-clock timeouts on subprocess execution. These
// bound how long a single worker slot can be consumed by one
// repo's scan before ctx-cancel + cmd.Cancel reclaim the slot.
//
// scancodeCloneTimeout: 30 minutes. A `--depth 1` clone normally
// finishes in seconds-to-minutes. 30 min handles slow links and
// large repos (Linux kernel mirrors ~500 MB shallow) without
// allowing a wedged clone to consume a slot indefinitely.
//
// scancodeRunTimeout: 2 hours. The scancode binary's own
// `--timeout 300` is per-file. Total wall-clock has no upstream
// bound. 2 hours is generous for any realistic repo and prevents
// the 2026-05-21 wedge (both worker slots stuck for 6+ hours).
//
// stalePidCheckInterval: 5 minutes. The in-flight cleanup
// goroutine wakes on this cadence to clear NULL-PID lock states
// (the v0.21.0 inconsistency window).
const (
	scancodeCloneTimeout  = 30 * time.Minute
	scancodeRunTimeout    = 2 * time.Hour
	stalePidCheckInterval = 5 * time.Minute
	stalePidLockMaxAge    = 5 * time.Minute
)

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
	// v0.23.8: per-job wall-clock timeout. The runner computes
	// `min(runTimeoutBase * 2^job.TimeoutAttempts, runTimeoutCap)`
	// per job. Pre-v0.23.8 this was a package-level constant
	// (`scancodeRunTimeout = 2 * time.Hour`); now configurable +
	// adaptive so kernel-class repos can stretch up to the cap
	// without operator intervention.
	runTimeoutBase time.Duration
	runTimeoutCap  time.Duration
}

// NewScancodeWorker constructs a worker. Most time-based fields fall
// back to documented defaults when zero is passed.
//
// v0.23.8: runTimeoutBase + runTimeoutCap configure the adaptive
// per-job wall-clock timeout. Pre-v0.23.8 these were the hardcoded
// constant scancodeRunTimeout = 2*time.Hour; now caller-supplied.
// Defaults: base 2h, cap 24h.
//
// v0.23.7-fix: shutdownGrace now defaults to 0 (immediate kill) when
// the caller passes 0, matching the v0.23.7 contract that
// subprocesses outliving aveloxis can't deliver output anyway.
// Pre-v0.23.7 the zero-input fallback was 30 min — that masked the
// v0.23.7 config-default flip from 30 to 0.
func NewScancodeWorker(store *db.PostgresStore, logger *slog.Logger,
	workerCount int, startInterval, cadence time.Duration,
	cloneDir string, shutdownGrace, runTimeoutBase, runTimeoutCap time.Duration) *ScancodeWorker {
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
	// v0.23.7: shutdownGrace = 0 means immediate kill on stop.
	// Don't override.
	if runTimeoutBase <= 0 {
		runTimeoutBase = 2 * time.Hour
	}
	if runTimeoutCap <= 0 {
		runTimeoutCap = 24 * time.Hour
	}
	return &ScancodeWorker{
		store:          store,
		logger:         logger,
		workerCount:    workerCount,
		startInterval:  startInterval,
		cadence:        cadence,
		cloneDir:       cloneDir,
		shutdownGrace:  shutdownGrace,
		runTimeoutBase: runTimeoutBase,
		runTimeoutCap:  runTimeoutCap,
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

	// v0.23.3: in-flight orphan recovery — periodic cleanup of
	// stale NULL-PID lock rows (the v0.21.0 inconsistency state).
	// Distinct from recoverOrphans (startup-only): this fires every
	// stalePidCheckInterval while the worker is running, so an
	// inconsistent lock acquired during this serve's runtime gets
	// cleared without waiting for the next aveloxis restart.
	// See checkOwnLocks for the specific patterns it handles.
	go w.checkOwnLocks(ctx)

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
	// v0.23.3: wall-clock timeout on the git clone. A shallow clone
	// of any normal repo finishes in under 5 minutes; 30 minutes is
	// a comfortable cap that lets pathological cases (1 GB repos
	// over slow links) succeed while preventing the mid-run wedge
	// pattern from 2026-05-21 where one worker slot stayed locked
	// for 6+ hours on a clone that never returned.
	cloneCtx, cloneCancel := context.WithTimeout(ctx, scancodeCloneTimeout)
	defer cloneCancel()
	cloneCmd := exec.CommandContext(cloneCtx, "git", "clone", "--depth", "1", "--", job.RepoGit, tempDir)
	cloneCmd.Env = append(os.Environ(), "GIT_LFS_SKIP_SMUDGE=1")
	// v0.23.3: process-group cleanup. Setpgid puts the git subprocess
	// (and any grandchildren — git-lfs, git-remote-https, the smudge-
	// filter pre-check) into its own pgid. cmd.Cancel signals the
	// whole group on ctx cancel; WaitDelay bounds how long Wait()
	// blocks if anything refuses to exit. Without this, the operator's
	// `aveloxis stop` leaves git+lfs subprocesses running as orphans.
	cloneCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cloneCmd.Cancel = func() error {
		if cloneCmd.Process != nil {
			return syscall.Kill(-cloneCmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cloneCmd.WaitDelay = 10 * time.Second
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
	//
	// v0.23.3: wall-clock timeout on the scancode subprocess (was
	// hardcoded scancodeRunTimeout = 2h). The scancode binary takes
	// --timeout 300 (per-FILE seconds), but nothing bounds total
	// wall-clock. A repo with 10k files × 5 min each is
	// theoretically 833 hours.
	//
	// v0.23.8: the timeout is now adaptive. The runner computes
	// `min(base * 2^job.TimeoutAttempts, cap)` from the row's
	// scancode_timeout_attempts counter at claim time. Kernel-class
	// repos that have timed out before get progressively longer
	// timeouts (2h → 4h → 8h → 16h → 24h-capped) until the scan
	// completes (counter resets) or hits the cap. Repos that have
	// never timed out use the base (typically 2h, matches the
	// pre-v0.23.8 constant). Operator-tuned via
	// collection.scancode_run_timeout_hours +
	// scancode_run_timeout_cap_hours.
	effectiveTimeout := w.runTimeoutBase
	if job.TimeoutAttempts > 0 {
		// 1 << job.TimeoutAttempts overflows around attempt 62 on
		// int64; clamp at a safe range. With base 2h and cap 24h,
		// attempts >= 4 always hit the cap, so we never compute
		// large multipliers in practice.
		shift := job.TimeoutAttempts
		if shift > 30 {
			shift = 30
		}
		effectiveTimeout = time.Duration(int64(w.runTimeoutBase) * (1 << shift))
	}
	if effectiveTimeout > w.runTimeoutCap {
		effectiveTimeout = w.runTimeoutCap
	}
	if effectiveTimeout != w.runTimeoutBase {
		w.logger.Info("scancode runOne: using stretched timeout for repeat-timeout repo",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"timeout_attempts", job.TimeoutAttempts,
			"effective_timeout", effectiveTimeout.String(),
			"base", w.runTimeoutBase.String(), "cap", w.runTimeoutCap.String())
	}
	scanCtx, scanCancel := context.WithTimeout(ctx, effectiveTimeout)
	defer scanCancel()
	cmd := exec.CommandContext(scanCtx, scancodePath,
		"-clpi",
		"--only-findings",
		"--json", outputPath,
		"--quiet",
		"--timeout", "300",
		"--processes", strconv.Itoa(procs),
		"--max-in-memory", "5000",
		tempDir,
	)

	// Capture stdout + stderr. v0.23.3: we now keep BOTH a bounded
	// ring buffer (the tail, for quick triage in the log line) AND
	// a full bytes.Buffer (for the per-repo stderr file written on
	// failure). io.MultiWriter fans the stream into both. Pre-fix
	// the tail-only approach was almost always dominated by 10+
	// repetitions of the libmagic warning, hiding the actual error
	// message that scrolled out of the 4 KB window. The operator
	// had to grep the log every time to ask "what actually went
	// wrong"; with the per-repo file the answer is right there at
	// /tmp/aveloxis-scancode/repo_<id>_stderr.log.
	stderrTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stdoutTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stderrFull := &bytes.Buffer{}
	stdoutFull := &bytes.Buffer{}
	cmd.Stderr = io.MultiWriter(stderrTail, stderrFull)
	cmd.Stdout = io.MultiWriter(stdoutTail, stdoutFull)

	// v0.23.3: process group cleanup for the scancode subprocess.
	// Same shape as the git clone above. Critical here because
	// `scancode --processes 2` spawns a Python multiprocessing pool
	// whose worker processes inherit our stderr/stdout fds. Without
	// pgid kill, cmd.Wait() can block forever waiting for those
	// inherited fds to close even when the lead scancode process
	// has died — the 2026-05-21 wedge pattern. WaitDelay caps the
	// blocking at 10 s as a safety net.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second

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

	// v0.23.7: deferred best-effort straggler kill. The v0.23.3
	// cmd.Cancel path kills the process group only when ctx cancels
	// while cmd.Wait() is blocked. If the lead scancode python
	// process exits normally before ctx cancels, cmd.Wait() returns
	// and cmd.Cancel never runs — leaving any straggler children
	// (multiprocessing pool workers from scancode --processes, the
	// git-lfs subprocess from the earlier clone phase) as orphans
	// (PPID=1).
	//
	// The defer covers exactly that gap: when runOne returns
	// (whether via early-return error path or normal completion),
	// syscall.Kill with NEGATIVE pid targets the entire process
	// group, picking up any survivors. Idempotent — syscall.Kill
	// returns ESRCH on an already-dead group, which we ignore.
	//
	// Composes with v0.23.3: cmd.Cancel still fires on ctx cancel
	// (the original case); the defer fires unconditionally at
	// function exit (the new case). The pgid being signaled twice
	// is harmless.
	defer syscall.Kill(-pid, syscall.SIGKILL)

	bootID := readBootID()
	if err := w.store.RecordScancodeLockState(ctx, job.RepoID, pid, bootID, outputPath); err != nil {
		// v0.23.3: abort on lock-state failure. The pre-v0.23.3
		// "proceed anyway" path left rows with scancode_locked_at
		// SET and scancode_locked_pid NULL — indistinguishable from
		// a legitimate in-flight scan with delayed PID write. The
		// 2026-05-21 diagnostic showed one production row stuck in
		// this state (ropensci/neotoma), holding a worker slot
		// indefinitely. Better to kill the subprocess and surface
		// the DB-write error than to leave an unrecoverable lock.
		w.logger.Warn("scancode runOne: failed to record lock state — killing subprocess and aborting",
			"repo_id", job.RepoID, "pid", pid, "error", err)
		// Signal the process group so the scancode subprocess and
		// its Python worker pool all die. WaitDelay caps the wait.
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait() // reap; we don't care about its exit status
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	w.logger.Info("running ScanCode",
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"path", tempDir, "pid", pid)

	if err := cmd.Wait(); err != nil {
		// v0.23.3: write the FULL stderr to a per-repo file. Operator
		// can `less /tmp/aveloxis-scancode/repo_<id>_stderr.log` to
		// see the entire output, not just the 4 KB tail dominated by
		// libmagic preamble. Best-effort: if the file write fails,
		// the log line's stderr_tail is the fallback.
		stderrPath := filepath.Join(w.cloneDir, fmt.Sprintf("repo_%d_stderr.log", job.RepoID))
		stderrWriteErr := os.WriteFile(stderrPath, stderrFull.Bytes(), 0o644)
		stdoutPath := ""
		if stdoutFull.Len() > 0 {
			stdoutPath = filepath.Join(w.cloneDir, fmt.Sprintf("repo_%d_stdout.log", job.RepoID))
			_ = os.WriteFile(stdoutPath, stdoutFull.Bytes(), 0o644)
		}
		logArgs := []any{
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err, "pid", pid,
			"full_stderr_at", stderrPath,
			"stderr_bytes", stderrFull.Len(),
			"stderr_tail", stderrTail.String(),
		}
		if stdoutPath != "" {
			logArgs = append(logArgs, "full_stdout_at", stdoutPath)
		}
		if stderrWriteErr != nil {
			logArgs = append(logArgs, "stderr_file_write_error", stderrWriteErr)
		}
		w.logger.Warn("scancode runOne: scancode subprocess failed", logArgs...)

		// v0.23.4: scancode-toolkit-mini with `--quiet` returns exit 1
		// when any individual file fails to scan (e.g., a malformed
		// PDF that pdfminer crashes on), but the JSON output is fully
		// written with the successful scans intact. Pre-v0.23.4 this
		// path called recordFailureBestEffort unconditionally,
		// discarding usable scan data and advancing the
		// scancode_failed_attempts counter toward the v0.21.4 10-strike
		// sideline. Now: try to salvage the JSON; if it parses with
		// files_count > 0, log a WARN with the per-file errors and
		// fall through to the success path (ingest + MarkScancodeComplete).
		// Only treat as a real failure if the JSON is missing or
		// invalid (genuine scancode crash, or wall-clock-timeout
		// SIGKILL'd subprocess before the output completed).
		salvagedFilesCount, salvagedHeaderErrors, salvaged := salvageScancodeOutput(outputPath)
		if !salvaged {
			w.recordFailureBestEffort(ctx, job.RepoID)
			return
		}
		w.logger.Warn("scancode runOne: salvaged scan output despite subprocess exit 1",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"files_count", salvagedFilesCount,
			"header_errors", salvagedHeaderErrors,
			"pid", pid)
		// Fall through to the ingest + MarkScancodeComplete path
		// below. The unconditional `return` that ended this block
		// pre-v0.23.4 is gone.
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
// checkOwnLocks is the v0.23.3 in-flight orphan recovery loop.
// Wakes every stalePidCheckInterval to clear NULL-PID locks that
// have aged past stalePidLockMaxAge.
//
// Why this matters: v0.21.0's runOne had a "RecordScancodeLockState
// failed — proceeding anyway" path that left rows with
// scancode_locked_at SET but scancode_locked_pid NULL. The startup
// recoverOrphans pass handles those (4-state decision falls into
// "lost run") but only at startup. If the inconsistency happens
// during a live serve, the row stays locked until next restart —
// the exact pattern observed in production 2026-05-21
// (ropensci/neotoma, locked for 6+ hours, NULL PID).
//
// v0.23.3 also changes runOne to ABORT the scan on
// RecordScancodeLockState failure (see scancode_worker.go runOne)
// so new occurrences shouldn't happen. But existing inconsistent
// rows from pre-v0.23.3 runs need cleanup, and this loop is a
// defense in depth for any failure path that might still produce
// the state.
//
// Does NOT recover wedged workers (cmd.Wait() blocked indefinitely
// on a live subprocess). That's prevented at the source by the
// scancodeCloneTimeout and scancodeRunTimeout context-with-timeout
// wraps around the cmd.CommandContext calls — runOne's
// subprocesses now have a hard wall-clock cap of 30m / 2h
// respectively, so worker slots can't stay consumed beyond that.
func (w *ScancodeWorker) checkOwnLocks(ctx context.Context) {
	ticker := time.NewTicker(stalePidCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleared, err := w.store.ClearStaleNullPidLocks(ctx, stalePidLockMaxAge)
			if err != nil {
				w.logger.Warn("scancode in-flight orphan recovery: ClearStaleNullPidLocks failed",
					"error", err)
				continue
			}
			if cleared > 0 {
				w.logger.Info("scancode in-flight orphan recovery: cleared stale NULL-PID locks",
					"count", cleared,
					"older_than", stalePidLockMaxAge.String())
			}
		}
	}
}

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

// salvageScancodeOutput (v0.23.4) parses just the headers of a
// scancode JSON output file and returns (filesCount, perFileErrors,
// ok) where ok=true means the JSON is valid and the scan produced
// useful data (files_count > 0). The caller uses this from runOne's
// cmd.Wait() error branch to distinguish:
//
//   - Per-file scancode errors with valid output: scancode-toolkit-mini
//     with `--quiet` exits status 1 when ANY file fails to scan (e.g.,
//     a malformed PDF that pdfminer crashes on), but the JSON output
//     is fully written with the successful scans intact and the
//     per-file failures recorded in headers[0].errors. The 2026-05-21
//     post-v0.23.3 diagnostic showed 27 of 37 runs over 17 hours
//     hitting this pattern; aveloxis was throwing away usable scan
//     data and incrementing scancode_failed_attempts toward the
//     v0.21.4 10-strike sideline.
//
//   - Genuine scancode crash: JSON missing, truncated, or empty.
//     ok=false; caller falls through to recordFailureBestEffort.
//
//   - Wall-clock timeout SIGKILL: scancode killed mid-write. Either
//     JSON file is missing or it's truncated and json.Unmarshal
//     fails. ok=false.
//
// Uses a minimal struct (just headers — files[] not parsed) so this
// is cheap even on a 100 MB output file. The full ingest path in
// ingestScancodeOutput does the deep parse separately.
func salvageScancodeOutput(outputPath string) (filesCount int, headerErrors []string, ok bool) {
	if !fileExistsAndNonEmpty(outputPath) {
		return 0, nil, false
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		return 0, nil, false
	}
	var raw struct {
		Headers []struct {
			Errors    []string `json:"errors"`
			ExtraData struct {
				FilesCount int `json:"files_count"`
			} `json:"extra_data"`
		} `json:"headers"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, nil, false
	}
	if len(raw.Headers) == 0 {
		return 0, nil, false
	}
	filesCount = raw.Headers[0].ExtraData.FilesCount
	headerErrors = raw.Headers[0].Errors
	if filesCount <= 0 {
		return filesCount, headerErrors, false
	}
	return filesCount, headerErrors, true
}
