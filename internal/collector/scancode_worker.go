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
//     multiple aveloxis processes (or the v0.27.6 dedicated
//     `aveloxis scancode-worker` host) can coordinate without
//     coordination overhead.
//   - Re-clones each repo shallowly (git clone --depth 1) so it
//     doesn't depend on the facade's bare clone — the two paths
//     can run independently without lock-on-bare-clone hazards.
//   - Persists the OS PID + kernel boot_id + hostname immediately
//     after the scancode subprocess starts, so a crash (kill -9, OOM,
//     host reboot) leaves recoverable state for the next startup.
//   - Honors a configurable 6-month default cadence (was 30 days
//     hardcoded) because per-file license + copyright data
//     changes rarely on the timescale we care about.
//
// See docs/architecture/scancode.md for the full architectural
// write-up, the four-state recovery table, and the force-rerun
// cookbook. docs/guide/dedicated-scancode-host.md covers running the
// pool on its own machine.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostid"
	"github.com/aveloxis/aveloxis/internal/safego"
)

// scancodeStderrTailBytes is the cap on how much subprocess output
// we keep in memory for the LOG LINE tail. Big enough to surface a
// meaningful traceback or error message, small enough that 100+
// concurrent failures don't pressure the heap.
const scancodeStderrTailBytes = 4096

// Caps on the per-repo failure file (written to disk on a failed scan).
// v0.25.28: replaces the pre-fix unbounded bytes.Buffer that buffered the
// ENTIRE stream in RAM — a corrupt host libmagic made a large repo emit 15+ GB
// of warning spam, producing a multi-GB heap spike per failing repo AND a
// 15 GB on-disk file (June 2026 observed artifacts up to 9.5 GB). head shows
// the failure onset; tail shows the final error / exit context. 1 MB + 256 KB
// retains all diagnostic value at a fixed cost regardless of how much the
// subprocess spews — the on-disk repo_<id>_stderr.log can never exceed
// ~1.3 MB plus the truncation marker (see headTailBuffer.Bytes).
const (
	scancodeFailHeadBytes = 1 << 20   // 1 MB
	scancodeFailTailBytes = 256 << 10 // 256 KB
)

// v0.23.3: wall-clock timeouts on subprocess execution. These
// bound how long a single worker slot can be consumed by one
// repo's scan before ctx-cancel + cmd.Cancel reclaim the slot.
//
// scancodeCloneTimeout: 30 minutes. A `--depth 1` clone normally
// finishes in seconds-to-minutes. 30 min handles slow links and
// large repos (Linux kernel mirrors ~500 MB shallow) without
// allowing a wedged clone to consume a slot indefinitely.
//
// stalePidCheckInterval: 5 minutes. The in-flight cleanup
// goroutine wakes on this cadence to clear NULL-PID lock states
// (the v0.21.0 inconsistency window).
//
// scancodeHealthRecheckInterval: 15 minutes. While the toolchain is
// BROKEN (preflight/remediation exhausted), the paused dispatcher
// re-probes on this cadence and auto-resumes on a passing probe.
const (
	scancodeCloneTimeout = 30 * time.Minute
	// scancodeRunTimeout removed in v0.23.8 — wall-clock timeout
	// now lives on the ScancodeWorker as runTimeoutBase +
	// runTimeoutCap, configurable via collection.scancode_run_timeout_*
	// in aveloxis.json. See NewScancodeWorker.
	stalePidCheckInterval         = 5 * time.Minute
	stalePidLockMaxAge            = 5 * time.Minute
	scancodeHealthRecheckInterval = 15 * time.Minute
)

// ScancodeWorkerOptions configures a ScancodeWorker. v0.27.6 replaces
// NewScancodeWorker's 10-positional-parameter signature — the second
// spawn site (`aveloxis scancode-worker`) made the positional form an
// accident magnet. Zero values fall back to the documented defaults
// (see NewScancodeWorker).
type ScancodeWorkerOptions struct {
	// Workers is the number of concurrent scancode runners. Default 2.
	Workers int
	// StartInterval is the minimum gap between consecutive successful
	// claim starts (v0.21.3 pacing primitive). Default 90s.
	StartInterval time.Duration
	// Cadence is the minimum interval between successive scans of the
	// same repo. Default 180 days.
	Cadence time.Duration
	// CloneDir is the parent directory for per-run shallow clones.
	// Default /tmp/aveloxis-scancode.
	CloneDir string
	// ShutdownGrace is added to ScancodeShutdownBookkeepingGrace to
	// bound how long Run waits, on stop, for the runners' post-kill DB
	// bookkeeping. It cannot let a scan finish — every scan is
	// SIGKILLed at cancel. Default 0 (v0.23.7 contract).
	ShutdownGrace time.Duration
	// RunTimeoutBase / RunTimeoutCap drive the v0.23.8 adaptive
	// per-job wall-clock timeout `min(base * 2^attempts, cap)`.
	// Defaults 2h / 24h.
	RunTimeoutBase time.Duration
	RunTimeoutCap  time.Duration
	// MaxInMemory is scancode's --max-in-memory cap. Default 5000.
	MaxInMemory int
	// TimeoutCapStrikes (v0.27.6) is the consecutive at-cap timeout
	// count after which a repo is sidelined like the v0.21.4
	// 10-strike failure path. Default 3.
	TimeoutCapStrikes int
	// IgnoreGlobs (v0.27.6) are operator path globs passed to
	// scancode as repeated `--ignore <glob>` flags. Empty = none.
	IgnoreGlobs []string
}

// ScancodeWorker is the v0.21.0 decoupled scancode runner.
//
// Lifecycle:
//
//  1. Run() probes for the scancode binary on PATH; if absent,
//     logs once at INFO and returns (no goroutines spawned).
//  2. preflight() runs the toolchain health probe, verifies the
//     typecode-libmagic injection, and — on the corrupt-libmagic
//     fingerprint — runs the v0.27.6 auto-remediation ladder. The
//     resulting health state gates the dispatcher.
//  3. recoverOrphans() runs once — examines every row with a
//     non-null scancode_locked_at and applies the four-state
//     recovery (reboot survivor / live orphan / recoverable corpse
//     / lost run). Cross-host locks are left for the owning host.
//  4. The startup sweep removes stale repo_* clone dirs (no matching
//     own-host lock), aged stderr logs, and leaked preflight temp
//     dirs from the clone directory.
//  5. dispatcher() claims eligible repos (pausing entirely while the
//     toolchain health state is BROKEN) and feeds free runner slots,
//     pacing consecutive starts by startInterval.
//  6. runner() goroutines (workerCount of them) consume jobs and
//     call runOne(). Each runOne does: generated-content skip check,
//     shallow clone, scancode subprocess, outcome classification,
//     ingest, completion/failure/timeout bookkeeping.
//  7. On ctx.Done(), the dispatcher exits immediately and every scan
//     subprocess is SIGKILLed at once (its ctx derives from the
//     worker's — v0.23.3). Run then waits for the claimed jobs' DB
//     BOOKKEEPING (the post-kill lock clear / stamp retry), bounded by
//     shutdownGrace + ScancodeShutdownBookkeepingGrace and signalled
//     through bookkeepingDone; it does NOT wait for the runners
//     themselves, whose clone-dir removal needs no pool. Then the
//     shutdown sweep clears all clone dirs and Run() returns.
type ScancodeWorker struct {
	store         *db.PostgresStore
	logger        *slog.Logger
	workerCount   int
	startInterval time.Duration
	cadence       time.Duration
	cloneDir      string
	shutdownGrace time.Duration

	// bookkeeping counts claimed jobs whose DB bookkeeping is not over
	// (Add at the dispatcher handoff, Done from runOne before the
	// clone-dir removal); bookkeepingDone closes when Run observes it
	// drained after the dispatcher exits — or on any Run return
	// (bookkeepingClose) so a waiter never hangs on a worker that
	// bailed early. See ScancodeShutdownBookkeepingGrace.
	bookkeeping      sync.WaitGroup
	bookkeepingDone  chan struct{}
	bookkeepingClose sync.Once
	// v0.23.8: per-job wall-clock timeout. The runner computes
	// `min(runTimeoutBase * 2^job.TimeoutAttempts, runTimeoutCap)`
	// per job. Pre-v0.23.8 this was a package-level constant
	// (`scancodeRunTimeout = 2 * time.Hour`); now configurable +
	// adaptive so kernel-class repos can stretch up to the cap
	// without operator intervention.
	runTimeoutBase time.Duration
	runTimeoutCap  time.Duration
	// v0.25.2: --max-in-memory cap. Default 5000 matches the
	// pre-v0.25.2 hardcoded value. Production hosts with >100 GB of
	// RAM raise this for faster monorepo scans (kernel-class repos
	// hit the default's spill threshold within seconds).
	maxInMemory int
	// v0.27.6: consecutive at-cap timeout count that sidelines a
	// repo (collection.scancode_timeout_cap_strikes, default 3).
	timeoutCapStrikes int
	// v0.27.6: operator --ignore globs for the scancode CLI.
	ignoreGlobs []string
	// v0.27.6: this machine's os.Hostname(), stamped into
	// scancode_locked_host so a dedicated scancode host and the
	// primary server can share the lock table. Empty when the
	// hostname can't be resolved (liveness adjudication then falls
	// back to pre-v0.27.6 single-host behavior).
	hostname string
	// v0.27.6: toolchain health gate. true = dispatcher claims
	// normally; false = the preflight (or a mid-run re-probe)
	// classified the toolchain BROKEN and the dispatcher claims
	// NOTHING until a probe passes. Defaults to true (fail-open when
	// the probe itself cannot run).
	healthy atomic.Bool
	// healthRecheck is scancodeHealthRecheckInterval, overridable in
	// tests so the pause loop doesn't need 15 real minutes.
	healthRecheck time.Duration
	// typecodeEnv (v0.27.6) holds the discovered typecode-libmagic
	// wheel pair. When set, EVERY scancode subprocess (the preflight
	// probe and every real scan) carries TYPECODE_LIBMAGIC_PATH +
	// TYPECODE_LIBMAGIC_DB_PATH so typecode can never cross-load a
	// wheel .so against a foreign compiled magic DB — the July 2026
	// chaoss.tv root cause. See scancode_remediate.go.
	typecodeEnv atomic.Pointer[typecodeEnvPair]
}

// setTypecodeEnvPair pins the discovered wheel pair for every
// subsequent scancode subprocess.
func (w *ScancodeWorker) setTypecodeEnvPair(pair typecodeEnvPair) {
	w.typecodeEnv.Store(&pair)
}

// scancodeEnv builds the environment for a scancode subprocess: the
// process environment plus — when discovery succeeded — the pinned
// TYPECODE_LIBMAGIC_* pair. Every exec of the scancode binary in this
// worker MUST source its Env from here (tripwired) so no scan can
// silently fall back to typecode's broken plugin resolution.
func (w *ScancodeWorker) scancodeEnv() []string {
	env := os.Environ()
	if pair := w.typecodeEnv.Load(); pair != nil {
		env = append(env, pair.asEnv()...)
	}
	return env
}

// NewScancodeWorker constructs a worker from options. Zero-value
// fields fall back to documented defaults.
//
// v0.23.7-fix: ShutdownGrace 0 means 0 (immediate kill) — the zero
// value is a real setting here, matching the v0.23.7 contract that
// subprocesses outliving aveloxis can't deliver output anyway.
func NewScancodeWorker(store *db.PostgresStore, logger *slog.Logger, opts ScancodeWorkerOptions) *ScancodeWorker {
	if opts.Workers <= 0 {
		opts.Workers = 2
	}
	if opts.StartInterval <= 0 {
		opts.StartInterval = 90 * time.Second
	}
	if opts.Cadence <= 0 {
		opts.Cadence = 180 * 24 * time.Hour
	}
	if opts.CloneDir == "" {
		opts.CloneDir = "/tmp/aveloxis-scancode"
	}
	// v0.23.7: ShutdownGrace = 0 means immediate kill on stop.
	// Don't override.
	if opts.RunTimeoutBase <= 0 {
		opts.RunTimeoutBase = 2 * time.Hour
	}
	if opts.RunTimeoutCap <= 0 {
		opts.RunTimeoutCap = 24 * time.Hour
	}
	// v0.25.2: clamp non-positive MaxInMemory to the safe default
	// even though the config accessor does the same — defense in
	// depth so a direct call from tests or future code can't slip a
	// bogus value through to the subprocess CLI argument.
	if opts.MaxInMemory <= 0 {
		opts.MaxInMemory = 5000
	}
	// v0.27.6: same defense-in-depth clamp as the config accessor —
	// there is no "unlimited at-cap retries" setting.
	if opts.TimeoutCapStrikes <= 0 {
		opts.TimeoutCapStrikes = 3
	}
	hostname, _ := os.Hostname()
	w := &ScancodeWorker{
		store:             store,
		logger:            logger,
		workerCount:       opts.Workers,
		startInterval:     opts.StartInterval,
		cadence:           opts.Cadence,
		cloneDir:          opts.CloneDir,
		shutdownGrace:     opts.ShutdownGrace,
		bookkeepingDone:   make(chan struct{}),
		runTimeoutBase:    opts.RunTimeoutBase,
		runTimeoutCap:     opts.RunTimeoutCap,
		maxInMemory:       opts.MaxInMemory,
		timeoutCapStrikes: opts.TimeoutCapStrikes,
		ignoreGlobs:       opts.IgnoreGlobs,
		hostname:          hostname,
		healthRecheck:     scancodeHealthRecheckInterval,
	}
	w.healthy.Store(true)
	return w
}

// staleLockWindow derives the claim query's silent-corpse fallback
// window from the adaptive-timeout cap: runTimeoutCap + 2h, floored
// at db.ScancodeStaleLockWindow (12h) by the store.
//
// v0.27.6 fix: the window used to be the bare 12h constant while the
// v0.23.8 stretched timeouts legitimately ran to the 24h cap — any
// scan past 12h had its lock treated as stale and a SECOND worker
// claimed the same repo (confirmed interleaving in the June 2026
// production logs). Deriving the window from the cap keeps it above
// the longest legitimate scan no matter how operators tune the cap.
func (w *ScancodeWorker) staleLockWindow() time.Duration {
	return w.runTimeoutCap + 2*time.Hour
}

// BookkeepingDone closes when every claimed job's DB bookkeeping is
// over after shutdown (or when Run returns for any other reason) — the
// scheduler waits for it before closing the pool (pass 39).
func (w *ScancodeWorker) BookkeepingDone() <-chan struct{} { return w.bookkeepingDone }

// Run starts the worker pool and blocks until ctx is done. The scans
// themselves die AT cancellation (cmd.Cancel SIGKILLs each process
// group since v0.23.3); after that Run waits up to
// shutdownGrace + ScancodeShutdownBookkeepingGrace for the claimed
// jobs' DB BOOKKEEPING — the post-kill Wait and the lock clear or
// stamp retry on a background context — then sweeps the clone dir and
// returns. The runner goroutines' own return (which trails their
// clone-dir removal) is deliberately unwaited (passes 38–41).
//
// Probes for the scancode binary at startup; if not installed, logs
// once and returns without spawning goroutines (matches the pre-
// v0.21.0 silent-skip behavior of the inline analysis-phase scan).
func (w *ScancodeWorker) Run(ctx context.Context) {
	defer w.bookkeepingClose.Do(func() { close(w.bookkeepingDone) })
	if _, err := exec.LookPath("scancode"); err != nil {
		w.logger.Info("scancode binary not installed; ScancodeWorker disabled",
			"install_hint", "pipx install scancode-toolkit")
		// Record not-installed so the operator can see why scancode produces no data.
		st, detail := classifyScancodeHealth(false, runtime.GOOS, "", false)
		w.recordScancodeStatus(ctx, st, detail)
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
		"shutdown_grace", w.shutdownGrace.String(),
		"stale_lock_window", w.staleLockWindow().String(),
		"host", w.hostname)

	// Scancode health preflight: one scan of a tiny synthetic input to
	// detect a system-level toolchain failure (corrupt libmagic, etc.),
	// verify the typecode-libmagic injection, auto-remediate the
	// corrupt-libmagic fingerprint (v0.27.6 ladder), and record the
	// outcome in aveloxis_ops.aveloxis_status. The resulting health
	// state GATES the dispatcher — see the dispatcher's comment for
	// why the original awareness-only decision was revised.
	w.preflight(ctx)

	// One-shot recovery pass before the dispatcher starts claiming
	// new work. This is what makes graceful shutdown + kill -9
	// survivable: any orphaned subprocess from a prior run is
	// either adopted (monitor goroutine spawned) or its lock
	// cleared, so the dispatcher's claim query sees a clean state.
	w.recoverOrphans(ctx)

	// v0.27.6 startup sweep: reconcile the clone DIRECTORY against
	// the (post-recovery) lock rows. runOne's defer only removes its
	// clone on clean exits; hard kills leaked clone dirs forever.
	//
	// v0.27.13: runs in the BACKGROUND. On 2026-07-15 kate's first
	// post-upgrade start spent 20+ minutes inside this call — serial
	// os.RemoveAll of multi-GB stale clones on a spinning disk ran
	// 10+ minutes PER DIRECTORY — and the dispatcher below never
	// started, so scancode looked dead. Backgrounding is race-free
	// by construction: sweepScancodeDir takes ONE os.ReadDir
	// snapshot before removing anything, so clone dirs created by
	// jobs the dispatcher claims meanwhile are never in its list,
	// and the keep-set already protects live-locked repos.
	w.logger.Info("scancode startup sweep running in background — dispatcher is NOT blocked",
		"clone_dir", w.cloneDir)
	safego.Go(w.logger, "scancode-startup-sweep", func() { w.sweepCloneDirAtStartup(ctx) })

	// v0.23.3: in-flight orphan recovery — periodic cleanup of
	// stale NULL-PID lock rows (the v0.21.0 inconsistency state).
	// Distinct from recoverOrphans (startup-only): this fires every
	// stalePidCheckInterval while the worker is running, so an
	// inconsistent lock acquired during this serve's runtime gets
	// cleared without waiting for the next aveloxis restart.
	// See checkOwnLocks for the specific patterns it handles.
	safego.Go(w.logger, "scancode-lock-check", func() { w.checkOwnLocks(ctx) })

	jobs := make(chan db.ScancodeJob)

	// Runner pool — each runner consumes one job at a time. The
	// runners' RETURN is deliberately unwaited (pass 40 removed the
	// dead WaitGroup): only their DB bookkeeping gates the shutdown.
	for i := 0; i < w.workerCount; i++ {
		safego.Go(w.logger, "scancode-runner", func() { w.runner(ctx, jobs) })
	}

	// Dispatcher claims jobs and feeds the channel. When ctx is
	// canceled, dispatcher exits, closes the channel, and the
	// runners drain naturally.
	dispatcherDone := make(chan struct{})
	go func() {
		defer safego.Recover(w.logger, "scancode-dispatcher")
		defer close(dispatcherDone)
		w.dispatcher(ctx, jobs)
		close(jobs)
	}()

	<-dispatcherDone

	// Graceful shutdown. The scans themselves die at cancel (cmd.Cancel
	// ctx); what the runners still need is their DB BOOKKEEPING — the
	// post-kill Wait and the lock clear / stamp retry on a Background
	// ctx. Wait for THAT (not for the runners' clone-dir removal, which
	// needs no pool and can take minutes on a spinning disk) on top of
	// the operator's scan grace, and signal it so the scheduler's pool
	// close never lands under it (passes 38/39).
	go func() {
		defer safego.Recover(w.logger, "scancode-bookkeeping-wait")
		w.bookkeeping.Wait()
		w.bookkeepingClose.Do(func() { close(w.bookkeepingDone) })
	}()
	select {
	case <-w.bookkeepingDone:
		w.logger.Info("scancode worker stopped cleanly — every runner finished its shutdown bookkeeping")
	case <-time.After(w.shutdownGrace + ScancodeShutdownBookkeepingGrace):
		w.logger.Warn("scancode worker shutdown grace expired — runners still inside their bookkeeping; their locks are recovered on the next start",
			"grace", w.shutdownGrace.String(), "bookkeeping_allowance", ScancodeShutdownBookkeepingGrace.String())
	}

	// v0.27.6 shutdown sweep: remove ALL repo_* clone dirs and
	// preflight temp dirs. A clone can't outlive the worker usefully
	// — the subprocess that would consume it is dead — and the next
	// startup re-clones from scratch. stderr diagnostics are kept
	// (they age out via the startup sweep's 14-day window).
	w.sweepCloneDirAtShutdown()
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
//
// v0.27.6 health gating: while the toolchain health state is BROKEN
// the dispatcher claims NOTHING — it parks in awaitHealthyToolchain,
// re-probing every 15 minutes, and auto-resumes on a passing probe
// (one WARN on entering the pause, one INFO on exit — the v0.25.0
// distribution-dispatcher pattern). This deliberately REVISES the
// v0.25.x "awareness-only" operator decision: on 2026-06-11 the
// preflight logged SYSTEM-LEVEL FAILURE (corrupt libmagic) at
// startup and this dispatcher then ran 2,473 scans on the broken
// toolchain anyway, producing stderr artifacts up to 9.5 GB and
// wedging every worker. Detection without gating just timestamps
// the damage.
func (w *ScancodeWorker) dispatcher(ctx context.Context, jobs chan<- db.ScancodeJob) {
	// Zero value (Time{}) means "no gap required yet" — the first
	// claim on startup happens immediately. Subsequent claims wait
	// until time.Now() >= nextStartAllowed.
	var nextStartAllowed time.Time

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		// v0.27.6 toolchain health gate — checked BEFORE every claim
		// so a mid-run trip (future re-probes) also pauses claims.
		if !w.healthy.Load() {
			w.awaitHealthyToolchain(ctx)
			if ctx.Err() != nil {
				return
			}
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

		job, err := w.store.ClaimNextScancodeRepo(ctx, w.cadence, w.staleLockWindow())
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
		// The bookkeeping count is taken HERE, before the handoff, so
		// Run's post-dispatcher Wait can never observe a runner between
		// receiving a job and registering it (pass 39).
		w.bookkeeping.Add(1)
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
			relCtx, relCancel := context.WithTimeout(context.Background(), scancodeBestEffortDBTimeout)
			_ = w.store.ClearScancodeLock(relCtx, job.RepoID)
			relCancel()
			w.bookkeeping.Done() // the claim was never handed off (pass 39)
			return
		}
	}
}

// runner consumes jobs and runs them serially. When jobs is closed
// (dispatcher exit), the loop falls through and the goroutine returns
// — unwaited: each job's DB bookkeeping is what Run tracks
// (w.bookkeeping), and the trailing clone-dir removal is covered by
// the startup sweep when a process exit cuts it.
func (w *ScancodeWorker) runner(ctx context.Context, jobs <-chan db.ScancodeJob) {
	for job := range jobs {
		w.runOne(ctx, job)
	}
}

// runOne is the scan pipeline for one repo, decomposed (v0.27.6)
// into phase methods so policy insertions stay clean:
//
//	skip check → prepareClone → executeScan → finishScan
//
// Always clears the scancode_locked_* columns on exit (success,
// skip, or failure) so the row becomes eligible for the next claim
// cycle without waiting for the stale-lock fallback. Errors at any
// phase are logged (with repo identity) and route to the failure-
// recording path. We never crash the runner on a single repo's
// failure — the worker stays up.
func (w *ScancodeWorker) runOne(ctx context.Context, job db.ScancodeJob) {
	// The dispatcher counted this job on w.bookkeeping before the
	// handoff; signal once when the DB bookkeeping is over — before the
	// clone-dir removal, which needs no pool (pass 39).
	bookkeepingDone := sync.OnceFunc(w.bookkeeping.Done)
	defer bookkeepingDone()
	// v0.27.6 generated-content skip policy — decided from the
	// claimed row's repos.languages breakdown BEFORE any clone I/O.
	// pytorch/docs (~6 GB, 100% HTML) burned a 24h worker slot 27×;
	// the skip costs one DB write instead.
	if generatedContentSkip(job.Languages) {
		w.logger.Info("scancode runOne: skipping generated-content repo without cloning",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"skip_reason", scancodeSkipReasonGeneratedContent)
		if err := w.store.MarkScancodeSkipped(ctx, job.RepoID, scancodeSkipReasonGeneratedContent); err != nil {
			if ctx.Err() != nil {
				// Shutdown cut the skip stamp off: clear the lock so the
				// row re-claims instead of waiting out the stale window
				// (pass 40 — the one DB write in runOne the sweep missed).
				w.logger.Info("scancode runOne: skip stamp interrupted by shutdown — lock cleared",
					"repo_id", job.RepoID)
				w.clearLockBestEffort(ctx, job.RepoID)
				return
			}
			w.logger.Warn("scancode runOne: MarkScancodeSkipped failed",
				"repo_id", job.RepoID, "error", err)
		}
		return
	}

	tempDir := filepath.Join(w.cloneDir,
		fmt.Sprintf("repo_%d_%d", job.RepoID, time.Now().UnixNano()))
	defer func() {
		bookkeepingDone() // every DB write is behind us; the removal is filesystem only
		if err := os.RemoveAll(tempDir); err != nil {
			w.logger.Warn("scancode worker failed to remove temp clone",
				"repo_id", job.RepoID, "temp_dir", tempDir, "error", err)
		}
	}()

	if !w.prepareClone(ctx, job, tempDir) {
		return
	}

	ex := w.executeScan(ctx, job, tempDir)
	if ex == nil {
		return
	}

	w.finishScan(ctx, job, ex)
}

// prepareClone creates the temp dir and shallow-clones the repo into
// it. Returns false (with the failure recorded) when either step
// fails.
func (w *ScancodeWorker) prepareClone(ctx context.Context, job db.ScancodeJob, tempDir string) bool {
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		w.logger.Warn("scancode runOne mkdir failed",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return false
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
	//
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
	cloneCmd.WaitDelay = scancodeWaitDelay
	if out, err := cloneCmd.CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			// Shutdown killed the clone: a clean release, not a strike
			// (pass 37 — the strike fed the quadratic claim backoff and,
			// at ten, the 180-day sideline, for a `stop serve`).
			w.logger.Info("scancode runOne: clone interrupted by shutdown — lock cleared, no strike recorded",
				"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName)
			w.clearLockBestEffort(ctx, job.RepoID)
			return false
		}
		w.logger.Warn("scancode runOne git clone failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err, "git_output", string(out))
		w.recordFailureBestEffort(ctx, job.RepoID)
		return false
	}
	return true
}

// scanExecution is the material executeScan hands to finishScan: the
// subprocess result plus the capture buffers the failure-artifact
// writer and outcome classifier consume.
type scanExecution struct {
	pid              int
	outputPath       string
	effectiveTimeout time.Duration
	waitErr          error
	stderrTail       *tailBuffer
	stdoutTail       *tailBuffer
	stderrFull       *headTailBuffer
	stdoutFull       *headTailBuffer
}

// executeScan builds and runs the scancode subprocess against the
// cloned tree: adaptive-timeout computation, bounded output capture,
// process-group setup, PID+boot_id+host lock-state persistence, and
// the wait. Returns nil when the scan could not be started (failure
// already recorded); otherwise returns the execution record with
// waitErr carrying cmd.Wait()'s result for classification.
func (w *ScancodeWorker) executeScan(ctx context.Context, job db.ScancodeJob, tempDir string) *scanExecution {
	scancodePath, err := exec.LookPath("scancode")
	if err != nil {
		// Should never happen — Run() already checked. Defensive.
		w.logger.Warn("scancode runOne: binary not on PATH at run time",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return nil
	}

	outputPath := filepath.Join(tempDir, "results.json")
	procs := 2

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
	// Flag set mirrors the legacy RunScanCode so the JSON output
	// format is identical and the existing parser + ingest path
	// keeps working. v0.27.6: operator scancode_ignore_globs append
	// as --ignore flags (empty by default = byte-identical args).
	cmd := exec.CommandContext(scanCtx, scancodePath,
		scancodeArgs(outputPath, tempDir, procs, w.maxInMemory, w.ignoreGlobs)...)
	// v0.27.6: carry the pinned typecode-libmagic env pair (when
	// discovered) so the scan uses the wheel's matched (.so, magic.mgc)
	// — never the system fallback that produced the offset-invalid
	// warning storms.
	cmd.Env = w.scancodeEnv()

	// Capture stdout + stderr. v0.23.3: we keep BOTH a bounded ring
	// buffer (the tail, for quick triage in the log line) AND a
	// bounded head+tail buffer (for the per-repo stderr file written
	// on failure — v0.25.28 made this bounded; it was an unbounded
	// bytes.Buffer that held 15+ GB in RAM on a corrupt-libmagic repo).
	// io.MultiWriter fans the stream into both. Pre-fix the tail-only
	// approach was almost always dominated by 10+ repetitions of the
	// libmagic warning, hiding the actual error message that scrolled
	// out of the 4 KB window. The operator had to grep the log every
	// time to ask "what actually went wrong"; with the per-repo file
	// the answer (head: onset; tail: exit context) is right there at
	// /tmp/aveloxis-scancode/repo_<id>_stderr.log.
	stderrTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stdoutTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stderrFull := &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes}
	stdoutFull := &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes}
	cmd.Stderr = io.MultiWriter(stderrTail, stderrFull)
	cmd.Stdout = io.MultiWriter(stdoutTail, stdoutFull)

	// v0.23.3: process group cleanup for the scancode subprocess.
	// Same shape as the git clone in prepareClone. Critical here
	// because `scancode --processes 2` spawns a Python multiprocessing
	// pool whose worker processes inherit our stderr/stdout fds.
	// Without pgid kill, cmd.Wait() can block forever waiting for
	// those inherited fds to close even when the lead scancode process
	// has died — the 2026-05-21 wedge pattern. WaitDelay caps the
	// blocking at 10 s as a safety net.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = scancodeWaitDelay

	// cmd.Start() is the key change from the legacy synchronous
	// invocation: we need the OS PID BEFORE the subprocess
	// finishes running, so we can persist it to scancode_locked_pid
	// for crash recovery. A blocking exec call would only return
	// the PID after the subprocess had already exited — useful
	// for post-mortem reaping but no help to the recovery pass.
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			w.logger.Info("scancode runOne: scan not started — shutdown; lock cleared, no strike recorded",
				"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName)
			w.clearLockBestEffort(ctx, job.RepoID)
			return nil
		}
		w.logger.Warn("scancode runOne: cmd.Start() failed",
			"repo_id", job.RepoID, "error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return nil
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
	// The defer covers exactly that gap: when executeScan returns
	// (whether via early-return error path or after cmd.Wait),
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
	if err := w.store.RecordScancodeLockState(ctx, job.RepoID, pid, bootID, outputPath, w.hostname); err != nil {
		// v0.23.3: abort on lock-state failure. The pre-v0.23.3
		// "proceed anyway" path left rows with scancode_locked_at
		// SET and scancode_locked_pid NULL — indistinguishable from
		// a legitimate in-flight scan with delayed PID write. The
		// 2026-05-21 diagnostic showed one production row stuck in
		// this state (ropensci/neotoma), holding a worker slot
		// indefinitely. Better to kill the subprocess and surface
		// the DB-write error than to leave an unrecoverable lock.
		if ctx.Err() != nil {
			// Shutdown, not a lock-state failure: kill the child, clear
			// the lock, no strike (pass 37).
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			_ = cmd.Wait()
			w.logger.Info("scancode runOne: scan interrupted by shutdown before its lock state was recorded — lock cleared, no strike recorded",
				"repo_id", job.RepoID, "pid", pid)
			w.clearLockBestEffort(ctx, job.RepoID)
			return nil
		}
		w.logger.Warn("scancode runOne: failed to record lock state — killing subprocess and aborting",
			"repo_id", job.RepoID, "pid", pid, "error", err)
		// Signal the process group so the scancode subprocess and
		// its Python worker pool all die. WaitDelay caps the wait.
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		_ = cmd.Wait() // reap; we don't care about its exit status
		w.recordFailureBestEffort(ctx, job.RepoID)
		return nil
	}

	w.logger.Info("running ScanCode",
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"path", tempDir, "pid", pid)

	waitErr := cmd.Wait()

	return &scanExecution{
		pid:              pid,
		outputPath:       outputPath,
		effectiveTimeout: effectiveTimeout,
		waitErr:          waitErr,
		stderrTail:       stderrTail,
		stdoutTail:       stdoutTail,
		stderrFull:       stderrFull,
		stdoutFull:       stdoutFull,
	}
}

// writeFailureArtifacts persists the bounded per-repo stderr/stdout
// captures and emits the diagnostic WARN when the subprocess exited
// non-zero. v0.23.3 behavior preserved verbatim: the artifacts are
// written for EVERY non-zero exit, including runs later salvaged by
// the v0.23.4 path — the operator may still want to see the per-file
// errors.
func (w *ScancodeWorker) writeFailureArtifacts(job db.ScancodeJob, ex *scanExecution) {
	// v0.23.3: write a per-repo stderr file so the operator can
	// `less /tmp/aveloxis-scancode/repo_<id>_stderr.log` instead of
	// reading only the 4 KB log-line tail. v0.25.28: the capture is now
	// bounded (head + tail) — a corrupt host libmagic can make a large
	// repo emit 15+ GB, which the pre-fix unbounded bytes.Buffer held
	// entirely in RAM. Best-effort: if the file write fails, the log
	// line's stderr_tail is the fallback.
	stderrBytes := ex.stderrFull.Bytes()
	stderrPath := filepath.Join(w.cloneDir, fmt.Sprintf("repo_%d_stderr.log", job.RepoID))
	stderrWriteErr := os.WriteFile(stderrPath, stderrBytes, 0o644)
	stdoutPath := ""
	if ex.stdoutFull.Total() > 0 {
		stdoutPath = filepath.Join(w.cloneDir, fmt.Sprintf("repo_%d_stdout.log", job.RepoID))
		_ = os.WriteFile(stdoutPath, ex.stdoutFull.Bytes(), 0o644)
	}
	logArgs := []any{
		"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
		"error", ex.waitErr, "pid", ex.pid,
		"full_stderr_at", stderrPath,
		"stderr_bytes", ex.stderrFull.Total(),
		"stderr_tail", ex.stderrTail.String(),
	}
	// v0.25.28: when the captured stderr is dominated by the libmagic
	// corruption fingerprint, say so explicitly. This is NOT a per-repo or
	// per-file problem — it's the host's magic database, the same condition
	// the startup preflight records in aveloxis_ops.aveloxis_status. Surfacing
	// it on the per-repo failure removes the "why does THIS repo choke?"
	// confusion (answer: it's large enough that the per-file warning spam
	// blows past the wall-clock timeout).
	if countLibmagicWarnings(string(stderrBytes)) >= scancodePreflightRepeatN {
		logArgs = append(logArgs,
			"likely_cause", "corrupt host libmagic (offset-invalid warning spam) — not a per-repo issue; see aveloxis_ops.aveloxis_status('scancode') and the scancode preflight; repair the host with 'apt-get install --reinstall libmagic-mgc libmagic1 file' or 'aveloxis upgrade-tools'")
	}
	if stdoutPath != "" {
		logArgs = append(logArgs, "full_stdout_at", stdoutPath)
	}
	if stderrWriteErr != nil {
		logArgs = append(logArgs, "stderr_file_write_error", stderrWriteErr)
	}
	w.logger.Warn("scancode runOne: scancode subprocess failed", logArgs...)
}

// finishScan classifies the finished subprocess (via the v0.27.6
// consolidated outcome classifier) and routes to the exact
// pre-v0.27.6 bookkeeping:
//
//   - success / salvaged (v0.23.4) → ingest + MarkScancodeComplete
//   - shutdown (ctx done — the same "signal: killed" text) → lock
//     cleared, nothing recorded (pass 36)
//   - timeout (v0.23.8 "signal: killed") → RecordScancodeTimeout,
//     never the 10-strike counter; the v0.27.6 at-cap strike
//     sideline is the one addition
//   - anything else → recordFailureBestEffort
func (w *ScancodeWorker) finishScan(ctx context.Context, job db.ScancodeJob, ex *scanExecution) {
	if ex.waitErr != nil && ctx.Err() != nil {
		// Shutdown killed the scan (scanCtx derives from ctx). The
		// SIGKILL text reads as a wall-clock timeout to the classifier,
		// so this used to record a timeout strike, print a false
		// "stretched timeout" line (and a false sideline at the cap),
		// and write failure artifacts — three misleading lines per
		// in-flight scan on every stop (pass 36). It is neither: clear
		// the lock and let the repo re-claim.
		w.logger.Info("scancode runOne: scan interrupted by shutdown — lock cleared, no strike recorded",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName, "pid", ex.pid)
		w.clearLockBestEffort(ctx, job.RepoID)
		return
	}
	if ex.waitErr != nil {
		w.writeFailureArtifacts(job, ex)
	}

	outcome := classifyScanOutcome(ex.waitErr, ex.outputPath,
		ex.effectiveTimeout, w.runTimeoutBase, w.runTimeoutCap,
		job.TimeoutAttempts, w.timeoutCapStrikes)

	switch outcome.kind {
	case outcomeTimeout:
		// v0.23.8: distinguish wall-clock-timeout failures from
		// genuine scancode failures. cmd.Cancel from scanCtx timeout
		// fires `syscall.Kill(-pid, SIGKILL)`, and cmd.Wait() then
		// returns an *exec.ExitError whose Error() string is exactly
		// "signal: killed". Route to RecordScancodeTimeout
		// (increments scancode_timeout_attempts so the next attempt
		// gets a bigger timeout) rather than RecordScancodeFailure
		// (which would advance the 10-strike sideline counter
		// against a kernel-class repo that just needs more time).
		w.logger.Info("scancode runOne: wall-clock timeout fired; row's next attempt will use a stretched timeout",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"prior_timeout_attempts", job.TimeoutAttempts,
			"timeout_used", ex.effectiveTimeout.String(),
			"at_cap_strikes", outcome.capStrikes)
		if outcome.sideline {
			// v0.27.6: N consecutive timeouts AT the cap — no bigger
			// timeout is coming; sideline exactly like the v0.21.4
			// 10-strike failure path (the June 2026 pytorch/docs /
			// WHO/smart-html 27-claim spin loop ends here). The
			// diagnostic trail stays in scancode_timeout_attempts.
			w.logger.Warn("scancode runOne: sidelining repo after consecutive at-cap timeouts — cadence gate will exclude it",
				"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
				"at_cap_strikes", outcome.capStrikes,
				"strikes_threshold", w.timeoutCapStrikes,
				"timeout_cap", w.runTimeoutCap.String())
		}
		if recErr := w.store.RecordScancodeTimeout(ctx, job.RepoID, outcome.sideline); recErr != nil {
			w.logger.Warn("scancode runOne: RecordScancodeTimeout failed",
				"repo_id", job.RepoID, "error", recErr)
		}
		return

	case outcomeFailure:
		w.recordFailureBestEffort(ctx, job.RepoID)
		return

	case outcomeSalvaged:
		// v0.23.4: scancode-toolkit-mini with `--quiet` returns exit 1
		// when any individual file fails to scan (e.g., a malformed
		// PDF that pdfminer crashes on), but the JSON output is fully
		// written with the successful scans intact. Treat as success:
		// log the per-file errors and fall through to ingest.
		w.logger.Warn("scancode runOne: salvaged scan output despite subprocess exit 1",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"files_count", outcome.salvagedFilesCount,
			"header_errors", outcome.salvagedHeaderNotes,
			"pid", ex.pid)
	}

	// Parse + ingest. The legacy parser in scancode.go takes a
	// localPath and re-invokes the binary; here we already have
	// the output file, so we parse it directly via the helper.
	version, err := ingestScancodeOutput(ctx, w.store, job.RepoID, ex.outputPath, w.logger)
	if err != nil && ctx.Err() != nil {
		// Shutdown landed inside the ingest: the output file is intact
		// and the ingest is idempotent — clear the lock, no strike, the
		// repo re-claims (pass 38).
		w.logger.Info("scancode runOne: ingest interrupted by shutdown — lock cleared, no strike recorded",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName)
		w.clearLockBestEffort(ctx, job.RepoID)
		return
	}
	if err != nil {
		w.logger.Warn("scancode runOne: ingest failed",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName,
			"error", err)
		w.recordFailureBestEffort(ctx, job.RepoID)
		return
	}

	err = w.store.MarkScancodeComplete(ctx, job.RepoID, version)
	if err != nil && ctx.Err() != nil {
		// The scan is ingested; only the stamp was cut off by shutdown.
		// Retry it once on a bounded Background ctx (cheap, idempotent);
		// if that fails too, clear the lock so the next claim re-runs.
		stampCtx, cancel := context.WithTimeout(context.Background(), scancodeBestEffortDBTimeout)
		defer cancel()
		if serr := w.store.MarkScancodeComplete(stampCtx, job.RepoID, version); serr != nil {
			w.logger.Info("scancode runOne: completion stamp cut off by shutdown — lock cleared, the ingested scan is re-run next claim",
				"repo_id", job.RepoID, "error", serr)
			w.clearLockBestEffort(ctx, job.RepoID)
			return
		}
		w.logger.Info("scancode worker complete (completion stamp retried after shutdown)",
			"repo_id", job.RepoID, "owner", job.RepoOwner, "repo", job.RepoName, "version", version)
		return
	}
	if err != nil {
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
		"version", version, "pid", ex.pid)
}

// sweepCloneDirAtStartup reconciles the clone directory against the
// post-recovery lock rows (v0.27.6). A repo_* clone dir is kept ONLY
// while a live lock row for THIS host references its repo (a live
// orphan adopted by recoverOrphans may still be writing into it);
// everything else is a leak from a hard kill. Failure-diagnostic
// logs age out after scancodeStderrLogMaxAge; leaked preflight temp
// dirs are removed unconditionally.
func (w *ScancodeWorker) sweepCloneDirAtStartup(ctx context.Context) {
	rows, err := w.store.ListLockedScancodeRows(ctx)
	if err != nil {
		w.logger.Warn("scancode startup sweep: ListLockedScancodeRows failed — skipping sweep",
			"error", err)
		return
	}
	keep := make(map[int64]bool, len(rows))
	for _, r := range rows {
		// Own-host locks protect their clone dirs. Empty-host locks
		// (recorded by pre-v0.27.6 binaries) are treated as
		// possibly-ours out of caution. Cross-host locks do NOT
		// protect local dirs — a dir on OUR disk was created by OUR
		// workers, and if another host now owns the repo's lock, our
		// copy is stale by definition.
		if r.LockedHost == "" || r.LockedHost == w.hostname {
			keep[r.RepoID] = true
		}
	}
	dirs, logs := sweepScancodeDir(w.logger, w.cloneDir,
		func(repoID int64) bool { return keep[repoID] },
		scancodeStderrLogMaxAge, time.Now())
	if dirs+logs > 0 {
		w.logger.Info("scancode startup sweep complete",
			"removed_dirs", dirs, "removed_logs", logs, "clone_dir", w.cloneDir)
	}
}

// sweepCloneDirAtShutdown removes ALL repo_* clone dirs and preflight
// temp dirs (v0.27.6). Best-effort — races with runOne's own
// RemoveAll defers are harmless (RemoveAll on a missing path is nil).
// stderr diagnostics are deliberately kept; they age out via the
// startup sweep.
func (w *ScancodeWorker) sweepCloneDirAtShutdown() {
	dirs, _ := sweepScancodeDir(w.logger, w.cloneDir, nil, 0, time.Now())
	if dirs > 0 {
		w.logger.Info("scancode shutdown sweep complete",
			"removed_dirs", dirs, "clone_dir", w.cloneDir)
	}
}

// scancodeWaitDelay bounds how long cmd.Wait blocks after the process
// group is SIGKILLed (pipes held by stragglers); scancodeBestEffortDBTimeout
// bounds each Background-ctx bookkeeping write a runner issues after its
// ctx is dead. ScancodeShutdownBookkeepingGrace is the worst case of
// one runner's DB bookkeeping once the scan is killed: the post-kill
// Wait plus up to TWO best-effort writes (the completion-stamp retry
// and then the lock clear). Run signals BookkeepingDone when every
// runner's DB work is over — the clone-dir removal that follows needs
// no pool and is not waited for — and the scheduler waits for that
// signal (bounded by the operator's scan grace + this allowance)
// before it closes the pool (passes 38/39: with the default grace of 0
// the worker returned immediately and the scheduler closed the pool
// under the runners' lock clears, so every "lock cleared" on stop was a
// WARN and a next-start recovery instead).
const (
	scancodeWaitDelay           = 10 * time.Second
	scancodeBestEffortDBTimeout = 30 * time.Second

	ScancodeShutdownBookkeepingGrace = scancodeWaitDelay + 2*scancodeBestEffortDBTimeout
)

// clearLockBestEffort attempts to clear the lock and logs if it
// fails. Used by the recovery paths and (v0.28.18) every runOne
// SHUTDOWN branch — a scan cut off by `stop serve` is a clean release,
// pinned by the enclosing-branch test in
// scancode_failure_backoff_test.go. FAILURE paths still route through
// recordFailureBestEffort so the failure-counter + last_failed_at
// columns update and the backoff gate applies (v0.21.4). The
// dispatcher's claimed-but-undispatched release inlines its own
// bounded ClearScancodeLock.
func (w *ScancodeWorker) clearLockBestEffort(ctx context.Context, repoID int64) {
	// Try the live ctx first; fall back to a BOUNDED background ctx if
	// canceled (v0.27.40: a hung DB at shutdown must not block forever).
	useCtx := ctx
	if ctx.Err() != nil {
		var cancel context.CancelFunc
		useCtx, cancel = context.WithTimeout(context.Background(), scancodeBestEffortDBTimeout)
		defer cancel()
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
		// v0.27.40: bounded fallback — see clearLockBestEffort.
		var cancel context.CancelFunc
		useCtx, cancel = context.WithTimeout(context.Background(), scancodeBestEffortDBTimeout)
		defer cancel()
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
// v0.27.6: the four-state decision only applies to locks whose
// scancode_locked_host matches THIS machine (or is empty — a
// pre-v0.27.6 row). A (pid, boot_id) pair recorded on another host
// is meaningless here: PID 12345 on the dedicated scancode host
// trivially collides with an unrelated process on the primary
// server, and the boot_id files differ per machine. Cross-host
// locks are left alone — the owning host's recovery pass (or the
// claim query's derived stale-lock age window) handles them.
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
// RecordScancodeLockState failure (see executeScan) so new
// occurrences shouldn't happen. But existing inconsistent rows from
// pre-v0.23.3 runs need cleanup, and this loop is a defense in depth
// for any failure path that might still produce the state.
//
// Does NOT recover wedged workers (cmd.Wait() blocked indefinitely
// on a live subprocess). That's prevented at the source by the
// scancodeCloneTimeout and the adaptive run-timeout
// context-with-timeout wraps around the cmd.CommandContext calls —
// runOne's subprocesses have a hard wall-clock cap, so worker slots
// can't stay consumed beyond it.
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
		"count", len(rows), "current_boot_id", currentBootID, "host", w.hostname)

	for _, r := range rows {
		// v0.27.6: cross-host locks are not ours to adjudicate —
		// (pid, boot_id) liveness only means something on the
		// machine that recorded it. Leave the row for the owning
		// host's recovery pass; the claim query's derived stale-lock
		// age window is the fallback if that host never comes back.
		if r.LockedHost != "" && w.hostname != "" && r.LockedHost != w.hostname {
			w.logger.Info("scancode recover: cross-host lock — leaving for owning host (age-window fallback applies)",
				"repo_id", r.RepoID, "owner", r.RepoOwner, "repo", r.RepoName,
				"locked_host", r.LockedHost, "our_host", w.hostname)
			continue
		}

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
			safego.Go(w.logger, "scancode-orphan-monitor", func() { w.monitorOrphan(ctx, r) })
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

// readBootID returns the kernel boot UUID on Linux and "" elsewhere;
// v0.28.18: one shared reader in internal/hostid (the migrate-time
// mailing-list lock liveness decision reads the same value).
func readBootID() string {
	return hostid.BootID()
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
// useful data (files_count > 0). The outcome classifier uses this
// from the cmd.Wait() error branch to distinguish:
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
