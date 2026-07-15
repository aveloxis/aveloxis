// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.21.0 — ScancodeWorker source-contract tests.
//
// These tests pin the structural invariants of internal/collector/
// scancode_worker.go so a future refactor can't accidentally regress
// the architectural decisions documented in
// docs/architecture/scancode.md.
//
// The 2026-05-14 production incident showed 177 of 180 collection
// workers parked at internal/collector/scancode.go:114 (the 2-slot
// semaphore) for 7+ hours. The new architecture moves scancode out
// of the per-job pipeline entirely; these tests ensure no future
// change re-introduces the coupling.

func readScancodeWorkerSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scancode_worker.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// scancodeMethodBody extracts one top-level func's body from src.
// v0.27.6 decomposed runOne into phase methods (prepareClone /
// executeScan / finishScan), so the per-behavior pins below anchor on
// the phase that owns the behavior instead of one giant runOne slice.
func scancodeMethodBody(t *testing.T, src, decl string) string {
	t.Helper()
	idx := strings.Index(src, decl)
	if idx < 0 {
		t.Fatalf("cannot find %q — the v0.27.6 runOne decomposition renamed or removed it", decl)
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	return tail[:1+endRel]
}

func TestScancodeWorkerHasRunMethod(t *testing.T) {
	src := readScancodeWorkerSource(t)
	if !strings.Contains(src, "func (w *ScancodeWorker) Run(ctx context.Context)") {
		t.Error("ScancodeWorker must declare Run(ctx context.Context). The scheduler invokes this as a goroutine alongside the main collection loop.")
	}
}

func TestScancodeWorkerCallsRecoverBeforeDispatcher(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find Run method")
	}
	body := src[idx:]
	recoverIdx := strings.Index(body, "w.recoverOrphans(")
	dispatchIdx := strings.Index(body, "w.dispatcher(")
	if recoverIdx < 0 || dispatchIdx < 0 || recoverIdx > dispatchIdx {
		t.Error("Run() must call w.recoverOrphans(ctx) BEFORE w.dispatcher(...). If a previous aveloxis serve exited with in-flight scancode subprocesses (kill -9 or OOM), the recovery pass must adopt or clear those orphans before new claims happen — otherwise the dispatcher would see locked rows and skip them indefinitely.")
	}
}

func TestRunOneSplitsStartFromWait(t *testing.T) {
	src := readScancodeWorkerSource(t)
	// v0.27.6: the subprocess phase of the runOne pipeline lives in
	// executeScan.
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) executeScan(")

	if !strings.Contains(body, "cmd.Start()") {
		t.Error("executeScan must call cmd.Start() (not cmd.Run()). We need the OS PID before the scan starts so we can persist it to scancode_locked_pid; cmd.Run() doesn't return until the subprocess exits.")
	}
	if !strings.Contains(body, "cmd.Wait()") {
		t.Error("executeScan must call cmd.Wait() after cmd.Start() to wait for scan completion.")
	}
	if strings.Contains(body, "cmd.Run()") {
		t.Error("executeScan must NOT call cmd.Run() — use cmd.Start() + cmd.Wait() so the PID is captured before waiting. The split is what makes crash recovery work: if aveloxis crashes between Start() and Wait(), the recovery pass sees scancode_locked_pid and can decide whether to adopt or clear.")
	}
}

func TestRunOnePersistsPidAndBootId(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) executeScan(")

	// The executeScan body must reach for store methods that persist
	// PID + boot_id + host BEFORE Wait() starts.
	if !strings.Contains(body, "RecordScancodeLockState(") {
		t.Error("executeScan must call store.RecordScancodeLockState(ctx, repoID, pid, bootID, outputPath, host) AFTER cmd.Start() but BEFORE cmd.Wait(). Without persisting the (pid, boot_id, output_path, host) tuple, a crash between Start and Wait leaves an orphan with no row-level state for the recovery pass to find.")
	}
	// v0.27.6: the lock state must carry this machine's hostname so a
	// dedicated scancode host and the primary server can share the
	// table — (pid, boot_id) liveness is only adjudicable on the
	// machine that recorded it.
	if !strings.Contains(body, "w.hostname") {
		t.Error("executeScan must pass w.hostname into RecordScancodeLockState — without the host column, recoverOrphans on machine A would adjudicate PIDs recorded on machine B (trivial PID collisions).")
	}
}

func TestRunOneClearsLockOnSuccess(t *testing.T) {
	src := readScancodeWorkerSource(t)
	if !strings.Contains(src, "MarkScancodeComplete(") {
		t.Error("runOne must call store.MarkScancodeComplete(ctx, repoID, version) on successful scan completion. This sets scancode_last_run = NOW(), scancode_version = $version, and clears all 4 lock columns in one UPDATE.")
	}
}

func TestRunOneClearsLockOnFailure(t *testing.T) {
	src := readScancodeWorkerSource(t)
	if !strings.Contains(src, "ClearScancodeLock(") {
		t.Error("runOne must call store.ClearScancodeLock(ctx, repoID) on failure paths (clone error, scancode crash, output parse error). Without this, a failure leaves scancode_locked_at populated and the row gets stuck waiting for the 12h stale-lock fallback before becoming re-eligible.")
	}
}

func TestClaimUsesForUpdateSkipLocked(t *testing.T) {
	// The claim SQL lives in the db package, but the worker's
	// store method is named ClaimNextScancodeRepo.
	data, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "FOR UPDATE SKIP LOCKED") {
		t.Error("ClaimNextScancodeRepo's SQL must use FOR UPDATE SKIP LOCKED on the candidate CTE. Without it, concurrent claim attempts from multiple worker dispatchers could double-claim the same row.")
	}
}

func TestClaimGatesOnLastCollected(t *testing.T) {
	data, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "last_collected IS NOT NULL") {
		t.Error("ClaimNextScancodeRepo must filter on q.last_collected IS NOT NULL — per the v0.21.0 plan, scancode does NOT run on repos that have never been collected. Newly added repos collect basic metrics first, then enter scancode eligibility.")
	}
}

func TestClaimGatesOnCadenceAndStaleLock(t *testing.T) {
	data, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// Cadence check: never-scanned OR past cadence window.
	if !strings.Contains(src, "scancode_last_run IS NULL") {
		t.Error("ClaimNextScancodeRepo must filter on scancode_last_run IS NULL OR scancode_last_run < NOW() - cadence")
	}
	// 12-hour stale-lock filter so a row whose previous runner
	// crashed silently can be re-claimed (the recovery pass
	// handles the noisy case; this is the silent-corpse fallback).
	if !strings.Contains(src, "scancode_locked_at IS NULL") {
		t.Error("ClaimNextScancodeRepo must filter rows whose scancode_locked_at is NULL OR older than the stale-lock window so an interrupted prior run doesn't permanently lock the row")
	}
}

func TestClaimExcludesArchivedRepos(t *testing.T) {
	data, err := os.ReadFile("../db/scancode_worker_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "repo_archived") {
		t.Error("ClaimNextScancodeRepo must exclude archived repos via COALESCE(repo_archived, FALSE) = FALSE. The supporting index (idx_repos_scancode_due) is partial on this predicate, so the WHERE clause must match exactly for the planner to use it.")
	}
}

func TestScancodeWorkerSkipsWhenBinaryMissing(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find Run method")
	}
	body := src[idx:]
	if !strings.Contains(body, "exec.LookPath") {
		t.Error("ScancodeWorker.Run must probe exec.LookPath(\"scancode\") at startup. When the binary is absent, the worker must log ONE INFO line and exit without spawning goroutines. Matches the existing pre-v0.21.0 behavior of silently skipping the analysis phase when scancode isn't installed.")
	}
}

// v0.21.3 — TestDispatcherEnforcesMinimumStartGap replaces the
// pre-v0.21.3 TestDispatcherUsesStartTicker. The old design used
// time.NewTicker(startInterval) as the dispatcher's primary loop
// gate, which conflated "minimum gap between starts" with
// "maximum throughput". At 90 s/tick with 7 workers and ~3-min
// average scan times, that capped first-pass throughput at
// 40 claims/hour — 6 of 7 workers sat idle. On a 40K-repo fleet
// this meant a first scancode pass took ~42 days when actual
// worker capacity could finish it in ~12 days.
//
// New design: the dispatcher polls quickly and gates each new
// claim on a `nextStartAllowed` deadline. When all workers are
// busy the unbuffered jobs channel blocks the send, naturally
// preventing over-claiming. When workers free up rapidly the
// dispatcher claims continuously, spaced only by the
// startInterval gap. The semantic of "minimum gap between
// individual starts" is preserved (still prevents clone-bandwidth
// bursts on restart), but the throughput cap is removed.
func TestDispatcherEnforcesMinimumStartGap(t *testing.T) {
	src := readScancodeWorkerSource(t)
	// Must NOT use ticker as the primary loop gate. (A short
	// poll-interval ticker for idle-when-queue-empty backoff is
	// fine and won't false-match this needle.)
	if strings.Contains(src, "time.NewTicker(w.startInterval)") {
		t.Error("dispatcher must NOT use time.NewTicker(w.startInterval) as its primary gate. That design caps fleet-wide claim rate at one-per-tick regardless of worker availability — exactly the v0.21.3 regression this test pins against. Use a `nextStartAllowed` time.Time gate around each successful claim instead.")
	}
	// Must reference a deadline variable that tracks "when the
	// next start is allowed". The variable name is pinned loosely
	// (any of nextStartAllowed / nextStart / startGate) so a
	// future refactor can rename without breaking this test as
	// long as the semantic is preserved.
	if !strings.Contains(src, "nextStartAllowed") &&
		!strings.Contains(src, "nextStart") &&
		!strings.Contains(src, "startGate") {
		t.Error("dispatcher must declare a deadline variable (nextStartAllowed / nextStart / startGate) that enforces the minimum gap between successful claims. Without it, the dispatcher either over-claims or throttles to one-per-poll.")
	}
	// Must still reference w.startInterval (it's the gap, just
	// not the loop ticker).
	if !strings.Contains(src, "w.startInterval") {
		t.Error("dispatcher must reference w.startInterval — even with the new design, the operator-configured gap is the rate limit applied between successful starts.")
	}
}

// TestDispatcherClaimsAheadOfNextStartGate is a behavioral pin
// against the pre-v0.21.3 regression. The new dispatcher must be
// able to claim multiple repos rapidly (back-to-back successful
// starts spaced only by startInterval) — NOT capped at one
// per startInterval polling cycle.
//
// We can't easily exercise the live worker with a real DB here
// (that's the integration tier), but we CAN verify the dispatcher
// source contains the rapid-fire pattern: a `for { … }` loop
// where the gate is computed AFTER a successful send, not as the
// loop's primary scheduling primitive.
func TestDispatcherClaimsAheadOfNextStartGate(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) dispatcher(")
	if idx < 0 {
		t.Fatal("cannot find dispatcher")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// The gate update must happen AFTER a successful channel
	// send (the `case jobs <- *job:` arm), not before the claim.
	// If the gate update appears before the claim attempt, the
	// dispatcher would still throttle to one-per-interval even
	// when workers are idle.
	jobsSendIdx := strings.Index(body, "case jobs <- *job:")
	gateUpdateIdx := strings.Index(body, "nextStartAllowed = time.Now().Add(w.startInterval)")
	if jobsSendIdx < 0 {
		t.Error("dispatcher must do a `case jobs <- *job:` channel send to feed a runner")
	}
	if gateUpdateIdx < 0 {
		t.Error("dispatcher must stamp nextStartAllowed = time.Now().Add(w.startInterval) after a successful start")
	}
	if jobsSendIdx >= 0 && gateUpdateIdx >= 0 && gateUpdateIdx < jobsSendIdx {
		t.Error("nextStartAllowed must be stamped AFTER the successful send, not before the claim. Otherwise the dispatcher throttles to one-per-startInterval regardless of worker availability — the v0.21.3 regression this test pins against.")
	}

	// The unbuffered channel `make(chan db.ScancodeJob)` (declared
	// in Run, not dispatcher) provides the back-pressure when all
	// workers are busy. If a future refactor switches to a buffered
	// channel, the dispatcher could over-claim. Pin the
	// unbuffered shape here.
	runStart := strings.Index(src, "func (w *ScancodeWorker) Run(")
	if runStart < 0 {
		t.Fatal("cannot find Run")
	}
	runTail := src[runStart:]
	runEnd := strings.Index(runTail[1:], "\nfunc ")
	if runEnd < 0 {
		runEnd = len(runTail) - 1
	}
	runBody := runTail[:1+runEnd]
	if !strings.Contains(runBody, "make(chan db.ScancodeJob)") {
		t.Error("Run must create the jobs channel as UNBUFFERED `make(chan db.ScancodeJob)`. A buffered channel would let the dispatcher over-claim past the worker pool size, locking more rows than runners can actually process.")
	}
}

// TestRecoverOrphansSkipsCrossHostLocks (v0.27.6) pins the
// dedicated-host safety rule: (pid, boot_id) liveness is only
// adjudicable on the machine that recorded it — PID 12345 on the
// scancode host trivially collides with an unrelated process on the
// primary server. Cross-host locks must be LEFT for the owning host;
// only the claim query's derived stale-lock age window may reclaim
// them.
func TestRecoverOrphansSkipsCrossHostLocks(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) recoverOrphans(")
	crossHost := strings.Index(body, "r.LockedHost != w.hostname")
	bootCheck := strings.Index(body, "r.LockedBootID != currentBootID")
	if crossHost < 0 {
		t.Fatal("recoverOrphans must compare each lock's LockedHost against w.hostname and SKIP cross-host locks — adjudicating another machine's PIDs clears locks for scans that are still running there")
	}
	if bootCheck >= 0 && crossHost > bootCheck {
		t.Error("the cross-host skip must run BEFORE the (boot_id, pid) four-state decision — otherwise a cross-host lock is mis-handled as a reboot survivor (different machines always have different boot_ids)")
	}
	// Empty-host rows (locked by pre-v0.27.6 binaries) keep the
	// legacy single-host adjudication.
	if !strings.Contains(body, `r.LockedHost != ""`) {
		t.Error("recoverOrphans must treat empty-host locks (pre-v0.27.6 rows) as own-host so single-host fleets keep their recovery behavior through the upgrade")
	}
}

func TestGracefulShutdownUsesWaitGroupAndGrace(t *testing.T) {
	src := readScancodeWorkerSource(t)
	idx := strings.Index(src, "func (w *ScancodeWorker) Run(")
	if idx < 0 {
		t.Fatal("cannot find Run method")
	}
	body := src[idx:]
	if !strings.Contains(body, "sync.WaitGroup") && !strings.Contains(src, "sync.WaitGroup") {
		t.Error("Run must use sync.WaitGroup to track in-flight runner goroutines. On ctx.Done(), Run must wait for all runners to finish (up to ShutdownGrace) before returning so the scheduler's pool-close happens AFTER scancode runners have updated DB state.")
	}
	if !strings.Contains(src, "shutdownGrace") {
		t.Error("Run must reference w.shutdownGrace. Without a grace budget, a 4-hour Linux-kernel scan would block `aveloxis stop` for the full duration.")
	}
}
