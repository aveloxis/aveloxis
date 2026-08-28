// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
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

// The shutdown contract, restated for passes 38–48: ScancodeWorker.Run,
// on shutdown, waits for the claimed jobs' DB BOOKKEEPING
// (w.bookkeeping, signalled through w.bookkeepingDone) — not for the
// runner goroutines, whose clone-dir removal takes minutes on a
// spinning disk and needs no pool — and that wait is BOUNDED by
// w.shutdownGrace + ScancodeShutdownBookkeepingGrace so `aveloxis stop`
// can never hang.
//
// Seventeen reviewer escapes across passes 43–48 taught this pin its
// shape. Each preserved whatever the pin was counting, naming or
// spelling; each was found by APPLYING the mutation, never by reading:
//
//	43   extract the wait into a helper, block Run on a channel that
//	     helper closes
//	45a  make the drain SYNCHRONOUS (counts unchanged; the signal is
//	     already closed when the select runs, so the bound is dead)
//	45b  a one-hop helper w.awaitRunners()
//	46a  delete the two characters `go ` — still "inside a FuncLit",
//	     synchronous again
//	46b  a *sync.WaitGroup PARAMETER (receiver spelled `wg`)
//	46c  a `default:` arm
//	46d  the helper in a SIBLING FILE
//	47a  `<-w.bookkeepingDone` ahead of the select — the allowlist
//	     named the signal, so blocking on it anywhere was legal
//	47b  `<-drained`, a channel Run makes and closes, closed by the
//	     DRAIN goroutine: a perfectly derived join that IS the wait
//	47c  a third select arm on a pre-closed channel — no `default:`
//	47d  rename ONE receiver `w`→`s`: the walk dead-ends below it
//	47e  name a WaitGroup `cmd` beside any exec.Command
//	48a  `time.After(w.shutdownGrace)` — the allowance dropped. The
//	     operand check scanned Run's whole BODY, and both operands
//	     already appear in the WARN log line inside the very same
//	     arm, so it could never fail. With the shipped default grace
//	     of 0 that arm is time.After(0): the select falls through and
//	     the pool close lands under the runners' lock clears.
//	48b  `for range w.bookkeepingDone {}` — 47a respelled. A range is
//	     not an *ast.UnaryExpr, so the receive sweep never saw it.
//	48c  `joinRunners := w.awaitRunnerDrain; joinRunners()` — a method
//	     VALUE is not a call the walk could resolve, so the runner
//	     join pass 40 deleted came straight back.
//	48d  one unrelated `go w.watchdog()` that merely MENTIONS the
//	     drain laundered a synchronous `w.drainBookkeeping()` call:
//	     async-ness was a property of the DECLARATION, never of the
//	     call site Run actually used.
//	48e  the pin RECOMMENDED context.WithTimeout(ctx, …) as a legal
//	     bound — but ctx being canceled is WHY Run is here, so that
//	     child is born expired (measured: the arm fired after 1.1µs
//	     instead of the grace). Following the test's own advice
//	     reduced the wait to zero.
//
// The rules below are therefore stated at the level the contract
// actually lives at — expressions and call sites, not text and
// declarations:
//
//  1. No SYNCHRONOUS path from Run to the drain. Not "the drain's
//     declaration appears somewhere async" (48d) — every path Run can
//     take to reach it must cross a `go`/safego.Go edge.
//  2. The deadline arm's ARGUMENT is w.shutdownGrace +
//     ScancodeShutdownBookkeepingGrace, compared as an expression
//     (48a). A context bound must be built from Background/TODO, never
//     from the canceled worker ctx (48e).
//  3. Between launching the drain and entering the select, Run
//     performs no operation that can block — receive, send, range or
//     lock (47a, 47b, 48b) — and the select has exactly two arms, the
//     signal and that deadline (47c, 46c).
//  4. No `.Wait()` on any receiver anywhere Run reaches, except
//     w.bookkeeping and a local bound to exec.Command/CommandContext
//     and never rebound or shadowed (46b, 47e, 48f). The walk spans
//     the package, resolves a method call whatever its receiver is
//     spelled (46d, 47d), and follows method and func VALUES (48c).
func TestGracefulShutdownWaitsForBookkeepingWithinGrace(t *testing.T) {
	fset, files := scancodePackageFiles(t)
	run := scancodeMethodDecl(t, files, "Run")
	byName := scancodeDecls(files)
	safego := safegoAlias(declFile(fset, files, run))

	reachable := scancodeReachable(byName, run)
	// The walk must not go decorative (lens L4). It resolves ~47
	// functions across 6 files today; the floor sits just under that
	// because the 47d escape broke resolution in the way that mattered
	// while still resolving plenty, so only a floor near the real
	// value says anything at all.
	walkedFiles := map[string]bool{}
	for _, fn := range reachable {
		walkedFiles[filepath.Base(fset.Position(fn.Pos()).Filename)] = true
	}
	if len(reachable) < 30 || len(walkedFiles) < 4 {
		t.Errorf("the reachability walk from Run resolved %d function(s) across %d file(s) (%v) — it resolved ~47 across 6 when this floor was set, so a number this low means call resolution is broken and the wait checks below are scanning almost nothing.", len(reachable), len(walkedFiles), walkedFiles)
	}

	// 1. Exactly one bookkeeping wait, and no synchronous path to it.
	var drainSites []waitCall
	for _, fn := range append([]*ast.FuncDecl{run}, sortedDecls(reachable)...) {
		for _, wc := range waitCalls(fn) {
			if wc.recv == "w.bookkeeping" {
				drainSites = append(drainSites, waitCall{recv: wc.recv, pos: wc.pos, owner: fn})
			}
		}
	}
	if len(drainSites) != 1 {
		t.Fatalf("expected exactly one w.bookkeeping.Wait() reachable from Run, found %d — that wait is what closes w.bookkeepingDone, and the scheduler's pool close waits on it so a runner's lock clear is never cut short (passes 38/39).", len(drainSites))
	}
	drain := drainSites[0]
	goSpans := goroutineSpans(run, safego)
	if syncPathToDrain(run, drain, byName, safego, goSpans) {
		t.Error("Run can reach w.bookkeeping.Wait() SYNCHRONOUSLY — inline, as an immediately-invoked literal, or through a plain call. It must only be reachable across a `go`/safego.Go edge: run synchronously, w.bookkeepingDone is already closed by the time the select runs, its deadline arm can never fire, and a wedged bookkeeping write blocks `aveloxis stop` indefinitely (passes 45a, 46a, 48d).")
	}
	launchPos, launched := drainLaunchPos(run, drain, byName, safego, goSpans)
	if !launched {
		t.Error("no `go`/safego.Go statement in Run reaches w.bookkeeping.Wait() — the drain must be launched by Run itself, so the select below bounds it (passes 38/39).")
	}

	// 2. The bookkeeping select: exactly two arms, the signal and a
	//    deadline built from the two grace operands.
	bookSelect := selectReceiving(run, "w.bookkeepingDone")
	if bookSelect == nil {
		t.Fatal("Run must select on w.bookkeepingDone — the claimed jobs' DB bookkeeping is what the scheduler's pool close must not race (passes 38/39).")
	}
	bound := boundSpec{
		timers:       runLocalsBoundTo(run, "time.NewTimer"),
		deadlines:    runLocalsBoundTo(run, "time.After"),
		ctxDeadlines: backgroundTimeoutLocals(run),
	}
	hasBound, hasDefault, arms := false, false, 0
	for _, stmt := range bookSelect.Body.List {
		cc, ok := stmt.(*ast.CommClause)
		if !ok {
			continue
		}
		arms++
		if cc.Comm == nil {
			hasDefault = true
			continue
		}
		operand, isRecv := commReceiveOperand(cc)
		if !isRecv {
			continue
		}
		switch {
		case operand == "w.bookkeepingDone":
		case bound.is(operand):
			hasBound = true
		}
	}
	if hasDefault {
		t.Error("the select that waits on w.bookkeepingDone carries a `default:` arm, so it never blocks: Run falls straight through and the scheduler's pool close lands under the runners' in-flight lock clears (pass 46).")
	}
	if !hasBound {
		t.Error("the select that waits on w.bookkeepingDone must carry a deadline arm built from w.shutdownGrace + ScancodeShutdownBookkeepingGrace — time.After(that sum) inline or in a local, or a time.NewTimer local's .C. NOTE that a context.WithTimeout child of the WORKER ctx is not a bound at all: ctx being canceled is why Run is here, so the child is born expired and the arm fires immediately (pass 48). Without a real bound a wedged bookkeeping write blocks `aveloxis stop` indefinitely.")
	}
	if arms != 2 {
		t.Errorf("the bookkeeping select must have exactly two arms — the signal and the deadline — but has %d. A third arm on an already-closed channel makes the select fall through without waiting for bookkeeping at all, and needs no `default:` keyword to do it (pass 47).", arms)
	}

	// 3. The shutdown sequence: nothing that can block between the
	//    drain launch and the select, and the signal nowhere else.
	joins := runJoinChannels(run, drain)
	for _, op := range runBlockingOps(run, goSpans) {
		inSelect := op.pos > bookSelect.Pos() && op.pos < bookSelect.End()
		switch {
		case inSelect:
			// Arm shapes are policed above.
		case op.kind == blockRecv && op.operand == "w.bookkeepingDone":
			t.Errorf("Run receives w.bookkeepingDone outside the bounded select (line %d) — blocking on the signal anywhere else makes the deadline arm decorative: only the drain closes it, so a wedged bookkeeping write hangs `aveloxis stop` forever, which is the failure the bound exists to prevent (pass 47).", fset.Position(op.pos).Line)
		case launched && op.pos > launchPos && op.pos < bookSelect.Pos():
			t.Errorf("Run %s at line %d — after launching the bookkeeping drain and before the bounded select, which makes it an UNBOUNDED wait ahead of the bound, whatever it is spelled (a receive, a range over the signal channel, a send or a lock all block the same way: passes 47, 48). Do it before the drain is launched, or make it an arm of the bounded select.", op.desc, fset.Position(op.pos).Line)
		case op.kind == blockRecv && !joins[op.operand]:
			t.Errorf("Run blocks on an unexpected receive %q (line %d) — before the drain launch Run may wait only on channels it both makes and closes itself, and not on one the DRAIN closes. Pass 40 removed the runner WaitGroup as dead plumbing and pass 39 established that the runners' clone-dir removal must not gate shutdown; a channel some OTHER function closes after waiting re-adds exactly that (passes 44, 47).", op.operand, fset.Position(op.pos).Line)
		}
	}

	// 4. Nothing Run reaches may wait on anything but w.bookkeeping.
	for _, fn := range append([]*ast.FuncDecl{run}, sortedDecls(reachable)...) {
		handles := subprocessHandles(fn)
		for _, wc := range waitCalls(fn) {
			if wc.recv == "w.bookkeeping" || handles[wc.recv] {
				continue
			}
			t.Errorf("%s is reachable from Run and calls %s.Wait() — Run must not block on the runners, directly or through a helper, whatever the receiver is spelled (a *sync.WaitGroup parameter was the pass-46 escape, a local renamed `cmd` the pass-47 one, a method value the pass-48 one). That wait covers the clone-dir removal, which takes minutes on a spinning disk and needs no pool (passes 39/40). Only w.bookkeeping.Wait() and a subprocess handle bound to exec.Command/exec.CommandContext — never rebound, never shadowing a parameter — are legal.", declKey(fn), wc.recv)
		}
	}
}

// selectReceiving returns the first select in fn carrying a receive on
// operand.
func selectReceiving(fn ast.Node, operand string) *ast.SelectStmt {
	var found *ast.SelectStmt
	ast.Inspect(fn, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectStmt)
		if !ok || found != nil {
			return true
		}
		for _, stmt := range sel.Body.List {
			if cc, ok := stmt.(*ast.CommClause); ok {
				if got, isRecv := commReceiveOperand(cc); isRecv && got == operand {
					found = sel
					return false
				}
			}
		}
		return true
	})
	return found
}

// span is a half-open source range.
type span struct{ from, to, launch token.Pos }

func (s span) holds(p token.Pos) bool { return p > s.from && p < s.to }

// goroutineSpans returns the literals Run launches as goroutines: `go
// func(){…}()` and literals handed to safego.Go, plus literals held in
// a local and launched by name. A literal that is merely INVOKED is
// deliberately excluded — deleting `go ` was the pass-46 escape.
func goroutineSpans(run *ast.FuncDecl, safego string) []span {
	lits := map[string]*ast.FuncLit{}
	ast.Inspect(run, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range as.Rhs {
			if lit, ok := rhs.(*ast.FuncLit); ok && i < len(as.Lhs) {
				if id, ok := as.Lhs[i].(*ast.Ident); ok {
					lits[id.Name] = lit
				}
			}
		}
		return true
	})
	var out []span
	add := func(target ast.Expr, launch token.Pos) {
		switch f := target.(type) {
		case *ast.FuncLit:
			out = append(out, span{f.Pos(), f.End(), launch})
		case *ast.Ident:
			if lit, ok := lits[f.Name]; ok {
				out = append(out, span{lit.Pos(), lit.End(), launch})
			}
		}
	}
	ast.Inspect(run, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.GoStmt:
			add(s.Call.Fun, s.Pos())
		case *ast.CallExpr:
			if types.ExprString(s.Fun) == safego+".Go" {
				for _, arg := range s.Args {
					add(arg, s.Pos())
				}
			}
		}
		return true
	})
	return out
}

// callTargets returns the declarations a call expression can resolve
// to, following method values and func values bound in fn. A method
// call resolves whatever its receiver is spelled, because renaming one
// receiver `w`→`s` made the walk a dead end below it (pass 47), and a
// method VALUE assigned to a local is still a call edge (pass 48).
func callTargets(call *ast.CallExpr, recv string, aliases map[string]string, byName map[string]*ast.FuncDecl) []*ast.FuncDecl {
	var keys []string
	switch f := call.Fun.(type) {
	case *ast.Ident:
		keys = append(keys, f.Name)
		if target, ok := aliases[f.Name]; ok {
			keys = append(keys, target)
		}
	case *ast.SelectorExpr:
		// A method call on the enclosing function's OWN receiver,
		// whatever that receiver is spelled — renaming one `w`→`s`
		// made the walk a dead end below it (pass 47). Deliberately
		// NOT any ident: `cmd.Run()` would resolve to
		// (*ScancodeWorker).Run and make everything reach everything.
		if id, isIdent := f.X.(*ast.Ident); isIdent && recv != "" && id.Name == recv {
			keys = append(keys, "*ScancodeWorker."+f.Sel.Name)
		}
	}
	var out []*ast.FuncDecl
	for _, k := range keys {
		if fn, ok := byName[k]; ok {
			out = append(out, fn)
		}
	}
	return out
}

// receiverName returns the name fn binds its receiver to, or "".
func receiverName(fn *ast.FuncDecl) string {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

// funcValueAliases maps locals in fn that hold a method or func VALUE
// to the declaration key they name — `joinRunners := w.awaitRunners`
// is a call edge the walk must follow (pass 48).
func funcValueAliases(fn *ast.FuncDecl) map[string]string {
	recv := receiverName(fn)
	out := map[string]string{}
	record := func(lhs, rhs []ast.Expr) {
		for i, r := range rhs {
			if i >= len(lhs) {
				continue
			}
			id, ok := lhs[i].(*ast.Ident)
			if !ok {
				continue
			}
			switch v := r.(type) {
			case *ast.SelectorExpr:
				if x, isIdent := v.X.(*ast.Ident); isIdent && recv != "" && x.Name == recv {
					out[id.Name] = "*ScancodeWorker." + v.Sel.Name
				}
			case *ast.Ident:
				out[id.Name] = v.Name
			}
		}
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			record(s.Lhs, s.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, 0, len(s.Names))
			for _, nm := range s.Names {
				lhs = append(lhs, nm)
			}
			record(lhs, s.Values)
		}
		return true
	})
	return out
}

// syncCallees returns the declarations fn calls SYNCHRONOUSLY: not the
// target of a `go`/safego.Go, and not sitting inside a literal fn
// launches as a goroutine.
func syncCallees(fn *ast.FuncDecl, byName map[string]*ast.FuncDecl, safego string, spans []span) []*ast.FuncDecl {
	aliases := funcValueAliases(fn)
	launched := map[*ast.CallExpr]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.GoStmt:
			launched[s.Call] = true
		case *ast.CallExpr:
			if types.ExprString(s.Fun) == safego+".Go" {
				launched[s] = true
			}
		}
		return true
	})
	var out []*ast.FuncDecl
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || launched[call] {
			return true
		}
		for _, sp := range spans {
			if sp.holds(call.Pos()) {
				return true
			}
		}
		out = append(out, callTargets(call, receiverName(fn), aliases, byName)...)
		return true
	})
	return out
}

// syncPathToDrain reports whether Run can reach the drain without
// crossing a goroutine boundary — the property that decides whether
// the bound below is live or decorative. Asked of the CALL SITES Run
// actually uses, because one unrelated `go w.watchdog()` that merely
// mentions the drain used to launder every synchronous call to it
// (pass 48).
func syncPathToDrain(run *ast.FuncDecl, drain waitCall, byName map[string]*ast.FuncDecl, safego string, spans []span) bool {
	if drain.owner == run {
		for _, sp := range spans {
			if sp.holds(drain.pos) {
				return false
			}
		}
		return true
	}
	seen := map[*ast.FuncDecl]bool{run: true}
	queue := []*ast.FuncDecl{run}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		useSpans := spans
		if fn != run {
			useSpans = goroutineSpans(fn, safego)
		}
		for _, callee := range syncCallees(fn, byName, safego, useSpans) {
			if seen[callee] {
				continue
			}
			if callee == drain.owner {
				return true
			}
			seen[callee] = true
			queue = append(queue, callee)
		}
	}
	return false
}

// drainLaunchPos returns the position of the earliest `go`/safego.Go
// statement in Run that reaches the drain.
func drainLaunchPos(run *ast.FuncDecl, drain waitCall, byName map[string]*ast.FuncDecl, safego string, spans []span) (token.Pos, bool) {
	best, found := token.Pos(0), false
	consider := func(p token.Pos) {
		if !found || p < best {
			best, found = p, true
		}
	}
	aliases := funcValueAliases(run)
	runRecv := receiverName(run)
	// When the drain sits lexically inside Run, the launch can only be
	// the goroutine literal that holds it — resolving further would
	// ask whether some other goroutine "reaches Run", which is both
	// meaningless and true (a runner's cmd.Run() resolves to
	// *ScancodeWorker.Run under receiver-blind matching).
	reaches := func(fn *ast.FuncDecl) bool {
		if drain.owner == run {
			return false
		}
		if fn == drain.owner {
			return true
		}
		_, ok := scancodeReachable(byName, fn)[declKey(drain.owner)]
		return ok
	}
	for _, sp := range spans {
		if sp.holds(drain.pos) {
			consider(sp.launch)
			continue
		}
		// The drain may sit one hop below the launched literal —
		// `go func(){ defer safego.Recover(…); w.drain() }()` is the
		// house idiom, and it kept panic recovery that the bare
		// `go w.drain()` form loses (pass 48).
		ast.Inspect(run, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !sp.holds(call.Pos()) {
				return true
			}
			for _, fn := range callTargets(call, runRecv, aliases, byName) {
				if reaches(fn) {
					consider(sp.launch)
				}
			}
			return true
		})
	}
	ast.Inspect(run, func(n ast.Node) bool {
		var targets []ast.Expr
		var pos token.Pos
		switch s := n.(type) {
		case *ast.GoStmt:
			targets, pos = []ast.Expr{s.Call.Fun}, s.Pos()
		case *ast.CallExpr:
			if types.ExprString(s.Fun) != safego+".Go" {
				return true
			}
			targets, pos = s.Args, s.Pos()
		default:
			return true
		}
		for _, target := range targets {
			call := &ast.CallExpr{Fun: target}
			for _, fn := range callTargets(call, runRecv, aliases, byName) {
				if reaches(fn) {
					consider(pos)
				}
			}
		}
		return true
	})
	return best, found
}

// blocking operation kinds Run may not perform between launching the
// drain and entering the bounded select.
const (
	blockRecv = iota
	blockSend
	blockRange
	blockLock
)

type blockingOp struct {
	kind    int
	operand string
	desc    string
	pos     token.Pos
}

// runBlockingOps returns everything Run itself can block on —
// receives, sends, ranges and lock acquisitions — excluding whatever
// sits inside the goroutines it launches, which belongs to those
// goroutines. A receive expression was not enough: `for range
// w.bookkeepingDone {}` blocks identically and is an *ast.RangeStmt
// (pass 48).
func runBlockingOps(run *ast.FuncDecl, spans []span) []blockingOp {
	var out []blockingOp
	inGoroutine := func(p token.Pos) bool {
		for _, sp := range spans {
			if sp.holds(p) {
				return true
			}
		}
		return false
	}
	ast.Inspect(run, func(n ast.Node) bool {
		var op blockingOp
		switch s := n.(type) {
		case *ast.UnaryExpr:
			if s.Op != token.ARROW {
				return true
			}
			operand := types.ExprString(s.X)
			op = blockingOp{blockRecv, operand, "blocks on receive " + strconv.Quote(operand), s.Pos()}
		case *ast.SendStmt:
			operand := types.ExprString(s.Chan)
			op = blockingOp{blockSend, operand, "sends on " + strconv.Quote(operand), s.Pos()}
		case *ast.RangeStmt:
			operand := types.ExprString(s.X)
			op = blockingOp{blockRange, operand, "ranges over " + strconv.Quote(operand), s.Pos()}
		case *ast.CallExpr:
			sel, ok := s.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Lock", "RLock":
				operand := types.ExprString(sel.X)
				op = blockingOp{blockLock, operand, "locks " + strconv.Quote(operand), s.Pos()}
			default:
				return true
			}
		default:
			return true
		}
		if !inGoroutine(op.pos) {
			out = append(out, op)
		}
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

// assignedTargets returns, for every target fn binds, whether it was
// ever bound by a call the predicate accepts and whether it was ever
// bound to anything else. Parameters, range variables and `var x T`
// declarations count as "something else": an exemption earned by one
// binding must not transfer to a same-named parameter of an inner
// closure, which is how a runner join got itself exempted (pass 48).
func assignedTargets(fn ast.Node, accept func(*ast.CallExpr) bool) (matched, other map[string]bool) {
	matched, other = map[string]bool{}, map[string]bool{}
	name := func(e ast.Expr) (string, bool) {
		switch t := e.(type) {
		case *ast.Ident:
			return t.Name, true
		case *ast.SelectorExpr:
			return types.ExprString(t), true
		}
		return "", false
	}
	record := func(lhs, rhs []ast.Expr) {
		for i, l := range lhs {
			n, ok := name(l)
			if !ok {
				continue
			}
			if i >= len(rhs) {
				// A multi-value RHS binds this target to something
				// this walk cannot inspect; treat it as "other" so an
				// exemption can never be inherited by accident.
				other[n] = true
				continue
			}
			if call, isCall := rhs[i].(*ast.CallExpr); isCall && accept(call) {
				matched[n] = true
			} else {
				other[n] = true
			}
		}
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			record(s.Lhs, s.Rhs)
		case *ast.ValueSpec:
			lhs := make([]ast.Expr, 0, len(s.Names))
			for _, nm := range s.Names {
				lhs = append(lhs, nm)
			}
			if len(s.Values) == 0 {
				for _, nm := range s.Names {
					other[nm.Name] = true
				}
				return true
			}
			record(lhs, s.Values)
		case *ast.RangeStmt:
			for _, e := range []ast.Expr{s.Key, s.Value} {
				if id, ok := e.(*ast.Ident); ok {
					other[id.Name] = true
				}
			}
		case *ast.FuncLit:
			for _, p := range s.Type.Params.List {
				for _, nm := range p.Names {
					other[nm.Name] = true
				}
			}
		case *ast.FuncDecl:
			if s.Type.Params != nil {
				for _, p := range s.Type.Params.List {
					for _, nm := range p.Names {
						other[nm.Name] = true
					}
				}
			}
		}
		return true
	})
	return matched, other
}

// runLocalsBoundTo returns fn's locals bound to a call to any of callees.
func runLocalsBoundTo(fn ast.Node, callees ...string) map[string]bool {
	matched, _ := assignedTargets(fn, func(call *ast.CallExpr) bool {
		name := types.ExprString(call.Fun)
		for _, c := range callees {
			if name == c {
				return true
			}
		}
		return false
	})
	return matched
}

// backgroundTimeoutLocals returns locals bound to a context timeout
// built from context.Background()/TODO(). A child of the WORKER ctx is
// excluded on purpose: that ctx being canceled is why Run is in its
// shutdown path at all, so the child is already expired and its
// .Done() fires immediately — a bound that bounds nothing (pass 48).
func backgroundTimeoutLocals(fn ast.Node) map[string]bool {
	return runLocalsBoundToFiltered(fn, func(call *ast.CallExpr) bool {
		name := types.ExprString(call.Fun)
		if name != "context.WithTimeout" && name != "context.WithDeadline" {
			return false
		}
		if len(call.Args) == 0 {
			return false
		}
		parent := types.ExprString(call.Args[0])
		return parent == "context.Background()" || parent == "context.TODO()"
	})
}

func runLocalsBoundToFiltered(fn ast.Node, accept func(*ast.CallExpr) bool) map[string]bool {
	matched, _ := assignedTargets(fn, accept)
	return matched
}

// subprocessHandles returns the locals fn binds to exec.Command or
// exec.CommandContext and never rebinds — the only receivers besides
// w.bookkeeping whose .Wait() is legal.
func subprocessHandles(fn ast.Node) map[string]bool {
	matched, other := assignedTargets(fn, func(call *ast.CallExpr) bool {
		name := types.ExprString(call.Fun)
		return name == "exec.Command" || name == "exec.CommandContext"
	})
	out := map[string]bool{}
	for n := range matched {
		if !other[n] {
			out[n] = true
		}
	}
	return out
}

// runJoinChannels returns the channel handles fn both make()s and
// close()s, EXCLUDING any the drain goroutine closes: waiting on one
// of those is the unbounded wait wearing a derivation the pin used to
// bless (pass 47).
func runJoinChannels(fn *ast.FuncDecl, drain waitCall) map[string]bool {
	made, _ := assignedTargets(fn, func(call *ast.CallExpr) bool {
		if types.ExprString(call.Fun) != "make" || len(call.Args) == 0 {
			return false
		}
		_, isChan := call.Args[0].(*ast.ChanType)
		return isChan
	})
	var drainLit *ast.FuncLit
	ast.Inspect(fn, func(n ast.Node) bool {
		if lit, ok := n.(*ast.FuncLit); ok && drain.pos > lit.Pos() && drain.pos < lit.End() {
			if drainLit == nil || lit.Pos() > drainLit.Pos() {
				drainLit = lit
			}
		}
		return true
	})
	joins := map[string]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || types.ExprString(call.Fun) != "close" || len(call.Args) != 1 {
			return true
		}
		if drainLit != nil && call.Pos() > drainLit.Pos() && call.Pos() < drainLit.End() {
			return true
		}
		if name := types.ExprString(call.Args[0]); made[name] {
			joins[name] = true
		}
		return true
	})
	return joins
}

// boundSpec knows the shutdown deadline's legal spellings, all derived
// from Run rather than hardcoded.
type boundSpec struct {
	timers, deadlines, ctxDeadlines map[string]bool
}

// is reports whether a receive operand is the shutdown deadline built
// from the two grace operands. The ARGUMENT is checked, not just the
// function name: `time.After(w.shutdownGrace)` drops the allowance,
// and with the shipped default grace of 0 that is `time.After(0)` —
// a select that falls straight through (pass 48).
func (b boundSpec) is(operand string) bool {
	if inner, ok := strings.CutPrefix(operand, "time.After("); ok {
		return isGraceSum(strings.TrimSuffix(inner, ")"))
	}
	if name, ok := strings.CutSuffix(operand, ".C"); ok && b.timers[name] {
		return true
	}
	if b.deadlines[operand] {
		return true
	}
	name, ok := strings.CutSuffix(operand, ".Done()")
	return ok && b.ctxDeadlines[name]
}

// isGraceSum reports whether an expression adds the operator's grace
// to the fixed bookkeeping allowance, in either order.
func isGraceSum(expr string) bool {
	parts := strings.Split(expr, "+")
	if len(parts) != 2 {
		return false
	}
	a, b := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	return (a == "w.shutdownGrace" && b == "ScancodeShutdownBookkeepingGrace") ||
		(b == "w.shutdownGrace" && a == "ScancodeShutdownBookkeepingGrace")
}

// commReceiveOperand returns the operand of `case <-X:` or
// `case v := <-X:`, and whether the clause is a receive at all. A
// `default:` clause has a nil Comm; callers must reject those
// separately rather than skipping them (pass 46).
func commReceiveOperand(cc *ast.CommClause) (string, bool) {
	var expr ast.Expr
	switch s := cc.Comm.(type) {
	case *ast.ExprStmt:
		expr = s.X
	case *ast.AssignStmt:
		if len(s.Rhs) == 1 {
			expr = s.Rhs[0]
		}
	}
	u, ok := expr.(*ast.UnaryExpr)
	if !ok || u.Op != token.ARROW {
		return "", false
	}
	return types.ExprString(u.X), true
}

// scancodeReachable walks the PACKAGE-WIDE call graph from start to a
// fixpoint (the pass-34 pattern), following method calls whatever the
// receiver is spelled and locals holding method or func values.
func scancodeReachable(byName map[string]*ast.FuncDecl, start *ast.FuncDecl) map[string]*ast.FuncDecl {
	seen := map[string]*ast.FuncDecl{}
	queue := []*ast.FuncDecl{start}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		aliases := funcValueAliases(fn)
		recv := receiverName(fn)
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			for _, target := range callTargets(call, recv, aliases, byName) {
				key := declKey(target)
				if _, done := seen[key]; done || target == start {
					continue
				}
				seen[key] = target
				queue = append(queue, target)
			}
			return true
		})
	}
	return seen
}

// scancodeMethodDecl returns the (*ScancodeWorker) method named name,
// from whichever file of the package declares it. The receiver type is
// checked so a same-named method on another type can never be pinned
// by mistake.
func scancodeMethodDecl(t *testing.T, files []*ast.File, name string) *ast.FuncDecl {
	t.Helper()
	for _, file := range files {
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Name.Name != name || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) != 1 {
				continue
			}
			if types.ExprString(fn.Recv.List[0].Type) != "*ScancodeWorker" {
				continue
			}
			return fn
		}
	}
	t.Fatalf("cannot find (*ScancodeWorker).%s", name)
	return nil
}

// scancodePackageFiles parses every non-test source of package
// collector. Package-wide because (*ScancodeWorker)'s methods span
// scancode_worker.go and scancode_preflight.go, and Run calls into
// both — a same-file walk let a sibling-file helper hold a runner
// wait invisibly (pass 46).
func scancodePackageFiles(t *testing.T) (*token.FileSet, []*ast.File) {
	t.Helper()
	dir := filepath.Join(srctest.Root(t), "internal", "collector")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read internal/collector: %v", err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files = append(files, f)
	}
	if len(files) < 10 {
		t.Fatalf("parsed only %d non-test sources in internal/collector — the walk below would be scoped to almost nothing; fix the file discovery", len(files))
	}
	return fset, files
}

// declKey names a declaration the way scancodeDecls keys it: methods
// by receiver type so a method and a same-named package function can
// never collide.
func declKey(fn *ast.FuncDecl) string {
	if fn.Recv != nil && len(fn.Recv.List) == 1 {
		return types.ExprString(fn.Recv.List[0].Type) + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// scancodeDecls indexes every declaration of the package by declKey.
func scancodeDecls(files []*ast.File) map[string]*ast.FuncDecl {
	byName := map[string]*ast.FuncDecl{}
	for _, file := range files {
		for _, d := range file.Decls {
			if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
				byName[declKey(fn)] = fn
			}
		}
	}
	return byName
}

// sortedDecls returns a walk's members in a stable order so failures
// read the same way run to run.
func sortedDecls(set map[string]*ast.FuncDecl) []*ast.FuncDecl {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*ast.FuncDecl, 0, len(keys))
	for _, k := range keys {
		out = append(out, set[k])
	}
	return out
}

// declFile returns the parsed file that declares fn.
func declFile(fset *token.FileSet, files []*ast.File, fn *ast.FuncDecl) *ast.File {
	want := fset.Position(fn.Pos()).Filename
	for _, f := range files {
		if fset.Position(f.Pos()).Filename == want {
			return f
		}
	}
	return nil
}

// safegoAlias returns the name internal/safego is imported under in
// file — "safego" normally, but an aliased import must not silently
// stop counting as a goroutine launch (pass 47).
func safegoAlias(file *ast.File) string {
	if file == nil {
		return "safego"
	}
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if !strings.HasSuffix(path, "/safego") {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		return "safego"
	}
	return "safego"
}

// waitCall is one X.Wait() call site, with the declaration it sits in.
type waitCall struct {
	recv  string
	pos   token.Pos
	owner *ast.FuncDecl
}

// waitCalls returns every X.Wait() call inside n. An AST walk, so
// ".Wait()" inside a log string is not a call and cannot count.
func waitCalls(n ast.Node) []waitCall {
	var out []waitCall
	ast.Inspect(n, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Wait" {
			out = append(out, waitCall{recv: types.ExprString(sel.X), pos: call.Pos()})
		}
		return true
	})
	return out
}
