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

// What is left for a SOURCE pin, after pass 49 moved the contract to
// a runtime test.
//
// The shutdown contract — Run waits for the claimed jobs' DB
// BOOKKEEPING, not for the runner goroutines, and that wait is bounded
// so `aveloxis stop` can never hang — was pinned at the source level
// through passes 38–48. A reviewer escaped it in EVERY one of passes
// 43–48 — fifteen labelled one-line refactors, nine of them
// re-introducing the exact hang the bound exists to prevent — and
// three of the last four fixes were themselves defective. Pass 48 found the pin RECOMMENDING
// a refactor (a context.WithTimeout child of the already-canceled
// worker ctx) that silently reduced the wait to zero.
//
// The cause was structural, not carelessness: "the wait is bounded" is
// a property of what the program DOES, and a source pin can only
// approximate it by naming shapes. So the semantics now live in
// scancode_shutdown_runtime_test.go, which wedges a claimed job's
// bookkeeping and measures whether awaitBookkeeping returns. All eight
// bound-class escapes fail there without anyone naming a shape.
//
// This pin keeps exactly what runtime cannot reach:
//
//  1. The wait stays in awaitBookkeeping — the function those tests
//     drive. Inlined back into Run, the runtime coverage would
//     silently evaporate, so this is the wiring that keeps the real
//     test honest.
//  2. Run delegates to it rather than doing the wait itself.
//  3. Run blocks on nothing but the channels it makes and closes
//     (its dispatcher join). A wait ahead of the delegation still
//     delays `aveloxis stop`, and Run itself is not runtime-tested:
//     its store is a concrete *db.PostgresStore, so standing it up
//     would need a store interface this pin is not worth.
//  4. Nothing Run reaches waits on the RUNNERS. Runtime cannot test
//     this at all — pass 40 deleted the runner WaitGroup, so there is
//     nothing left to wedge. Escapes 46b/46d/47d/47e/48c/48f all
//     re-added such a wait, so the walk stays package-wide, resolves a
//     method call whatever its receiver is spelled, follows method and
//     func VALUES, and derives its one exemption (a local bound to
//     exec.Command/CommandContext and never rebound or shadowed)
//     rather than naming it.
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

	// 1. Exactly one bookkeeping wait, and it lives where the runtime
	//    tests can drive it.
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
	if owner := declKey(drainSites[0].owner); owner != drainHome {
		t.Errorf("w.bookkeeping.Wait() now lives in %s, not %s — the shutdown bound is proven by the runtime tests in scancode_shutdown_runtime_test.go, which call %s directly and measure whether it returns while the bookkeeping is wedged. Moving the wait out of it (inlining it back into Run, say) leaves those tests driving dead code, and every escape they catch — a synchronous drain, a dropped allowance, an expired bound, an extra blocking wait — becomes invisible again.", owner, drainHome, drainHome)
	}

	joins := runJoinChannels(run)

	// 2. Run must delegate to it — as a plain call, in the right
	//    place. `go w.awaitBookkeeping()` deletes the wait outright
	//    and every test stayed green (pass 50); so does moving it
	//    behind the clone-dir sweep, which v0.27.13 measured at 10+
	//    minutes PER DIRECTORY — long past the scheduler's own bound,
	//    so it closes the pool under the runners' lock clears.
	awaitIdx, ok := topLevelCallIndex(run, "awaitBookkeeping")
	if !ok {
		t.Error("Run must call w.awaitBookkeeping() as a plain statement in its own body — not with `go`, not with `defer`, not inside a literal. Launched in a goroutine it does not delay Run at all: the shutdown wait simply does not happen, and the scheduler's pool close lands on the runners' in-flight lock clears (passes 38/39, pass 50).")
	}
	sweepIdx, sweepOK := topLevelCallIndex(run, "sweepCloneDirAtShutdown")
	if ok && sweepOK && awaitIdx > sweepIdx {
		t.Error("Run performs its clone-dir sweep BEFORE awaiting the bookkeeping. The drain goroutine is launched inside awaitBookkeeping, so nothing signals bookkeepingDone until the sweep finishes — minutes on a spinning disk, and 10+ minutes per directory when v0.27.13 measured it. The scheduler's bound (scancode grace + the allowance) expires first and it closes the pool under the runners' lock clears (passes 38/39, pass 50).")
	}
	joinIdx, joinOK := topLevelReceiveIndex(run, joins)
	if ok && joinOK && awaitIdx < joinIdx {
		t.Error("Run awaits the bookkeeping BEFORE joining the dispatcher — the claim loop is still handing out jobs, so runners can register bookkeeping after the wait has already passed (passes 38/39).")
	}

	// 3. Run blocks on nothing but its own join channels.
	for _, op := range runBlockingOps(run, goroutineSpans(run, safego)) {
		if op.kind == blockRecv && joins[op.operand] {
			continue
		}
		t.Errorf("Run %s at line %d — the only thing Run may block on is a channel it both makes and closes itself (its dispatcher join); the shutdown wait belongs in awaitBookkeeping, where it is bounded and runtime-tested. A receive on the bookkeeping signal, a range over it, a send or a lock all hold `aveloxis stop` open just the same (passes 44, 47, 48).", op.desc, fset.Position(op.pos).Line)
	}

	// 4b. A helper Run calls SYNCHRONOUSLY blocks Run exactly as if it
	//     were inlined, so the runner join can simply be respelled as
	//     a channel receive one hop away — which pin 3 cannot see and
	//     `.Wait()` matching never could (pass 50). awaitBookkeeping is
	//     the one sanctioned blocker; its semantics belong to the
	//     runtime tests.
	for _, fn := range sortedDecls(syncReachable(run, byName, safego)) {
		if declKey(fn) == drainHome {
			continue
		}
		for _, op := range channelBlockingOps(fn, goroutineSpans(fn, safego)) {
			t.Errorf("%s is called synchronously from Run and %s at line %d — that blocks Run just as if it were written inline, so it is another way to re-add the runner join `.Wait()` matching was meant to stop (passes 39/40, pass 50). The only blocking Run may delegate is awaitBookkeeping, which is bounded and runtime-tested.", declKey(fn), op.desc, fset.Position(op.pos).Line)
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

// drainHome is the function the runtime shutdown tests drive. A rename
// is safe: those tests call the method directly, so the compiler
// catches them, and this constant is the only textual anchor.
const drainHome = "*ScancodeWorker.awaitBookkeeping"

// topLevelCallIndex returns the position, in fn's own statement list,
// of a PLAIN call to a method of fn's receiver by that name — and
// whether such a statement exists at all. Top-level and plain on
// purpose: `go w.awaitBookkeeping()` and `defer w.awaitBookkeeping()`
// both call the method and neither performs the wait Run is supposed
// to perform (pass 50).
func topLevelCallIndex(fn *ast.FuncDecl, name string) (int, bool) {
	recv := receiverName(fn)
	for i, stmt := range fn.Body.List {
		expr, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		call, ok := expr.X.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != name {
			continue
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == recv {
			return i, true
		}
	}
	return 0, false
}

// topLevelReceiveIndex returns the position of fn's first top-level
// receive on one of its own join channels — the dispatcher join.
func topLevelReceiveIndex(fn *ast.FuncDecl, joins map[string]bool) (int, bool) {
	for i, stmt := range fn.Body.List {
		expr, ok := stmt.(*ast.ExprStmt)
		if !ok {
			continue
		}
		u, ok := expr.X.(*ast.UnaryExpr)
		if !ok || u.Op != token.ARROW {
			continue
		}
		if joins[types.ExprString(u.X)] {
			return i, true
		}
	}
	return 0, false
}

// syncReachable returns what fn reaches WITHOUT crossing a goroutine
// boundary — the functions whose blocking is fn's own blocking.
func syncReachable(start *ast.FuncDecl, byName map[string]*ast.FuncDecl, safego string) map[string]*ast.FuncDecl {
	seen := map[string]*ast.FuncDecl{}
	queue := []*ast.FuncDecl{start}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		spans := goroutineSpans(fn, safego)
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
		aliases := funcValueAliases(fn)
		recv := receiverName(fn)
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

// channelBlockingOps is runBlockingOps restricted to operations that
// are unambiguously channel or lock waits. Ranges are excluded unless
// the operand is a known channel: without type information a `for _, x
// := range someSlice` is indistinguishable from a channel range, and
// the synchronously-reachable set is full of the former (measured: 8
// slice ranges, 0 channel ranges).
func channelBlockingOps(fn *ast.FuncDecl, spans []span) []blockingOp {
	chans := channelIdents(fn)
	var out []blockingOp
	for _, op := range runBlockingOps(fn, spans) {
		if op.kind == blockRange && !chans[op.operand] {
			continue
		}
		out = append(out, op)
	}
	return out
}

// channelIdents returns the identifiers fn holds channels in: its
// channel-typed parameters and its own make(chan …) locals.
func channelIdents(fn *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	if fn.Type.Params != nil {
		for _, p := range fn.Type.Params.List {
			if _, isChan := p.Type.(*ast.ChanType); !isChan {
				continue
			}
			for _, nm := range p.Names {
				out[nm.Name] = true
			}
		}
	}
	made, _ := assignedTargets(fn, func(call *ast.CallExpr) bool {
		if types.ExprString(call.Fun) != "make" || len(call.Args) == 0 {
			return false
		}
		_, isChan := call.Args[0].(*ast.ChanType)
		return isChan
	})
	for n := range made {
		out[n] = true
	}
	return out
}

// span is a half-open source range.
type span struct{ from, to, launch token.Pos }

func (s span) holds(p token.Pos) bool { return p > s.from && p < s.to }

// goroutineSpans returns the literals fn launches as goroutines: `go
// func(){…}()` and literals handed to safego.Go, plus literals held in
// a local and launched by name. Receives inside them belong to the
// goroutine, not to fn. A literal that is merely INVOKED is
// deliberately excluded — deleting `go ` was the pass-46 escape.
func goroutineSpans(fn *ast.FuncDecl, safego string) []span {
	lits := map[string]*ast.FuncLit{}
	ast.Inspect(fn, func(n ast.Node) bool {
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
	ast.Inspect(fn, func(n ast.Node) bool {
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

// receiverName returns the name fn binds its receiver to, or "".
func receiverName(fn *ast.FuncDecl) string {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

// callTargets returns the declarations a call can resolve to,
// following method values and func values bound in the enclosing
// function. A method call resolves whatever its receiver is spelled,
// because renaming one receiver `w`→`s` made the walk a dead end below
// it (pass 47), and a method VALUE assigned to a local is still a call
// edge (pass 48). Deliberately NOT any ident: `cmd.Run()` would
// resolve to (*ScancodeWorker).Run and make everything reach
// everything.
func callTargets(call *ast.CallExpr, recv string, aliases map[string]string, byName map[string]*ast.FuncDecl) []*ast.FuncDecl {
	var keys []string
	switch f := call.Fun.(type) {
	case *ast.Ident:
		keys = append(keys, f.Name)
		if target, ok := aliases[f.Name]; ok {
			keys = append(keys, target)
		}
	case *ast.SelectorExpr:
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

// blocking operation kinds Run may perform only on its own join
// channels.
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

// runBlockingOps returns everything fn itself can block on —
// receives, sends, ranges and lock acquisitions — excluding whatever
// sits inside the goroutines it launches, which belongs to those
// goroutines. A receive expression was not enough: `for range
// w.bookkeepingDone {}` blocks identically and is an *ast.RangeStmt
// (pass 48).
func runBlockingOps(fn *ast.FuncDecl, spans []span) []blockingOp {
	var out []blockingOp
	inGoroutine := func(p token.Pos) bool {
		for _, sp := range spans {
			if sp.holds(p) {
				return true
			}
		}
		return false
	}
	ast.Inspect(fn, func(n ast.Node) bool {
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
// close()s — its own goroutine-join handles. Derived rather than
// hard-coded so renaming one, or hoisting it to a struct field, stays
// a legal refactor.
func runJoinChannels(fn *ast.FuncDecl) map[string]bool {
	made, _ := assignedTargets(fn, func(call *ast.CallExpr) bool {
		if types.ExprString(call.Fun) != "make" || len(call.Args) == 0 {
			return false
		}
		_, isChan := call.Args[0].(*ast.ChanType)
		return isChan
	})
	joins := map[string]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || types.ExprString(call.Fun) != "close" || len(call.Args) != 1 {
			return true
		}
		if name := types.ExprString(call.Args[0]); made[name] {
			joins[name] = true
		}
		return true
	})
	return joins
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
