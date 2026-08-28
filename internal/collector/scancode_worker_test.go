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

// The shutdown contract, restated for passes 38–47: ScancodeWorker.Run,
// on shutdown, waits for the claimed jobs' DB BOOKKEEPING
// (w.bookkeeping, signalled through w.bookkeepingDone) — not for the
// runner goroutines, whose clone-dir removal takes minutes on a
// spinning disk and needs no pool — and that wait is BOUNDED by
// w.shutdownGrace + ScancodeShutdownBookkeepingGrace so `aveloxis stop`
// can never hang.
//
// Nine reviewer escapes across passes 43–47 taught the shape this pin
// has to take. Every one preserved whatever the pin was counting or
// spelling, and every one was found by APPLYING the mutation:
//
//	43  extract the wait into a helper, block Run on a channel that
//	    helper closes                    (counts unchanged)
//	45a make the drain SYNCHRONOUS       (counts unchanged; the signal
//	    is already closed when the select runs, so the bound is dead
//	    and a wedged write hangs stop forever)
//	45b a one-hop helper w.awaitRunners()
//	46a delete the two characters `go `  (still "inside a FuncLit",
//	    synchronous again — an escape needing no new code)
//	46b a *sync.WaitGroup PARAMETER helper (receiver spelled `wg`)
//	46d put the helper in a SIBLING FILE (the walk was same-file)
//	47a `<-w.bookkeepingDone` on its own line ahead of the select —
//	    the allowlist named the signal, so blocking on it anywhere was
//	    legal, and the bound below became decorative
//	47b `<-drained`, where `drained` is made and closed by Run and so
//	    was DERIVED as a legitimate join handle — except it is closed
//	    by the drain goroutine, making the receive the unbounded wait
//	47c a third select arm on a pre-closed channel: no `default:`
//	    keyword, so the 46c check never saw it, and Run stopped
//	    waiting for bookkeeping altogether
//
// 47a–c share one root cause — Run was allowed to block on things
// other than the bounded select — so they are answered by one rule
// about the shutdown SEQUENCE rather than three about identifiers:
//
//	Run launches the drain asynchronously; between that launch and the
//	bookkeeping select it blocks on nothing; the select has exactly two
//	arms, the signal and a deadline built from the two grace operands;
//	and the signal is received nowhere else in Run.
//
// The remaining checks are scoped by semantics, not spelling: the
// drain counts as asynchronous when it is `go`-launched or handed to
// safego.Go (46a) — through a literal, a local holding one, or a
// method, at any depth (47g); no `.Wait()` on ANY receiver is legal
// anywhere Run reaches except w.bookkeeping (46b), with the one
// exception DERIVED — a local bound to exec.Command/exec.CommandContext
// and never rebound to anything else, so naming a WaitGroup `cmd`
// cannot buy an exemption (47e); the walk spans the PACKAGE and
// resolves a method call whatever its receiver is spelled, because
// renaming one receiver `w`→`s` turned every call below it into a dead
// end (46d, 47d); and channel, timer and deadline locals are DERIVED,
// including context.WithTimeout, `var` declarations and struct fields,
// so the idiomatic bound and ordinary hoists stay legal (45-7, 46f,
// 47f, 47h, 47i).
func TestGracefulShutdownWaitsForBookkeepingWithinGrace(t *testing.T) {
	fset, files := scancodePackageFiles(t)
	run := scancodeMethodDecl(t, files, "Run")
	runFile := declFile(fset, files, run)
	byName := scancodeDecls(files)

	// Everything Run reaches, package-wide. Built first: the drain may
	// legitimately live one hop away (47g), so "the wait" is a property
	// of the reachable set, not of Run's own text.
	reachable := scancodeReachable(byName, run)
	// The walk must not go decorative (lens L4). It resolves ~47
	// functions across 6 files today; a floor of 5/2 would still pass
	// after a 90% loss, and during the 47d escape the walk was broken
	// in exactly the way that hid a runner wait while resolving
	// plenty. Kept near the observed value on purpose.
	walkedFiles := map[string]bool{}
	for _, fn := range reachable {
		walkedFiles[filepath.Base(fset.Position(fn.Pos()).Filename)] = true
	}
	if len(reachable) < 30 || len(walkedFiles) < 4 {
		t.Errorf("the reachability walk from Run resolved %d function(s) across %d file(s) (%v) — it resolved ~47 across 6 when this floor was set, so a number this low means call resolution is broken and the wait checks below are scanning almost nothing.", len(reachable), len(walkedFiles), walkedFiles)
	}

	// 1. Exactly one bookkeeping wait, anywhere Run reaches, and it
	//    must be reached ASYNCHRONOUSLY. Synchronously — inline, as an
	//    immediately-invoked literal, or through a plain call — the
	//    signal is already closed when the select runs and the bound
	//    below can never fire (45a, 46a).
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
	async := asyncLaunches(run, byName, safegoAlias(runFile))
	launchPos, ok := async.reaches(drain)
	if !ok {
		t.Error("w.bookkeeping.Wait() must be reached asynchronously from Run — `go func(){…}()`, `go w.helper()`, or a literal/method handed to safego.Go. Run synchronously (an immediately-invoked literal counts), w.bookkeepingDone is already closed by the time the select runs, its deadline arm can never fire, and a wedged bookkeeping write blocks `aveloxis stop` indefinitely (passes 45a, 46a).")
	}

	// 2. The bookkeeping select: exactly two arms, the signal and a
	//    deadline. A third arm on a pre-closed channel drops the wait
	//    entirely without ever writing `default:` (47c).
	timers := runLocalsFrom(run, "time.NewTimer")
	deadlines := runLocalsFrom(run, "time.After")
	ctxDeadlines := runLocalsFrom(run, "context.WithTimeout", "context.WithDeadline")
	bound := boundSpec{timers: timers, deadlines: deadlines, ctxDeadlines: ctxDeadlines}

	var bookSelect *ast.SelectStmt
	ast.Inspect(run, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectStmt)
		if !ok || bookSelect != nil {
			return true
		}
		for _, stmt := range sel.Body.List {
			if cc, ok := stmt.(*ast.CommClause); ok {
				if operand, isRecv := commReceiveOperand(cc); isRecv && operand == "w.bookkeepingDone" {
					bookSelect = sel
					return false
				}
			}
		}
		return true
	})
	if bookSelect == nil {
		t.Fatal("Run must select on w.bookkeepingDone — the claimed jobs' DB bookkeeping is what the scheduler's pool close must not race (passes 38/39).")
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
		if operand, isRecv := commReceiveOperand(cc); isRecv && bound.is(operand) {
			hasBound = true
		}
	}
	if hasDefault {
		t.Error("the select that waits on w.bookkeepingDone carries a `default:` arm, so it never blocks: Run falls straight through and the scheduler's pool close lands under the runners' in-flight lock clears (pass 46).")
	}
	if !hasBound {
		t.Error("the select that waits on w.bookkeepingDone must carry its deadline arm in the SAME select — time.After(...) inline or in a local, a time.NewTimer local's .C, or a context.WithTimeout/WithDeadline local's .Done(). Bounded elsewhere, or not at all, a wedged bookkeeping write blocks `aveloxis stop` indefinitely.")
	}
	if arms != 2 {
		t.Errorf("the bookkeeping select must have exactly two arms — the signal and the deadline — but has %d. A third arm on an already-closed channel makes the select fall through without waiting for bookkeeping at all, and needs no `default:` keyword to do it (pass 47).", arms)
	}

	// The bound's operands, matched as identifiers so extracting them
	// into a local stays legal.
	src := srctest.StripGoComments(srctest.FuncBody(t, declSource(t, fset, run), "func (w *ScancodeWorker) Run("))
	for _, operand := range []string{"w.shutdownGrace", "ScancodeShutdownBookkeepingGrace"} {
		if !strings.Contains(src, operand) {
			t.Errorf("Run's shutdown bound must be built from %s — without the operator's grace `aveloxis stop` ignores their setting; without the allowance the default grace of 0 returns before the runners' lock clears land (pass 38).", operand)
		}
	}

	// 3. The shutdown sequence. Every receive Run performs itself —
	//    receives inside the goroutines it launches belong to those
	//    goroutines — must be either an arm of the bookkeeping select,
	//    or a join handle awaited BEFORE the drain is launched. A
	//    receive in between is an unbounded wait ahead of the bound
	//    (47a, 47b), whatever it is spelled and however it was derived.
	joins := runJoinChannels(run)
	for _, r := range runOwnReceives(run, async) {
		switch {
		case r.pos > bookSelect.Pos() && r.pos < bookSelect.End():
			if r.operand == "w.bookkeepingDone" || bound.is(r.operand) {
				continue
			}
			t.Errorf("the bookkeeping select receives %q — its only arms may be w.bookkeepingDone and the deadline (pass 47).", r.operand)
		case r.operand == "w.bookkeepingDone":
			t.Errorf("Run receives w.bookkeepingDone outside the bounded select (at offset %d) — blocking on the signal anywhere else makes the deadline arm decorative: only the drain goroutine closes it, so a wedged bookkeeping write hangs `aveloxis stop` forever, which is the failure the bound exists to prevent (pass 47).", fset.Position(r.pos).Line)
		case ok && r.pos > launchPos:
			t.Errorf("Run blocks on %q after launching the bookkeeping drain and before the bounded select (line %d) — everything between the drain launch and the select is an UNBOUNDED wait, including a channel Run makes and closes itself when the drain goroutine is what closes it (pass 47). Wait for it before the drain is launched, or make it an arm of the bounded select.", r.operand, fset.Position(r.pos).Line)
		case !joins[r.operand]:
			t.Errorf("Run blocks on an unexpected receive %q (line %d) — before the drain launch Run may wait only on channels it both makes and closes itself (the dispatcher join). Pass 40 removed the runner WaitGroup as dead plumbing and pass 39 established that the runners' clone-dir removal must not gate shutdown; a channel some OTHER function closes after waiting re-adds exactly that (pass 44).", r.operand, fset.Position(r.pos).Line)
		}
	}

	// 4. Nothing Run reaches may wait on anything but w.bookkeeping
	//    (45b, 46b, 46d). The one exception is DERIVED: a local bound
	//    to exec.Command/exec.CommandContext and never rebound — the
	//    runners' own cmd.Wait(). Naming a WaitGroup `cmd` in a
	//    function that also builds a command buys nothing (47e).
	for _, fn := range sortedDecls(reachable) {
		name := declKey(fn)
		for _, wc := range waitCalls(fn) {
			if wc.recv == "w.bookkeeping" || subprocessHandles(fn)[wc.recv] {
				continue
			}
			t.Errorf("%s is reachable from Run and calls %s.Wait() — Run must not block on the runners, directly or through a helper, whatever the receiver is spelled (a *sync.WaitGroup parameter is the pass-46 escape, a local renamed to `cmd` the pass-47 one). That wait covers the clone-dir removal, which takes minutes on a spinning disk and needs no pool (passes 39/40). Only w.bookkeeping.Wait() and a subprocess handle bound to exec.Command/exec.CommandContext — and never rebound — are legal.", name, wc.recv)
		}
	}
	for _, wc := range waitCalls(run) {
		if wc.recv != "w.bookkeeping" && !subprocessHandles(run)[wc.recv] {
			t.Errorf("Run calls %s.Wait() — the only wait Run may perform is w.bookkeeping.Wait(). Waiting on the RUNNERS covers minutes of clone-dir removal that needs no pool (passes 39/40).", wc.recv)
		}
	}
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

// sortedDecls returns the walk's members in a stable order so failures
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

// declSource reads the source that declares fn, through srctest so
// this file adds no read helper of its own (SR-12). The path is
// derived from the AST rather than hardcoded, so the textual operand
// check follows Run if it ever moves to a sibling file (pass 47).
func declSource(t *testing.T, fset *token.FileSet, fn *ast.FuncDecl) string {
	t.Helper()
	abs := fset.Position(fn.Pos()).Filename
	rel, err := filepath.Rel(srctest.Root(t), abs)
	if err != nil {
		t.Fatalf("locate %s under the repo root: %v", abs, err)
	}
	return srctest.Read(t, rel)
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

// asyncSet records what Run launches asynchronously: the spans of
// goroutine literals, and the declarations reached from them.
type asyncSet struct {
	spans []struct {
		from, to, launch token.Pos
	}
	decls map[*ast.FuncDecl]token.Pos
}

// reaches reports whether the drain site is launched asynchronously,
// and where that launch sits in Run.
func (a asyncSet) reaches(w waitCall) (token.Pos, bool) {
	for _, s := range a.spans {
		if w.pos > s.from && w.pos < s.to {
			return s.launch, true
		}
	}
	if pos, ok := a.decls[w.owner]; ok {
		return pos, true
	}
	return 0, false
}

// asyncLaunches collects what Run starts in a goroutine: `go` statements
// and safego.Go calls, whether they carry a literal, a local holding
// one, or a method — plus, transitively, everything those reach. A
// literal that is merely invoked is deliberately excluded: deleting
// `go ` was the pass-46 escape.
func asyncLaunches(run *ast.FuncDecl, byName map[string]*ast.FuncDecl, safego string) asyncSet {
	out := asyncSet{decls: map[*ast.FuncDecl]token.Pos{}}
	lits := map[string]*ast.FuncLit{} // locals holding a literal
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
	add := func(target ast.Expr, launch token.Pos) {
		switch f := target.(type) {
		case *ast.FuncLit:
			out.spans = append(out.spans, struct{ from, to, launch token.Pos }{f.Pos(), f.End(), launch})
		case *ast.Ident:
			if lit, ok := lits[f.Name]; ok {
				out.spans = append(out.spans, struct{ from, to, launch token.Pos }{lit.Pos(), lit.End(), launch})
				return
			}
			if fn, ok := byName[f.Name]; ok {
				out.decls[fn] = launch
			}
		case *ast.SelectorExpr:
			if fn, ok := byName["*ScancodeWorker."+f.Sel.Name]; ok {
				out.decls[fn] = launch
			}
		}
	}
	ast.Inspect(run, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.GoStmt:
			add(s.Call.Fun, s.Pos())
		case *ast.CallExpr:
			if types.ExprString(s.Fun) != safego+".Go" {
				return true
			}
			for _, arg := range s.Args {
				add(arg, s.Pos())
			}
		}
		return true
	})
	// Transitive closure: a drain one more hop below an async entry
	// point is still asynchronous.
	for fn, launch := range out.decls {
		for _, sub := range scancodeReachable(byName, fn) {
			if _, seen := out.decls[sub]; !seen {
				out.decls[sub] = launch
			}
		}
	}
	return out
}

// receive is one receive Run performs itself.
type receive struct {
	operand string
	pos     token.Pos
}

// runOwnReceives returns the receives Run performs, excluding those
// inside the goroutines it launches — those belong to the goroutine.
// A receive inside a merely-invoked literal still counts as Run's,
// because an immediately-invoked literal is synchronous code.
func runOwnReceives(run *ast.FuncDecl, async asyncSet) []receive {
	var out []receive
	ast.Inspect(run, func(n ast.Node) bool {
		u, ok := n.(*ast.UnaryExpr)
		if !ok || u.Op != token.ARROW {
			return true
		}
		for _, s := range async.spans {
			if u.Pos() > s.from && u.Pos() < s.to {
				return true
			}
		}
		out = append(out, receive{operand: types.ExprString(u.X), pos: u.Pos()})
		return true
	})
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

// assignedTargets returns, for every target fn binds (an identifier or
// a struct field), whether it was ever bound by a call the predicate
// accepts and whether it was ever bound to anything else. Covering
// ValueSpec keeps `var x = make(chan struct{})` legal (46f); covering
// SelectorExpr targets keeps a channel hoisted to a field legal (47i);
// and tracking "bound to something else" is what stops a WaitGroup
// named `cmd` from inheriting a subprocess exemption (47e).
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
				// Multi-value RHS: only the first target can be
				// attributed, and it was handled at i == 0.
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
			record(lhs, s.Values)
		}
		return true
	})
	return matched, other
}

// runLocalsFrom returns fn's locals bound to a call to any of callees.
func runLocalsFrom(fn ast.Node, callees ...string) map[string]bool {
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

// subprocessHandles returns the locals fn binds to exec.Command or
// exec.CommandContext and never rebinds to anything else — the only
// receivers besides w.bookkeeping whose .Wait() is legal.
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
// a legal refactor. A channel closed by some OTHER function is
// excluded: that is the pass-44 escape shape.
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

// boundSpec knows the shutdown deadline's legal spellings, all of them
// derived from Run rather than hardcoded.
type boundSpec struct {
	timers, deadlines, ctxDeadlines map[string]bool
}

// is reports whether a receive operand is the shutdown deadline:
// time.After(...) inline or in a local, a time.NewTimer local's .C, or
// a context.WithTimeout/WithDeadline local's .Done().
func (b boundSpec) is(operand string) bool {
	if strings.HasPrefix(operand, "time.After(") || b.deadlines[operand] {
		return true
	}
	if name, ok := strings.CutSuffix(operand, ".C"); ok && b.timers[name] {
		return true
	}
	name, ok := strings.CutSuffix(operand, ".Done()")
	return ok && b.ctxDeadlines[name]
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
// fixpoint (the pass-34 pattern). A method call resolves whatever its
// receiver is spelled: renaming one receiver `w`→`s` made the walk a
// dead end for everything below it, hiding a runner wait two hops
// down (pass 47).
func scancodeReachable(byName map[string]*ast.FuncDecl, start *ast.FuncDecl) map[string]*ast.FuncDecl {
	seen := map[string]*ast.FuncDecl{}
	queue := []*ast.FuncDecl{start}
	for len(queue) > 0 {
		fn := queue[0]
		queue = queue[1:]
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			var keys []string
			switch f := call.Fun.(type) {
			case *ast.Ident:
				keys = []string{f.Name}
			case *ast.SelectorExpr:
				if _, isIdent := f.X.(*ast.Ident); isIdent {
					keys = []string{"*ScancodeWorker." + f.Sel.Name}
				}
			}
			for _, key := range keys {
				if _, done := seen[key]; done {
					continue
				}
				target, ok := byName[key]
				if !ok || target == start {
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
