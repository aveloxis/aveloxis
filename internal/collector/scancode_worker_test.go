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

// The shutdown contract, restated for passes 38–45: Run waits for the
// claimed jobs' DB BOOKKEEPING (w.bookkeeping, signalled through
// bookkeepingDone) — not for the runner goroutines, whose WaitGroup was
// dead plumbing and is gone — and the wait is bounded by the operator's
// scan grace plus the fixed bookkeeping allowance, so `aveloxis stop`
// can never hang on a multi-hour scan and the scheduler's pool close
// never lands under a runner's lock clear.
//
// Pinned on the AST, and on STRUCTURE rather than counts. Counting
// pins have now been escaped three times by one-line refactors:
//
//   - pass 43: "Run contains exactly one .Wait()" survived extracting
//     the runner wait into a helper and blocking Run on a channel that
//     helper closes.
//   - pass 45a: counting receives AND waits survived making the drain
//     SYNCHRONOUS — w.bookkeeping.Wait() inline ahead of the select.
//     Both counts are untouched, but bookkeepingDone is already closed
//     when the select runs, so the deadline arm can never fire and a
//     wedged bookkeeping write hangs `aveloxis stop` forever: exactly
//     the failure the bound exists to prevent.
//   - pass 45b: the receive allowlist survived a ONE-HOP helper
//     (w.awaitRunners() whose body is w.runners.Wait()). Run's own
//     receives and waits are unchanged, and Run once again blocks on
//     the runners' clone-dir removal — minutes on a spinning disk,
//     needing no pool (passes 39/40).
//
// So four things are asserted: the drain runs in a goroutine; the
// signal and the bound are two arms of ONE select; Run's blocking
// receives are its own join channels plus those two; and nothing Run
// reaches in this file waits on a w.<field> other than w.bookkeeping.
// Channel and timer locals are DERIVED, so renaming one or swapping
// time.After for a time.NewTimer stays legal — pass 45 (finding 7)
// caught the old pin misdiagnosing both as a re-added runner wait.
func TestGracefulShutdownWaitsForBookkeepingWithinGrace(t *testing.T) {
	file := scancodeWorkerAST(t)
	run := scancodeFuncNamed(t, file, "Run")

	joins := runJoinChannels(run) // channels Run both makes and closes
	timers := runTimerLocals(run) // time.NewTimer locals, for <-t.C
	if len(joins) == 0 {
		t.Fatal("Run declares no channel that it also closes — the dispatcher join is how Run learns the claim loop stopped, and deriving it is what lets the receive allowlist below tell a legitimate join from a re-added wait")
	}

	// 1. The drain must sit in a goroutine. Inline, the select below is
	//    dead and the bound is decorative (pass 45a).
	var lits []*ast.FuncLit
	ast.Inspect(run, func(n ast.Node) bool {
		if l, ok := n.(*ast.FuncLit); ok {
			lits = append(lits, l)
		}
		return true
	})
	insideFuncLit := func(p token.Pos) bool {
		for _, l := range lits {
			if p > l.Pos() && p < l.End() {
				return true
			}
		}
		return false
	}
	waits := waitCalls(run)
	if len(waits) != 1 {
		t.Errorf("Run must perform exactly one wait, w.bookkeeping.Wait() (found %d: %v) — dropping it leaves bookkeepingDone closed only by Run's deferred close, so the scheduler stops waiting for the runners' lock clears.", len(waits), waitReceiverNames(waits))
	}
	for _, wc := range waits {
		if wc.recv != "w.bookkeeping" {
			t.Errorf("Run calls %s.Wait() — the only wait Run may perform is w.bookkeeping.Wait(). Waiting on the RUNNERS covers minutes of clone-dir removal that needs no pool (passes 39/40).", wc.recv)
			continue
		}
		if !insideFuncLit(wc.pos) {
			t.Error("w.bookkeeping.Wait() must run inside a goroutine literal, not inline ahead of the select — synchronously, bookkeepingDone is already closed when the select runs, the deadline arm can never fire, and a wedged bookkeeping write blocks `aveloxis stop` indefinitely (pass 45).")
		}
	}

	// 2. The signal and the bound must be two arms of the SAME select.
	//    A deadline in some other select bounds nothing.
	signalSelects, boundedSelects := 0, 0
	ast.Inspect(run, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectStmt)
		if !ok {
			return true
		}
		hasSignal, hasBound := false, false
		for _, stmt := range sel.Body.List {
			cc, ok := stmt.(*ast.CommClause)
			if !ok {
				continue
			}
			operand, ok := commReceiveOperand(cc)
			if !ok {
				continue
			}
			if operand == "w.bookkeepingDone" {
				hasSignal = true
			}
			if isBoundReceive(operand, timers) {
				hasBound = true
			}
		}
		if hasSignal {
			signalSelects++
			if hasBound {
				boundedSelects++
			}
		}
		return true
	})
	if signalSelects == 0 {
		t.Error("Run must select on w.bookkeepingDone — the claimed jobs' DB bookkeeping is what the scheduler's pool close must not race (passes 38/39).")
	} else if boundedSelects == 0 {
		t.Error("the select that waits on w.bookkeepingDone must carry its deadline arm in the SAME select — time.After(...) or a time.NewTimer local's .C. Bounded in a different select, or not at all, a wedged bookkeeping write blocks `aveloxis stop` indefinitely.")
	}

	// The bound's operands, matched as identifiers so extracting them
	// into a local stays legal.
	src := srctest.StripGoComments(srctest.FuncBody(t, readScancodeWorkerSource(t), "func (w *ScancodeWorker) Run("))
	for _, operand := range []string{"w.shutdownGrace", "ScancodeShutdownBookkeepingGrace"} {
		if !strings.Contains(src, operand) {
			t.Errorf("Run's shutdown bound must be built from %s — without the operator's grace `aveloxis stop` ignores their setting; without the allowance the default grace of 0 returns before the runners' lock clears land (pass 38).", operand)
		}
	}

	// 3. Run's blocking receives: its own join channels, the signal and
	//    the deadline. A fourth is a wait on something else.
	var receives []string
	ast.Inspect(run, func(n ast.Node) bool {
		if u, ok := n.(*ast.UnaryExpr); ok && u.Op == token.ARROW {
			receives = append(receives, types.ExprString(u.X))
		}
		return true
	})
	sort.Strings(receives)
	got := strings.Join(receives, ", ")
	for _, r := range receives {
		switch {
		case joins[r], r == "w.bookkeepingDone", isBoundReceive(r, timers):
		default:
			t.Errorf("Run blocks on an unexpected receive %q — Run may wait on the channels it closes itself, on the bookkeeping signal, and on the deadline; nothing else. Pass 40 removed the runner WaitGroup as dead plumbing and pass 39 established that the runners' clone-dir removal (minutes on a spinning disk) must not gate shutdown; a channel some OTHER function closes after waiting re-adds exactly that (pass 44). Receives found: %s", r, got)
		}
	}

	// 4. Nothing Run reaches may wait on another w.<field> (pass 45b).
	//    Scoped to w.<field> receivers so the runners' own cmd.Wait()
	//    deep inside runOne stays legal.
	for name, fn := range scancodeReachable(file, run) {
		for _, wc := range waitCalls(fn) {
			if strings.HasPrefix(wc.recv, "w.") && wc.recv != "w.bookkeeping" {
				t.Errorf("%s is reachable from Run and calls %s.Wait() — Run must not block on the runners, directly or through a helper. That wait covers the clone-dir removal, which takes minutes on a spinning disk and needs no pool (passes 39/40).", name, wc.recv)
			}
		}
	}
}

// scancodeWorkerAST parses internal/collector/scancode_worker.go.
func scancodeWorkerAST(t *testing.T) *ast.File {
	t.Helper()
	path := filepath.Join(srctest.Root(t), "internal", "collector", "scancode_worker.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse scancode_worker.go: %v", err)
	}
	return file
}

// scancodeFuncNamed returns the *ScancodeWorker method named name.
// The receiver type is checked so a same-named method on another type
// in this file can never be pinned by mistake.
func scancodeFuncNamed(t *testing.T, file *ast.File, name string) *ast.FuncDecl {
	t.Helper()
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
	t.Fatalf("cannot find (*ScancodeWorker).%s", name)
	return nil
}

// waitCall is one X.Wait() call site.
type waitCall struct {
	recv string
	pos  token.Pos
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

func waitReceiverNames(calls []waitCall) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, c.recv)
	}
	return out
}

// runJoinChannels returns the channel locals fn both make()s and
// close()s — its own goroutine-join handles. Derived rather than
// hard-coded so renaming the local stays a legal refactor. A channel
// closed by some OTHER function is deliberately excluded: that is the
// pass-44 escape shape (a helper waits on the runners, then closes).
func runJoinChannels(fn *ast.FuncDecl) map[string]bool {
	made := map[string]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range as.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || types.ExprString(call.Fun) != "make" || len(call.Args) == 0 {
				continue
			}
			if _, isChan := call.Args[0].(*ast.ChanType); !isChan {
				continue
			}
			if i < len(as.Lhs) {
				if id, ok := as.Lhs[i].(*ast.Ident); ok {
					made[id.Name] = true
				}
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
		if id, ok := call.Args[0].(*ast.Ident); ok && made[id.Name] {
			joins[id.Name] = true
		}
		return true
	})
	return joins
}

// runTimerLocals returns fn's time.NewTimer locals so `<-t.C` reads as
// a deadline arm rather than an unexpected receive (pass 45).
func runTimerLocals(fn *ast.FuncDecl) map[string]bool {
	timers := map[string]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for i, rhs := range as.Rhs {
			call, ok := rhs.(*ast.CallExpr)
			if !ok || types.ExprString(call.Fun) != "time.NewTimer" {
				continue
			}
			if i < len(as.Lhs) {
				if id, ok := as.Lhs[i].(*ast.Ident); ok {
					timers[id.Name] = true
				}
			}
		}
		return true
	})
	return timers
}

// isBoundReceive reports whether a receive operand is a shutdown
// deadline: time.After(...) or a time.NewTimer local's .C.
func isBoundReceive(operand string, timers map[string]bool) bool {
	if strings.HasPrefix(operand, "time.After(") {
		return true
	}
	name, ok := strings.CutSuffix(operand, ".C")
	return ok && timers[name]
}

// commReceiveOperand returns the operand of `case <-X:` or
// `case v := <-X:`, and whether the clause is a receive at all.
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

// scancodeReachable walks the same-file call graph from start to a
// fixpoint (the pass-34 pattern) and returns what Run can reach, keyed
// by function name. Used to catch a runner wait re-added one hop away.
func scancodeReachable(file *ast.File, start *ast.FuncDecl) map[string]*ast.FuncDecl {
	byName := map[string]*ast.FuncDecl{}
	for _, d := range file.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok && fn.Body != nil {
			byName[fn.Name.Name] = fn
		}
	}
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
			var name string
			switch f := call.Fun.(type) {
			case *ast.Ident:
				name = f.Name
			case *ast.SelectorExpr:
				if id, ok := f.X.(*ast.Ident); ok && id.Name == "w" {
					name = f.Sel.Name
				}
			}
			if name == "" {
				return true
			}
			if _, done := seen[name]; done {
				return true
			}
			target, ok := byName[name]
			if !ok || target == start {
				return true
			}
			seen[name] = target
			queue = append(queue, target)
			return true
		})
	}
	return seen
}
