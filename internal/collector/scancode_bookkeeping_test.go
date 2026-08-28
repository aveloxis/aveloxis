// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 40 (v0.28.18): the pass-38/39 bookkeeping machinery had no
// enforcement — deleting the dispatcher's Add, the runner's Done, or
// Run's close would silently revert the pool-close fix (the scheduler
// would stop waiting, or wait forever). The worker holds a concrete
// *db.PostgresStore, so the contract is pinned at the source; the
// zero-in-flight close half is covered behaviorally by the scheduler's
// shutdown path in the AVELOXIS_TEST_DB tier.
func TestScancodeBookkeepingContract(t *testing.T) {
	src := srctest.Read(t, "internal/collector/scancode_worker.go")

	// 1. The count is taken at the dispatcher handoff, BEFORE the
	//    select — so Run's Wait can never miss a runner between
	//    receive and register.
	dispatcher := srctest.StripGoComments(srctest.FuncBody(t, src, "func (w *ScancodeWorker) dispatcher("))
	add := strings.Index(dispatcher, "w.bookkeeping.Add(1)")
	handoff := strings.Index(dispatcher, "case jobs <- *job:")
	if add < 0 || handoff < 0 || add > handoff {
		t.Errorf("dispatcher must w.bookkeeping.Add(1) BEFORE the handoff select (add=%d handoff=%d)", add, handoff)
	}
	// …and the undispatched-claim arm must balance it.
	done := strings.Index(dispatcher, "w.bookkeeping.Done()")
	if done < add {
		t.Errorf("the dispatcher's ctx.Done arm must w.bookkeeping.Done() the claim it never handed off")
	}

	// 2. runOne signals exactly once, via a deferred OnceFunc, and the
	//    RemoveAll defer fires it BEFORE the removal so the wait never
	//    covers filesystem work.
	runOne := srctest.StripGoComments(srctest.FuncBody(t, src, "func (w *ScancodeWorker) runOne("))
	if !strings.Contains(runOne, "bookkeepingDone := sync.OnceFunc(w.bookkeeping.Done)") ||
		!strings.Contains(runOne, "defer bookkeepingDone()") {
		t.Errorf("runOne must defer a sync.OnceFunc(w.bookkeeping.Done) so every exit path signals exactly once")
	}
	// Anchor structurally on the defer block that owns the removal —
	// a trailing-space needle depended on an inline comment surviving
	// StripGoComments (pass 41).
	remove := strings.Index(runOne, "os.RemoveAll(tempDir)")
	if remove < 0 {
		t.Fatalf("runOne's tempDir RemoveAll moved; re-anchor this pin")
	}
	blockStart := strings.LastIndex(runOne[:remove], "defer func() {")
	if blockStart < 0 {
		t.Fatalf("the tempDir RemoveAll must live in a defer block")
	}
	if !strings.Contains(runOne[blockStart:remove], "bookkeepingDone()") {
		t.Errorf("the RemoveAll defer must signal bookkeepingDone() BEFORE the removal — the shutdown wait must not cover minutes of filesystem work")
	}

	// 3. Run closes the signal channel when the WaitGroup drains and on
	//    any return, both through the sync.Once.
	run := srctest.StripGoComments(srctest.FuncBody(t, src, "func (w *ScancodeWorker) Run("))
	if strings.Count(run, "w.bookkeepingClose.Do(") < 2 || !strings.Contains(run, "w.bookkeeping.Wait()") {
		t.Errorf("Run must close bookkeepingDone via bookkeepingClose.Do both when the WaitGroup drains and on any return")
	}
}

// The scheduler tracks the worker's DB bookkeeping (not its return) and
// bounds its shutdown wait by the operator's grace + the allowance.
func TestSchedulerWaitsForScancodeBookkeeping(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	for _, needle := range []string{
		"<-scancodeWorker.BookkeepingDone()",
		"s.cfg.Collection.ScancodeShutdownGrace() + collector.ScancodeShutdownBookkeepingGrace",
		"s.background.Wait()",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("scheduler.go must contain %q — the shutdown arm waits for the tracked pools' DB bookkeeping before the pool closes (passes 38/39)", needle)
		}
	}
}
