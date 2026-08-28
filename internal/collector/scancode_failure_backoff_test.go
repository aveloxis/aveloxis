// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.21.4 — pin that the runOne failure paths route through
// RecordScancodeFailure (the new failure-tracking helper) instead of
// ClearScancodeLock. The 2026-05-14 production loop bug was that
// ClearScancodeLock left scancode_last_run = NULL and never tracked
// the failure, so failed repos were always at the head of the claim
// queue. v0.21.4 routes ALL repo-specific failure paths through a
// counter-incrementing path; only the dispatcher's "ctx canceled
// after claim, never dispatched" cleanup keeps using ClearScancodeLock
// (that's a clean release, not a failure).

func TestRunOneRoutesFailuresThroughRecordFailure(t *testing.T) {
	src := readScancodeWorkerSource(t)

	// v0.27.6 decomposed runOne into phase methods. The failure
	// routing lives in each phase: prepareClone (mkdir/clone errors),
	// executeScan (LookPath / Start / lock-state errors), finishScan
	// (real-failure classification + ingest errors). Every one of
	// them must route through recordFailureBestEffort, and NONE of
	// the pipeline may call clearLockBestEffort (that's reserved for
	// the dispatcher's clean release).
	for _, decl := range []string{
		"func (w *ScancodeWorker) prepareClone(",
		"func (w *ScancodeWorker) executeScan(",
		"func (w *ScancodeWorker) finishScan(",
	} {
		body := scancodeMethodBody(t, src, decl)
		if !strings.Contains(body, "recordFailureBestEffort") {
			t.Errorf("%s must call w.recordFailureBestEffort on its failure paths. The pre-v0.21.4 clearLockBestEffort path discarded the failure event entirely — losing the failure history, so the next claim cycle saw the row as healthy and immediately re-dispatched it. recordFailureBestEffort wraps RecordScancodeFailure which increments the counter and stamps last_failed_at for the backoff gate.", decl)
		}
	}

	// The runOne pipeline may call clearLockBestEffort ONLY inside a
	// shutdown branch (`ctx.Err() != nil` — passes 36/37: a scan, clone
	// or start killed by `stop serve` is a clean release like the
	// dispatcher's, not a failure and not a timeout); on a failure path
	// it is the pre-v0.21.4 silent-clear shape. Comment-stripped so a
	// commented-out call or a `return` in prose cannot satisfy the pin.
	// ENCLOSING-block semantics (pass 38): the call must be inside the
	// braces of an `if … ctx.Err() != nil {` — a proximity window let a
	// failure-path call placed after a shutdown branch pass (the
	// pass-37 pin was decorative for the three sites nearest their
	// shutdown branches, mutation-proved). And every shutdown branch
	// must contain a clear: a branch that returns without clearing
	// strands the lock until the next start.
	for _, decl := range []string{
		"func (w *ScancodeWorker) runOne(",
		"func (w *ScancodeWorker) prepareClone(",
		"func (w *ScancodeWorker) executeScan(",
		"func (w *ScancodeWorker) finishScan(",
	} {
		body := srctest.StripGoComments(scancodeMethodBody(t, src, decl))
		for _, loc := range regexp.MustCompile(`w\.clearLockBestEffort\(`).FindAllStringIndex(body, -1) {
			at := loc[0]
			if !insideShutdownBranch(body, at) {
				t.Errorf("%s: w.clearLockBestEffort at offset %d is not inside the braces of a `ctx.Err() != nil` shutdown branch — failure paths route through w.recordFailureBestEffort so the failure is tracked and backed off", decl, at)
			}
			rest := body[loc[1]:]
			following := ""
			if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
				following = strings.TrimSpace(rest[nl+1:])
			}
			if !strings.HasPrefix(following, "return") {
				t.Errorf("%s: the statement after w.clearLockBestEffort at offset %d must be a return", decl, at)
			}
		}
		for _, m := range shutdownGuardRe.FindAllStringIndex(body, -1) {
			block := blockAfter(body, m[1]-1)
			if !strings.Contains(block, "w.clearLockBestEffort(") {
				t.Errorf("%s: the shutdown branch at offset %d returns without w.clearLockBestEffort — the lock would stay set until the next start's recovery", decl, m[0])
			}
		}
	}
	// finishScan's shutdown branch must precede the artifact write and
	// the outcome classification (which reads the SIGKILL text as a
	// timeout).
	finish := srctest.StripGoComments(scancodeMethodBody(t, src, "func (w *ScancodeWorker) finishScan("))
	branch := strings.Index(finish, "if ex.waitErr != nil && ctx.Err() != nil {")
	artifacts := strings.Index(finish, "writeFailureArtifacts")
	if branch < 0 || artifacts < 0 || artifacts < branch {
		t.Errorf("finishScan must check `ex.waitErr != nil && ctx.Err() != nil` BEFORE writeFailureArtifacts / classifyScanOutcome")
	}
}

// shutdownGuardRe is THE shutdown-branch shape: an `if` whose condition
// ends in `ctx.Err() != nil {` (pass 39: the "inside" check and the
// per-branch check share it, so a disjunction such as
// `ctx.Err() != nil || retries > 3` is neither an inside-anchor nor an
// unchecked branch).
var shutdownGuardRe = regexp.MustCompile(`if [^\n{]*ctx\.Err\(\) != nil \{`)

// insideShutdownBranch reports whether offset at lies inside the braces
// of a preceding shutdown guard (brace depth between the guard's `{`
// and at stays above zero).
func insideShutdownBranch(body string, at int) bool {
	guards := shutdownGuardRe.FindAllStringIndex(body[:at], -1)
	if len(guards) == 0 {
		return false
	}
	open := guards[len(guards)-1][1] - 1 // the guard's `{`
	depth := 0
	for i := open; i < at; i++ {
		switch body[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return false
			}
		}
	}
	return depth > 0
}

// blockAfter returns the text of the brace block opening at open.
func blockAfter(body string, open int) string {
	depth := 0
	for i := open; i < len(body); i++ {
		switch body[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return body[open : i+1]
			}
		}
	}
	return body[open:]
}

func TestRecordFailureBestEffortHelperExists(t *testing.T) {
	src := readScancodeWorkerSource(t)
	if !strings.Contains(src, "func (w *ScancodeWorker) recordFailureBestEffort(") {
		t.Error("scancode_worker.go must declare recordFailureBestEffort(ctx, repoID) wrapping store.RecordScancodeFailure with the same canceled-ctx fallback as clearLockBestEffort. Pre-v0.21.4 only had clearLockBestEffort; the new helper is what runOne calls on failure paths.")
	}
}

func TestDispatcherStillUsesClearLockForCleanRelease(t *testing.T) {
	// Carve-out test: pre-v0.21.4 dispatcher had a `case
	// <-ctx.Done()` arm that called ClearScancodeLock to release a
	// row we'd claimed but never dispatched to a runner. That's a
	// clean release (no actual scan attempt happened), so it
	// should NOT increment the failure counter — verify that
	// dispatcher still uses ClearScancodeLock, not the new failure
	// helper.
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

	if !strings.Contains(body, "ClearScancodeLock") {
		t.Error("dispatcher must keep calling ClearScancodeLock on its ctx-canceled-after-claim cleanup path. That's a clean release (no scan attempt happened), and routing it through RecordScancodeFailure would falsely accumulate failure counts on every graceful shutdown.")
	}
	if strings.Contains(body, "recordFailureBestEffort") || strings.Contains(body, "RecordScancodeFailure") {
		t.Error("dispatcher must NOT call recordFailureBestEffort / RecordScancodeFailure — the ctx-canceled cleanup is a clean release, not a failure event.")
	}
}
