// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"
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
	idx := strings.Index(src, "func (w *ScancodeWorker) runOne(")
	if idx < 0 {
		t.Fatal("cannot find runOne method")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	// runOne must call recordFailureBestEffort on its failure paths.
	// We pin the helper name rather than counting occurrences so a
	// future refactor (e.g. consolidating defers) doesn't trip the
	// test for a stylistic reason.
	if !strings.Contains(body, "recordFailureBestEffort") {
		t.Error("runOne must call w.recordFailureBestEffort on failure paths (clone error, scancode subprocess crash, ingest error). The pre-v0.21.4 clearLockBestEffort path discarded the failure event entirely — losing the failure history, so the next claim cycle saw the row as healthy and immediately re-dispatched it. recordFailureBestEffort wraps RecordScancodeFailure which increments the counter and stamps last_failed_at for the backoff gate.")
	}

	// runOne must NOT call clearLockBestEffort on its failure
	// paths (it's allowed at one location — the dispatcher's
	// ctx-canceled-after-claim cleanup — but that lives in
	// `dispatcher`, not `runOne`). Pin the actual call-site
	// shape `w.clearLockBestEffort(` rather than the bare token,
	// because the body slice may extend into the next function's
	// docstring which legitimately mentions the helper name.
	if strings.Contains(body, "w.clearLockBestEffort(") {
		t.Error("runOne must not call w.clearLockBestEffort(...) — failure paths must route through w.recordFailureBestEffort(...) so the failure is tracked and backed off, not just silently cleared. (clearLockBestEffort is still allowed in dispatcher's ctx-canceled-after-claim cleanup, which is a clean release rather than a failure.)")
	}
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
