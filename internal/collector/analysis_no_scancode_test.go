// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestAnalyzeRepoNoLongerInvokesScancode is the regression guard
// against the 2026-05-14 incident pattern. v0.21.0 moves scancode
// out of the per-job AnalysisCollector.AnalyzeRepo path entirely
// and into a dedicated ScancodeWorker pool. If a future refactor
// accidentally re-couples scancode to AnalyzeRepo, this test fires
// before the change ships.
//
// Why this is a separate test (not a comment in analysis.go):
// production once parked 177 of 180 workers behind a 2-slot scancode
// semaphore for 7+ hours. A code comment that "scancode is now done
// elsewhere" wouldn't catch the next well-intentioned developer who
// re-adds a per-job scancode call. A test does.
func TestAnalyzeRepoNoLongerInvokesScancode(t *testing.T) {
	data, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate AnalyzeRepo's body.
	idx := strings.Index(src, "func (ac *AnalysisCollector) AnalyzeRepo(")
	if idx < 0 {
		t.Fatal("cannot find AnalysisCollector.AnalyzeRepo")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		endRel = len(tail) - 1
	}
	body := tail[:1+endRel]

	if strings.Contains(body, "ac.scanScanCode(") {
		t.Error("AnalysisCollector.AnalyzeRepo must NOT call ac.scanScanCode(). The v0.21.0 ScancodeWorker pool owns scancode entirely; calling it from the per-job analysis phase re-introduces the 2-slot semaphore bottleneck that stalled 177 of 180 workers for 7+ hours on 2026-05-14. If you genuinely need per-job scancode (rare — discuss in a design doc first), call RunScanCode directly with an opt-in flag rather than putting it back in the default path.")
	}
}

// TestScancodeSemaphoreNoLongerExists pins the removal of the
// package-level scancodeSem channel. The semaphore was the
// architectural anti-pattern that caused the 2026-05-14 incident:
// a 2-slot channel that blocked every additional caller for the
// duration of the in-flight scans. The v0.21.0 worker uses its own
// channel-based dispatcher with the explicit ScancodeWorkers
// config knob, so the global semaphore no longer has a role.
//
// If a future refactor reintroduces `var scancodeSem = make(chan
// struct{}, N)` in this package, it's almost certainly putting
// scancode back on the per-job hot path. This test catches that.
func TestScancodeSemaphoreNoLongerExists(t *testing.T) {
	data, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if strings.Contains(src, "var scancodeSem ") {
		t.Error("scancode.go must NOT declare a package-level scancodeSem channel. Pre-v0.21.0 this 2-slot semaphore was the bottleneck that stalled 177 of 180 workers on 2026-05-14. The v0.21.0 ScancodeWorker uses its own channel-based dispatcher with the configurable scancode_workers count. If you need to limit scancode concurrency, raise/lower that config value.")
	}
}

// TestScancodeNoLongerHas30DaySkipCheck pins the removal of the
// in-function cadence check from RunScanCode. The cadence is now
// enforced at the worker's claim query level — checking it again
// inside RunScanCode would (a) cause a confusing double-gate where
// operators couldn't force a rescan even via UPDATE
// scancode_last_run=NULL, and (b) leave the worker holding a
// claimed row while the scan no-ops out, then having to clear the
// lock with no useful state captured.
func TestScancodeNoLongerHas30DaySkipCheck(t *testing.T) {
	data, err := os.ReadFile("scancode.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if strings.Contains(src, "ScancodeRunInterval") && strings.Contains(src, "time.Since(lastRun)") {
		t.Error("scancode.go must not contain the time.Since(lastRun) < ScancodeRunInterval skip check. The v0.21.0 ScancodeWorker enforces cadence at claim time via collection.scancode_cadence_days. Keeping the inline check causes a double-gate where operators can't force a rescan from the SQL side and the worker pays a wasted claim → no-op → clear cycle for already-fresh rows.")
	}
}
