// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestBreadthWorkerIsHoistedAcrossTicks pins the v0.27.18 fix for the
// W4-flagged follow-up: runBreadth used to construct a NEW
// BreadthWorker every tick, so the v0.22.12 circuit-breaker pause
// (circuitOpenUntil on the worker struct) never persisted across
// ticks — a GitHub 5xx storm re-tripped from scratch every 15 minutes
// instead of pausing for its full hour. The worker must be a
// scheduler FIELD, constructed once in Run (NOT lazily inside
// runBreadth, which executes in a per-tick goroutine and would race
// the field write).
func TestBreadthWorkerIsHoistedAcrossTicks(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)

	if !strings.Contains(s, "breadthWorker *collector.BreadthWorker") {
		t.Error("Scheduler must carry the breadth worker as a field so circuit-breaker state survives ticks")
	}

	// runBreadth must NOT construct the worker.
	idx := strings.Index(s, "func (s *Scheduler) runBreadth(")
	if idx < 0 {
		t.Fatal("runBreadth not found")
	}
	body := s[idx : idx+900]
	if strings.Contains(body, "NewBreadthWorker") {
		t.Error("runBreadth must not construct a BreadthWorker — per-tick construction resets the circuit breaker (the bug this pins against)")
	}
	if !strings.Contains(body, "s.breadthWorker.Run(") {
		t.Error("runBreadth must run the hoisted s.breadthWorker")
	}

	// Construction happens once, in Run, guarded on ghKeys.
	if !strings.Contains(s, "s.breadthWorker = collector.NewBreadthWorker(") {
		t.Error("Run must construct the breadth worker once at startup")
	}
}
