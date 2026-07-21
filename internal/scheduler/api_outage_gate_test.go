// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestFillWorkerSlotsGatesOnAPIHealth pins the v0.27.34 scheduler-side
// half of the API-outage breaker: fillWorkerSlots must refuse to claim
// new work while the GitHub pool's breaker is open (the pool-side
// counters alone change nothing — the dispatcher gate is what stops
// workers burning retry budgets against a dead gateway, the
// per-call-breaker-insufficient lesson from ecosyste.ms/v0.25.0).
func TestFillWorkerSlotsGatesOnAPIHealth(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	start := strings.Index(s, "func (s *Scheduler) fillWorkerSlots(")
	if start < 0 {
		t.Fatal("fillWorkerSlots not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}

	gate := strings.Index(body, "s.ghKeys.APIHealthy()")
	if gate < 0 {
		t.Fatal("fillWorkerSlots must consult s.ghKeys.APIHealthy() before claiming")
	}
	// The gate must sit AFTER the db-health check (DB outage handling
	// wins) and BEFORE the claim loop.
	db := strings.Index(body, "s.dbHealthy.Load()")
	claim := strings.Index(body, "DequeueNext")
	if db < 0 || claim < 0 || !(db < gate && gate < claim) {
		t.Error("gate ordering must be: dbHealthy → APIHealthy → claim loop")
	}
	// Transition-only logging so a 2-hour outage produces two lines,
	// not one per poll tick.
	if !strings.Contains(body, "apiClaimsPaused.CompareAndSwap") {
		t.Error("claim pausing must log transitions only (CompareAndSwap on apiClaimsPaused)")
	}
}
