// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestClassifyHealthTransition(t *testing.T) {
	cases := []struct {
		was, now bool
		want     healthTransition
	}{
		{true, false, transitionDown},
		{false, true, transitionUp},
		{true, true, transitionNone},
		{false, false, transitionNone},
	}
	for _, c := range cases {
		if got := classifyHealthTransition(c.was, c.now); got != c.want {
			t.Errorf("classifyHealthTransition(%v,%v)=%v want %v", c.was, c.now, got, c.want)
		}
	}
}

// TestDBHealthGuardWired pins the guard's three load-bearing pieces:
// fillWorkerSlots pauses when the DB is unhealthy, Run starts the monitor
// (healthy-by-default), and the monitor probes + records status + flips the flag.
func TestDBHealthGuardWired(t *testing.T) {
	sched, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(sched)
	if !strings.Contains(s, "if !s.dbHealthy.Load() {") {
		t.Error("fillWorkerSlots must pause (return) when !s.dbHealthy.Load()")
	}
	// The gate must be inside fillWorkerSlots, before the claim loop.
	fw := strings.Index(s, "func (s *Scheduler) fillWorkerSlots(")
	gate := strings.Index(s, "if !s.dbHealthy.Load() {")
	deq := strings.Index(s, "s.store.DequeueNext(")
	if fw < 0 || gate < fw || (deq > 0 && gate > deq) {
		t.Error("the dbHealthy gate must be at the top of fillWorkerSlots, before DequeueNext")
	}
	if !strings.Contains(s, "s.dbHealthy.Store(true)") || !strings.Contains(s, `safego.Go(s.logger, "db-health-monitor", func() { s.runDBHealthMonitor(ctx) })`) {
		t.Error("Run must start healthy and launch the DB-health monitor goroutine")
	}

	mon, err := os.ReadFile("db_health.go")
	if err != nil {
		t.Fatal(err)
	}
	m := string(mon)
	for _, needle := range []string{"s.store.Ping(ctx)", "s.dbHealthy.Swap(", "SetAveloxisStatus(", "classifyHealthTransition(", "debouncedHealthy(", "consecutiveFail"} {
		if !strings.Contains(m, needle) {
			t.Errorf("runDBHealthMonitor must use %q", needle)
		}
	}
}

// TestDebouncedHealthy pins the debounce that suppresses the transient
// connect/auth-timeout "false outage" blips (2026-06-11): healthy until
// dbHealthFailureThreshold consecutive failures, then unavailable.
func TestDebouncedHealthy(t *testing.T) {
	// Sanity-check the threshold is a debounce (>1), not a single-probe flip.
	if dbHealthFailureThreshold < 2 {
		t.Fatalf("dbHealthFailureThreshold must be >= 2 to debounce transient blips, got %d", dbHealthFailureThreshold)
	}
	for n := 0; n < dbHealthFailureThreshold; n++ {
		if !debouncedHealthy(n) {
			t.Errorf("debouncedHealthy(%d) must be true (below threshold %d) — a transient blip must not pause the fleet", n, dbHealthFailureThreshold)
		}
	}
	if debouncedHealthy(dbHealthFailureThreshold) {
		t.Errorf("debouncedHealthy(%d) must be false at the threshold — a sustained failure pauses collection", dbHealthFailureThreshold)
	}
	if debouncedHealthy(dbHealthFailureThreshold + 5) {
		t.Error("debouncedHealthy must stay false past the threshold")
	}
}

// TestClassifyProbeFailure — v0.29.58: a pooled ping that could not
// acquire a connection is the POOL's failure, not the server's; the
// thirteen "database unavailable" pauses in the 2026-09-22 log were all
// this shape (135/135 acquired, 0 idle) while Postgres was up.
func TestClassifyProbeFailure(t *testing.T) {
	timeout := errors.New("context deadline exceeded")
	cases := []struct {
		name string
		err  error
		st   db.PoolState
		want probeCause
	}{
		{"pool full, nothing idle", timeout, db.PoolState{MaxConns: 135, AcquiredConns: 135, IdleConns: 0}, probeCausePoolExhausted},
		{"pool full but an idle connection exists", timeout, db.PoolState{MaxConns: 135, AcquiredConns: 135, IdleConns: 1}, probeCauseUnreachable},
		{"pool has headroom", timeout, db.PoolState{MaxConns: 135, AcquiredConns: 40, IdleConns: 0}, probeCauseUnreachable},
		{"connect refused with an empty pool", errors.New("connection refused"), db.PoolState{MaxConns: 135, AcquiredConns: 0, IdleConns: 0}, probeCauseUnreachable},
		{"no error", nil, db.PoolState{MaxConns: 135, AcquiredConns: 135}, probeCauseUnreachable},
		{"zero max (unknown pool)", timeout, db.PoolState{}, probeCauseUnreachable},
	}
	for _, c := range cases {
		if got := classifyProbeFailure(c.err, c.st); got != c.want {
			t.Errorf("%s: classifyProbeFailure = %v, want %v", c.name, got, c.want)
		}
	}
	if probeCausePoolExhausted.String() == probeCauseUnreachable.String() || !strings.Contains(probeCausePoolExhausted.message(), "NOT known to be down") {
		t.Error("the two causes must read differently, and the pool message must say the database is not known to be down")
	}
}
