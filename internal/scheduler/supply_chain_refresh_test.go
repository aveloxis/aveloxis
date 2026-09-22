// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The JSON value → the tick (SR-10, end to end through the one accessor):
// absent ticks daily, 0 never ticks (a nil channel), n ticks every n hours.
func TestSupplyChainRefreshTickerFollowsTheConfig(t *testing.T) {
	var cc config.CollectionConfig
	c, stop := supplyChainRefreshTicker(&cc)
	stop()
	if c == nil {
		t.Error("absent: the refresh must be scheduled (daily default)")
	}
	zero := 0
	cc.SupplyChainRefreshHours = &zero
	c, stop = supplyChainRefreshTicker(&cc)
	stop()
	if c != nil {
		t.Error("0: the refresh must not be scheduled — a nil channel never fires")
	}
	if got := supplyChainConfiguredHours(&cc); got != 0 {
		t.Errorf("configured hours rendered %v, want 0", got)
	}
	cc.SupplyChainRefreshHours = nil
	if got := supplyChainConfiguredHours(&cc); got != "default" {
		t.Errorf("configured hours rendered %v, want default", got)
	}
	if d, on := cc.SupplyChainRefreshInterval(); !on || d != 24*time.Hour {
		t.Errorf("the ticker's source accessor: %v/%v", d, on)
	}
}

// The startup decision at runtime (round 2 finding 4): off → never; the
// whole pair built this start → skip; otherwise refresh.
func TestSupplyChainStartupRefreshDecision(t *testing.T) {
	for _, tc := range []struct{ scheduled, built, want bool }{
		{false, false, false}, {false, true, false}, {true, true, false}, {true, false, true},
	} {
		if got := supplyChainStartupRefresh(tc.scheduled, tc.built); got != tc.want {
			t.Errorf("scheduled=%v built=%v: got %v want %v", tc.scheduled, tc.built, got, tc.want)
		}
	}
}

// Wiring pin: the run loop has an arm for the ticker and it goes through
// singleFlight under the name the pool-demand registry carries; the 8Knot
// gate (MatviewRebuildActive) is never touched by the refresh.
func TestSupplyChainRefreshIsWiredIntoTheRunLoop(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, string(src), "func (s *Scheduler) Run("))
	if !strings.Contains(body, "case <-supplyChainC:") {
		t.Error("Run has no select arm for the supply-chain refresh ticker")
	}
	const call = `s.singleFlight(&s.supplyChainRefreshActive, "supply-chain-refresh", func() { s.runSupplyChainRefresh(ctx) })`
	if strings.Count(body, call) != 2 {
		t.Error("the refresh must run under singleFlight at startup and on every tick, both under the registered name")
	}
	if !strings.Contains(body, "if supplyChainStartupRefresh(supplyChainC != nil, s.store.SupplyChainViewsBuiltThisRun()) {") {
		t.Error("the startup refresh must be gated by supplyChainStartupRefresh on the cadence and the built-this-run flag")
	}
	refresh, err := os.ReadFile("supply_chain_refresh.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(srctest.StripGoComments(string(refresh)), "MatviewRebuildActive") {
		t.Error("the supply-chain refresh must not pause collection — that gate belongs to the hours-long 8Knot rebuild")
	}
	demand, err := os.ReadFile("pool_demand.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(demand), `"supply-chain-refresh"`) {
		t.Error("pool_demand.go does not list the refresh — the pool budget would be one connection short")
	}
}
