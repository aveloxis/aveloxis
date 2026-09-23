// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
)

// supplyChainRefreshTicker is the run loop's clock for the supply-chain
// view refresh: the configured cadence, or a nil channel (never fires)
// when collection.supply_chain_refresh_hours is 0. The one accessor
// (SupplyChainRefreshInterval) decides both, so the JSON value reaches the
// tick unchanged (SR-10).
func supplyChainRefreshTicker(cc *config.CollectionConfig) (<-chan time.Time, func()) {
	interval, on := cc.SupplyChainRefreshInterval()
	if !on {
		return nil, func() {}
	}
	ticker := time.NewTicker(interval)
	return ticker.C, ticker.Stop
}

// supplyChainConfiguredHours renders the raw JSON value for the schedule
// log line beside the effective interval: "default" when absent.
func supplyChainConfiguredHours(cc *config.CollectionConfig) any {
	if cc.SupplyChainRefreshHours == nil {
		return "default"
	}
	return *cc.SupplyChainRefreshHours
}

// supplyChainStartupRefresh decides serve's ONE refresh at startup, which
// covers the age a restart's downtime added to the views: wanted when the
// cadence is on and this start did not just build the whole pair WITH
// DATA (round 1 finding 4; a partial pair's pre-existing member still
// needs it, round 2 finding 3).
func supplyChainStartupRefresh(scheduled, builtWholePairThisRun bool) bool {
	return scheduled && !builtWholePairThisRun
}

// runSupplyChainRefresh refreshes the two Aveloxis-owned views (a few
// seconds at fleet scale; CONCURRENTLY, so the API keeps reading). It runs
// off the run loop under singleFlight and never pauses collection — the
// MatviewRebuildActive gate is the 8Knot rebuild's, whose cost this does
// not share. A shutdown mid-refresh is logged at INFO by the store; any
// other failure is an ERROR here and the next tick retries.
func (s *Scheduler) runSupplyChainRefresh(ctx context.Context) {
	start := time.Now()
	err := db.RefreshSupplyChainViews(ctx, s.store, s.logger)
	if errors.Is(err, context.Canceled) {
		s.logger.Info("supply-chain view refresh interrupted by shutdown")
		return
	}
	if err != nil {
		s.logger.Error("supply-chain view refresh FAILED — the dependencies page reads the previous data until the next tick succeeds",
			"error", err, "duration", time.Since(start).Truncate(time.Millisecond))
		return
	}
	s.logger.Info("supply-chain view refresh done", "duration", time.Since(start).Truncate(time.Millisecond))
}
