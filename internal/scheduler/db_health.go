// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

const (
	// dbHealthProbeInterval is how often the monitor pings the database.
	dbHealthProbeInterval = 5 * time.Second
	// dbHealthReminderInterval throttles "still paused" reminders while the DB
	// stays down, so a long outage logs once a minute instead of per-probe.
	dbHealthReminderInterval = 60 * time.Second
	// dbHealthFailureThreshold is how many CONSECUTIVE failed probes must occur
	// before the monitor declares the database unavailable and pauses
	// collection. Debounce, added after the 2026-06-11 diagnostic: on a
	// CPU-saturated host the TLS + SCRAM-SHA-256 handshake for a NEW connection
	// can briefly exceed the 5s connect deadline (the probe's Ping cold-opens a
	// pooled connection), producing "context deadline exceeded" blips that clear
	// in 0–5s. Those are not outages. A real restart (the nightly
	// unattended-upgrades event) is down for minutes and fails many consecutive
	// probes. At a 5s interval, 3 consecutive failures = ~15s of sustained
	// unreachability before pausing — past the transient blips, well within a
	// real restart. The pause exists to suppress the multi-minute restart storm,
	// so arming it ~10s later costs nothing.
	dbHealthFailureThreshold = 3
	// dbHealthStatusName is this subsystem's row in aveloxis_ops.aveloxis_status.
	dbHealthStatusName  = "database"
	dbHealthSource      = "db health monitor"
	dbStatusUnavailable = "unavailable"
)

// healthTransition is the up/down edge a probe produced.
type healthTransition int

const (
	transitionNone healthTransition = iota
	transitionDown                  // healthy -> unavailable
	transitionUp                    // unavailable -> healthy
)

// classifyHealthTransition is the pure decision the monitor acts on.
func classifyHealthTransition(was, now bool) healthTransition {
	switch {
	case was && !now:
		return transitionDown
	case !was && now:
		return transitionUp
	default:
		return transitionNone
	}
}

// debouncedHealthy maps a running count of consecutive failed probes to the
// effective (debounced) health state. The database is considered healthy until
// the failures reach dbHealthFailureThreshold — so a single transient
// connect/auth timeout (a CPU-pressure blip) doesn't pause the fleet, while a
// sustained failure (a real restart) does. A single success resets the count to
// 0, so recovery is reported immediately.
func debouncedHealthy(consecutiveFailures int) bool {
	return consecutiveFailures < dbHealthFailureThreshold
}

// runDBHealthMonitor probes the database every dbHealthProbeInterval and
// maintains s.dbHealthy (which fillWorkerSlots gates on). The point is to turn
// a Postgres restart — the nightly unattended-upgrades/needrestart event — from
// a 500K-line connection-error storm + reconnect deadlock pile-up into a clean
// pause/resume:
//
//   - On healthy → unavailable: log ONCE (WARN), best-effort record
//     aveloxis_status='unavailable' (the write usually fails — the DB is down —
//     which is fine; the log is the live signal), and stop new claims.
//   - While down: a throttled "still paused" reminder (once/minute), not per-probe.
//   - On unavailable → healthy: log ONCE (INFO) and record aveloxis_status='ok'
//     with the outage duration, so the operator sees what happened.
func (s *Scheduler) runDBHealthMonitor(ctx context.Context) {
	t := time.NewTicker(dbHealthProbeInterval)
	defer t.Stop()
	var downSince time.Time
	var lastReminder time.Time
	var consecutiveFail int
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := s.store.Ping(ctx)
			if errors.Is(err, context.Canceled) {
				return // shutdown mid-probe is not an outage (pass 35)
			}
			if err != nil {
				consecutiveFail++
				lastErr = err
			} else {
				consecutiveFail = 0
				lastErr = nil
			}
			// Debounced: a single transient connect/auth timeout (CPU-pressure
			// blip) keeps us healthy; only dbHealthFailureThreshold consecutive
			// failures pauses the fleet. See dbHealthFailureThreshold.
			healthy := debouncedHealthy(consecutiveFail)
			was := s.dbHealthy.Swap(healthy)
			switch classifyHealthTransition(was, healthy) {
			case transitionDown:
				downSince = time.Now()
				lastReminder = downSince
				s.logger.Warn("database unavailable — pausing new collection until it returns",
					"error", lastErr, "consecutive_failures", consecutiveFail,
					"probe_interval", dbHealthProbeInterval.String())
				// Best-effort: the DB is down, so this write typically fails.
				_ = s.store.SetAveloxisStatus(ctx, dbHealthStatusName, dbStatusUnavailable,
					"database probe failed "+intString(consecutiveFail)+"x consecutively ("+errString(lastErr)+") — collection paused", dbHealthSource)
			case transitionUp:
				dur := time.Since(downSince).Round(time.Second)
				s.logger.Info("database back — resuming collection", "unavailable_for", dur.String())
				_ = s.store.SetAveloxisStatus(ctx, dbHealthStatusName, db.StatusOK,
					"recovered; collection was paused ~"+dur.String(), dbHealthSource)
			case transitionNone:
				switch {
				case !healthy && time.Since(lastReminder) >= dbHealthReminderInterval:
					lastReminder = time.Now()
					s.logger.Warn("collection still paused — database unavailable",
						"unavailable_for", time.Since(downSince).Round(time.Second).String())
				case err != nil && healthy:
					// Sub-threshold transient failure: a CPU-pressure connect/auth
					// blip, not an outage. Visible at DEBUG for investigation,
					// quiet at the default INFO so it doesn't read as a real
					// outage. If these are frequent, the host is under connection
					// pressure (raise pool MinConns / reduce CPU load).
					s.logger.Debug("database probe failed transiently — not pausing (below threshold)",
						"error", err, "consecutive_failures", consecutiveFail,
						"threshold", dbHealthFailureThreshold)
				}
			}
		}
	}
}

func intString(n int) string {
	return strconv.Itoa(n)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
