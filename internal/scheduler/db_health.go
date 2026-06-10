// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

const (
	// dbHealthProbeInterval is how often the monitor pings the database.
	dbHealthProbeInterval = 5 * time.Second
	// dbHealthReminderInterval throttles "still paused" reminders while the DB
	// stays down, so a long outage logs once a minute instead of per-probe.
	dbHealthReminderInterval = 60 * time.Second
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := s.store.Ping(ctx)
			healthy := err == nil
			was := s.dbHealthy.Swap(healthy)
			switch classifyHealthTransition(was, healthy) {
			case transitionDown:
				downSince = time.Now()
				lastReminder = downSince
				s.logger.Warn("database unavailable — pausing new collection until it returns",
					"error", err, "probe_interval", dbHealthProbeInterval.String())
				// Best-effort: the DB is down, so this write typically fails.
				_ = s.store.SetAveloxisStatus(ctx, dbHealthStatusName, dbStatusUnavailable,
					"database probe failed ("+errString(err)+") — collection paused", dbHealthSource)
			case transitionUp:
				dur := time.Since(downSince).Round(time.Second)
				s.logger.Info("database back — resuming collection", "unavailable_for", dur.String())
				_ = s.store.SetAveloxisStatus(ctx, dbHealthStatusName, db.StatusOK,
					"recovered at startup-of-probe; was unavailable ~"+dur.String(), dbHealthSource)
			case transitionNone:
				if !healthy && time.Since(lastReminder) >= dbHealthReminderInterval {
					lastReminder = time.Now()
					s.logger.Warn("collection still paused — database unavailable",
						"unavailable_for", time.Since(downSince).Round(time.Second).String())
				}
			}
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
