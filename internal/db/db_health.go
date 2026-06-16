// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"time"
)

// pingTimeout bounds a health probe so a hung/half-open connection during a DB
// restart can't block the scheduler's health monitor.
const pingTimeout = 5 * time.Second

// Ping reports whether the database is reachable. It returns an error when the
// server is down, restarting (SQLSTATE 57P03 "shutting down" / "starting up"),
// or unreachable — the signal the scheduler's DB-health monitor uses to pause
// collection (the 2026-06-09 nightly-restart storm). Bounded by pingTimeout.
func (s *PostgresStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	return s.pool.Ping(ctx)
}
