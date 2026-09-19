// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"time"
)

// PingTimeout bounds a health probe so a hung/half-open connection during a DB
// restart can't block the scheduler's health monitor. Exported because the
// scheduler's stall detector reports stalls at least this long: a stall that
// can fail a probe is the one worth explaining (v0.29.56).
const PingTimeout = 5 * time.Second

// Ping reports whether the database is reachable. It returns an error when the
// server is down, restarting (SQLSTATE 57P03 "shutting down" / "starting up"),
// or unreachable — the signal the scheduler's DB-health monitor uses to pause
// collection (the 2026-06-09 nightly-restart storm). Bounded by PingTimeout.
func (s *PostgresStore) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, PingTimeout)
	defer cancel()
	return s.pool.Ping(ctx)
}

// PoolState is a reading of the connection pool, for the log line that
// explains a collection pause (v0.29.56).
type PoolState struct {
	MaxConns             int32
	TotalConns           int32
	AcquiredConns        int32
	IdleConns            int32
	ConstructingConns    int32
	EmptyAcquireCount    int64
	CanceledAcquireCount int64
	AcquireDuration      time.Duration
}

// PoolState reports the pool's current state.
func (s *PostgresStore) PoolState() PoolState {
	st := s.pool.Stat()
	return PoolState{
		MaxConns:             st.MaxConns(),
		TotalConns:           st.TotalConns(),
		AcquiredConns:        st.AcquiredConns(),
		IdleConns:            st.IdleConns(),
		ConstructingConns:    st.ConstructingConns(),
		EmptyAcquireCount:    st.EmptyAcquireCount(),
		CanceledAcquireCount: st.CanceledAcquireCount(),
		AcquireDuration:      st.AcquireDuration(),
	}
}
