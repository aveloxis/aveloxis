// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// advisoryUnlockTimeout bounds the unlock a held lock's release sends: it
// runs on a fresh context, because the caller's may be the one that was
// just canceled (the v0.27.130 lesson).
const advisoryUnlockTimeout = 10 * time.Second

// heldAdvisoryLock is a session-level advisory lock and the pool connection
// whose session holds it. The lock lives exactly as long as that session, so
// work that must happen only while the lock is held runs on conn: if the
// session dies, the work fails instead of running unlocked.
type heldAdvisoryLock struct {
	conn *pgxpool.Conn
	id   int64
}

// tryHoldAdvisoryLock takes advisory lock id on a connection of its own,
// without waiting: held is false when another session holds it. The one
// spelling of a try-once session lock (the dm_ aggregate rebuild, the API's
// saved re-warm list); the migrate lock polls while it waits and the Jira
// registration lock is transaction-scoped, so they stay apart (whole-branch
// review of 0.29.119–0.29.123, A1).
func (s *PostgresStore) tryHoldAdvisoryLock(ctx context.Context, id int64) (*heldAdvisoryLock, bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquiring a connection for advisory lock %#x: %w", id, err)
	}
	var held bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, id).Scan(&held); err != nil {
		conn.Release()
		return nil, false, fmt.Errorf("advisory lock %#x: %w", id, err)
	}
	if !held {
		conn.Release()
		return nil, false, nil
	}
	return &heldAdvisoryLock{conn: conn, id: id}, true, nil
}

// release unlocks and returns the connection to the pool. An unconfirmed
// unlock destroys the session instead, so the lock can never outlive its
// holder on a pooled connection.
func (l *heldAdvisoryLock) release() {
	ctx, cancel := context.WithTimeout(context.Background(), advisoryUnlockTimeout)
	defer cancel()
	var unlocked bool
	if err := l.conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, l.id).Scan(&unlocked); err != nil || !unlocked {
		_ = l.conn.Conn().Close(ctx)
	}
	l.conn.Release()
}
