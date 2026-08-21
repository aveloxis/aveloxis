// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// testMigrate — the collector-side twin of internal/db's version-gated
// test migrate (see internal/db/testexec_test.go for the deadlock-class
// rationale): the stamp is written only when every migration step
// succeeded, so a matching stamp proves this binary's schema is fully
// applied and the shared scratch DB needs no re-migrate.
func testMigrate(ctx context.Context, t testing.TB, store *db.PostgresStore) {
	t.Helper()
	// Fast path: steady-state stamped DB — no lock traffic.
	if store.GetSchemaVersion(ctx) == db.ToolVersion {
		return
	}
	// v0.27.125 (Copilot round 16): recheck under the shared test-scoped
	// advisory lock — on fresh parallel runs every binary sees the old
	// stamp, and without this only the QUEUEING was serialized, not the
	// redundant DDL. Same lock value as internal/db's testexec_test.go
	// (the two packages' binaries must serialize against each other).
	const testMigrateLockID int64 = 0x41564C5854455354 // "AVLXTEST"
	conn, err := store.Pool().Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn for test-migrate lock: %v", err)
	}
	// v0.27.130 (round 18): unlock with a FRESH bounded context and
	// destroy the session if the unlock cannot be confirmed — releasing
	// a still-locked session to the pool wedges the db twin's poll loop
	// (twin of internal/db/testexec_test.go).
	defer func() {
		uctx, ucancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ucancel()
		if _, uerr := conn.Exec(uctx, "SELECT pg_advisory_unlock($1)", testMigrateLockID); uerr != nil {
			_ = conn.Conn().Close(uctx)
		}
		conn.Release()
	}()
	// v0.27.128 (Copilot round 17): pg_try_advisory_lock POLLING, never
	// the blocking form — a blocked waiter holds a snapshot the holder's
	// CREATE INDEX CONCURRENTLY waits on (the v0.27.20 undetectable
	// deadlock). Twin of internal/db/testexec_test.go.
	for {
		var got bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", testMigrateLockID).Scan(&got); err != nil {
			t.Fatalf("test-migrate advisory lock poll: %v", err)
		}
		if got {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done waiting for test-migrate lock: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if store.GetSchemaVersion(ctx) == db.ToolVersion {
		return // another test binary migrated while we waited
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
