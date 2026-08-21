// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"testing"

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
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", testMigrateLockID); err != nil {
		t.Fatalf("test-migrate advisory lock: %v", err)
	}
	defer func() { _, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", testMigrateLockID) }()
	if store.GetSchemaVersion(ctx) == db.ToolVersion {
		return // another test binary migrated while we waited
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
