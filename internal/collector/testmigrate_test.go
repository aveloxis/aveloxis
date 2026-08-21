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
	if store.GetSchemaVersion(ctx) == db.ToolVersion {
		return
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
