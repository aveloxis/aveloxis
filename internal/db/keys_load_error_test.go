// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestLoadAPIKeysDistinguishesAMissingTableFromAFailure pins worklist item
// 55: LoadAPIKeys swallowed EVERY error from either key table, so a lost
// connection or a cancelled context read as "no keys configured" (and with
// --augur-keys a failed worker_oauth read silently returned the Augur keys).
// The swallow exists for one real case — the table (or its schema) does not
// exist yet, before migrate, or augur_operations absent — and only that case
// is "none"; every other failure is returned.
func TestLoadAPIKeysDistinguishesAMissingTableFromAFailure(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	// A missing table and a missing schema are "none".
	if keys, err := loadAPIKeysFrom(ctx, store.pool, "aveloxis_ops._av_no_such_table", "no_such_schema.worker_oauth", "github", true); err != nil || len(keys) != 0 {
		t.Errorf("missing tables = (%v, %v); want (none, nil)", keys, err)
	}
	// Anything else is the failure it is.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := LoadAPIKeys(canceled, store.pool, "github", false); err == nil {
		t.Error("a cancelled lookup = nil error; want the failure returned, not \"no keys\"")
	}
	if _, err := LoadAPIKeys(canceled, store.pool, "github", true); err == nil {
		t.Error("a cancelled lookup with --augur-keys = nil error; want the failure, not the Augur fallback")
	}
	// The real tables answer without error (possibly empty).
	if _, err := LoadAPIKeys(ctx, store.pool, "github", false); err != nil {
		t.Errorf("LoadAPIKeys on the migrated database: %v", err)
	}
}
