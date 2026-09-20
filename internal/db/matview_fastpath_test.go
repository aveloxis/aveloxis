// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 (Copilot review 5260880711 on PR #210) — serve's F13 fast path
// returns as soon as the schema stamp matches the binary, which is BEFORE the
// matview block. So an operator who set collection.materialized_views back to
// true got nothing at the next serve: the stamp already matched, the views
// stayed absent, and only a manual `aveloxis migrate` would have built them —
// while the config's own documentation said serve creates the missing ones.
//
// The fast path skips the migration WALK. Whether this deployment's views
// exist is a different question, and it costs one catalog query to answer.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestFastPathStillCreatesMissingViews(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	base := os.Getenv("AVELOXIS_TEST_DB")
	dsn2 := emptyTestDatabase(t, ctx, base)

	store, err := NewPostgresStore(ctx, dsn2, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	// A deployment that ran with materialized_views off: fully migrated and
	// stamped, no views.
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("first migrate (views off): %v", err)
	}
	var present int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data'`).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present != 0 {
		t.Fatalf("the views-off migrate built %d materialized views, want 0 — this fixture is not the state it claims", present)
	}

	// The operator turns them on and restarts serve. serve takes the fast
	// path, because the stamp already matches this binary.
	store.SetMigrateFastPath(true)
	store.SetMatviewMode(MatviewsIfMissing)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("serve migrate (views on): %v", err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data'`).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present != len(matviewNames) {
		t.Errorf("after re-enabling the feature and restarting serve, the database has %d of %d materialized views — the fast path returned before the view block, so the setting takes effect only on a manual migrate", present, len(matviewNames))
	}
}
