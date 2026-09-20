// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.29.57 — materialized views became optional, and the weekly rebuild was
// left ungated: on a deployment that turned them off, every rebuild day
// refreshed twenty views that do not exist, logging twenty WARNs and one
// ERROR ("refresh left 20 of 20 views stale") about a feature the operator
// had switched off. `aveloxis refresh-views` had been given a CONFIG gate
// instead, which disagreed with the weekly pass in the other direction: with
// the views still present but the knob off, it announced there was nothing to
// refresh and exited 0 over real, stale views.
//
// One predicate, owned by the primitive both callers share, and it asks the
// DATABASE rather than the config: what is true of this database is what
// decides (SR-18).

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestRefreshMaterializedViewsOnADatabaseWithoutThem(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	// The package database is prepared without views (that is the default
	// now), so this is the state an operator with materialized_views off
	// runs in every day.
	var present int
	if err := store.pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data'`).Scan(&present); err != nil {
		t.Fatal(err)
	}
	if present != 0 {
		t.Skipf("this database has %d materialized views; the absent case cannot be driven here", present)
	}

	if err := RefreshMaterializedViews(ctx, store, logger); err != nil {
		t.Errorf("RefreshMaterializedViews on a database with no views = %v, want nil — there is nothing stale about a view that does not exist, and the weekly pass reports this as a failure every rebuild day", err)
	}
}
