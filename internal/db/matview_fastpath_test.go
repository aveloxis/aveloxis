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
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
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

// TestIfMissingReportsAPartialSet (AVELOXIS_TEST_DB) — Copilot review
// 5271953014: the startup probe checked one sentinel view, so a set with any
// OTHER relation dropped stayed partial SILENTLY on every restart. The probe
// counts the whole managed set (matviews and alias views); a partial set is
// reported at ERROR naming the missing relation and is NOT rebuilt at
// startup (matviews.sql is one batch — that would re-create all twenty, the
// multi-hour startup ruled out in 2024); an empty set is built.
func TestIfMissingReportsAPartialSet(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	// Its own database: the package's shared one is prepared without views
	// and a sibling test skips when any exist (fix-review round 1).
	dsn2 := emptyTestDatabase(t, ctx, dsn)
	store, err := NewPostgresStore(ctx, dsn2, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewMode(MatviewsRebuild)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("migrate with views: %v", err)
	}
	managed := len(matviewNames) + len(MatviewAliasNames)
	present, err := managedMatviewsPresent(ctx, store)
	if err != nil || present != managed {
		t.Fatalf("after a rebuild %d of %d managed relations exist (%v)", present, managed, err)
	}
	for _, drop := range []struct{ kind, name string }{
		{"MATERIALIZED VIEW", matviewNames[len(matviewNames)-1]}, // a non-sentinel matview
		{"VIEW", "aveloxis_data." + MatviewAliasNames[0]},        // an alias view (round 1)
	} {
		if _, err := store.pool.Exec(ctx, `DROP `+drop.kind+` IF EXISTS `+drop.name+` CASCADE`); err != nil {
			t.Fatal(err)
		}
		if present, err := managedMatviewsPresent(ctx, store); err != nil || present != managed-1 {
			t.Fatalf("the drop of %s did not land: %d present, %v", drop.name, present, err)
		}
		logs.Reset()
		if err := CreateMaterializedViewsIfNotExist(ctx, store, logger); err != nil {
			t.Fatalf("if-missing on a partial set: %v", err)
		}
		if present, err := managedMatviewsPresent(ctx, store); err != nil || present != managed-1 {
			t.Errorf("if-missing on a partial set without %s changed the set (%d of %d present, %v) — startup must report, never rebuild", drop.name, present, managed, err)
		}
		bare := strings.TrimPrefix(drop.name, "aveloxis_data.")
		if !strings.Contains(logs.String(), "level=ERROR") || !strings.Contains(logs.String(), "aveloxis migrate") {
			t.Errorf("a partial set must be reported at ERROR with the plain migrate: %s", logs.String())
		}
		// EXACTLY the dropped relation — a list that names everything managed
		// would satisfy a Contains and leave the operator no wiser (round 3
		// mutant).
		if missing, err := managedMatviewsMissing(ctx, store); err != nil || len(missing) != 1 || missing[0] != bare {
			t.Errorf("managedMatviewsMissing = %v, %v; want exactly [%s]", missing, err, bare)
		}
		if !strings.Contains(logs.String(), "["+bare+"]") {
			t.Errorf("the ERROR must name exactly %s as missing: %s", bare, logs.String())
		}
		// The plain migrate (the documented fix) restores it.
		if err := CreateMaterializedViews(ctx, store, logger); err != nil {
			t.Fatalf("plain rebuild: %v", err)
		}
		if present, err := managedMatviewsPresent(ctx, store); err != nil || present != managed {
			t.Fatalf("after the plain rebuild %d of %d present (%v)", present, managed, err)
		}
	}
	// An EMPTY set is built at startup (the first-run and turned-back-on cases).
	if _, err := store.pool.Exec(ctx, `DROP MATERIALIZED VIEW IF EXISTS `+strings.Join(matviewNames, ", ")+` CASCADE`); err != nil {
		t.Fatal(err)
	}
	if present, _ := managedMatviewsPresent(ctx, store); present != 0 {
		t.Fatalf("the full drop left %d relations (the alias views depend on matviews and go with CASCADE)", present)
	}
	if err := CreateMaterializedViewsIfNotExist(ctx, store, logger); err != nil {
		t.Fatalf("if-missing on an empty set: %v", err)
	}
	if present, err := managedMatviewsPresent(ctx, store); err != nil || present != managed {
		t.Errorf("if-missing on an empty set built %d of %d (%v)", present, managed, err)
	}
}
