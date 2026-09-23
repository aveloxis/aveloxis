// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSupplyChainViewsAreAveloxisOwned pins the split (worklist 48): the
// supply-chain pair is built from the Go SQL the API runs live, never from
// matviews.sql (the 8Knot batch), and is absent from the 8Knot registry, so
// the weekly rebuild, serve's whole-set probe and `refresh-views --set
// 8knot` leave it alone.
func TestSupplyChainViewsAreAveloxisOwned(t *testing.T) {
	src, err := os.ReadFile("matviews.sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(SupplyChainViewNames) != 2 {
		t.Fatalf("SupplyChainViewNames = %v, want the exposure and advisory views", SupplyChainViewNames)
	}
	for _, v := range SupplyChainViewNames {
		if strings.Contains(string(src), v) {
			t.Errorf("matviews.sql mentions %s — the supply-chain views are Go-owned; a copy in the 8Knot batch is a second definition (SR-17) and ties the pair to the hours-long rebuild", v)
		}
		for _, n := range matviewNames {
			if strings.TrimPrefix(n, "aveloxis_data.") == v {
				t.Errorf("%s is in matviewNames — the 8Knot registry; the pair has its own lifecycle", v)
			}
		}
	}
	if _, err := os.Stat("../../scripts/gen-package-exposure-sql"); err == nil {
		t.Error("scripts/gen-package-exposure-sql still exists — the generator wrote the copy that no longer exists")
	}
}

// TestSupplyChainViewsBuildRefreshAndProbe (AVELOXIS_TEST_DB): the pair
// builds from nothing, is left alone when complete, rebuilds a missing
// member on its own (no whole-set rule — each view is one statement), and
// refreshes CONCURRENTLY (the unique index exists). The 8Knot set is never
// touched by any of it.
func TestSupplyChainViewsBuildRefreshAndProbe(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dropSupplyChainViews(t, store)

	eightKnot := func() int {
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_matviews WHERE schemaname = 'aveloxis_data' AND matviewname <> ALL($1)`, SupplyChainViewNames).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	before := eightKnot()

	if n := mustPresent(t, store); n != 0 {
		t.Fatalf("fixture: %d supply-chain views present before the build, want 0", n)
	}
	built, err := CreateSupplyChainViewsIfMissing(ctx, store, logger)
	if err != nil {
		t.Fatalf("build from nothing: %v", err)
	}
	if !built {
		t.Error("an empty pair must report built=true")
	}
	if n := mustPresent(t, store); n != 2 {
		t.Fatalf("after the build %d present, want 2", n)
	}
	built, err = CreateSupplyChainViewsIfMissing(ctx, store, logger)
	if err != nil || built {
		t.Errorf("a complete pair must be left alone: built=%v err=%v", built, err)
	}

	// The unique index is what lets REFRESH run CONCURRENTLY.
	for _, v := range SupplyChainViewNames {
		var idx int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes WHERE schemaname = 'aveloxis_data' AND tablename = $1 AND indexdef LIKE 'CREATE UNIQUE INDEX%'`, v).Scan(&idx); err != nil {
			t.Fatal(err)
		}
		if idx != 1 {
			t.Errorf("%s has %d unique indexes, want 1 (REFRESH CONCURRENTLY needs it)", v, idx)
		}
	}
	if err := RefreshSupplyChainViews(ctx, store, logger); err != nil {
		t.Errorf("refresh: %v", err)
	}

	// One member dropped by hand: the next IfMissing rebuilds THAT one —
	// and reports built=false, because the other member is pre-existing
	// and still owed the startup refresh (round 2 finding 3).
	mustExecRetry(ctx, t, store, `DROP MATERIALIZED VIEW aveloxis_data.`+SupplyChainViewNames[1])
	built, err = CreateSupplyChainViewsIfMissing(ctx, store, logger)
	if err != nil || built {
		t.Errorf("completing a partial pair must not claim the whole pair was built: built=%v err=%v", built, err)
	}
	if n := mustPresent(t, store); n != 2 {
		t.Errorf("after completing the pair %d present, want 2", n)
	}

	// Rebuild re-creates both (a changed definition lands here).
	if err := CreateSupplyChainViews(ctx, store, logger); err != nil {
		t.Errorf("rebuild: %v", err)
	}
	if n := mustPresent(t, store); n != 2 {
		t.Errorf("after the rebuild %d present, want 2", n)
	}

	// Nothing to refresh is not an error.
	dropSupplyChainViews(t, store)
	if err := RefreshSupplyChainViews(ctx, store, logger); err != nil {
		t.Errorf("refresh with no views: %v", err)
	}
	if after := eightKnot(); after != before {
		t.Errorf("the 8Knot set changed from %d to %d views — the pair's lifecycle must not touch it", before, after)
	}
}

// TestMigrateBuildsTheSupplyChainPairByItsOwnMode (AVELOXIS_TEST_DB): the
// pair's mode is independent of the 8Knot mode — a deployment with
// collection.materialized_views off (this test store) still gets the pair
// when it asks, and the zero value still builds nothing.
func TestMigrateBuildsTheSupplyChainPairByItsOwnMode(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dropSupplyChainViews(t, store)

	var zero PostgresStore
	if zero.supplyChainMode != MatviewsOff {
		t.Error("the zero PostgresStore must build no supply-chain views")
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("migrate (off): %v", err)
	}
	if n := mustPresent(t, store); n != 0 {
		t.Fatalf("MatviewsOff built %d supply-chain views", n)
	}
	for _, mode := range []MatviewMode{MatviewsIfMissing, MatviewsRebuild} {
		store.SetSupplyChainViewMode(mode)
		if err := RunMigrations(ctx, store, logger); err != nil {
			t.Fatalf("migrate (%d): %v", mode, err)
		}
		if n := mustPresent(t, store); n != 2 {
			t.Errorf("mode %d: %d supply-chain views present, want 2", mode, n)
		}
	}
	// serve's fast path (a current stamp) still completes a missing member.
	mustExecRetry(ctx, t, store, `DROP MATERIALIZED VIEW aveloxis_data.`+SupplyChainViewNames[0])
	store.SetMigrateFastPath(true)
	store.SetSupplyChainViewMode(MatviewsIfMissing)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("fast-path migrate: %v", err)
	}
	if n := mustPresent(t, store); n != 2 {
		t.Errorf("the fast path left %d of 2 supply-chain views", n)
	}
}

// TestEightKnotRefreshIgnoresTheSupplyChainPair (AVELOXIS_TEST_DB) — review
// round 1 finding 1: the 8Knot refresh asked "any matview in aveloxis_data?"
// so, with the pair now built on every deployment, a database WITHOUT the
// 8Knot set answered "present" and got twenty REFRESHes of absent views
// (20 WARNs + an ERROR, nonzero exit, every rebuild day under the claims
// gate) — the v0.29.57 symptom back. The probe must count the 8Knot set
// by name and no-op when it is absent.
func TestEightKnotRefreshIgnoresTheSupplyChainPair(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	dropSupplyChainViews(t, store)
	if n := eightKnotViewsPresentForTest(t, store); n != 0 {
		t.Skipf("this database has %d 8Knot views; the fixture needs none", n)
	}
	if err := CreateSupplyChainViews(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	var log strings.Builder
	logger := slog.New(slog.NewTextHandler(&log, nil))
	if err := RefreshMaterializedViews(ctx, store, logger); err != nil {
		t.Errorf("8Knot refresh with only the pair present must be a no-op, got %v", err)
	}
	if strings.Contains(log.String(), "refreshing materialized views") || strings.Contains(log.String(), "failed to refresh") {
		t.Errorf("the 8Knot refresh issued REFRESHes for absent views:\n%s", log.String())
	}
	if !strings.Contains(log.String(), "no 8Knot materialized views") {
		t.Errorf("the no-op must be said out loud:\n%s", log.String())
	}
	// And the pair's own refresh still works beside an absent 8Knot set.
	if err := RefreshSupplyChainViews(ctx, store, logger); err != nil {
		t.Errorf("pair refresh: %v", err)
	}
}

func eightKnotViewsPresentForTest(t *testing.T, store *PostgresStore) int {
	t.Helper()
	n, err := eightKnotMatviewsPresent(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestServeSkipsTheStartupRefreshItJustBuilt (AVELOXIS_TEST_DB) — review
// round 1 finding 4: a start that BUILT the pair (WITH DATA) must not
// refresh it seconds later; a start that found it must.
func TestServeSkipsTheStartupRefreshItJustBuilt(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dropSupplyChainViews(t, store)
	store.SetSupplyChainViewMode(MatviewsIfMissing)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	if !store.SupplyChainViewsBuiltThisRun() {
		t.Error("the migrate that built the pair must say so")
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	if store.SupplyChainViewsBuiltThisRun() {
		t.Error("a migrate that found the pair complete must not claim to have built it")
	}
	// A partial pair: the pre-existing member aged through the downtime,
	// so the startup refresh must still run (round 2 finding 3).
	mustExecRetry(ctx, t, store, `DROP MATERIALIZED VIEW aveloxis_data.`+SupplyChainViewNames[1])
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatal(err)
	}
	if store.SupplyChainViewsBuiltThisRun() {
		t.Error("completing a partial pair must not skip the startup refresh — the other member is stale")
	}
	if n := mustPresent(t, store); n != 2 {
		t.Errorf("the partial pair was not completed: %d present", n)
	}
}

// TestRebuildFailureIsAnErrorForTheChecklist (AVELOXIS_TEST_DB) — round 2
// finding 1: on the Rebuild path a re-create that did not land leaves the
// previous views served, and the deploy checklist greps the migrate log
// for a `supply-chain view` ERROR — a WARN would pass the gate over an
// unapplied definition. A cancelled context is the one failure a test can
// provoke without touching the database.
func TestRebuildFailureIsAnErrorForTheChecklist(t *testing.T) {
	store := openTestStore(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		mode  MatviewMode
		level string
	}{{MatviewsRebuild, "level=ERROR"}, {MatviewsIfMissing, "level=WARN"}} {
		var log strings.Builder
		store.SetSupplyChainViewMode(tc.mode)
		err := applySupplyChainViewMode(cancelled, store, slog.New(slog.NewTextHandler(&log, nil)))
		line := ""
		for _, l := range strings.Split(log.String(), "\n") {
			if strings.Contains(l, "supply-chain view") && strings.Contains(l, tc.level) {
				line = l
			}
		}
		if line == "" {
			t.Errorf("mode %d: want ONE %s line naming the supply-chain view, got:\n%s", tc.mode, tc.level, log.String())
		}
		// Round 3: the Rebuild path is a FAILED migrate (exit nonzero, no
		// stamp); IfMissing (serve) never blocks a start.
		if (tc.mode == MatviewsRebuild) != (err != nil) {
			t.Errorf("mode %d: returned %v", tc.mode, err)
		}
	}
	src, rerr := os.ReadFile("migrate.go")
	if rerr != nil {
		t.Fatal(rerr)
	}
	if !strings.Contains(srctest.StripGoComments(srctest.FuncBody(t, string(src), "func RunMigrations(")), "if err := applySupplyChainViewMode(ctx, pg, logger); err != nil {\n\t\terrs = append(errs, err)") {
		t.Error("the full migrate walk must append the supply-chain re-create failure to errs (fail closed, no stamp)")
	}
}

func mustPresent(t *testing.T, store *PostgresStore) int {
	t.Helper()
	n, err := supplyChainViewsPresent(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func dropSupplyChainViews(t *testing.T, store *PostgresStore) {
	t.Helper()
	for _, v := range SupplyChainViewNames {
		mustExecRetry(context.Background(), t, store, `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.`+v)
	}
	t.Cleanup(func() {
		for _, v := range SupplyChainViewNames {
			cleanupExecRetry(context.Background(), store, `DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.`+v)
		}
	})
}

// Test-only (PR #212 review: production code uses supplyChainViewsPresentSet).
// supplyChainViewsPresent counts the members of the pair that exist.
func supplyChainViewsPresent(ctx context.Context, pg *PostgresStore) (int, error) {
	present, err := supplyChainViewsPresentSet(ctx, pg)
	if err != nil {
		return 0, err
	}
	return len(present), nil
}
