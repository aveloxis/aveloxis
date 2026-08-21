// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.130 — F13: the stamp-trust migrate fast path for `aveloxis
// serve`. Production observation (v0.27.96): serve was still inside
// RunMigrations 1h42m after restart — the one-shot keyset backfills
// re-walk ALL their PK windows on every boot just to find nothing to
// do — with 141,799 repos queued and zero collection. The stamp in
// schema_meta is written ONLY after every migration step succeeded, so
// stamp == ToolVersion proves this binary's schema is fully applied.
//
// Contract:
//   - serve opts in via SetMigrateFastPath(true); `aveloxis migrate`
//     NEVER opts in (the explicit command is the operator's full-run /
//     self-heal path — the escape hatch for hand-edited schemas);
//   - the gate sits BEFORE the advisory lock (a matching stamp means
//     the migration for this version COMPLETED at least once; steps
//     are idempotent, so racing a concurrent full run is safe);
//   - a skipped run also skips views.sql + matview creation — those
//     only change with a version bump, which changes ToolVersion and
//     misses the stamp; the documented heal path for hand-dropped
//     views is `aveloxis migrate`.
package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestMigrateFastPathContract(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func RunMigrations(")
	gate := strings.Index(body, "migrateFastPath")
	lock := strings.Index(body, "pg.pool.Acquire")
	if gate < 0 {
		t.Fatal("RunMigrations must carry the F13 fast-path gate")
	}
	if lock >= 0 && gate > lock {
		t.Error("the fast-path gate must sit BEFORE the advisory lock — a matching stamp needs no serialization (steps are idempotent; the stamp only lands after a COMPLETE run)")
	}
	pg, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pg), "func (s *PostgresStore) SetMigrateFastPath(") {
		t.Error("PostgresStore must expose SetMigrateFastPath (the SetMatviewSkip pattern)")
	}
}

func TestServeOptsIntoFastPathMigrateCmdDoesNot(t *testing.T) {
	main := srctest.Read(t, "cmd/aveloxis/main.go")
	serveBody := srctest.FuncBody(t, main, "func runServe(")
	if !strings.Contains(serveBody, "SetMigrateFastPath(true)") {
		t.Error("runServe must enable the F13 fast path — serve restarts must not re-walk every backfill window")
	}
	// The explicit migrate command must stay a FULL run: it is the
	// operator's self-heal path for hand-edited schemas.
	mig := srctest.Read(t, "cmd/aveloxis/main.go")
	migBody := srctest.FuncBody(t, mig, "func migrateCmd(")
	if strings.Contains(migBody, "SetMigrateFastPath(true)") {
		t.Error("`aveloxis migrate` must NEVER fast-path — it is the documented full-run escape hatch")
	}
}

// TestMigrateFastPathSkipsAndFullRunHeals — behavioral, against the
// live scratch DB: with a current stamp, a fast-path run must SKIP the
// migration steps (a hand-dropped migration-owned index stays absent),
// and a full run must still heal it.
func TestMigrateFastPathSkipsAndFullRunHeals(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)

	// Ensure the stamp is current (full migrate if needed).
	testMigrate(ctx, t, store)
	if v := store.GetSchemaVersion(ctx); v != ToolVersion {
		t.Fatalf("precondition: stamp %q != ToolVersion %q", v, ToolVersion)
	}

	// Drop a small migration-owned index, then fast-path migrate: the
	// index must STAY absent (proof the steps were skipped).
	const probeIdx = "idx_repos_added_at"
	mustExecRetry(ctx, t, store, `DROP INDEX IF EXISTS aveloxis_data.`+probeIdx)
	t.Cleanup(func() {
		// Whatever happened, leave the scratch DB healed for other tests.
		cctx, ccancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer ccancel()
		healStore, herr := NewPostgresStore(cctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if herr != nil {
			return
		}
		defer healStore.Close()
		healStore.SetMatviewSkip(true)
		_ = RunMigrations(cctx, healStore, slog.New(slog.NewTextHandler(io.Discard, nil)))
	})

	indexExists := func() bool {
		var n int
		if err := store.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM pg_indexes WHERE schemaname='aveloxis_data' AND indexname=$1`,
			probeIdx).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	if indexExists() {
		t.Fatal("precondition: probe index should be dropped")
	}

	store.SetMigrateFastPath(true)
	start := time.Now()
	if err := RunMigrations(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("fast-path migrate: %v", err)
	}
	fastDur := time.Since(start)
	if indexExists() {
		t.Error("fast path re-created the probe index — the migration steps were NOT skipped")
	}
	if fastDur > 5*time.Second {
		t.Errorf("fast path took %v — it must be a stamp read, not a migration", fastDur)
	}

	// Full run (fast path off) heals the index — the escape hatch works.
	store.SetMigrateFastPath(false)
	if err := RunMigrations(ctx, store, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("full migrate: %v", err)
	}
	if !indexExists() {
		t.Error("the full run must heal the dropped index")
	}
}
