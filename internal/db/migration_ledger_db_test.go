// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// migration_ledger_db_test.go — behavioral tests for the v0.28.4
// completed-backfill ledger against a live Postgres (gated on
// AVELOXIS_TEST_DB). The source pins in migration_ledger_test.go
// freeze the shapes; these prove the runtime semantics: miss → run +
// record, hit → skip, failure → unrecorded (retries next migrate).

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

func ledgerConnect(t *testing.T) *PostgresStore {
	t.Helper()
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
	testMigrate(ctx, t, store)
	return store
}

func TestRunOnceRecordsAndSkips(t *testing.T) {
	store := ledgerConnect(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const label = "_avtest ledger sentinel run-and-skip"
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	})

	runs := 0
	var errs []error
	runOnce(ctx, store, logger, &errs, label, func(e *[]error) { runs++ })
	if runs != 1 || len(errs) != 0 {
		t.Fatalf("first runOnce: runs=%d errs=%v (want 1 run, no errors)", runs, errs)
	}
	var version string
	if err := store.Pool().QueryRow(ctx,
		`SELECT tool_version FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label).Scan(&version); err != nil {
		t.Fatalf("ledger row missing after successful step: %v", err)
	}
	if version != ToolVersion {
		t.Errorf("ledger tool_version = %q, want %q (the version that completed the step)", version, ToolVersion)
	}

	// Second invocation: the ledger hit must SKIP the step entirely.
	runOnce(ctx, store, logger, &errs, label, func(e *[]error) { runs++ })
	if runs != 1 {
		t.Errorf("recorded step ran again (runs=%d) — the ledger skip is the entire point of F13", runs)
	}
	if len(errs) != 0 {
		t.Errorf("skip produced errors: %v", errs)
	}
}

func TestRunOnceFailureDoesNotRecord(t *testing.T) {
	store := ledgerConnect(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const label = "_avtest ledger sentinel failure-no-record"
	_, _ = store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label)
	})

	runs := 0
	var errs []error
	runOnce(ctx, store, logger, &errs, label, func(e *[]error) {
		runs++
		*e = append(*e, os.ErrInvalid)
	})
	if runs != 1 {
		t.Fatalf("failing step ran %d times, want 1", runs)
	}
	if len(errs) != 1 {
		t.Fatalf("the step's error must fold into the shared collector (fail-closed); got %v", errs)
	}
	var count int
	if err := store.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("a FAILED step was recorded as complete — it would never retry; the fail-closed contract is broken")
	}

	// The retry (next migrate) succeeds and records.
	errs = nil
	runOnce(ctx, store, logger, &errs, label, func(e *[]error) { runs++ })
	if runs != 2 || len(errs) != 0 {
		t.Fatalf("retry after failure: runs=%d errs=%v (want 2 runs, no errors)", runs, errs)
	}
	if err := store.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("successful retry must record the label (count=%d)", count)
	}
}

// TestMigrateRecordsLedgeredSteps proves the wiring end-to-end: after
// a full migrate has run against this database (testMigrate above, or
// any earlier package's migrate), the ledger holds the expensive
// walkers' labels — the next version-bump migrate skips them.
func TestMigrateRecordsLedgeredSteps(t *testing.T) {
	store := ledgerConnect(t)
	ctx := context.Background()
	for _, label := range []string{
		"cleanup garbage timestamps from prior versions",
		"v0.27.104 backfill pull_requests.meta_head_id/meta_base_id",
		"v0.27.15 msg_ref bridge repairs (dedup + data_source + inline-comment backfills)",
	} {
		var count int
		if err := store.Pool().QueryRow(ctx,
			`SELECT COUNT(*) FROM aveloxis_ops.migration_ledger WHERE step_label = $1`, label).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Errorf("ledger missing %q after a completed migrate (count=%d) — the runOnce wiring for this step is broken or the step failed", label, count)
		}
	}
}
