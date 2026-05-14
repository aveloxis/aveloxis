// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration tests for RunMigrations against a live Postgres.
//
// # Why this exists (v0.21.1)
//
// v0.21.0 shipped a migration step whose SQL referenced
// aveloxis_scan.scancode_scans.created_at — but that table uses the
// aveloxis-wide convention data_collection_date. The source-contract
// test for that migration pinned MAX(created_at) as a needle in
// migrate.go, which silently passed because both sides of the
// contract agreed on the wrong answer. Production migrate failed
// with SQLSTATE 42703 (undefined_column).
//
// The failure mode is general: any source-contract test that
// references a column or table NOT also pinned in schema.sql is
// vulnerable to the same shape of bug — typo, wrong schema prefix,
// stale column rename, etc. The only sufficient safety net is
// actually running the migration against a Postgres that knows the
// real schema and observing whether each statement parses + executes
// cleanly.
//
// This test does exactly that. It runs once per CI invocation
// against the Postgres service container provisioned by GitHub
// Actions (or any env-pointed Postgres for local dev). Every
// addColumnIfMissing call, every execMigrationStep, every
// execCreateIndexConcurrently is exercised end-to-end. A wrong
// column name surfaces as a test failure with the exact SQLSTATE
// and offending statement in the error message.
//
// # Local dev
//
// To run locally:
//
//   docker run --rm -d --name aveloxis-test-pg -p 5433:5432 \
//     -e POSTGRES_PASSWORD=test -e POSTGRES_DB=aveloxis_test postgres:16
//   AVELOXIS_TEST_DB="postgres://postgres:test@localhost:5433/aveloxis_test?sslmode=disable" \
//     go test ./internal/db/ -run TestRunMigrationsOnFreshDB -v
//
// The Docker container is empty Postgres; the test owns the entire
// DB and is destructive (it CREATEs the aveloxis_* schemas and all
// tables). Don't point AVELOXIS_TEST_DB at a DB you care about.
//
// # CI
//
// .github/workflows/integration.yml provisions a postgres:16
// service container and sets AVELOXIS_TEST_DB to point at it. The
// integration job runs in parallel with the existing unit job.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestRunMigrationsOnFreshDB is the v0.21.1 safety net. It runs the
// schema + RunMigrations against an empty Postgres and asserts no
// errors. Any column-name typo / wrong-schema-prefix / SQL syntax
// error in any migration step fails here.
//
// Pre-test state: DB must exist and be reachable, but the
// aveloxis_data / aveloxis_ops / aveloxis_scan / aveloxis_augur_data
// schemas can be either absent (fresh DB) or pre-migrated (rerun on
// the same DB). Both states are handled — schema.sql uses
// CREATE SCHEMA IF NOT EXISTS / CREATE TABLE IF NOT EXISTS
// throughout, and every migration step is idempotent.
//
// Post-test state: the DB has the full aveloxis schema applied.
// This is intentionally NOT cleaned up because (a) the CI container
// is ephemeral and gets discarded after the job, and (b) on local
// dev, leaving the schema in place lets a subsequent unit-tier
// integration test (e.g. queue_realign_integration_test.go) reuse
// the same DB without re-migrating.
func TestRunMigrationsOnFreshDB(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test. See test docstring for setup.")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	defer store.Close()

	// Run migrations. Any non-nil return means at least one
	// schema-changing step failed. The error message includes
	// errors.Join of every collected failure, so the test failure
	// surfaces the full list — operators don't have to fix
	// failures one at a time.
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations on fresh DB failed — this is exactly the v0.21.0 bug shape (wrong column / table / schema name) that source-contract tests can't catch. Error:\n%v", err)
	}

	// Sanity-check: a representative sample of the new v0.21.0
	// columns exists. This isn't strictly necessary (a successful
	// RunMigrations call already proves the column-adds ran without
	// SQLSTATE error) but it's a fast confirmation that the
	// integration test is actually exercising the schema and not
	// silently no-op-ing.
	var exists bool
	err = store.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'aveloxis_data'
			  AND table_name = 'repos'
			  AND column_name = 'scancode_last_run'
		)`).Scan(&exists)
	if err != nil {
		t.Fatalf("information_schema query: %v", err)
	}
	if !exists {
		t.Error("aveloxis_data.repos.scancode_last_run does not exist after RunMigrations — the v0.21.0 column adds either silently no-op'd or the migration ran against a different DB than the test verifies.")
	}
}

// TestRunMigrationsIsIdempotent runs RunMigrations twice on the
// same DB and asserts both runs succeed. Migrations are documented
// as idempotent (every column add uses IF NOT EXISTS, every
// execMigrationStep filters on the not-yet-applied state, etc.).
// A regression that breaks idempotency would manifest as the second
// run returning an error. Specific bugs this catches:
//   - addColumnIfMissing accidentally dropping the IF NOT EXISTS.
//   - A backfill UPDATE missing its "WHERE state = unbackfilled"
//     filter and re-touching already-backfilled rows (with side
//     effects beyond the no-op contract).
//   - A CREATE INDEX CONCURRENTLY missing its IF NOT EXISTS and
//     failing the second run with "relation already exists."
func TestRunMigrationsIsIdempotent(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	defer store.Close()

	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("second RunMigrations (idempotency check): %v", err)
	}
}

// TestScancodeBackfillReferencesCorrectColumn is the regression
// guard SPECIFICALLY for the v0.21.0 created_at/data_collection_date
// typo. The pin is in addition to the broader
// TestRunMigrationsOnFreshDB safety net so a future refactor that
// breaks the column reference fails this test with a precise
// message naming the affected column.
//
// This is a hybrid: source-contract pin AGAINST the existence of
// the wrong column in scancode_scans. Reads both migrate.go and
// schema.sql, asserts that any column referenced by the backfill
// against scancode_scans is one that actually exists in the
// scancode_scans table definition.
func TestScancodeBackfillReferencesCorrectColumn(t *testing.T) {
	migrateData, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	schemaData, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}

	// Locate the scancode backfill SQL inside migrate.go. It's
	// inside the execMigrationStep call with the canonical label.
	migrateSrc := string(migrateData)
	labelIdx := strings.Index(migrateSrc, "v0.21.0 backfill scancode_last_run from aveloxis_scan.scancode_scans")
	if labelIdx < 0 {
		t.Fatal("cannot locate v0.21.0 scancode backfill step")
	}
	// Grab a generous slice around the label — the SQL body
	// follows on subsequent lines.
	tail := migrateSrc[labelIdx:]
	endRel := strings.Index(tail, "\n\t// ")
	if endRel < 0 {
		endRel = len(tail)
	}
	backfillBlock := tail[:endRel]

	// Find which timestamp column the backfill SQL is using.
	// Pre-fix this was created_at (wrong). Post-fix this is
	// data_collection_date (correct, matches the table definition).
	if strings.Contains(backfillBlock, "created_at") {
		t.Error("v0.21.0 scancode backfill SQL references created_at — but aveloxis_scan.scancode_scans uses data_collection_date. This is the exact bug shape that shipped in v0.21.0 and failed migrate on 2026-05-14 with SQLSTATE 42703.")
	}

	// Now verify the column the backfill DOES use actually exists
	// in the scancode_scans table definition.
	schemaSrc := string(schemaData)
	tableIdx := strings.Index(schemaSrc, "CREATE TABLE IF NOT EXISTS aveloxis_scan.scancode_scans")
	if tableIdx < 0 {
		t.Fatal("cannot locate aveloxis_scan.scancode_scans in schema.sql")
	}
	tableEnd := strings.Index(schemaSrc[tableIdx:], ");")
	if tableEnd < 0 {
		t.Fatal("cannot find end of scancode_scans CREATE TABLE")
	}
	tableBlock := schemaSrc[tableIdx : tableIdx+tableEnd]

	if strings.Contains(backfillBlock, "data_collection_date") &&
		!strings.Contains(tableBlock, "data_collection_date") {
		t.Error("backfill SQL uses data_collection_date, but the scancode_scans table definition no longer declares that column. Schema and backfill have drifted.")
	}
}
