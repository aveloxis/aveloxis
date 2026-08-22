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
)

// v0.25.1 — hotfix for a schema-design bug from v0.24.0 that v0.25.0
// exposed as a tight dispatcher-loop ERROR every ~30s.
//
// The history tables were declared with
//
//	CREATE TABLE ... (LIKE parent INCLUDING ALL);
//
// `INCLUDING ALL` copies the parent's UNIQUE constraints. So both
// history tables ended up with:
//   - repo_distribution_history: UNIQUE (repo_id, ecosystem, package_name, source)
//   - repo_distribution_manifest_history: UNIQUE (repo_id, manifest_path)
//
// But a history table is supposed to hold MANY snapshots over time
// per logical key (v0.24.0 changelog: "let analysts observe over
// time when a package first appeared / disappeared"). The inherited
// UNIQUE prevents that. First rotation works (history is empty);
// every subsequent rotation hits 23505 on the same logical key
// that's already in history.
//
// In v0.24.0 the bug was rare (cadence = 180 days). In v0.25.0
// distribution_scan_complete=FALSE makes partial-scan repos
// immediately re-eligible, so MarkDistributionComplete tries to
// rotate on every dispatcher tick (~30s) and fails the same way
// forever — burning API budget and spamming the log.
//
// Fix shape (v0.25.1): keep `LIKE ... INCLUDING ALL` (so the PK on
// distribution_id / manifest_id and the BIGSERIAL behavior + NOT
// NULLs + comments survive), then explicitly DROP the natural-key
// UNIQUE constraints by their auto-generated names. The constraint
// names are the postgres default `<table>_<col1>_..._key` form —
// confirmed in the live ERROR log line:
//
//	violates unique constraint "repo_distribution_manifest_history_repo_id_manifest_path_key"

func TestSchemaDropsHistoryUniqueConstraints(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	src := string(data)

	// History tables must still be declared with INCLUDING ALL so the
	// PRIMARY KEY (distribution_id / manifest_id) and BIGSERIAL
	// default-from-sequence behavior survive into the history tables.
	// Dropping INCLUDING ALL entirely would remove the PK, which is
	// the wrong fix.
	for _, needle := range []string{
		"LIKE aveloxis_data.repo_distribution INCLUDING ALL",
		"LIKE aveloxis_data.repo_distribution_manifest INCLUDING ALL",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must keep %q so the PK on distribution_id / manifest_id survives — v0.25.1's fix is to selectively drop the natural-key UNIQUEs, NOT to drop the entire INCLUDING ALL", needle)
		}
	}

	// The natural-key UNIQUEs must be dropped on the history tables
	// because history holds many snapshots over time per logical key.
	// Constraint names are postgres's auto-generated `<table>_<cols>_key`,
	// with NAMEDATALEN-1 = 63 char truncation. The full intended name
	// for the distribution history constraint would be
	// `repo_distribution_history_repo_id_ecosystem_package_name_source_key`
	// (67 chars); postgres truncates the trailing chars before `_key`
	// to fit, producing `..._so_key`. Verified against the live DB
	// (PGAPPNAME=psql aveloxis_cascade_test → pg_constraint.conname).
	// The manifest history constraint (60 chars) is under the limit.
	for _, needle := range []string{
		"ALTER TABLE aveloxis_data.repo_distribution_history",
		"DROP CONSTRAINT IF EXISTS repo_distribution_history_repo_id_ecosystem_package_name_so_key",
		"ALTER TABLE aveloxis_data.repo_distribution_manifest_history",
		"DROP CONSTRAINT IF EXISTS repo_distribution_manifest_history_repo_id_manifest_path_key",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("schema.sql must drop the inherited natural-key UNIQUE constraint on history tables. Missing needle: %q. Without this, the second rotation of any repo trips 23505 and MarkDistributionComplete rolls back forever.", needle)
		}
	}
}

func TestMigrationDropsHistoryUniqueConstraintsOnExistingFleets(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	src := string(data)

	// Operators on v0.24.0–v0.25.0 created their history tables with
	// the inherited UNIQUEs. Schema declarations on subsequent runs
	// won't re-process those (CREATE TABLE IF NOT EXISTS short-circuits),
	// so the migration must ALTER the existing tables explicitly.
	const label = `"v0.25.1 drop inherited natural-key UNIQUEs on distribution history tables"`
	if !strings.Contains(src, label) {
		t.Errorf("migrate.go must register a migration step labelled %s — the v0.25.1 fix MUST run on existing v0.24.x/v0.25.0 fleets, not just on fresh installs, otherwise the dispatcher loop persists", label)
	}

	for _, needle := range []string{
		// Must use execMigrationStep (fail-closed contract).
		"execMigrationStep(ctx, pg, logger, errs",
		"ALTER TABLE aveloxis_data.repo_distribution_history",
		"DROP CONSTRAINT IF EXISTS repo_distribution_history_repo_id_ecosystem_package_name_so_key",
		"ALTER TABLE aveloxis_data.repo_distribution_manifest_history",
		"DROP CONSTRAINT IF EXISTS repo_distribution_manifest_history_repo_id_manifest_path_key",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go missing v0.25.1 ALTER TABLE needle: %q", needle)
		}
	}
}

func TestV0251MigrationIsIdempotent(t *testing.T) {
	// The IF EXISTS clauses are the self-disabling guard: once the
	// constraints are dropped, subsequent runs are no-ops. A future
	// refactor that "tidies up" by removing IF EXISTS would fail
	// every migrate run on already-fixed DBs. Pin the guard.
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("read migrate.go: %v", err)
	}
	body := string(data)

	const label = "v0.25.1 drop inherited natural-key UNIQUEs on distribution history tables"
	idx := strings.Index(body, label)
	if idx < 0 {
		t.Fatal("cannot find v0.25.1 migration label in migrate.go")
	}
	end := idx + 800
	if end > len(body) {
		end = len(body)
	}
	region := body[idx:end]

	// Both DROP CONSTRAINT statements must carry IF EXISTS within the
	// v0.25.1 step's SQL region.
	count := strings.Count(region, "DROP CONSTRAINT IF EXISTS")
	if count < 2 {
		t.Errorf("v0.25.1 migration block must contain BOTH DROP CONSTRAINT IF EXISTS clauses (one per history table); found %d. Without IF EXISTS the migration fails on second run after the constraints are gone.", count)
	}
}

// TestV0251HistoryRotationAllowsRepeatedKeys is the integration test
// gated on AVELOXIS_TEST_DB. It runs the v0.25.1-corrected schema
// against a live Postgres and asserts that the same (repo_id,
// manifest_path) row can be rotated to history twice without the
// 23505 we saw in the production log.
//
// Without the v0.25.1 fix this test fails with:
//
//	duplicate key value violates unique constraint
//	"repo_distribution_manifest_history_repo_id_manifest_path_key"
//
// on the second INSERT. With the fix, both INSERTs succeed.
func TestV0251HistoryRotationAllowsRepeatedKeys(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	pool := store.pool

	// Insert two history rows with the SAME (repo_id, manifest_path).
	// Pre-v0.25.1 schema fails on the second INSERT.
	const repoID = 999999
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos (repo_id, repo_group_id, platform_id, repo_git, repo_owner, repo_name)
		VALUES ($1, 1, 1, 'https://example.com/x/y-v0251-test', 'x', 'y-v0251-test')
		ON CONFLICT (repo_id) DO NOTHING`, repoID); err != nil {
		t.Fatalf("seed repo: %v", err)
	}
	// Clean up any leftover state from a prior failed run.
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest_history WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("cleanup history: %v", err)
	}

	for i := range 2 {
		_, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution_manifest_history
			    (repo_id, manifest_path, manifest_type, package_name_declared, tool_version)
			VALUES ($1, 'setup.py', 'pypi', 'foo', 'v0.25.1-test')`, repoID)
		if err != nil {
			t.Fatalf("rotate %d failed: %v — v0.25.1 must drop the inherited UNIQUE so history can accumulate multiple snapshots per (repo_id, manifest_path)", i+1, err)
		}
	}

	// Same shape on the other history table.
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_history WHERE repo_id = $1`, repoID); err != nil {
		t.Fatalf("cleanup repo_distribution_history: %v", err)
	}
	for i := range 2 {
		_, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_distribution_history
			    (repo_id, ecosystem, package_name, version_count, source, tool_version)
			VALUES ($1, 'pypi', 'foo', 0, 'deps.dev', 'v0.25.1-test')`, repoID)
		if err != nil {
			t.Fatalf("repo_distribution_history rotate %d failed: %v", i+1, err)
		}
	}

	// Final cleanup.
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_manifest_history WHERE repo_id = $1`, repoID); err != nil {
		t.Logf("cleanup history: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_distribution_history WHERE repo_id = $1`, repoID); err != nil {
		t.Logf("cleanup repo_distribution_history: %v", err)
	}
}

// v0251Connect is the shared integration-test gate. Mirrors the
// realignConnect pattern from queue_realign_integration_test.go so
// we go through NewPostgresStore (which wires up the slog logger
// migrate.go expects) instead of building a bare PostgresStore that
// nil-panics inside RunMigrations.
func v0251Connect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	testMigrate(ctx, t, store)
	// Consumers of this helper seed repos with a hardcoded
	// repo_group_id = 1. On a POPULATED scratch DB group 1 has always
	// existed; on a truly FRESH database (the CI / fresh-DB-gate tier,
	// v0.27.9 lesson) it only exists if some earlier test happened to
	// trigger UpsertRepo's lazy default-group creation — a cross-test
	// (and cross-PACKAGE, under go test's parallel package execution)
	// ordering race observed 2026-07-15. Seed it deterministically,
	// and advance the sequence so a later lazy INSERT (which takes
	// nextval) can't collide with the explicit id.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_groups (repo_group_id, rg_name)
		VALUES (1, 'fresh-db-test-default')
		ON CONFLICT (repo_group_id) DO NOTHING`); err != nil {
		t.Fatalf("ensure test repo group: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		SELECT setval(pg_get_serial_sequence('aveloxis_data.repo_groups','repo_group_id'),
		              GREATEST((SELECT MAX(repo_group_id) FROM aveloxis_data.repo_groups), 1))`); err != nil {
		t.Fatalf("advance repo_groups sequence: %v", err)
	}
	return store, ctx
}

// TestV0251HistoryTablesStillHavePrimaryKey is the regression guard
// against my own fix. The original (rejected) shape was to drop
// `INCLUDING INDEXES` entirely, which would have also lost the PK
// on distribution_id / manifest_id. Pin that the PKs survive.
func TestV0251HistoryTablesStillHavePrimaryKey(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)
	pool := store.pool

	for _, tbl := range []string{
		"repo_distribution_history",
		"repo_distribution_manifest_history",
	} {
		var hasPK bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			    SELECT 1
			    FROM information_schema.table_constraints
			    WHERE table_schema = 'aveloxis_data'
			      AND table_name = $1
			      AND constraint_type = 'PRIMARY KEY'
			)`, tbl).Scan(&hasPK)
		if err != nil {
			t.Fatalf("introspect %s: %v", tbl, err)
		}
		if !hasPK {
			t.Errorf("aveloxis_data.%s has no PRIMARY KEY — the v0.25.1 fix must keep INCLUDING ALL and selectively drop only the natural-key UNIQUEs. Dropping INCLUDING ALL or INCLUDING INDEXES would lose the PK.", tbl)
		}
	}

	// Inverse check: the natural-key UNIQUEs must be GONE.
	for _, c := range []struct{ tbl, constraint string }{
		{"repo_distribution_history", "repo_distribution_history_repo_id_ecosystem_package_name_so_key"},
		{"repo_distribution_manifest_history", "repo_distribution_manifest_history_repo_id_manifest_path_key"},
	} {
		var exists bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			    SELECT 1
			    FROM information_schema.table_constraints
			    WHERE table_schema = 'aveloxis_data'
			      AND table_name = $1
			      AND constraint_name = $2
			)`, c.tbl, c.constraint).Scan(&exists)
		if err != nil {
			t.Fatalf("introspect %s/%s: %v", c.tbl, c.constraint, err)
		}
		if exists {
			t.Errorf("aveloxis_data.%s still has UNIQUE constraint %q — v0.25.1's whole point is to drop it so history tables can accumulate multiple snapshots per logical key", c.tbl, c.constraint)
		}
	}
}
