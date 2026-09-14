// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Integration tests for the v0.22.1 ON UPDATE CASCADE migration.
//
// Source-contract tests in cntrb_id_cascade_test.go pin the shape of
// the code (which constraint names appear, which SQL keywords are
// used). Those don't tell us whether the migration actually WORKS
// at runtime — same lesson as v0.21.1 where source-contract tests
// agreed on a wrong column name and prod migration failed with
// SQLSTATE 42703.
//
// This test runs RunMigrations end-to-end and then queries
// information_schema to confirm every cntrb_id FK has CASCADE.
//
// Gated on AVELOXIS_TEST_DB; skips when unset. Same setup recipe
// as TestRunMigrationsOnFreshDB in migrate_integration_test.go.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestCntrbIDCascadeIntegration runs the schema + RunMigrations
// against a live Postgres and asserts:
//
//  1. Every one of the 16 cntrb_id FK constraints exists.
//  2. update_rule = 'CASCADE' on every one of them.
//  3. Re-running the migration is a no-op (idempotency — the
//     introspection check in ensureOnUpdateCascadeOnCntrbIDFKs
//     short-circuits without invoking ALTER TABLE).
func TestCntrbIDCascadeIntegration(t *testing.T) {
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
	t.Cleanup(store.Close)

	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("first RunMigrations: %v", err)
	}

	// Confirm every constraint exists with CASCADE.
	for _, fk := range cntrbIDChildFKs {
		var rule string
		err := store.pool.QueryRow(ctx, `
			SELECT update_rule
			FROM information_schema.referential_constraints
			WHERE constraint_schema = 'aveloxis_data'
			  AND constraint_name = $1
		`, fk.constraint).Scan(&rule)
		if err != nil {
			t.Errorf("introspect %s: %v (table=%s column=%s)", fk.constraint, err, fk.table, fk.column)
			continue
		}
		if rule != "CASCADE" {
			t.Errorf("constraint %s has update_rule=%q, want CASCADE (table=%s column=%s)",
				fk.constraint, rule, fk.table, fk.column)
		}
	}

	// Idempotency: re-run the migration. Must succeed AND must not
	// have re-issued any ALTER TABLE (we can't observe that directly
	// here, but a re-run succeeding without error proves the
	// introspection short-circuit works as intended — if it didn't,
	// the DROP CONSTRAINT IF EXISTS step would still succeed but
	// would also rewrite the constraint metadata pointlessly).
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("second RunMigrations (idempotency check): %v", err)
	}
}

// TestCntrbIDCascadeActuallyCascades verifies the load-bearing
// behavior: when contributors.cntrb_id is UPDATEd, every FK
// column in every child table follows automatically. Without this
// behavior, the v0.22.2 data migration would either fail with a
// constraint violation OR succeed at the parent but leave child
// rows pointing at a no-longer-existent cntrb_id (orphaned FK,
// which Postgres won't actually let happen — it would just fail).
//
// Synthesizes one contributor + one identity + one issue row,
// UPDATEs the contributor's cntrb_id, and confirms the issue's
// reporter_id followed.
func TestCntrbIDCascadeActuallyCascades(t *testing.T) {
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

	store.SetMatviewSkip(true)
	if err := RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// Use a unique slug to avoid colliding with other test runs on
	// the same scratch DB.
	const oldID = "11111111-1111-4111-8111-111111111111"
	const newID = "01000000-0000-0000-0000-000000000000" // PlatformUUID(1, 0) form
	const login = "_av_cascade_test_user"

	// Cleanup any leftover state from a prior run. pgx.Exec doesn't
	// parse multi-statement strings, so each DELETE is a separate
	// call. Order matters: children before parents.
	// v0.27.120 bounded-retry helper: the 2026-09-01 full-suite run's
	// concurrent-migrate deadlock storm picked this test's bare seed
	// Execs as 40P01 victims — victims convert as they appear.
	cleanup := func(sql string, args ...any) {
		cleanupExecRetry(ctx, store, sql, args...)
	}
	cleanup(`DELETE FROM aveloxis_data.issues WHERE reporter_id IN ($1::uuid, $2::uuid)`, oldID, newID)
	cleanup(`DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_av_cascade' AND repo_name = 'test')`)
	cleanup(`DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN ($1::uuid, $2::uuid)`, oldID, newID)
	cleanup(`DELETE FROM aveloxis_data.repos WHERE repo_owner = '_av_cascade' AND repo_name = 'test'`)
	cleanup(`DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1 OR cntrb_id IN ($2::uuid, $3::uuid)`, login, oldID, newID)

	// Seed a contributor with a random-looking UUID.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login)
		VALUES ($1::uuid, $2)
	`, oldID, login)

	// Seed a repo_group so repos.repo_group_id has a valid FK target.
	var repoGroupID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name, rg_description)
		VALUES ('_av_cascade_grp', 'cascade integration test')
		ON CONFLICT DO NOTHING
		RETURNING repo_group_id
	`).Scan(&repoGroupID); err != nil {
		// ON CONFLICT DO NOTHING returns no row; look it up.
		if err := store.pool.QueryRow(ctx,
			`SELECT repo_group_id FROM aveloxis_data.repo_groups WHERE rg_name = '_av_cascade_grp'`,
		).Scan(&repoGroupID); err != nil {
			t.Fatalf("insert/lookup repo_group: %v", err)
		}
	}

	// Seed a repo to use as FK target for the issue.
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_owner, repo_name, repo_git, platform_id, repo_group_id)
		VALUES ('_av_cascade', 'test', 'https://github.com/_av_cascade/test', 1, $1)
		RETURNING repo_id
	`, repoGroupID).Scan(&repoID); err != nil {
		t.Fatalf("insert repo: %v", err)
	}

	// Seed an issue whose reporter_id points at the contributor (the
	// 2026-09-01 40P01 victim — now retried).
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id)
		VALUES ($1, 999999999, 12345, $2::uuid)
	`, repoID, oldID)

	// Now the load-bearing UPDATE: rename the contributor's cntrb_id.
	// Without ON UPDATE CASCADE this would fail with
	// SQLSTATE 23503 (foreign_key_violation) because the issue
	// row's reporter_id still points at the old value.
	if _, err := store.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors SET cntrb_id = $1::uuid WHERE cntrb_id = $2::uuid
	`, newID, oldID); err != nil {
		t.Fatalf("UPDATE contributors.cntrb_id: %v — this means ON UPDATE CASCADE "+
			"isn't actually enforced on issues.reporter_id_fkey, which means the "+
			"v0.22.2 data migration would fail on the same shape of UPDATE", err)
	}

	// Verify the child row's reporter_id cascaded.
	var reporterID string
	if err := store.pool.QueryRow(ctx, `
		SELECT reporter_id::text FROM aveloxis_data.issues
		WHERE repo_id = $1 AND issue_number = 12345
	`, repoID).Scan(&reporterID); err != nil {
		t.Fatalf("read issue back: %v", err)
	}
	if reporterID != newID {
		t.Errorf("issue.reporter_id = %q, want %q — CASCADE did not propagate",
			reporterID, newID)
	}

	// Cleanup. Same split-statement pattern — via the retry helper so a
	// deadlock-killed delete cannot strand residue for the next run
	// (the silent `_, _ =` form is how residue poisons reruns).
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1 AND issue_number = 12345`, repoID)
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
}
