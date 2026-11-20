// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// (0.29.157: the rotation is the first half of ReplaceRepoLibyear's one
// transaction; these tests rotate by replacing the inventory with nothing.)
//
// The libyear history table is created `LIKE repo_deps_libyear INCLUDING ALL`,
// which copies the columns the current table has on the day it is created and
// follows no later ALTER. The rotation used to copy with `INSERT ... SELECT *`,
// so a column added to the current table alone made every rotation fail, and a
// failed rotation aborts the libyear pass for that repository (v0.27.17). The
// vulnerability program (summary/70, 4.1.1) adds columns to this table, so the
// rotation names its columns and the migrate keeps the two tables' columns in
// step.

// libyearHistoryRows counts a repository's rows in the history table.
func libyearHistoryRows(t *testing.T, store *PostgresStore, ctx context.Context, repoID int64) int {
	t.Helper()
	var n int
	if err := store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`,
		repoID).Scan(&n); err != nil {
		t.Fatalf("count history rows: %v", err)
	}
	return n
}

// seedLibyearRotationRepo is seedRepoForDeps plus the history table's cleanup,
// which that helper does not do (it predates any test that rotates).
func seedLibyearRotationRepo(t *testing.T, store *PostgresStore, ctx context.Context, name string) int64 {
	t.Helper()
	repoID := seedRepoForDeps(t, store, ctx, "_avlyrot", name)
	clear := func() {
		cleanupExecRetry(context.Background(), store,
			`DELETE FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`, repoID)
	}
	clear()
	// Registered after seedRepoForDeps' cleanup, so it runs before the
	// repository row is deleted (cleanups run last-in, first-out).
	t.Cleanup(clear)
	return repoID
}

// TestLibyearRotationSurvivesAColumnTheHistoryTableLacks is the red test
// for the named column list: a column on the current table that the history
// table does not have must not stop the rotation.
func TestLibyearRotationSurvivesAColumnTheHistoryTableLacks(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "extra-column")

	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{&model.LibyearRow{
		Name: "left-pad", Requirement: "^1.3.0", Type: "runtime", PackageManager: "npm",
		CurrentVersion: "1.3.0", LatestVersion: "1.3.0", Libyear: 0,
		License: "WTFPL", Purl: "pkg:npm/left-pad@1.3.0",
	}}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("insert libyear row: %v", err)
	}

	// A later release adds a column to the current table only: exactly what
	// an ALTER in a migrate does, since the history table follows no ALTER.
	if _, err := store.pool.Exec(ctx,
		`ALTER TABLE aveloxis_data.repo_deps_libyear ADD COLUMN IF NOT EXISTS zz_rotation_probe TEXT DEFAULT ''`); err != nil {
		t.Fatalf("add probe column: %v", err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(context.Background(), store,
			`ALTER TABLE aveloxis_data.repo_deps_libyear DROP COLUMN IF EXISTS zz_rotation_probe`)
	})

	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("rotation must not depend on the two tables having the same columns: %v", err)
	}

	if got := libyearHistoryRows(t, store, ctx, repoID); got != 1 {
		t.Errorf("history rows after rotation = %d, want 1", got)
	}
	var current int
	if err := store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`, repoID).Scan(&current); err != nil {
		t.Fatal(err)
	}
	if current != 0 {
		t.Errorf("current rows after rotation = %d, want 0", current)
	}

	// Every column the rotation names arrives with its value.
	var name, license, purl, manager string
	if err := store.pool.QueryRow(ctx, `
		SELECT name, license, purl, package_manager
		FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`, repoID).
		Scan(&name, &license, &purl, &manager); err != nil {
		t.Fatalf("read rotated row: %v", err)
	}
	if name != "left-pad" || license != "WTFPL" || purl != "pkg:npm/left-pad@1.3.0" || manager != "npm" {
		t.Errorf("rotated row = (%q, %q, %q, %q), want the inserted values",
			name, license, purl, manager)
	}
}

// TestLibyearRotationKeepsTheRowsKey pins what the rotation does today
// and must keep doing: the history row carries the current row's surrogate key
// (the history table copies the primary key, so a second rotation of a row
// with the same key would fail, which is why re-inserted rows take new keys).
func TestLibyearRotationKeepsTheRowsKey(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID := seedLibyearRotationRepo(t, store, ctx, "keeps-key")

	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{&model.LibyearRow{
		Name: "requests", PackageManager: "pypi", CurrentVersion: "2.31.0",
		Purl: "pkg:pypi/requests@2.31.0", NoLibyear: true,
	}}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	var before int64
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_deps_libyear_id FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1`,
		repoID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	var after int64
	var libyearIsNull bool
	if err := store.pool.QueryRow(ctx, `
		SELECT repo_deps_libyear_id, libyear IS NULL
		FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`,
		repoID).Scan(&after, &libyearIsNull); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("history row key = %d, want the current row's key %d", after, before)
	}
	if !libyearIsNull {
		t.Error("an unknown libyear (NULL) must stay NULL in the history table")
	}
}

// TestMigrateGivesTheLibyearHistoryTableEveryRotatedColumn is the old-shape
// test: `license` and `purl` were added to the current table by an ALTER and
// nothing ever altered the history table, so a deployment whose history table
// was created before them lacks both. The migrate adds every column the
// rotation names, and the rotation then works on that database.
func TestMigrateGivesTheLibyearHistoryTableEveryRotatedColumn(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repoID := seedLibyearRotationRepo(t, store, ctx, "old-shape")

	// Make this database's history table the old shape.
	if _, err := store.pool.Exec(ctx, `
		ALTER TABLE aveloxis_data.repo_deps_libyear_history
			DROP COLUMN IF EXISTS license, DROP COLUMN IF EXISTS purl`); err != nil {
		t.Fatalf("make the history table old-shape: %v", err)
	}
	// Whatever the test does, the table ends with every column again.
	t.Cleanup(func() {
		var errs []error
		ensureLibyearHistoryColumns(context.Background(), store, logger, &errs)
	})

	var errs []error
	ensureLibyearHistoryColumns(ctx, store, logger, &errs)
	if len(errs) > 0 {
		t.Fatalf("ensureLibyearHistoryColumns: %v", errs)
	}

	for _, col := range libyearRotatedColumns {
		var present bool
		if err := store.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'aveloxis_data'
				  AND table_name = 'repo_deps_libyear_history'
				  AND column_name = $1)`, col).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if !present {
			t.Errorf("history table lacks rotated column %q after the migrate step", col)
		}
	}

	if err := store.ReplaceRepoLibyear(ctx, repoID, []*model.LibyearRow{&model.LibyearRow{
		Name: "serde", PackageManager: "cargo", CurrentVersion: "1.0.200",
		License: "MIT OR Apache-2.0", Purl: "pkg:cargo/serde@1.0.200",
	}}, nil, model.InventoryCoverage{}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoLibyear(ctx, repoID, nil, nil, model.InventoryCoverage{}); err != nil {
		t.Fatalf("rotate on a migrated old-shape database: %v", err)
	}
	var license, purl string
	if err := store.pool.QueryRow(ctx, `
		SELECT license, purl FROM aveloxis_data.repo_deps_libyear_history WHERE repo_id = $1`,
		repoID).Scan(&license, &purl); err != nil {
		t.Fatal(err)
	}
	if license != "MIT OR Apache-2.0" || purl != "pkg:cargo/serde@1.0.200" {
		t.Errorf("rotated (license, purl) = (%q, %q), want the inserted values", license, purl)
	}
}

// TestLibyearRotatedColumnsAreTheCurrentTablesColumns holds the column list
// and the schema together: a column added to repo_deps_libyear without being
// added to the list would be silently left out of the history from then on.
func TestLibyearRotatedColumnsAreTheCurrentTablesColumns(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()

	rows, err := store.pool.Query(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'aveloxis_data' AND table_name = 'repo_deps_libyear'
		  AND column_name <> 'zz_rotation_probe'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	inTable := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		inTable[c] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	inList := map[string]bool{}
	for _, c := range libyearRotatedColumns {
		if inList[c] {
			t.Errorf("libyearRotatedColumns names %q twice", c)
		}
		inList[c] = true
		if !inTable[c] {
			t.Errorf("libyearRotatedColumns names %q, which repo_deps_libyear does not have", c)
		}
	}
	for c := range inTable {
		if !inList[c] {
			t.Errorf("repo_deps_libyear has column %q that the rotation does not copy: add it to libyearRotatedColumns and to ensureLibyearHistoryColumns", c)
		}
	}
}
