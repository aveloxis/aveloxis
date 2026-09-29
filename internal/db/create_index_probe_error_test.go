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

// TestExecCreateIndexConcurrentlyReturnsProbeError (PR #218 review B8,
// SR-5/SR-16): the invalid-index probe's failure was read as "not invalid"
// (`err == nil && isInvalid`), so a failed pg_index read went straight on to
// CREATE INDEX — over an INVALID leftover, that is "relation already exists"
// forever, blamed on the wrong step. Only "no such index" (no rows) means
// there is nothing to drop; any other probe error is the step's error.
//
// The probe is made to fail on its own: the store under test puts a schema
// ahead of pg_catalog on its search_path, holding a pg_index view whose
// indisvalid is text, so the probe's NOT fails to parse while the CREATE
// INDEX (schema-qualified) would succeed. The step must record an error and
// must not have built the index.
func TestExecCreateIndexConcurrentlyReturnsProbeError(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	admin, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)

	const shadow, scratch = "_avb8_shadow", "_avb8_scratch"
	drop := func() {
		_, _ = admin.pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+shadow+` CASCADE`)
		_, _ = admin.pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+scratch+` CASCADE`)
	}
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		`CREATE SCHEMA ` + shadow,
		`CREATE VIEW ` + shadow + `.pg_index AS SELECT indexrelid, indisvalid::text AS indisvalid FROM pg_catalog.pg_index`,
		`CREATE SCHEMA ` + scratch,
		`CREATE TABLE ` + scratch + `.t (v int)`,
	} {
		if _, err := admin.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}

	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	shadowed, err := NewPostgresStore(ctx, dsn+sep+"search_path="+shadow+",pg_catalog", quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadowed.Close)

	var errs []error
	execCreateIndexConcurrently(ctx, shadowed, quiet, &errs, scratch, "b8_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS b8_idx ON `+scratch+`.t (v)`)
	if len(errs) == 0 {
		t.Error("a failed invalid-index probe recorded no error; want the probe's failure as the step's error")
	}
	var built bool
	if err := admin.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, scratch+".b8_idx").Scan(&built); err != nil {
		t.Fatal(err)
	}
	if built {
		t.Error("CREATE INDEX ran after the invalid-index probe failed; want the step to stop at the probe")
	}

	// The probe's real "no" is unchanged: with the catalog readable, a
	// missing index is built.
	errs = nil
	execCreateIndexConcurrently(ctx, admin, quiet, &errs, scratch, "b8_idx",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS b8_idx ON `+scratch+`.t (v)`)
	if len(errs) != 0 {
		t.Fatalf("a missing index (probe: no rows) was not built: %v", errs)
	}
	if err := admin.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, scratch+".b8_idx").Scan(&built); err != nil {
		t.Fatal(err)
	}
	if !built {
		t.Error("the index was not built after a no-rows probe")
	}
}
