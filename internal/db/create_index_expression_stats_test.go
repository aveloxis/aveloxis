// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
)

// TestExecCreateIndexConcurrentlyAnalyzesAnExpressionIndexWithoutStatistics
// (0.29.142, the snapshot evaluation): PostgreSQL gathers statistics on an
// index EXPRESSION only when the table is analyzed, never when the index is
// built, and autovacuum analyzes a table only after about a tenth of its rows
// change. Without them the planner guesses 0.5% of the table per lookup, so
// aveloxis-cve's lower(email) joins kept a full scan of a kate-shaped
// contributors table (4.45M rows: 7.1 s) with the 0.29.141 indexes in place,
// and used them (0.5 s) one ANALYZE later. The build helper owns the rule:
// a valid expression index with no statistics gets its table analyzed —
// decided on the missing statistics, not on "just built", so a host that
// built the index without them (0.29.141, or an ANALYZE that failed) gets
// them at the next migrate.
func TestExecCreateIndexConcurrentlyAnalyzesAnExpressionIndexWithoutStatistics(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	const scratch = "_avexpr_scratch"
	drop := func() { _, _ = store.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+scratch+` CASCADE`) }
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		`CREATE SCHEMA ` + scratch,
		// Autovacuum off: 2,000 rows pass its analyze threshold, and an
		// autoanalyze between a build and a probe would make the test pass
		// vacuously (or, below, fail).
		`CREATE TABLE ` + scratch + `.t (id int, email text) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ` + scratch + `.t SELECT g, 'User' || g || '@Example.org' FROM generate_series(1, 2000) g`,
	} {
		if _, err := store.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	hasStats := func(index string) bool {
		t.Helper()
		var ok bool
		if err := store.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_stats WHERE schemaname = $1 AND tablename = $2)`, scratch, index).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}

	// Built by the helper.
	var errs []error
	execCreateIndexConcurrently(ctx, store, quiet, &errs, scratch, "t_email_lower",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS t_email_lower ON `+scratch+`.t (lower(email))`)
	if len(errs) != 0 {
		t.Fatalf("build: %v", errs)
	}
	if !hasStats("t_email_lower") {
		t.Error("an expression index the helper built has no statistics; want its table analyzed after the build")
	}

	// Already there without statistics (a host that built it under
	// 0.29.141): the helper's CREATE … IF NOT EXISTS is a no-op, and the
	// missing statistics still decide.
	if _, err := store.pool.Exec(ctx, `CREATE INDEX t_id_text ON `+scratch+`.t ((id::text))`); err != nil {
		t.Fatal(err)
	}
	if hasStats("t_id_text") {
		t.Fatal("premise: a freshly built expression index already has statistics")
	}
	execCreateIndexConcurrently(ctx, store, quiet, &errs, scratch, "t_id_text",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS t_id_text ON `+scratch+`.t ((id::text))`)
	if len(errs) != 0 {
		t.Fatalf("second call: %v", errs)
	}
	if !hasStats("t_id_text") {
		t.Error("an existing expression index without statistics was left without them; want the table analyzed")
	}
}

// TestExpressionIndexTableWithoutStatistics pins the decision itself: only a
// valid index on an expression, with no statistics, names its table; a plain
// column index never does (its column's statistics are the table's own), nor
// an expression index once analyzed, nor a missing index (SR-5: no rows is
// "nothing to do", not an error).
func TestExpressionIndexTableWithoutStatistics(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	const scratch = "_avexpr_decide"
	drop := func() { _, _ = store.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+scratch+` CASCADE`) }
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		`CREATE SCHEMA ` + scratch,
		`CREATE TABLE ` + scratch + `."T x" (id int, email text) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ` + scratch + `."T x" SELECT g, 'a' || g FROM generate_series(1, 500) g`,
		`CREATE INDEX plain_idx ON ` + scratch + `."T x" (email)`,
		`CREATE INDEX expr_idx ON ` + scratch + `."T x" (lower(email))`,
	} {
		if _, err := store.pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	for _, c := range []struct {
		index, want string
	}{
		{"plain_idx", ""},
		{"expr_idx", scratch + `."T x"`},
		{"no_such_idx", ""},
	} {
		got, err := expressionIndexTableWithoutStatistics(ctx, store, scratch, c.index)
		if err != nil {
			t.Fatalf("%s: %v", c.index, err)
		}
		if got != c.want {
			t.Errorf("%s: got table %q, want %q", c.index, got, c.want)
		}
	}
	if _, err := store.pool.Exec(ctx, `ANALYZE `+scratch+`."T x"`); err != nil {
		t.Fatal(err)
	}
	if got, err := expressionIndexTableWithoutStatistics(ctx, store, scratch, "expr_idx"); err != nil || got != "" {
		t.Errorf("an analyzed expression index: got (%q, %v), want nothing to do", got, err)
	}
}
