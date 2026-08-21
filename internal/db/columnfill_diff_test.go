// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.26.1: column-fill diff for `aveloxis data-test`. Row counts can't
// see a column that the new binary stopped populating — the canonical
// example is issue_labels.platform_label_id, which is 0 on every row
// under the GraphQL path while row counts match exactly. The fill diff
// compares, per column, how many rows carry a MEANINGFUL value
// (type-aware: non-empty for text, non-zero for numerics, non-NULL
// otherwise) between the released and new databases.

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClassifyColumnFill(t *testing.T) {
	cases := []struct {
		name          string
		released, new int64
		want          string
	}{
		// The signal this diff exists for: a column populated under the
		// released binary that is COMPLETELY unpopulated under the new
		// one ("went dark") — a dropped mapping, a renamed JSON tag, a
		// field the new code path never fills.
		{"went dark", 500, 0, "FAIL"},
		{"single value lost entirely", 1, 0, "FAIL"},
		// Partial differences are FLAG, not FAIL: the two collections
		// run ~30 minutes apart against a live repo, so small drift
		// (an edited profile, a new comment) is expected. A hard FAIL
		// on any inequality would make the harness flaky; the report
		// shows both numbers for human review.
		{"partial drop", 500, 480, "FLAG"},
		{"new coverage", 480, 500, "FLAG"},
		{"equal", 500, 500, "PASS"},
		{"both empty", 0, 0, "PASS"},
		{"new-only column", 0, 300, "FLAG"},
	}
	for _, c := range cases {
		if got := classifyColumnFill(c.released, c.new); got != c.want {
			t.Errorf("%s: classifyColumnFill(%d, %d) = %q, want %q",
				c.name, c.released, c.new, got, c.want)
		}
	}
}

// The "populated" predicate must be type-aware: TEXT DEFAULT '' and
// INTEGER DEFAULT 0 are this schema's idioms for "not filled in", so a
// plain IS NOT NULL check would miss both the platform_label_id=0 case
// and every empty-string default. Booleans are exempt from the
// zero-check — false is a legitimate value.
func TestPopulatedPredicateByType(t *testing.T) {
	cases := []struct {
		dataType string
		contains string
		excludes string
	}{
		{"text", `<> ''`, "<> 0"},
		{"character varying", `<> ''`, "<> 0"},
		{"bigint", "<> 0", "''"},
		{"integer", "<> 0", "''"},
		{"numeric", "<> 0", "''"},
		{"boolean", "IS NOT NULL", "<>"},
		{"timestamp with time zone", "IS NOT NULL", "<>"},
		{"uuid", "IS NOT NULL", "<>"},
		{"jsonb", "IS NOT NULL", "<>"},
	}
	for _, c := range cases {
		got := populatedPredicate(`"col"`, c.dataType)
		if !strings.Contains(got, c.contains) {
			t.Errorf("populatedPredicate(col, %q) = %q — must contain %q",
				c.dataType, got, c.contains)
		}
		if c.excludes != "" && strings.Contains(got, c.excludes) {
			t.Errorf("populatedPredicate(col, %q) = %q — must NOT contain %q",
				c.dataType, got, c.excludes)
		}
	}
}

func TestColumnFillReportHasFailures(t *testing.T) {
	r := &ColumnFillDiffReport{Rows: []ColumnFillDiffRow{
		{Status: "PASS"}, {Status: "FLAG"},
	}}
	if r.HasFailures() {
		t.Error("PASS+FLAG must not count as failure")
	}
	r.Rows = append(r.Rows, ColumnFillDiffRow{Status: "FAIL"})
	if !r.HasFailures() {
		t.Error("a FAIL row must surface via HasFailures")
	}
}

// The harness must actually run the column diff and fold it into the
// exit code — a diff that exists but isn't wired detects nothing.
func TestDataTestRunsColumnFillDiff(t *testing.T) {
	src := readSourceFile(t, "../../cmd/aveloxis/data_test_cmd.go")
	for _, needle := range []string{"ColumnFillDiff(", "colReport.HasFailures()"} {
		if !strings.Contains(src, needle) {
			t.Errorf("data_test_cmd.go must contain %q — the column-fill diff has to "+
				"run after the row-count diff and participate in the exit code.", needle)
		}
	}
}

// TestColumnFillDiffHandlesShapeDrift — v0.27.129. The 2026-08-21
// main-vs-branch data-test crashed on the first deliberate table drop
// it met (contributors_old, removed in v0.27.115): enumeration was
// released-side-only and every released table was queried against the
// new side (SQLSTATE 42P01). Shape drift must be REPORTED, not fatal.
//
// Asymmetry is manufactured with a SECOND scratch database (committed
// shapes on both sides — a first draft used transactional DDL, whose
// ACCESS EXCLUSIVE lock deadlocked the other side's count query). The
// dedicated schema/database keep parallel tests from perturbing counts
// (the v0.26.1 self-comparison lesson).
func TestColumnFillDiffHandlesShapeDrift(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	const schema = "_avcolfill_s"
	const sideDB = "_avcolfill_sidedb"

	// Side A lives in a dedicated schema of the main scratch DB.
	mustExecRetry(ctx, t, store, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	mustExecRetry(ctx, t, store, `CREATE SCHEMA `+schema)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		cleanupExecRetry(cctx, store, `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
	})
	mustExecRetry(ctx, t, store, `CREATE TABLE `+schema+`._base (a TEXT, b TEXT)`)
	mustExecRetry(ctx, t, store, `INSERT INTO `+schema+`._base (a, b) VALUES ('x', 'y'), ('', NULL)`)
	mustExecRetry(ctx, t, store, `CREATE TABLE `+schema+`._probe (x TEXT)`)
	mustExecRetry(ctx, t, store, `INSERT INTO `+schema+`._probe (x) VALUES ('v')`)

	// Side B is a second scratch DATABASE carrying the narrower shape:
	// _base without column b, no _probe at all.
	if _, err := store.pool.Exec(ctx, `DROP DATABASE IF EXISTS `+sideDB+` WITH (FORCE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `CREATE DATABASE `+sideDB); err != nil {
		t.Skipf("cannot create the side database (role lacks CREATEDB?): %v", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		cleanupExecRetry(cctx, store, `DROP DATABASE IF EXISTS `+sideDB+` WITH (FORCE)`)
	})
	sideCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	sideCfg.ConnConfig.Database = sideDB
	sidePool, err := pgxpool.NewWithConfig(ctx, sideCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sidePool.Close)
	for _, sql := range []string{
		`CREATE SCHEMA ` + schema,
		`CREATE TABLE ` + schema + `._base (a TEXT)`,
		`INSERT INTO ` + schema + `._base (a) VALUES ('x'), ('')`,
	} {
		if _, err := sidePool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}

	// Direction 1: released = A (richer), new = B. The drop direction:
	// _probe is a released-only table; _base.b a removed column that
	// CARRIED DATA (populated=1) → REMOVED, a failure.
	rep, err := columnFillDiff(ctx, store.pool, sidePool, []string{schema})
	if err != nil {
		t.Fatalf("released-richer diff must not error (the 42P01 crash class): %v", err)
	}
	if len(rep.TablesOnlyInReleased) != 1 || rep.TablesOnlyInReleased[0] != schema+"._probe" {
		t.Errorf("want TablesOnlyInReleased [_probe], got %v", rep.TablesOnlyInReleased)
	}
	var removedRow *ColumnFillDiffRow
	for i := range rep.Rows {
		if rep.Rows[i].Column == "b" {
			removedRow = &rep.Rows[i]
		}
	}
	if removedRow == nil || removedRow.Status != "REMOVED" || removedRow.ReleasedPopulated != 1 {
		t.Errorf("released-only populated column must be REMOVED with its count, got %+v", removedRow)
	}
	if !rep.HasFailures() {
		t.Error("a removed column that carried data is a data-loss shape — HasFailures must be true")
	}

	// Direction 2: released = B, new = A. The add direction: _probe
	// only-in-new, _base.b an ADDED column with its new-side count. No
	// failures.
	rep2, err := columnFillDiff(ctx, sidePool, store.pool, []string{schema})
	if err != nil {
		t.Fatalf("new-richer diff must not error: %v", err)
	}
	if len(rep2.TablesOnlyInNew) != 1 || rep2.TablesOnlyInNew[0] != schema+"._probe" {
		t.Errorf("want TablesOnlyInNew [_probe], got %v", rep2.TablesOnlyInNew)
	}
	if len(rep2.AddedColumns) != 1 || rep2.AddedColumns[0].Column != "b" || rep2.AddedColumns[0].NewPopulated != 1 {
		t.Errorf("want AddedColumns [_base.b populated=1], got %+v", rep2.AddedColumns)
	}
	if rep2.HasFailures() {
		t.Errorf("additions are never failures, got rows %+v", rep2.Rows)
	}
}
