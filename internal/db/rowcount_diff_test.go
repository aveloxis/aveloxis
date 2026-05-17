// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.22.8 — RowCountDiff is the generic table-row-count comparison
// primitive that backs the `aveloxis data-test` harness. Unlike the
// per-table semantic shadow-diff (cmd/aveloxis/shadow_diff.go),
// RowCountDiff:
//
//   - Self-discovers tables via information_schema.tables (no
//     per-table fixture to maintain).
//   - Scopes to the operator-specified schemas (default:
//     aveloxis_data, aveloxis_ops).
//   - Reports per-table row-count differences with PASS/FLAG/FAIL
//     status: PASS when counts are equal, FLAG when the new DB has
//     more rows (likely new coverage), FAIL when the released DB
//     has more rows (regression / data loss).
//
// The status semantics match the operator's stated v0.22.8 goal:
// catch data loss introduced by schema changes (FAIL), surface
// expanded coverage from new code (FLAG), confirm equivalence
// elsewhere (PASS).

func TestRowCountDiffSourceContract(t *testing.T) {
	src, err := os.ReadFile("rowcount_diff.go")
	if err != nil {
		t.Fatalf("rowcount_diff.go does not exist — v0.22.8 helper required: %v", err)
	}
	code := string(src)
	// Must export the function used by data-test
	if !strings.Contains(code, "func RowCountDiff(") {
		t.Error("rowcount_diff.go must export RowCountDiff(...) — that's the entry " +
			"point the cmd/aveloxis/data_test_cmd.go uses")
	}
	// Must self-discover tables from information_schema (not a
	// hardcoded fixture)
	if !strings.Contains(code, "information_schema.tables") {
		t.Error("RowCountDiff must self-discover tables via information_schema.tables " +
			"— a hardcoded fixture would drift from schema.sql every time someone " +
			"adds a new table")
	}
	// Must exclude views and matviews — only base tables matter
	if !strings.Contains(code, "BASE TABLE") {
		t.Error("RowCountDiff must filter to BASE TABLE (excluding views and matviews) " +
			"so COUNT(*) doesn't recurse through derived data")
	}
	// Status semantics must be encoded
	for _, kw := range []string{"PASS", "FLAG", "FAIL"} {
		if !strings.Contains(code, `"`+kw+`"`) {
			t.Errorf("RowCountDiff status field must use the literal %q for the operator-visible report", kw)
		}
	}
}

func TestRowCountDiffStatusLogicIsCorrect(t *testing.T) {
	// Pure-Go test of the classification helper (no DB needed).
	// Pins the FAIL/FLAG/PASS semantics: released > new = data loss
	// (FAIL); new > released = new coverage (FLAG); equal = PASS.
	cases := []struct {
		released, new int64
		want          string
	}{
		{0, 0, "PASS"},
		{100, 100, "PASS"},
		{100, 101, "FLAG"},
		{100, 0, "FAIL"},
		{0, 5, "FLAG"},
		{5, 0, "FAIL"},
		{1000, 999, "FAIL"},
	}
	for _, c := range cases {
		got := classifyRowCountDiff(c.released, c.new)
		if got != c.want {
			t.Errorf("classifyRowCountDiff(%d, %d) = %q, want %q", c.released, c.new, got, c.want)
		}
	}
}

func TestRowCountDiffSchemaScope(t *testing.T) {
	src, err := os.ReadFile("rowcount_diff.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Default scope should include both aveloxis_data and
	// aveloxis_ops — the two schemas where collection data lives.
	// (aveloxis_augur_data is a view-only compatibility schema and
	// aveloxis_scan is scancode-specific; both omitted by default
	// but the function should accept any schema list.)
	for _, schema := range []string{"aveloxis_data", "aveloxis_ops"} {
		if !strings.Contains(code, schema) {
			t.Errorf("rowcount_diff.go must reference schema %q — at minimum as a "+
				"documented default for the data-test harness", schema)
		}
	}
}

func TestRowCountDiffReportHasFailureSummary(t *testing.T) {
	src, err := os.ReadFile("rowcount_diff.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Must expose a way to tell if the diff had any FAIL rows so
	// the calling command can exit non-zero on regression.
	if !strings.Contains(code, "HasFailures") && !strings.Contains(code, "Failed") {
		t.Error("RowCountDiff result type must expose a failure-detection method " +
			"(HasFailures() or Failed bool) — needed so `aveloxis data-test` exits " +
			"non-zero when data loss is detected, suitable for CI gating")
	}
}
