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
	"strings"
	"testing"
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

// The "populated" predicate must be type-aware: TEXT DEFAULT ” and
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
