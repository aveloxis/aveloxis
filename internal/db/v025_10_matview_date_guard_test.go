// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// TestExplorerNewContributorsGuardsMalformedAuthorDate pins the v0.25.10
// guard: the commit branch of explorer_new_contributors must filter
// cmt_author_date to well-formed ISO dates before to_timestamp() runs.
//
// Without it, a single commit whose cmt_author_date is the literal '%aI'
// (collected by a git < 2.2.0 that doesn't expand the strict-ISO placeholder)
// aborts the ENTIRE matview build with:
//
//	ERROR: invalid value "%aI" for "YYYY" (SQLSTATE 22007)
//
// observed on a production rebuild 2026-06-03. The matview must be resilient
// to one poisoned row out of ~474M, not fail the whole build.
func TestExplorerNewContributorsGuardsMalformedAuthorDate(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	// The regex guard must be present.
	if !strings.Contains(src, `cmt_author_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'`) {
		t.Error("explorer_new_contributors commit branch must guard cmt_author_date " +
			"with `~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` so malformed values (e.g. the " +
			"literal '%aI' from an old git) can't abort the matview build (SQLSTATE 22007).")
	}

	// The guard must sit in the same vicinity as the to_timestamp call it
	// protects — i.e. both reference cmt_author_date. Sanity check that the
	// to_timestamp parse is still present (we guard it, not remove it).
	if !strings.Contains(src, "to_timestamp(co.cmt_author_date") {
		t.Error("explorer_new_contributors must still parse cmt_author_date via to_timestamp")
	}
}

// TestLibyearSummaryOrdersUnknownsLast — v0.29.57. PostgreSQL's DESC
// defaults to NULLS FIRST, so explorer_libyear_summary's definition put
// every repo with an unknown libyear at the top of its "stalest first"
// ordering, and unknown repos became the majority when v0.29.57 stopped
// storing "could not work it out" as 0. The definition's ORDER BY sets
// the order rows are written in; it does not order a reader's SELECT
// (Copilot on PR #210), which needs its own ORDER BY — the view's docs
// say so.
func TestLibyearSummaryOrdersUnknownsLast(t *testing.T) {
	raw, err := os.ReadFile("matviews.sql")
	if err != nil {
		t.Fatal(err)
	}
	// Comment-stripped before matching, so prose in the definition can
	// neither satisfy the check nor truncate it at a semicolon.
	var b strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	sql := b.String()
	start := strings.Index(sql, "CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_libyear_summary")
	if start < 0 {
		t.Fatal("cannot find explorer_libyear_summary")
	}
	end := strings.Index(sql[start:], ";")
	if end < 0 {
		t.Fatal("unterminated explorer_libyear_summary definition")
	}
	def := sql[start : start+end]
	if !strings.Contains(def, "avg(b.libyear) DESC NULLS LAST") {
		t.Error("explorer_libyear_summary must order avg(b.libyear) DESC NULLS LAST — a bare DESC puts unknown repos first")
	}
}
