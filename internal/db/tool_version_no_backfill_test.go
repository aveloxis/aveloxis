// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestMigrateNeverBackfillsToolVersion — 2026-09-23 log review: migrate
// ran `UPDATE <table> SET tool_version = <current> WHERE tool_version IS
// NULL OR empty` over ~31 tables on EVERY run. With no index on tool_version
// each is a full scan whether or not a row matches — commits alone took
// 2,700–3,500 s per migrate on kate, about 1.5 h per run (worklist 32).
// It was also wrong: tool_version records the binary that GATHERED the
// row (v0.25.11 operator decision), so stamping today's version on rows
// an older binary wrote is fabricated provenance. Column DEFAULTs
// (setToolVersionDefaults) and `tool_version = EXCLUDED.tool_version` on
// every refreshing upsert are the only writers.
//
// Bans the OPERATION (an UPDATE that sets tool_version) in every non-test
// source under internal/db, comments stripped, not just the old name.
func TestMigrateNeverBackfillsToolVersion(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	// The rule is the provenance pairing: an UPDATE may set tool_version
	// only in the same SET list that refreshes data_collection_date — the
	// gathering run restamping its own row (renameRecoveryUpdateSQL does
	// this). A tool_version assignment without that refresh is a backfill.
	// Each statement is read from UPDATE <table> SET to its WHERE or the
	// end of the SQL literal (a backtick), so an upsert's DO UPDATE SET
	// (no table name) is not a match.
	stmtRe := regexp.MustCompile("(?is)\\bUPDATE\\s+[\\w.%]+\\s+SET\\s+([^`]*?)(?:\\bWHERE\\b|`)")
	setsTV := regexp.MustCompile(`(?i)(?:^|,)\s*tool_version\s*=`)
	// The refresh must be a real one: NOW() or a parameter, never the
	// column assigned to itself (review round 1 on v0.29.62).
	setsDCD := regexp.MustCompile(`(?i)(?:^|,)\s*data_collection_date\s*=\s*(?:NOW\(\)|\$\d+)`)
	// A statement assembled by concatenation ("UPDATE "+table+" SET
	// tool_version = $1") has no table-and-SET in one literal; the
	// backfill shape — tool_version assigned a parameter as the FIRST
	// SET item — is flagged wherever it appears.
	firstSetParam := regexp.MustCompile(`(?i)\bSET\s+tool_version\s*=\s*\$\d`)
	pairedSeen := false
	checked := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		code := srctest.StripGoComments(string(src))
		if m := firstSetParam.FindString(code); m != "" {
			// A paired statement puts data_collection_date in the same SET
			// list; only an unpaired one starts with a bare parameter
			// assignment AND lacks the refresh (checked below).
			for _, st := range stmtRe.FindAllStringSubmatch(code, -1) {
				if firstSetParam.MatchString("SET "+st[1]) && setsDCD.MatchString(strings.TrimSpace(st[1])) {
					m = ""
				}
			}
			if m != "" {
				t.Errorf("%s: %q — a bulk tool_version assignment (the removed backfill's shape)", f, m)
			}
		}
		for _, m := range stmtRe.FindAllStringSubmatch(code, -1) {
			set := strings.TrimSpace(m[1])
			if !setsTV.MatchString(set) {
				continue
			}
			if setsDCD.MatchString(set) {
				pairedSeen = true
				continue
			}
			t.Errorf("%s: %q — sets tool_version without refreshing data_collection_date: that stamps the current binary on rows it did not gather (and a bulk one costs a full table scan per migrate)", f, strings.Join(strings.Fields(m[0]), " "))
		}
	}
	if checked < 20 {
		t.Fatalf("scanned only %d files — the glob is not seeing internal/db", checked)
	}
	if !pairedSeen {
		t.Fatal("the statement scan found no legitimate paired UPDATE (renameRecoveryUpdateSQL) — the pattern is not reading SET lists")
	}
}
