// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestUpsertsRefreshToolVersionWithDataCollectionDate pins the v0.25.11
// contract: every `INSERT ... ON CONFLICT DO UPDATE` that refreshes
// `data_collection_date = NOW()` MUST also refresh `tool_version`
// (`= EXCLUDED.tool_version` or `= DEFAULT`).
//
// Background: tool_version is populated on first INSERT via the column
// DEFAULT that setToolVersionDefaults keeps current. But a DO UPDATE that
// advances data_collection_date without also refreshing tool_version freezes
// tool_version at the version that FIRST inserted the row. A row first
// collected under 0.14.3 then re-collected today showed
// data_collection_date = today but tool_version = '0.14.3' — the bug
// reported 2026-06-03. tool_version must pair with data_collection_date (the
// run that produced the data), matching Augur semantics.
//
// EXCLUDED.tool_version carries the column DEFAULT (current version) even
// when tool_version is omitted from the INSERT column list — verified
// empirically before the fix.
func TestUpsertsRefreshToolVersionWithDataCollectionDate(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")

	insertRe := regexp.MustCompile(`INSERT INTO (aveloxis_\w+\.\w+)`)
	tvRe := regexp.MustCompile(`tool_version = (EXCLUDED|DEFAULT)`)

	type block struct {
		table   string
		line    int
		inDoUpd bool
		hasDCD  bool
		hasTV   bool
	}
	var cur *block
	var offenders []string

	closeBlock := func() {
		if cur != nil && cur.inDoUpd && cur.hasDCD && !cur.hasTV {
			offenders = append(offenders, cur.table)
		}
		cur = nil
	}

	for n, ln := range lines {
		if m := insertRe.FindStringSubmatch(ln); m != nil {
			closeBlock()
			cur = &block{table: m[1], line: n + 1}
		}
		if cur == nil {
			continue
		}
		if strings.Contains(ln, "DO UPDATE") {
			cur.inDoUpd = true
		}
		if cur.inDoUpd && strings.Contains(ln, "data_collection_date = NOW()") {
			cur.hasDCD = true
		}
		if cur.inDoUpd && tvRe.MatchString(ln) {
			cur.hasTV = true
		}
		st := strings.TrimSpace(ln)
		if cur.inDoUpd && cur.hasDCD &&
			(strings.Contains(ln, "RETURNING") || strings.HasSuffix(st, "`,") || strings.HasSuffix(st, "`)")) {
			closeBlock()
		}
	}
	closeBlock()

	if len(offenders) > 0 {
		t.Errorf("these upserts refresh data_collection_date but freeze tool_version "+
			"(add `tool_version = EXCLUDED.tool_version,` to the DO UPDATE SET): %v", offenders)
	}
}
