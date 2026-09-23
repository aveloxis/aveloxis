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
		bad, paired := toolVersionAssignments(srctest.StripGoComments(string(src)))
		if paired > 0 {
			pairedSeen = true
		}
		for _, b := range bad {
			t.Errorf("%s: %q — sets tool_version without refreshing data_collection_date in the same SET list: that stamps the current binary on rows it did not gather (and a bulk one costs a full table scan per migrate)", f, b)
		}
	}
	if checked < 20 {
		t.Fatalf("scanned only %d files — the glob is not seeing internal/db", checked)
	}
	if !pairedSeen {
		t.Fatal("the statement scan found no legitimate paired UPDATE (renameRecoveryUpdateSQL) — the pattern is not reading SET lists")
	}
}

// The rule is the provenance pairing: an UPDATE may set tool_version only
// in the same SET list that refreshes data_collection_date — the gathering
// run restamping its own row (renameRecoveryUpdateSQL does this). A
// tool_version assignment without that refresh is a backfill.
//
// Every assignment whose value is a parameter, a quoted literal or a Go
// concatenation (`'"+ToolVersion+"'`) is judged by ITS OWN SET list: the
// text from the nearest SET before it to the next WHERE, RETURNING or end
// of the literal after it. An upsert's `tool_version =
// EXCLUDED.tool_version` is the refresh path and does not match. Judging
// each site alone is the v0.29.62 round-2 fix: the old scan let one paired
// statement anywhere in a file excuse an unpaired one elsewhere in it, and
// saw only a first-item parameter.
var (
	tvAssignRe = regexp.MustCompile(`(?i)\btool_version\s*=\s*(?:\$\d|'|"\s*\+)`)
	tvSetRe    = regexp.MustCompile(`(?i)\bSET\b`)
	tvEndRe    = regexp.MustCompile("(?i)\\bWHERE\\b|\\bRETURNING\\b|`")
	// The refresh must be a real one: NOW() or a parameter, never the
	// column assigned to itself (review round 1 on v0.29.62).
	tvDCDRe = regexp.MustCompile(`(?i)\bdata_collection_date\s*=\s*(?:NOW\(\)|\$\d+)`)
)

// toolVersionAssignments returns the unpaired assignments (each with its
// SET list) and how many paired ones it saw.
func toolVersionAssignments(code string) (bad []string, paired int) {
	for _, loc := range tvAssignRe.FindAllStringIndex(code, -1) {
		sets := tvSetRe.FindAllStringIndex(code[:loc[0]], -1)
		if len(sets) == 0 {
			continue // not an UPDATE's SET list
		}
		from := sets[len(sets)-1][0]
		if tvEndRe.MatchString(code[from:loc[0]]) {
			continue // past its SET list: a WHERE comparison, not an assignment
		}
		to := len(code)
		if e := tvEndRe.FindStringIndex(code[loc[1]:]); e != nil {
			to = loc[1] + e[0]
		}
		list := code[from:to]
		if tvDCDRe.MatchString(list) {
			paired++
			continue
		}
		bad = append(bad, strings.Join(strings.Fields(list), " "))
	}
	return bad, paired
}

// TestToolVersionAssignmentsJudgesEachSetList drives the scanner with the
// shapes the round-2 review named, so the tripwire above is proven to
// fire, not only to pass.
func TestToolVersionAssignmentsJudgesEachSetList(t *testing.T) {
	cases := []struct {
		name   string
		code   string
		bad    int
		paired int
	}{
		{"concatenated first item", "q := \"UPDATE \" + tbl + \" SET tool_version = $1 WHERE tool_version IS NULL\"", 1, 0},
		{"non-first item", "`UPDATE aveloxis_data.repos SET repo_name = $2, tool_version = $3 WHERE repo_id = $1`", 1, 0},
		{"ToolVersion concatenated", "q := \"UPDATE \" + tbl + \" SET tool_version = '\"+ToolVersion+\"' WHERE tool_version = ''\"", 1, 0},
		{"quoted literal", "`UPDATE aveloxis_data.commits SET tool_version = '0.29.1'`", 1, 0},
		{"paired", "`UPDATE aveloxis_data.repo_info SET tool_version = $2, data_collection_date = NOW() WHERE repo_id = $1`", 0, 1},
		{"self-assigned refresh is not a refresh", "`UPDATE aveloxis_data.repo_info SET tool_version = $2, data_collection_date = data_collection_date WHERE repo_id = $1`", 1, 0},
		{"a paired statement does not excuse another in the same file",
			"a := `UPDATE t1 SET tool_version = $2, data_collection_date = NOW() WHERE id = $1`\n" +
				"b := \"UPDATE \" + tbl + \" SET tool_version = $1\"", 1, 1},
		{"a WHERE comparison is not an assignment", "`UPDATE t SET cntrb_login = $2 WHERE tool_version = $1`", 0, 0},
		{"upsert refresh path", "`INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET tool_version = EXCLUDED.tool_version`", 0, 0},
	}
	for _, c := range cases {
		bad, paired := toolVersionAssignments(c.code)
		if len(bad) != c.bad || paired != c.paired {
			t.Errorf("%s: got %d unpaired %q and %d paired, want %d and %d", c.name, len(bad), bad, paired, c.bad, c.paired)
		}
	}
}
