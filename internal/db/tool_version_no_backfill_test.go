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
// Every assignment is judged by ITS OWN SET list: the text from the nearest
// SET before it to the next WHERE, RETURNING, `;` or end of a string
// literal (a backtick or a double quote) after it. Any value counts —
// a parameter, a literal, a Go concatenation, a Sprintf verb, COALESCE —
// except `EXCLUDED.tool_version`, the upsert refresh path. A `DO UPDATE SET`
// list belongs to an upsert, which
// TestUpsertsRefreshToolVersionWithDataCollectionDate owns. The tuple form
// `SET (tool_version, …) = (…)` is an assignment too.
//
// History: v0.29.62 round 2 found one paired statement excusing another
// anywhere in its file; the v0.29.63 review found a SET list running on
// into the NEXT statement's WHERE when the SQL sat in double-quoted
// literals (no backtick to stop it), and the Sprintf / backtick-concat /
// COALESCE / tuple shapes unseen.
var (
	tvAssignRe = regexp.MustCompile(`(?i)\btool_version\s*=\s*`)
	tvTupleRe  = regexp.MustCompile(`(?i)\bSET\s*\(([^)]*)\)\s*=`)
	tvSetRe    = regexp.MustCompile(`(?i)\bSET\b`)
	tvDoUpdRe  = regexp.MustCompile(`(?i)\bDO\s+UPDATE\s*$`)
	tvEndRe    = regexp.MustCompile("(?i)\\bWHERE\\b|\\bRETURNING\\b|;|`|\"")
	// Between a SET and its assignment: a string boundary is allowed there
	// (a SET list may be split across concatenated literals), a clause end
	// is not.
	tvClauseEndRe = regexp.MustCompile("(?i)\\bWHERE\\b|\\bRETURNING\\b|;|`")
	// The refresh must be a real one: NOW() or a parameter, never the
	// column assigned to itself (review round 1 on v0.29.62).
	tvDCDRe      = regexp.MustCompile(`(?i)\bdata_collection_date\s*=\s*(?:NOW\(\)|\$\d+)`)
	tvExcludedRe = regexp.MustCompile(`(?i)^(?:EXCLUDED\.)?tool_version\b`)
	tvColumnRe   = regexp.MustCompile(`(?i)\btool_version\b`)
)

// toolVersionAssignments returns the unpaired assignments (each with its
// SET list) and how many paired ones it saw.
func toolVersionAssignments(code string) (bad []string, paired int) {
	var sites [][2]int // [start of the value or tuple, end of the match]
	for _, loc := range tvAssignRe.FindAllStringIndex(code, -1) {
		if tvExcludedRe.MatchString(code[loc[1]:]) {
			continue // EXCLUDED.tool_version (or itself): not a new value
		}
		sites = append(sites, [2]int{loc[0], loc[1]})
	}
	for _, m := range tvTupleRe.FindAllStringSubmatchIndex(code, -1) {
		if tvColumnRe.MatchString(code[m[2]:m[3]]) {
			sites = append(sites, [2]int{m[0] + len("SET"), m[1]})
		}
	}
	for _, site := range sites {
		sets := tvSetRe.FindAllStringIndex(code[:site[0]], -1)
		if len(sets) == 0 {
			continue // not in an UPDATE's SET list
		}
		from := sets[len(sets)-1][0]
		if tvClauseEndRe.MatchString(code[from:site[0]]) {
			continue // past its SET list: a WHERE comparison, not an assignment
		}
		if tvDoUpdRe.MatchString(code[:from]) {
			continue // an upsert's DO UPDATE SET: TestUpsertsRefreshToolVersionWithDataCollectionDate
		}
		to := len(code)
		if e := tvEndRe.FindStringIndex(code[site[1]:]); e != nil {
			to = site[1] + e[0]
			// A value that is itself a string boundary (`= '"+v+"'`, `= ` + v`)
			// ends the literal at once: the list is what came before it.
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
		// The v0.29.63 review's probes:
		{"a later double-quoted paired statement does not excuse an earlier one",
			"a := \"UPDATE \" + tbl + \" SET tool_version = $1\"\n" +
				"b := \"UPDATE t2 SET data_collection_date = NOW() WHERE id = $1\"", 1, 0},
		{"Sprintf verb", "q := fmt.Sprintf(\"UPDATE %s SET tool_version = %s\", tbl, v)", 1, 0},
		{"backtick concatenation", "q := `UPDATE t SET tool_version = ` + v", 1, 0},
		{"COALESCE value", "`UPDATE t SET tool_version = COALESCE($1, tool_version)`", 1, 0},
		{"tuple assignment", "`UPDATE t SET (tool_version, x) = ($1, $2)`", 1, 0},
		{"upsert DO UPDATE SET with a parameter belongs to the upsert test", "`INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET tool_version = $2`", 0, 0},
	}
	for _, c := range cases {
		bad, paired := toolVersionAssignments(c.code)
		if len(bad) != c.bad || paired != c.paired {
			t.Errorf("%s: got %d unpaired %q and %d paired, want %d and %d", c.name, len(bad), bad, paired, c.bad, c.paired)
		}
	}
}
