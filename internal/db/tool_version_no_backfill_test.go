// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
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
		bad, paired := toolVersionAssignments(string(src))
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
// v0.29.63 review round 3: the scanner reads Go TOKENS (go/scanner), not
// raw source text. Five rounds of widening a regex over the source each
// left escapes, because which string literal a SET list sits in (raw or
// interpreted, where it ends, which statement is next) was being guessed
// from quote counts. Now each SQL expression is rebuilt from the tokens:
// string literals are unquoted, and literals joined by `+` through simple
// operands (identifiers, selectors, calls) form ONE expression, with each
// operand standing in as `?`. Anything else (a comma, a newline, an
// assignment) ends it, so two statements never share a SET list.
//
// Within one expression, every assignment is judged by ITS OWN SET list:
// from the nearest SET before it to the first WHERE, RETURNING, FROM or `;`
// at its own parenthesis depth, or an unmatched `)` (the end of a CTE's
// UPDATE). Any value counts — a parameter, a literal, a concatenated
// operand, a Sprintf verb, COALESCE, `tool_version || $1` — except
// `EXCLUDED.tool_version` or the column alone. Upserts' `DO UPDATE SET`
// lists are judged the same way. The tuple form `SET (tool_version, …) =
// (…)` and a quoted identifier `"tool_version"` are assignments too.
var (
	tvAssignRe = regexp.MustCompile(`(?i)"?\btool_version"?\s*=\s*`)
	tvTupleRe  = regexp.MustCompile(`(?i)\bSET\s*\(([^)]*)\)\s*=`)
	tvSetRe    = regexp.MustCompile(`(?i)\bSET\b`)
	// A clause that ends a SET list (checked at the list's own depth).
	tvStopRe = regexp.MustCompile(`(?i)\bWHERE\b|\bRETURNING\b|\bFROM\b|;`)
	// The refresh must be a real one: NOW() or a parameter, never the
	// column assigned to itself (review round 1 on v0.29.62).
	tvDCDRe = regexp.MustCompile(`(?i)"?\bdata_collection_date"?\s*=\s*(?:NOW\(\)|NOW_CALL|\$\d+)`)
	tvNowRe = regexp.MustCompile(`(?i)\bNOW\(\s*\)`)
	// The value is exactly EXCLUDED.tool_version or the column itself,
	// ending the item; `tool_version || $1` is a new value.
	tvExcludedRe = regexp.MustCompile(`(?i)^(?:EXCLUDED\.)?"?tool_version"?\s*(?:,|\)|$|\bWHERE\b|\bRETURNING\b|\bFROM\b|;)`)
	tvParenRe    = regexp.MustCompile(`\([^()]*\)`)
	tvColumnRe   = regexp.MustCompile(`(?i)\btool_version\b`)
	// IS [NOT] DISTINCT FROM is a comparison, not a FROM clause (round 4).
	tvDistinctRe = regexp.MustCompile(`(?i)\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b`)
)

// sqlExpressions rebuilds the string expressions of Go source from its
// tokens (comments are skipped by the scanner).
func sqlExpressions(src string) []string {
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(src))
	var sc scanner.Scanner
	sc.Init(file, []byte(src), nil, 0)
	var (
		out   []string
		cur   strings.Builder
		open  bool
		depth int // parentheses opened by a call operand inside the expression
	)
	flush := func() {
		if open {
			out = append(out, cur.String())
		}
		cur.Reset()
		open, depth = false, 0
	}
	for {
		_, tok, lit := sc.Scan()
		switch {
		case tok == token.EOF:
			flush()
			return out
		case tok == token.STRING:
			text, err := strconv.Unquote(lit)
			if err != nil {
				text = lit
			}
			cur.WriteString(text)
			open = true
		case open && tok == token.ADD:
		case open && (tok == token.IDENT || tok == token.PERIOD || tok == token.INT):
			cur.WriteString("?")
		case open && tok == token.LPAREN:
			depth++
		case open && tok == token.RPAREN && depth > 0:
			depth--
		case open && tok == token.COMMA && depth > 0:
		default:
			flush()
		}
	}
}

// blankParens removes parenthesized text, innermost first, so a subquery's
// own WHERE inside a SET item is not read as the end of the list.
func blankParens(s string) string {
	for {
		next := tvParenRe.ReplaceAllString(s, "")
		if next == s {
			return s
		}
		s = next
	}
}

// setListEnd returns the index in sql where the SET list continuing at
// from ends: the first stop clause at depth 0, or an unmatched `)`.
func setListEnd(sql string, from int) int {
	stops := map[int]bool{}
	for _, loc := range tvStopRe.FindAllStringIndex(sql[from:], -1) {
		stops[from+loc[0]] = true
	}
	depth := 0
	for i := from; i < len(sql); i++ {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return i
			}
		}
		if depth == 0 && stops[i] {
			return i
		}
	}
	return len(sql)
}

// toolVersionAssignments returns the unpaired assignments (each with its
// SET list) and how many paired ones it saw, in Go source.
func toolVersionAssignments(src string) (bad []string, paired int) {
	for _, sql := range sqlExpressions(src) {
		sql = tvDistinctRe.ReplaceAllString(sql, "IS_DISTINCT_COMPARISON")
		var sites [][2]int // [start of the assignment or tuple, end of the match]
		for _, loc := range tvAssignRe.FindAllStringIndex(sql, -1) {
			if tvExcludedRe.MatchString(sql[loc[1]:]) {
				continue // EXCLUDED.tool_version (or itself): not a new value
			}
			sites = append(sites, [2]int{loc[0], loc[1]})
		}
		for _, m := range tvTupleRe.FindAllStringSubmatchIndex(sql, -1) {
			if tvColumnRe.MatchString(sql[m[2]:m[3]]) {
				sites = append(sites, [2]int{m[0] + len("SET"), m[1]})
			}
		}
		for _, site := range sites {
			sets := tvSetRe.FindAllStringIndex(sql[:site[0]], -1)
			if len(sets) == 0 {
				continue // not in an UPDATE's SET list
			}
			from := sets[len(sets)-1][0]
			if tvStopRe.MatchString(blankParens(sql[from:site[0]])) {
				continue // past its SET list: a WHERE comparison, not an assignment
			}
			list := sql[from:setListEnd(sql, site[1])]
			// The refresh must be one of the list's own items, not text
			// inside a subquery (round 4).
			// NOW() is kept as a token so blanking parentheses keeps it.
			if tvDCDRe.MatchString(blankParens(tvNowRe.ReplaceAllString(list, "NOW_CALL"))) {
				paired++
				continue
			}
			bad = append(bad, strings.Join(strings.Fields(list), " "))
		}
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
		// Round 2's probes:
		{"upsert DO UPDATE SET with a parameter is judged too", "`INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET tool_version = $2`", 1, 0},
		{"paired upsert DO UPDATE SET", "`INSERT INTO t (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET tool_version = $2, data_collection_date = NOW()`", 0, 1},
		{"quoted identifier", "`UPDATE t SET \"tool_version\" = $1`", 1, 0},
		{"a subquery's WHERE before the assignment", "`UPDATE t SET x = (SELECT y FROM z WHERE z.id = t.id), tool_version = $1`", 1, 0},
		{"self-concatenation is a new value", "`UPDATE t SET tool_version = tool_version || $1`", 1, 0},
		// Round 3's probes:
		{"quoted identifier in an interpreted string", "q := \"UPDATE t SET \\\"tool_version\\\" = $1\"", 1, 0},
		{"a SET list split across raw literals", "q := `UPDATE t SET a = $2, ` + `tool_version = $1`", 1, 0},
		{"SET and the assignment in different raw literals", "q := `UPDATE t SET ` + `tool_version = $1`", 1, 0},
		{"a backtick inside an interpreted string does not flip anything",
			"k := \"press ` key\"\na := \"UPDATE \" + tbl + \" SET tool_version = $1\"\nb := \"UPDATE t2 SET data_collection_date = NOW() WHERE id = $1\"", 1, 0},
		{"a CTE's UPDATE ends at its closing parenthesis", "`WITH a AS (UPDATE t SET tool_version = $1) UPDATE t2 SET data_collection_date = NOW()`", 1, 0},
		{"a paired list split across two double-quoted literals now pairs", "q := \"UPDATE t SET tool_version = $1, \" +\n\t\"data_collection_date = NOW() WHERE id = $2\"", 0, 1},
		{"two statements in a slice literal stay apart", "qs := []string{\"UPDATE a SET tool_version = $1\", \"UPDATE b SET data_collection_date = NOW()\"}", 1, 0},
		{"a comment is not SQL", "// UPDATE t SET tool_version = $1\nx := 1", 0, 0},
		// Round 4's probes:
		{"IS DISTINCT FROM before the assignment", "`UPDATE t SET flag = a IS DISTINCT FROM b, tool_version = $1 WHERE id = $2`", 1, 0},
		{"CASE with IS NOT DISTINCT FROM before the assignment", "`UPDATE t SET x = CASE WHEN a IS NOT DISTINCT FROM b THEN 1 END, tool_version = $1`", 1, 0},
		{"IS DISTINCT FROM after a paired assignment", "`UPDATE t SET tool_version = $1, data_collection_date = NOW(), flag = a IS DISTINCT FROM b WHERE id = $2`", 0, 1},
		{"a subquery's data_collection_date is not the refresh", "`UPDATE t SET tool_version = $1, x = (SELECT 1 FROM z WHERE data_collection_date = $2)`", 1, 0},
		{"a quoted data_collection_date still pairs", "`UPDATE t SET tool_version = $2, \"data_collection_date\" = NOW() WHERE id = $1`", 0, 1},
	}
	for _, c := range cases {
		bad, paired := toolVersionAssignments(c.code)
		if len(bad) != c.bad || paired != c.paired {
			t.Errorf("%s: got %d unpaired %q and %d paired, want %d and %d", c.name, len(bad), bad, paired, c.bad, c.paired)
		}
	}
}
