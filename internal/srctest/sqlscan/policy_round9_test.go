// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Round 9 (0.29.216). The reviewer's two Mediums were one mistake: a list
// of the KINDS of FROM item whose last arm was permissive. The walks now
// refuse what they do not list. The fixture is round 7's (ar is
// AlwaysRefresh, fe FillEmptyOnly).

// Every node of a FROM list is an item, and an item of a kind that is not
// read is not clean.
func TestAFromItemThatIsNotReadIsNotClean(t *testing.T) {
	const tail = ` ON CONFLICT (repo_id) DO UPDATE SET `
	r7table(t, map[string]string{
		// A sub-select with no alias (legal since PostgreSQL 16).
		r8up + `prev FROM (SELECT x.repo_id AS rid, x.ar AS prev FROM aveloxis_data.repos x) WHERE rid = r.repo_id + 1`:                                                    KindAssign,
		r8up + `(SELECT prev FROM (SELECT x.ar AS prev FROM aveloxis_data.repos x WHERE x.repo_id = $1))`:                                                                  KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT rid, prev FROM (SELECT $1::int AS rid, x.ar AS prev FROM aveloxis_data.repos x)` + tail + `ar = EXCLUDED.ar`: KindAssign,
		r8up + `v FROM (SELECT o.ar AS v FROM aveloxis_data.other o) WHERE true`:                                                                                           "pass",
		r8up + `v FROM (VALUES ($1::text)) AS t(v)`:                                                                                                                        "pass",
		// JSON_TABLE, XMLTABLE, TABLESAMPLE: not read, so not clean, with
		// an alias or with none, whatever their arguments.
		r8up + `j.v FROM JSON_TABLE($1::jsonb, '$[*]' COLUMNS (v text PATH '$.v')) j`:                                     KindAssign,
		r8up + `v FROM JSON_TABLE((SELECT jsonb_agg(x.ar) FROM aveloxis_data.repos x), '$[*]' COLUMNS (v text PATH '$'))`: KindAssign,
		r8up + `x.v FROM XMLTABLE('/r' PASSING xmlforest($1 AS r) COLUMNS v text PATH '.') x`:                             KindAssign,
		r8up + `o.ar FROM aveloxis_data.other o TABLESAMPLE SYSTEM (10) WHERE o.repo_id = r.repo_id`:                      KindAssign,
		r8up + `(SELECT j.v FROM JSON_TABLE(to_jsonb(r), '$' COLUMNS (v text PATH '$.ar')) j)`:                            KindAssign,
		// A join's alias is one that IS read: its sides are the items.
		r8up + `j.ar FROM (aveloxis_data.other o JOIN aveloxis_data.other p ON p.repo_id = o.repo_id) AS j WHERE j.repo_id = r.repo_id`: "pass",
		r8up + `o.ar FROM (aveloxis_data.other o JOIN aveloxis_data.repos x ON x.repo_id = o.repo_id) WHERE o.repo_id = r.repo_id`:      KindAssign,
		// The whole row of EXCLUDED is what EXCLUDED.col is.
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT $2, l.ar FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `ar = (excluded).ar`:                KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT $2, l.ar FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `ar = row_to_json(excluded)->>'ar'`: KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) VALUES ($1, $2)` + tail + `ar = (excluded).ar`:                                                                "pass",
		`INSERT INTO aveloxis_data.repos (repo_id, ar) VALUES ($1, $2)` + tail + `ar = row_to_json(excluded)->>'ar'`:                                                 "pass",
	})
}

// An expression of a kind the walk does not list is not incoming: the walk
// used to descend into the parts of any node it did not know.
func TestAnExpressionKindThatIsNotListedIsNotIncoming(t *testing.T) {
	r7table(t, map[string]string{
		r8up + `$1::jsonb -> 'a' ->> 'b'`:                  "pass",
		r8up + `($1::text[])[1]`:                           "pass",
		r8up + `ROW($1, $2)::text`:                         "pass",
		r8up + `CASE WHEN $1 IS NULL THEN 'x' ELSE $1 END`: "pass",
		r8up + `xmlelement(name a, $1)::text`:              KindAssign,
		r8up + `JSON_OBJECT('a': $1)::text`:                KindAssign,
		r8up + `JSON_VALUE($1::jsonb, '$.a')`:              KindAssign,
		r8up + `GROUPING($1)::text`:                        KindAssign,
		r8up + `($1 IS JSON)::text`:                        KindAssign,
	})
	for kind := range valueKinds {
		switch kind {
		case "ParamRef", "A_Const", "SetToDefault", "SQLValueFunction", "TypeName", "TypeCast", "ColumnRef", "SelectStmt":
			t.Errorf("%s is read in its own way and is also on the list of kinds whose parts are walked", kind)
		}
	}
}

// The parser is WebAssembly, and a statement nested deep enough overflows
// its stack. That is a statement that is not read, not a panic of the
// check (round 9: both of these panicked out of CheckIn); and since the
// parser does not work after it, nothing after it is read either, and the
// check says so instead of calling a parser that does not return. Each
// case runs in a process of its own (the parser of this one must go on
// working), under a deadline.
func TestAStatementTheParserCannotHoldIsNotRead(t *testing.T) {
	cases := map[string]string{
		"terms":       "UPDATE aveloxis_data.repos SET ar = $1" + strings.Repeat(" || $1", 20000),
		"sub-selects": "UPDATE aveloxis_data.repos SET ar = " + strings.Repeat("(SELECT ", 3000) + "$1" + strings.Repeat(")", 3000),
	}
	if name := os.Getenv(r9ParserChild); name != "" {
		rep := r9check(t, cases[name])
		if len(rep.Unparsed) != 1 || !strings.Contains(rep.Unparsed[0].Detail, "too deeply nested") {
			t.Fatalf("the statement: %+v", rep.Unparsed)
		}
		// Nothing after it is read, whatever it is.
		for _, later := range []string{r8up + `$1`, r8up + `r.ar`, `DO $$ BEGIN UPDATE aveloxis_data.repos SET ar = ar; END $$`} {
			rep := r9check(t, later)
			if len(rep.Unparsed) != 1 || !strings.Contains(rep.Unparsed[0].Detail, "cannot be used again") {
				t.Fatalf("after the failure, %q: unparsed %+v, violations %d", later, rep.Unparsed, len(rep.Violations))
			}
		}
		return
	}
	if _, err := ParserVersion(); err != nil {
		t.Fatal(err)
	}
	for name := range cases {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "^TestAStatementTheParserCannotHoldIsNotRead$", "-test.count=1")
		cmd.Env = append(os.Environ(), r9ParserChild+"="+name)
		out, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			if len(out) > 2000 {
				out = out[:2000]
			}
			t.Errorf("%s: the check did not hold a statement the parser cannot hold (%v):\n%s", name, err, out)
		}
	}
}

const r9ParserChild = "SQLSCAN_R9_PARSER_CHILD"

// A chain of WITH queries, each reading the one before it twice, is judged
// once per query (round 9: 2^n walks; n=22 took 5.7 s, n=40 would not end).
func TestAChainOfWithQueriesIsJudgedOncePerQuery(t *testing.T) {
	chain := func(n int, first string) string {
		var b strings.Builder
		b.WriteString("WITH c0 AS (" + first + ")")
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, ", c%d AS (SELECT a.v FROM c%d a, c%d b)", i, i-1, i-1)
		}
		fmt.Fprintf(&b, " UPDATE aveloxis_data.repos r SET ar = c%d.v FROM c%d", n, n)
		return b.String()
	}
	for _, n := range []int{22, 40} {
		for first, want := range map[string]string{
			"SELECT $1::text AS v":                        "pass",
			"SELECT x.ar AS v FROM aveloxis_data.repos x": KindAssign,
		} {
			cat := r7catalog(t, nil)
			done := make(chan string, 1)
			go func() {
				rep := CheckIn(cat, []Stmt{{File: "f.go", SQL: chain(n, first)}}, round6Registry, nil)
				switch {
				case len(rep.Unparsed) > 0:
					done <- "unparsed"
				case len(rep.Violations) > 0:
					done <- rep.Violations[0].Kind
				default:
					done <- "pass"
				}
			}()
			select {
			case got := <-done:
				if got != want {
					t.Errorf("n=%d over %q: %s, want %s", n, first, got, want)
				}
			case <-time.After(20 * time.Second):
				t.Fatalf("n=%d over %q: not judged in 20 s: the WITH queries are walked again for every reading", n, first)
			}
		}
	}
	// A WITH that names itself is still judged (and is not remembered as
	// clean while it is being judged).
	r7table(t, map[string]string{
		`WITH RECURSIVE t(n, v) AS (SELECT x.repo_id, x.ar FROM aveloxis_data.repos x UNION ALL SELECT t.n + 1, t.v FROM t WHERE t.n < 3) ` + r8up + `t.v FROM t WHERE t.n = r.repo_id`: KindAssign,
		`WITH RECURSIVE a(v) AS (SELECT b.v FROM b), b(v) AS (SELECT a.v FROM a UNION ALL SELECT x.ar FROM aveloxis_data.repos x) ` + r8up + `a.v FROM a, b`:                            KindAssign,
		`WITH RECURSIVE a(v) AS (SELECT b.v FROM b), b(v) AS (SELECT a.v FROM a UNION ALL SELECT x.ar FROM aveloxis_data.repos x) ` + r8up + `b.v FROM b, a`:                            KindAssign,
	})
}

// A value nested deeper than maxIncomingDepth is refused, and one that is
// not is read.
func TestAValueNestedTooDeepIsRefused(t *testing.T) {
	nested := func(n int) string { return r8up + strings.Repeat("lower(", n) + "$1" + strings.Repeat(")", n) }
	judge := func(n int) string {
		rep := r9check(t, nested(n))
		if len(rep.Unparsed) > 0 {
			return "unparsed"
		}
		if len(rep.Violations) > 0 {
			return rep.Violations[0].Detail
		}
		return "pass"
	}
	// Each lower(...) is two steps of the walk (the call, its arguments):
	// 1,000 of them are the bound.
	if got := judge(maxIncomingDepth/2 - 10); got != "pass" {
		t.Errorf("just inside maxIncomingDepth (%d): %s", maxIncomingDepth, got)
	}
	if got := judge(maxIncomingDepth/2 + 10); !strings.Contains(got, "nested too deep") {
		t.Errorf("just past maxIncomingDepth (%d): %s", maxIncomingDepth, got)
	}
	if maxIncomingDepth != 2000 {
		t.Errorf("maxIncomingDepth is %d: the bound is 2,000 (the parser itself fails near 3,000 nested sub-selects)", maxIncomingDepth)
	}
}

func r9check(t *testing.T, sql string) Report {
	t.Helper()
	return CheckIn(r7catalog(t, nil), []Stmt{{File: "f.go", SQL: sql}}, round6Registry, nil)
}
