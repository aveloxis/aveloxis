// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The round-6 reviewer's probes (testdata/round6): statements written to
// get a write of a protected column past the judge. Each file is blocks of
// `@@tag description` and the SQL under it (`@;` on a line of its own
// separates two arguments of one case).
//
// The columns: fe FillEmptyOnly, pn PreferNonemptyIncoming, pnn
// PreferNonNullIncoming, g GreatestNonNull, ar AlwaysRefresh, io InsertOnly,
// e EarliestOfStoredAndIncoming, k KeepCarryOrNow, all on
// aveloxis_data.repos.
var round6Registry = []Registered{
	{Table: "aveloxis_data.repos", Column: "fe", Policy: FillEmptyOnly, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "pn", Policy: PreferNonemptyIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "pnn", Policy: PreferNonNullIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "g", Policy: GreatestNonNull, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "ar", Policy: AlwaysRefresh, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "io", Policy: InsertOnly, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "e", Policy: EarliestOfStoredAndIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "k", Policy: KeepCarryOrNow, Reason: "x"},
}

type round6Case struct {
	file, tag, what string
	stmts           []Stmt
}

func round6Cases(t *testing.T, name string) []round6Case {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "round6", name))
	if err != nil {
		t.Fatal(err)
	}
	var out []round6Case
	for _, blk := range strings.Split("\n"+string(b), "\n@@") {
		blk = strings.TrimSpace(blk)
		nl := strings.IndexByte(blk, '\n')
		if nl < 0 {
			continue
		}
		head := strings.TrimSpace(blk[:nl])
		c := round6Case{file: name, tag: head, what: ""}
		if sp := strings.IndexByte(head, ' '); sp > 0 {
			c.tag, c.what = head[:sp], head[sp+1:]
		}
		for _, s := range strings.Split(strings.TrimSpace(blk[nl+1:]), "\n@;\n") {
			c.stmts = append(c.stmts, Stmt{File: "f.go", SQL: s})
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		t.Fatalf("%s holds no case", name)
	}
	return out
}

// round6Verdict is how the judge takes a case: "pass", or the kinds of what
// it holds against it.
func round6Verdict(c round6Case) string {
	cat := CatalogOf(c.stmts).Table("aveloxis_data.repos")
	rep := CheckIn(cat, c.stmts, round6Registry, nil)
	set := map[string]bool{}
	for range rep.Unparsed {
		set["unparsed"] = true
	}
	for _, v := range rep.Violations {
		set[v.Kind] = true
	}
	if len(set) == 0 {
		return "pass"
	}
	var kinds []string
	for k := range set {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	return strings.Join(kinds, "+")
}

// round6StillPass are the probes the judge passes, each with why that is
// right. Every other case of the bad*.txt files (and every statement the
// reviewer's surviving mutants passed, surv.txt) must be held against.
var round6StillPass = map[string]string{
	"v147": "SET DEFAULT changes no stored value: a later INSERT that omits the column is an insert",
	"v149": "ADD GENERATED AS IDENTITY changes no stored value",
	"p44":  "control: a body that writes nothing",
	"p64":  "CREATE SCHEMA with a view inside defines a view and writes nothing; a write through it is then of a relation the catalog does not know as a base table",
	"p65":  "control: ALTER FUNCTION has no body",
	"x02":  "control: a Cyrillic letter makes it another column",
	"x06":  "control: SELECT f() names no table; what a function of the database does is not in the text",
	"x07":  "control: CALL p(), as SELECT f()",
	"x14":  "a cast placeholder is incoming, and the guard is the target's own column IS NULL: the guarded fill",
	"x17":  "(col) IS NULL is col IS NULL; ROW(col) IS NULL is another test and is refused (surv R37)",
	"b10":  "control: a declaration's default that reads",
	"g01":  "control: a conforming fill; RETURNING old.x is a table reference to PostgreSQL 17 and not a write to either",
	"g14":  "CREATE FOREIGN TABLE (LIKE t) defines a foreign table and writes nothing; a write of it is a write of a foreign table",
	"g16":  "a conforming fill qualified by the MERGE target's alias",
	"g17":  "a conforming fill in ON CONFLICT DO UPDATE",
	"g19":  "control: a read",
	"g20":  "control: a read",
	"g23":  "a conforming fill: the alias `old` is the target's",
	"y07":  "CREATE SUBSCRIPTION names no table; replication writes are not statements of the store",
	"y08":  "ALTER TYPE ... RENAME VALUE rewrites no row",
	"y09":  "DROP TYPE ... CASCADE names no table; that it can drop a column of the type is NOT modelled (the catalog holds no column types)",
	"y10":  "DELETE removes rows; the policies are of the values rows hold",
	"y15":  "control: the guarded fill in an SQL function, whose parameter is not named like the column",
}

// round6Refused are the conforming statements of fp.txt that are refused,
// each with why (the shape set is closed; the reviewer listed them as the
// cost of denying by default).
var round6Refused = map[string]string{
	"f18": "the value is a FROM item's column: no policy shape names that, whatever the guard",
	"f33": "PostgreSQL's PL/pgSQL parser, with no catalog, does not know a variable of a table's row type",
	"f35": "EXECUTE of text made at run time",
	"f36": "PostgreSQL 18 syntax the 17 grammar does not read",
	"f42": "an INSERT through a view: the target is not a base table",
}

func TestTheRound6ProbesAreHeldAgainst(t *testing.T) {
	bad, passed := 0, map[string]bool{}
	for _, f := range []string{"bad1.txt", "bad2.txt", "bad3.txt", "bad4.txt", "surv.txt"} {
		for _, c := range round6Cases(t, f) {
			bad++
			if v := round6Verdict(c); v == "pass" {
				passed[c.tag] = true
				if round6StillPass[c.tag] == "" {
					t.Errorf("%s %s (%s) passes:\n  %s", f, c.tag, c.what, c.stmts[0].SQL)
				}
			}
		}
	}
	for tag := range round6StillPass {
		if !passed[tag] {
			t.Errorf("%s is listed as passing and does not: remove it from round6StillPass", tag)
		}
	}
	if bad < 350 {
		t.Fatalf("%d probe cases read, the files hold 356", bad)
	}
	refused := map[string]bool{}
	for _, c := range round6Cases(t, "fp.txt") {
		// The conforming statements write tables of their own.
		cat := CatalogOf(c.stmts).Table("aveloxis_data.repos", "aveloxis_data.log", "aveloxis_data.other", "aveloxis_data.repo_info")
		rep := CheckIn(cat, c.stmts, round6Registry, nil)
		if len(rep.Violations)+len(rep.Unparsed) > 0 {
			refused[c.tag] = true
			if round6Refused[c.tag] == "" {
				t.Errorf("fp.txt %s (%s), a conforming statement, is refused: %+v %+v\n  %s", c.tag, c.what, rep.Violations, rep.Unparsed, c.stmts[0].SQL)
			}
		}
	}
	for tag := range round6Refused {
		if !refused[tag] {
			t.Errorf("%s is listed as refused and passes: remove it from round6Refused", tag)
		}
	}
}

// The verdict of every probe, kind by kind, is kept (testdata/round6/
// verdicts.tsv): a change to the judge that moves one shows which.
// ROUND6_VERDICTS=write rewrites the file.
func TestTheRound6VerdictsAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range []string{"bad1.txt", "bad2.txt", "bad3.txt", "bad4.txt", "surv.txt", "fp.txt"} {
		for _, c := range round6Cases(t, f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.tag, round6Verdict(c)))
		}
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", "round6", "verdicts.tsv")
	if os.Getenv("ROUND6_VERDICTS") == "write" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		wantLines := strings.Split(string(want), "\n")
		for i, l := range lines {
			if i >= len(wantLines) || wantLines[i] != l {
				t.Errorf("verdict changed: %q, kept %q", l, func() string {
					if i < len(wantLines) {
						return wantLines[i]
					}
					return ""
				}())
			}
		}
		t.Error("the probes' verdicts differ from testdata/round6/verdicts.tsv; if the change is meant, ROUND6_VERDICTS=write rewrites it")
	}
}
