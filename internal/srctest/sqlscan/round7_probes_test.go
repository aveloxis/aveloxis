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

// The round-7 reviewer's probes (testdata/round7), in the round-6 files'
// form (`@@head` and the SQL under it; `@;` on its own line separates two
// arguments). p1 to p3 and surv are statements written to get a write past
// the judge, with controls among them; fp are 54 CORRECT statements in the
// store's styles. The columns are round 6's (round6Registry). A case is
// known by its whole head line.

type round7Case struct {
	file, head string
	stmts      []Stmt
}

func round7Cases(t *testing.T, name string) []round7Case {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "round7", name))
	if err != nil {
		t.Fatal(err)
	}
	var out []round7Case
	for _, blk := range strings.Split("\n"+string(b), "\n@@") {
		blk = strings.TrimSpace(blk)
		nl := strings.IndexByte(blk, '\n')
		if nl < 0 {
			continue
		}
		c := round7Case{file: name, head: strings.TrimSpace(blk[:nl])}
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

// round7Catalog is the reviewer's: five base tables and a view over the
// registered one. The tables are DECLARED (their foreign keys are not
// known), so a TRUNCATE ... CASCADE is held against every registered table.
func round7Catalog(stmts []Stmt) *Catalog {
	c := CatalogOf(stmts).Table("aveloxis_data.repos", "aveloxis_data.other", "aveloxis_data.src", "aveloxis_ops.q", "public.pt")
	_ = c.Add("CREATE VIEW aveloxis_data.v_repos AS SELECT * FROM aveloxis_data.repos")
	return c
}

func round7Verdict(c round7Case) string {
	rep := CheckIn(round7Catalog(c.stmts), c.stmts, round6Registry, nil)
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

// round7StillPass are the cases of p1 to p3 that pass, each with why: a
// control, a conforming statement, a statement PostgreSQL itself refuses,
// or (said so) something this judge does not model.
var round7StillPass = map[string]string{
	"base-fe-coalesce (ok)":                           "control: the nullable fill",
	"g with selfjoin":                                 "the registered pair; the self-join is in FROM and not in the value",
	"conflict alias x":                                "the fill, stored value named by the target's alias",
	"conflict bare stored":                            "the fill; a bare name in DO UPDATE SET is the target's column",
	"conflict guard in DO UPDATE WHERE":               "the guarded fill: DO UPDATE ... WHERE is the write's own WHERE",
	"conflict excluded in subselect":                  "AlwaysRefresh from EXCLUDED, which is incoming",
	"conflict subselect aliased excluded over target": "AlwaysRefresh from another table that the sub-select calls excluded: not the stored value",
	"ar via quoted uppercase (a different column)":    "control: \"AR\" is another column",
	"ar default":                                      "DEFAULT reads nothing stored",
	"fe coalesce(fe, default)?":                       "COALESCE(fe, NULL) keeps the stored value",
	"fe case":                                         "round 7: the fill spelled as CASE WHEN col IS NULL THEN incoming ELSE col END",
	"merge update":                                    "a fill from the MERGE source, and AlwaysRefresh from it; the source is another table",
	"merge insert io":                                 "an INSERT arm: InsertOnly allows it",
	"delete":                                          "DELETE removes rows; the policies are of the values rows hold",
	"insert-select same table":                        "an INSERT; what it selects is not an assignment of a stored row",
	"insert overriding":                               "an INSERT",
	"D06 set default":                                 "SET DEFAULT changes no stored value",
	"D07 add generated identity on io":                "ADD GENERATED AS IDENTITY changes no stored value",
	"D13 create or replace view named like registered (unqualified)": "defines a view of an unqualified name and writes nothing; a write of that name is unqualified-target",
	"D14 create policy":                            "row security changes which rows a statement reaches, not what a write stores",
	"D15 event trigger":                            "an event trigger fires on DDL, not on rows",
	"D16 trigger on view over registered":          "a trigger ON A VIEW: a write through the view is view-target already",
	"D17 alter table rename constraint":            "renames a constraint",
	"D20 alter column generic: drop not null":      "changes no stored value",
	"D21 import foreign schema into aveloxis_data": "defines foreign tables; a write of one is unknown-target",
	"D23 subscription":                             "names no table; replication is not a statement of the store",
	"D24 delete all rows":                          "DELETE, as above (TRUNCATE is refused for what it is: a statement no policy was written for)",
	"D25 alter table set schema on other to become child? no: other renamed to registered name in other schema": "moves an unregistered table into the schema under its own name",
	"D26 alter index rename":                                   "an index",
	"D27 create table ... of type / partition by on new table": "a new partitioned table of its own",
	"D28 alter table all in tablespace":                        "storage only",
	"D29 alter sequence owned":                                 "a sequence's owner",
	"D30 alter table alter column type on FK-referenced parent (cascade rewrite?) other": "rewrites a column of an unregistered table; a referencing column is not rewritten by it",
	"D32 alter domain add constraint":                                                  "validates, rewrites nothing",
	"D35 alter table ... alter column set data type via ALTER TYPE composite":          "NOT MODELLED: ALTER TYPE ... ALTER ATTRIBUTE ... CASCADE rewrites columns of the composite type in typed tables; the store has no composite-typed column",
	"D38 refresh matview":                                                              "a materialized view is no registered table",
	"D39 create table as with same name as registered in other schema then set schema": "a table of another schema; a write of it is unknown-target",
	"D40 alter extension / create extension":                                           "NOT MODELLED: what an extension's script does is not in the text",
	"R17 conflict update pnn stored qualified by schema.table":                         "the registered shape, stored value named schema.table.col",
	"R24 returning (no effect)":                                                        "RETURNING reads; it writes nothing",
	"R30 keepcarry with stored cast / other col":                                       "the stored value or a cast placeholder: both allowed results",
	"R33 earliest LEAST($1, e, extra)":                                                 "the registered LEAST(stored, incoming), incoming cast",
	"R35 greatest with two different params":                                           "each side is an (incoming, stored) pair; that the two placeholders differ is the caller's",
	"R36 pnn with different param cast truncation":                                     "a cast on the incoming value is the caller's choice of value",
	"R37 pn nullif incoming cast then stored":                                          "the registered shape with a cast placeholder",
	"R38 fe second arg subselect of other table":                                       "the fill; what fills an empty column may come from anywhere",
	"R41 fe guard with char padding semantic: fe = '' OR fe IS NULL":                   "the guard (col IS NULL OR col = ''), either order",
	"R44 guard: 3-part":                                                                "the guard, the column named schema.table.col",
	"R45 guard NULLIF IS NULL":                                                         "the guard NULLIF(col, '') IS NULL",
	"R46 guard bare fe with FROM (ambiguous if other has fe; else target)":             "a bare name PostgreSQL would refuse if it could be the other table's",
	"R47 conflict update where guard bare":                                             "PostgreSQL refuses the bare name there as ambiguous; where it runs, it is the target's",
}

// round7Refused are the correct statements of fp.txt that are refused
// (round 7 accepted eight more: the batch forms from unnest of
// placeholders, plain GREATEST, TRIM of an incoming value,
// COALESCE(incoming, NOW()) and CURRENT_TIMESTAMP, the commuted guard, the
// fill as CASE). Why each is refused and what to write instead is what the
// refusal itself says (WaysOut, round 9: this list held that text, where
// nobody refused would read it); the test holds each refusal to it.
var round7Refused = map[string]bool{
	"F04 cast on the whole": true,
	"F09 guard length":      true,
	"F37 ar subselect whose other table has a column of the same name (bare)": true,
	"F54 ar subselect that locks/reads target for another column":             true,
	"F49 temp staging across statements in ONE text":                          true,
}

func TestTheRound7ProbesAreHeldAgainst(t *testing.T) {
	bad, passed := 0, map[string]bool{}
	for _, f := range []string{"p1.txt", "p2.txt", "p3.txt", "surv.txt"} {
		for _, c := range round7Cases(t, f) {
			bad++
			if v := round7Verdict(c); v == "pass" {
				passed[c.head] = true
				if round7StillPass[c.head] == "" {
					t.Errorf("%s %q passes:\n  %s", f, c.head, c.stmts[0].SQL)
				}
			}
		}
	}
	for head := range round7StillPass {
		if !passed[head] {
			t.Errorf("%q is listed as passing and does not: remove it from round7StillPass", head)
		}
	}
	if bad != 138 {
		t.Fatalf("%d probe cases read, the files hold 138", bad)
	}
	refused, n := map[string]bool{}, 0
	for _, c := range round7Cases(t, "fp.txt") {
		n++
		if v := round7Verdict(c); v != "pass" {
			refused[c.head] = true
			if !round7Refused[c.head] {
				t.Errorf("fp.txt %q, a correct statement, is refused (%s):\n  %s", c.head, v, c.stmts[0].SQL)
			}
			for _, viol := range CheckIn(r7catalog(t, c.stmts), c.stmts, round6Registry, nil).Violations {
				if out := wayOutOf(round6Registry, viol); viol.Kind == KindAssign && (out == "" || !strings.HasSuffix(viol.Detail, " — "+out)) {
					t.Errorf("fp.txt %q is refused and the refusal does not end with the policy's ways out:\n  %s", c.head, viol.Detail)
				}
			}
		}
	}
	for head := range round7Refused {
		if !refused[head] {
			t.Errorf("%q is listed as refused and passes: remove it from round7Refused", head)
		}
	}
	if n != 54 {
		t.Fatalf("%d correct statements read, fp.txt holds 54", n)
	}
}

// The verdict on every probe is kept (testdata/round7/verdicts.tsv), so
// that a change to the judge shows as a change to a named case.
func TestTheRound7VerdictsAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range []string{"p1.txt", "p2.txt", "p3.txt", "surv.txt", "fp.txt"} {
		for _, c := range round7Cases(t, f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.head, round7Verdict(c)))
		}
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", "round7", "verdicts.tsv")
	if os.Getenv("ROUND7_VERDICTS") == "write" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got == string(want) {
		return
	}
	kept := map[string]string{}
	for _, l := range strings.Split(string(want), "\n") {
		if i := strings.LastIndexByte(l, '\t'); i > 0 {
			kept[l[:i]] = l[i+1:]
		}
	}
	for _, l := range lines {
		i := strings.LastIndexByte(l, '\t')
		if kept[l[:i]] != l[i+1:] {
			t.Errorf("verdict changed: %q, kept %q", l, kept[l[:i]])
		}
	}
	t.Error("the probes' verdicts differ from testdata/round7/verdicts.tsv; if the change is meant, ROUND7_VERDICTS=write rewrites it")
}

// wayOutOf is the WaysOut entry of the policy the violation's column is
// registered with.
func wayOutOf(registry []Registered, v Violation) string {
	for _, reg := range registry {
		if reg.Table == v.Table && reg.Column == v.Column {
			return WaysOut[reg.Policy]
		}
	}
	return ""
}
