// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// The round-8 reviewer's two probe files that are judged with the store's
// OWN policies (testdata/round8; `@@ head` and the SQL under it): fp, 58
// statements a maintainer might write next, each correct by the policies'
// reasons; real, six ways a stored value reaches an AlwaysRefresh column of
// the real schema, in the store's own style, two of them controls.
// reviewer.tsv is what the judge of 0.29.214 said; verdicts.tsv what this
// one says.

type round8Case struct {
	file, head, sql string
}

func round8Cases(t *testing.T, name string) []round8Case {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "round8", name))
	if err != nil {
		t.Fatal(err)
	}
	var out []round8Case
	for _, blk := range strings.Split("\n"+string(b), "\n@@") {
		blk = strings.TrimSpace(blk)
		nl := strings.IndexByte(blk, '\n')
		if nl < 0 {
			continue
		}
		out = append(out, round8Case{file: name, head: strings.TrimSpace(blk[:nl]), sql: strings.TrimSpace(blk[nl+1:])})
	}
	if len(out) == 0 {
		t.Fatalf("%s holds no case", name)
	}
	return out
}

var round8Relation = regexp.MustCompile(`aveloxis_[a-z_]+\.[a-z_0-9]+`)

// round8Verdict judges one statement with the registry, in a catalog that
// knows as base tables every registered table and every table the
// statement itself names (the files' statements name real ones).
func round8Verdict(c round8Case) string {
	cat := sqlscan.NewCatalog()
	for _, p := range Policies {
		cat.Table(p.Table)
	}
	cat.Table(round8Relation.FindAllString(c.sql, -1)...)
	rep := sqlscan.CheckIn(cat, []sqlscan.Stmt{{File: "f.go", Decl: "d", SQL: c.sql}}, Policies, nil)
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

func TestTheRound8VerdictsWithTheStoresPoliciesAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range []string{"fp.txt", "real.txt"} {
		for _, c := range round8Cases(t, f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.head, round8Verdict(c)))
		}
	}
	if len(lines) != 64 {
		t.Fatalf("%d cases read, the files hold 64", len(lines))
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", "round8", "verdicts.tsv")
	if os.Getenv("ROUND8_VERDICTS") == "write" {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	kept, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept) != got {
		keptLines := map[string]bool{}
		for _, l := range strings.Split(string(kept), "\n") {
			keptLines[l] = true
		}
		for _, l := range lines {
			if !keptLines[l] {
				t.Errorf("verdict changed: %q", l)
			}
		}
		t.Error("the verdicts differ from testdata/round8/verdicts.tsv; if the change is meant, ROUND8_VERDICTS=write rewrites it")
	}
}

// The six routes of real.txt, the two controls included, are all held: a
// stored value of the real schema's AlwaysRefresh columns reaches none of
// them (RL2 a LATERAL sibling, RL3 a WITH query's column list, RL5 INSERT
// ... SELECT from the table read back as EXCLUDED, RL6 another row's value
// through LATERAL: each passed 0.29.214).
func TestTheRealSchemasRoutesAreHeld(t *testing.T) {
	n := 0
	for _, c := range round8Cases(t, "real.txt") {
		n++
		if v := round8Verdict(c); v == "pass" {
			t.Errorf("%s passes:\n  %s", c.head, c.sql)
		}
	}
	if n != 6 {
		t.Fatalf("%d cases read, real.txt holds 6", n)
	}
}

// round8Refused are the correct statements of fp.txt that are refused. A
// statement a maintainer might well write that is refused pushes toward an
// Exception, so the list is kept short; why each is refused and what to
// write instead is what the refusal itself says (sqlscan.WaysOut, round 9:
// this list held that text, where nobody refused would read it), and the
// test holds each refusal to it.
var round8Refused = map[string]bool{
	"V06 created_at guarded bare from FROM derived over other table":               true,
	"V46 package_key fill computed in SQL from other columns":                      true,
	"V10 updated_at conditional keep: CASE WHEN $2 > updated_at":                   true,
	"V12 forked_from prefer nonempty with lower()":                                 true,
	"V14 forked_from where guard instead":                                          true,
	"V17 vuln_scan_last_run from other table derived with join to repos to filter": true,
	"V24 msg_text_clean batch json":                                                true,
	"V30 cntrb_login fill CASE reversed arms":                                      true,
	"V32 node_id prefer nonempty via CASE":                                         true,
	"V33 node_id only when given (WHERE guard on param)":                           true,
	"V40 first_detected least coalesce param now":                                  true,
	"V42 entered_exposure COALESCE(stored, NOW())":                                 true,
	"V44 entered_exposure reset NULL on leaving exposure":                          true,
	"V49 package_key guard inside EXISTS / IN (keyset window)":                     true,
	"V50 merge fill": true,
	"V53 repo_description copy on dedup merge (InsertOnly -> must be exception)": true,
	"V54 created_at fill on merge from loser row (self join; fill)":              true,
	"V55 updated_at merge from loser greatest":                                   true,
	"V56 forked_from merge from loser":                                           true,
	"V57 platform id merge from loser guarded":                                   true,
	"V58 vuln_scan_last_run merge from loser (AR: stored of other row)":          true,
}

func TestTheCorrectStatementsRefusedEachHaveAWayOut(t *testing.T) {
	refused, n := map[string]bool{}, 0
	for _, c := range round8Cases(t, "fp.txt") {
		n++
		if v := round8Verdict(c); v != "pass" {
			refused[c.head] = true
			holdWayOut(t, c)
			if !round8Refused[c.head] {
				t.Errorf("fp.txt %q, a correct statement, is refused (%s):\n  %s", c.head, v, c.sql)
			}
		}
	}
	for head := range round8Refused {
		if !refused[head] {
			t.Errorf("%q is listed as refused and passes: remove it from round8Refused", head)
		}
	}
	if n != 58 {
		t.Fatalf("%d cases read, fp.txt holds 58", n)
	}
}

// holdWayOut: every refusal of an assignment in the statement ends with the
// ways out of the column's policy.
func holdWayOut(t *testing.T, c round8Case) {
	t.Helper()
	for _, v := range round9Report(c).Violations {
		out := ""
		for _, p := range Policies {
			if p.Table == v.Table && p.Column == v.Column {
				out = sqlscan.WaysOut[p.Policy]
			}
		}
		if v.Kind == sqlscan.KindAssign && (out == "" || !strings.HasSuffix(v.Detail, " — "+out)) {
			t.Errorf("%s %q is refused and the refusal does not end with the policy's ways out:\n  %s", c.file, c.head, v.Detail)
		}
	}
}
