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

// round8Refused are the correct statements of fp.txt that are refused,
// each with why and the way out. A statement a maintainer might well write
// that is refused pushes toward an Exception, so the list is kept short
// and every entry says what to write instead.
var round8Refused = map[string]string{
	"V06 created_at guarded bare from FROM derived over other table":               "a guarded bare fill takes one incoming operand (a placeholder, EXCLUDED's, an incoming batch's column). Way out: the COALESCE form, which may fill from another table: SET created_at = COALESCE(r.created_at, s.c)",
	"V46 package_key fill computed in SQL from other columns":                      "as V06. Way out: SET package_key = COALESCE(package_key, lower(name))",
	"V10 updated_at conditional keep: CASE WHEN $2 > updated_at":                   "GreatestNonNull has two shapes and a CASE is neither. Way out: GREATEST($2, updated_at), which is this (GREATEST ignores a NULL)",
	"V12 forked_from prefer nonempty with lower()":                                 "an incoming operand may be trimmed, not otherwise changed. Way out: lower it before it is handed in",
	"V14 forked_from where guard instead":                                          "the shape is the COALESCE; a guard on the placeholder is not read. Way out: COALESCE(NULLIF($2, ''), forked_from)",
	"V17 vuln_scan_last_run from other table derived with join to repos to filter": "round 8: the derived table JOINs the table being written, so it is an item that reads it, and nothing of such an item is taken. Way out: filter in its WHERE, which is a condition: WHERE EXISTS (SELECT 1 FROM aveloxis_data.repos rr WHERE rr.repo_id = v.repo_id AND rr.repo_archived IS NOT TRUE)",
	"V24 msg_text_clean batch json":                                                "round 8: a function in FROM is an incoming batch only as unnest of placeholders. Way out: two arrays, FROM unnest($1::bigint[], $2::text[]) AS j(msg_id, clean)",
	"V30 cntrb_login fill CASE reversed arms":                                      "the fill as CASE is read with the empty test first. Way out: COALESCE(NULLIF(contributors.cntrb_login, ''), EXCLUDED.cntrb_login)",
	"V32 node_id prefer nonempty via CASE":                                         "prefer-nonempty has one shape. Way out: COALESCE(NULLIF($3, ''), node_id)",
	"V33 node_id only when given (WHERE guard on param)":                           "as V14. Way out: COALESCE(NULLIF($3, ''), node_id)",
	"V40 first_detected least coalesce param now":                                  "the earliest-of shape is LEAST(stored, incoming) and COALESCE($2, NOW()) is not an incoming operand. Way out: hand the time in, or an Exception",
	"V42 entered_exposure COALESCE(stored, NOW())":                                 "KeepCarryOrNow is read as a CASE over the three results. Way out: CASE WHEN entered_exposure_at IS NULL THEN NOW() ELSE entered_exposure_at END, or an Exception",
	"V44 entered_exposure reset NULL on leaving exposure":                          "NULL is none of keep, carry or now: a reset is another operation. An Exception",
	"V49 package_key guard inside EXISTS / IN (keyset window)":                     "the guard of a bare fill is a condition ANDed at the top of the write's own WHERE. Way out: add AND package_key IS NULL there",
	"V50 merge fill": "a MERGE action's own condition is not read as a guard. Way out: UPDATE ... WHERE COALESCE(platform_repo_id, '') = ''",
	"V53 repo_description copy on dedup merge (InsertOnly -> must be exception)": "InsertOnly allows no UPDATE at all: an Exception, as the case itself says",
	"V54 created_at fill on merge from loser row (self join; fill)":              "round 8: what a fill fills with is incoming, and another row of the table is not. A merge of two rows of one repository is another operation than the policy describes: an Exception",
	"V55 updated_at merge from loser greatest":                                   "the loser row's value is neither the row's own stored value nor incoming: an Exception, as V54",
	"V56 forked_from merge from loser":                                           "as V55: an Exception",
	"V57 platform id merge from loser guarded":                                   "as V54: an Exception",
	"V58 vuln_scan_last_run merge from loser (AR: stored of other row)":          "AlwaysRefresh from another row's stored value is exactly what the policy forbids; a merge that means to keep it takes an Exception",
}

func TestTheCorrectStatementsRefusedEachHaveAWayOut(t *testing.T) {
	refused, n := map[string]bool{}, 0
	for _, c := range round8Cases(t, "fp.txt") {
		n++
		if v := round8Verdict(c); v != "pass" {
			refused[c.head] = true
			if round8Refused[c.head] == "" {
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
