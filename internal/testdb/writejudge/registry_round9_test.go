// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// The round-9 reviewer's probe files that are judged with the store's OWN
// policies (testdata/round9): fp9, 66 more statements a maintainer might
// write next, each correct by the policies' reasons; wo and wo9, the ways
// out the refusals name, written out (and, in wo9, eight routes X1 to X8
// by which a stored value reaches a column of the real schema, three of
// them controls). reviewer.tsv is what the judge of 0.29.215 said of fp9;
// verdicts.tsv what this one says of all three.

func round9Cases(t *testing.T, name string) []round8Case {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "round9", name))
	if err != nil {
		t.Fatal(err)
	}
	var out []round8Case
	for _, blk := range strings.Split("\n"+string(b), "\n@@") {
		blk = strings.TrimSpace(blk)
		if nl := strings.IndexByte(blk, '\n'); nl >= 0 {
			out = append(out, round8Case{file: name, head: strings.TrimSpace(blk[:nl]), sql: strings.TrimSpace(blk[nl+1:])})
		}
	}
	return out
}

// round9Report judges one statement as round8Verdict does, with the
// declared session tables.
func round9Report(c round8Case) sqlscan.Report {
	cat := sqlscan.NewCatalog()
	for _, p := range Policies {
		cat.Table(p.Table)
	}
	cat.Table(round8Relation.FindAllString(c.sql, -1)...)
	cat.Temp(TempTables...)
	return sqlscan.CheckIn(cat, []sqlscan.Stmt{{File: "f.go", Decl: "d", SQL: c.sql}}, Policies, nil)
}

func TestTheRound9VerdictsWithTheStoresPoliciesAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range []string{"fp9.txt", "wo.txt", "wo9.txt"} {
		for _, c := range round9Cases(t, f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.head, round8Verdict(c)))
		}
	}
	if len(lines) != 95 {
		t.Fatalf("%d cases read, the files hold 95", len(lines))
	}
	got := strings.Join(lines, "\n") + "\n"
	path := filepath.Join("testdata", "round9", "verdicts.tsv")
	if os.Getenv("ROUND9_VERDICTS") == "write" {
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
		t.Error("the verdicts differ from testdata/round9/verdicts.tsv; if the change is meant, ROUND9_VERDICTS=write rewrites it")
	}
}

// The correct statements of fp9 that are refused are 15 of 66 (the list
// is verdicts.tsv; nothing was added to what is accepted for them: an
// allowlist widened for each refused statement is how rounds 6 to 8 went),
// and each refusal ends with what to write instead. wo and wo9 are those
// ways out written out: each passes, but the one whose session table is
// not declared; and the routes of wo9 that the judge of 0.29.215 let
// through (X1 JSON_TABLE reading the table, X2 the whole row of EXCLUDED)
// are held.
func TestTheRound9RefusalsSayTheirWayOut(t *testing.T) {
	refused := 0
	for _, c := range round9Cases(t, "fp9.txt") {
		if round8Verdict(c) != "pass" {
			refused++
			holdWayOut(t, c)
		}
		// A guarded bare fill from another table is told what was found
		// (round 9: it read "expression matches AlwaysRefresh", which
		// names a policy the column does not have and no way out).
		if strings.HasPrefix(c.head, "N03 ") {
			vs := round9Report(c).Violations
			if len(vs) != 1 || !strings.HasPrefix(vs[0].Detail, "the write is guarded on the column being empty, and the value it fills with is not one incoming operand") {
				t.Errorf("%q: %+v", c.head, vs)
			}
		}
	}
	if refused != 15 {
		t.Errorf("%d of fp9's correct statements are refused; 15 were when the ways out were written", refused)
	}
	for _, f := range []string{"wo.txt", "wo9.txt"} {
		for _, c := range round9Cases(t, f) {
			v := round8Verdict(c)
			held := map[string]bool{"N25w": true, "X1": true, "X2": true, "X3": true, "X4": true}[strings.Fields(c.head)[0]]
			if held != (v != "pass") {
				t.Errorf("%s %q: %s", f, c.head, v)
			}
			if v != "pass" {
				holdWayOut(t, c)
			}
		}
	}
	// N25w is the way out once the session table is declared.
	for _, c := range round9Cases(t, "wo9.txt") {
		if strings.HasPrefix(c.head, "N25w") {
			cat := sqlscan.NewCatalog().Temp("tmp_clean")
			for _, p := range Policies {
				cat.Table(p.Table)
			}
			if rep := sqlscan.CheckIn(cat, []sqlscan.Stmt{{File: "f.go", Decl: "d", SQL: c.sql}}, Policies, nil); len(rep.Violations)+len(rep.Unparsed) != 0 {
				t.Errorf("with the session table declared, %q is refused: %+v", c.head, rep.Violations)
			}
		}
	}
}
