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

// The round-8 reviewer's probes (testdata/round8), in the earlier rounds'
// form (`@@ head` and the SQL under it). a1 to a4 are routes by which a
// stored value reaches an AlwaysRefresh value; b1 the neighbours of the
// forms round 7 began to accept; d1 and d2 DDL and identity (d2 in a
// catalog where the registered table has a parent and a partition); q1
// bodies; sv the statements that flip under four mutants; wo the ways out
// the documentation gave for four refused forms. reviewer.tsv is what the
// judge of 0.29.214 said of each; verdicts.tsv is what this one says. (The
// reviewer's fp and real files are judged with the store's own registry:
// internal/testdb/writejudge.)

type round8Case struct {
	file, head string
	stmts      []Stmt
}

var round8Files = []string{"a1.txt", "a2.txt", "a3.txt", "a4.txt", "b1.txt", "d1.txt", "d2.txt", "q1.txt", "sv.txt", "wo.txt"}

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
		c := round8Case{file: name, head: strings.TrimSpace(blk[:nl])}
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

// round8Registry: the reviewer's columns on repos.
var round8Registry = []Registered{
	{Table: "aveloxis_data.repos", Column: "fe", Policy: FillEmptyOnly, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "pn", Policy: PreferNonemptyIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "pnn", Policy: PreferNonNullIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "g", Policy: GreatestNonNull, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "ar", Policy: AlwaysRefresh, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "io", Policy: InsertOnly, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "e", Policy: EarliestOfStoredAndIncoming, Reason: "x"},
	{Table: "aveloxis_data.repos", Column: "k", Policy: KeepCarryOrNow, Reason: "x"},
}

// round8Catalog is the reviewer's own (its harness's r8cat): tables that
// are DECLARED, two views and a materialized view over the registered
// table, a table that references it and one that stands alone, both
// DEFINED. For d2 the registered table has a parent and a partition.
func round8Catalog(t *testing.T, file string) *Catalog {
	t.Helper()
	c := NewCatalog().Table("aveloxis_data.repos", "aveloxis_data.other", "aveloxis_data.src", "aveloxis_ops.q", "public.pt")
	ddl := []string{
		"CREATE VIEW aveloxis_data.v_repos AS SELECT * FROM aveloxis_data.repos",
		"CREATE VIEW aveloxis_data.v2 AS SELECT * FROM aveloxis_data.v_repos",
		"CREATE MATERIALIZED VIEW aveloxis_data.mv AS SELECT * FROM aveloxis_data.repos",
		"CREATE TABLE aveloxis_data.kid (id int, rid int REFERENCES aveloxis_data.repos (repo_id))",
		"CREATE TABLE aveloxis_data.lone (id int)",
	}
	if file == "d2.txt" {
		ddl = append(ddl,
			"CREATE TABLE aveloxis_data.anc (id int)",
			"CREATE TABLE aveloxis_data.repos () INHERITS (aveloxis_data.anc)",
			"CREATE TABLE aveloxis_data.repos_part PARTITION OF aveloxis_data.repos DEFAULT")
	}
	for _, d := range ddl {
		if err := c.Add(d); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func round8Verdict(t *testing.T, c round8Case) string {
	t.Helper()
	rep := CheckIn(round8Catalog(t, c.file), c.stmts, round8Registry, nil)
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

func tag8(head string) string {
	if i := strings.IndexByte(head, ' '); i > 0 {
		return head[:i]
	}
	return head
}

// TestTheRound8VerdictsAreTheOnesKept: the judge's verdict on every case
// is the one kept in verdicts.tsv, so that a change of any is seen.
func TestTheRound8VerdictsAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range round8Files {
		for _, c := range round8Cases(t, f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.head, round8Verdict(t, c)))
		}
	}
	if len(lines) != 329 {
		t.Fatalf("%d cases read, the files hold 329", len(lines))
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
		t.Error("the probes' verdicts differ from testdata/round8/verdicts.tsv; if the change is meant, ROUND8_VERDICTS=write rewrites it")
	}
}
