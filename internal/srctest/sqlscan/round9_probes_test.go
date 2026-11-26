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

// The round-9 reviewer's probes (testdata/round9), in the earlier rounds'
// form. p1 to p3 are edits of the forms round 8 judged; p4 a sub-select in
// FROM with no alias; p5 JSON_TABLE, XMLTABLE and the whole row of
// EXCLUDED. reviewer.tsv is what the judge of 0.29.215 said of each (pass
// or refused); verdicts.tsv is what this one says. (The reviewer's fp9, wo
// and wo9 files are judged with the store's own registry:
// internal/testdb/writejudge.)

var round9Files = []string{"p1.txt", "p2.txt", "p3.txt", "p4.txt", "p5.txt"}

// round9Catalog is the reviewer's own (its harness's r9cat): round 8's, and
// views, copies and renamed copies of the registered table.
func round9Catalog(t *testing.T) *Catalog {
	t.Helper()
	c := round8Catalog(t, "")
	for _, d := range []string{
		"CREATE VIEW aveloxis_data.v_other AS SELECT * FROM aveloxis_data.other",
		"CREATE VIEW aveloxis_data.v_filt AS SELECT o.* FROM aveloxis_data.other o WHERE EXISTS (SELECT 1 FROM aveloxis_data.repos r WHERE r.ar = o.x)",
		"CREATE VIEW aveloxis_data.v_scalar AS SELECT o.id, (SELECT r.ar FROM aveloxis_data.repos r WHERE r.repo_id = o.id) AS ar FROM aveloxis_data.other o",
		"CREATE TABLE aveloxis_data.cp AS SELECT * FROM aveloxis_data.repos",
		"CREATE TABLE aveloxis_data.cp2 AS SELECT * FROM aveloxis_data.repos",
		"ALTER TABLE aveloxis_data.cp2 RENAME TO cp3",
		"SELECT * INTO aveloxis_data.cp4 FROM aveloxis_data.repos",
		"CREATE TABLE aveloxis_data.cp5 (LIKE aveloxis_data.repos)",
		"CREATE VIEW aveloxis_data.v_fn AS SELECT * FROM aveloxis_data.other, aveloxis_data.readrepos() f",
		"CREATE VIEW aveloxis_data.v_fn2 AS SELECT o.id, aveloxis_data.readar(o.id) AS ar FROM aveloxis_data.other o",
		"CREATE FOREIGN TABLE aveloxis_data.ft () SERVER s",
		"CREATE TABLE aveloxis_data.cp6 AS TABLE aveloxis_data.repos",
		"CREATE MATERIALIZED VIEW aveloxis_data.mv_other AS SELECT * FROM aveloxis_data.other",
		"CREATE TABLE aveloxis_data.cpv AS SELECT * FROM aveloxis_data.v_repos",
		"CREATE TABLE aveloxis_data.cpw AS WITH w AS (SELECT * FROM aveloxis_data.repos) SELECT * FROM w",
		"CREATE VIEW aveloxis_data.v_cp AS SELECT * FROM aveloxis_data.cp",
		"CREATE TABLE aveloxis_data.cpcp AS SELECT * FROM aveloxis_data.cp",
		"CREATE TABLE aveloxis_data.cp8 AS SELECT * FROM aveloxis_data.repos WITH NO DATA",
		"CREATE TABLE aveloxis_data.cp9 AS SELECT * FROM aveloxis_data.repos; ALTER TABLE aveloxis_data.cp9 SET SCHEMA aveloxis_ops",
		"CREATE RECURSIVE VIEW aveloxis_data.v_rec (n) AS SELECT repo_id FROM aveloxis_data.repos UNION ALL SELECT n FROM v_rec",
		"CREATE TABLE aveloxis_data.repo_hist (LIKE aveloxis_data.repos)",
		"CREATE VIEW aveloxis_data.v_ft AS SELECT * FROM aveloxis_data.ft",
		"CREATE VIEW aveloxis_data.v_unk AS SELECT * FROM aveloxis_data.nowhere",
		"CREATE VIEW aveloxis_data.v_mix AS SELECT o.* FROM aveloxis_data.other o JOIN aveloxis_data.v_repos v ON v.repo_id = o.id",
		"CREATE VIEW aveloxis_data.v_noalias AS SELECT * FROM (SELECT * FROM aveloxis_data.repos)",
		"CREATE TABLE aveloxis_data.cp_noalias AS SELECT * FROM (SELECT * FROM aveloxis_data.repos)",
	} {
		if err := c.Add(d); err != nil {
			t.Fatal(d + ": " + err.Error())
		}
	}
	return c
}

func round9Verdict(t *testing.T, c round8Case) string {
	t.Helper()
	rep := CheckIn(round9Catalog(t), c.stmts, round8Registry, nil)
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

// TestTheRound9VerdictsAreTheOnesKept: the judge's verdict on every case is
// the one kept in verdicts.tsv, so that a change of any is seen.
func TestTheRound9VerdictsAreTheOnesKept(t *testing.T) {
	var lines []string
	for _, f := range round9Files {
		for _, c := range probeCases(t, "round9", f) {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%s", f, c.head, round9Verdict(t, c)))
		}
	}
	if len(lines) != 428 {
		t.Fatalf("%d cases read, the files hold 428", len(lines))
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
		t.Error("the probes' verdicts differ from testdata/round9/verdicts.tsv; if the change is meant, ROUND9_VERDICTS=write rewrites it")
	}
}
