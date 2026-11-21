// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"sort"
	"strings"
	"testing"
)

// Round 7 of the review of the judging (0.29.214). Each test is one rule;
// the reviewer's own statements are testdata/round7 (round7_probes_test.go).
//
// The columns are round 6's: fe FillEmptyOnly, pn PreferNonemptyIncoming,
// pnn PreferNonNullIncoming, g GreatestNonNull, ar AlwaysRefresh, io
// InsertOnly, e EarliestOfStoredAndIncoming, k KeepCarryOrNow, all on
// aveloxis_data.repos.

// r7schema is a schema read from DDL, so that the catalog knows its views
// and its foreign keys: repos, a table that references it (child), one
// that references that (grandchild), one that references nothing, a view
// over repos and a view over that view.
const r7schema = `
CREATE TABLE aveloxis_data.repo_groups (id int PRIMARY KEY);
CREATE TABLE aveloxis_data.repos (repo_id int PRIMARY KEY, grp int REFERENCES aveloxis_data.repo_groups (id), fe text, pn text, pnn text, g int, ar text, io int, e date, k timestamptz);
CREATE TABLE aveloxis_data.child (id int PRIMARY KEY, repo_id int, CONSTRAINT fk FOREIGN KEY (repo_id) REFERENCES aveloxis_data.repos (repo_id));
CREATE TABLE aveloxis_data.other (id int PRIMARY KEY, repo_id int, ar text, x text);
CREATE TABLE aveloxis_data.lonely (id int PRIMARY KEY);
CREATE VIEW aveloxis_data.v_repos AS SELECT * FROM aveloxis_data.repos;
CREATE VIEW aveloxis_data.vv_repos AS SELECT repo_id, ar AS renamed FROM aveloxis_data.v_repos;
CREATE VIEW aveloxis_data.v_other AS WITH w AS (SELECT * FROM aveloxis_data.other) SELECT * FROM w;
CREATE MATERIALIZED VIEW aveloxis_data.m_repos AS SELECT repo_id, ar FROM aveloxis_data.repos;
CREATE TABLE aveloxis_data.late (id int PRIMARY KEY);
CREATE TABLE aveloxis_data.late2 (id int PRIMARY KEY);
ALTER TABLE aveloxis_data.repos ADD COLUMN late_id int REFERENCES aveloxis_data.late (id);
ALTER TABLE aveloxis_data.repos ADD CONSTRAINT late2_fk FOREIGN KEY (g) REFERENCES aveloxis_data.late2 (id);
CREATE TABLE aveloxis_data.c1 (id int PRIMARY KEY);
CREATE TABLE aveloxis_data.c2 (id int PRIMARY KEY REFERENCES aveloxis_data.c1 (id));
CREATE TABLE aveloxis_data.c3 (id int PRIMARY KEY REFERENCES aveloxis_data.c2 (id));
CREATE TABLE aveloxis_data.c4 (id int PRIMARY KEY REFERENCES aveloxis_data.c3 (id));
CREATE TABLE aveloxis_data.c5 (id int PRIMARY KEY REFERENCES aveloxis_data.c4 (id));
CREATE TABLE aveloxis_data.c6 (id int PRIMARY KEY REFERENCES aveloxis_data.c5 (id));
ALTER TABLE aveloxis_data.repo_groups ADD COLUMN c6 int REFERENCES aveloxis_data.c6 (id);
`

func r7catalog(t *testing.T, stmts []Stmt) *Catalog {
	t.Helper()
	c := NewCatalog()
	if err := c.Add(r7schema); err != nil {
		t.Fatal(err)
	}
	for _, st := range stmts {
		_ = c.Add(st.SQL)
	}
	return c
}

// r7verdict judges one text: "pass", or the kinds held against it.
func r7verdict(t *testing.T, sql string) string {
	t.Helper()
	stmts := []Stmt{{File: "f.go", Decl: "d", SQL: sql}}
	rep := CheckIn(r7catalog(t, stmts), stmts, round6Registry, nil)
	set := map[string]bool{}
	for range rep.Unparsed {
		set["unparsed"] = true
	}
	for _, v := range rep.Violations {
		set[v.Kind] = true
		if v.Kind != KindUnknownTable && (v.Statement == "" || v.SQL != sql) {
			t.Errorf("a %s violation carries no statement (%q) or not the text it was read from:\n  %s", v.Kind, v.Statement, sql)
		}
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

func r7table(t *testing.T, cases map[string]string) {
	t.Helper()
	for sql, want := range cases {
		if got := r7verdict(t, sql); got != want {
			t.Errorf("%s, want %s:\n  %s", got, want, sql)
		}
	}
}

// Q7-2, and round 8's inversion of it: an AlwaysRefresh value is made of
// incoming leaves. A FROM item that reads the target (the table again, a
// view over it at any remove, a WITH query or a sub-select that reads it)
// is not a clean item, and NO column of it is taken: round 7 enumerated
// such an item's outputs to let the ones that could not hold the column
// through, and round 8 found three ways past the enumeration. (Confirmed
// on PostgreSQL 18: the self-join, the WITH and the renaming sub-select
// each wrote a stored value back.)
func TestAlwaysRefreshFollowsAFromItemToTheTarget(t *testing.T) {
	const up = `UPDATE aveloxis_data.repos r SET ar = `
	r7table(t, map[string]string{
		// The table again.
		up + `r2.ar FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`:               KindAssign,
		up + `r2.ar || 'x' FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`:        KindAssign,
		up + `to_jsonb(r2)->>'ar' FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`: KindAssign,
		up + `(r2).ar FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`:             KindAssign,
		up + `(SELECT r2.*)::text FROM aveloxis_data.repos r2`:                              KindAssign,
		// ANOTHER column of the ROW BEING WRITTEN is not this column's
		// stored value. Another column of the table read AGAIN is a column
		// of an item that is not clean (round 8: no enumeration).
		up + `r2.fe FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`: KindAssign,
		`UPDATE aveloxis_data.repos SET ar = fe`:                              "pass",
		`UPDATE aveloxis_data.repos r SET ar = r.fe`:                          "pass",
		// A view over it, at any remove: every column may be the stored one.
		up + `v.ar FROM aveloxis_data.v_repos v WHERE v.repo_id = r.repo_id`:          KindAssign,
		up + `v.renamed FROM aveloxis_data.vv_repos v WHERE v.repo_id = r.repo_id`:    KindAssign,
		up + `v.repo_id::text FROM aveloxis_data.vv_repos v`:                          KindAssign,
		up + `aveloxis_data.v_repos.ar FROM aveloxis_data.v_repos`:                    KindAssign,
		up + `m.ar FROM aveloxis_data.m_repos m WHERE m.repo_id = r.repo_id`:          KindAssign,
		up + `(SELECT v.ar FROM aveloxis_data.v_repos v WHERE v.repo_id = r.repo_id)`: KindAssign,
		// A view over another table is another relation.
		up + `v.ar FROM aveloxis_data.v_other v WHERE v.repo_id = r.repo_id`: "pass",
		// A WITH query that reads it.
		`WITH old AS (SELECT repo_id, ar FROM aveloxis_data.repos) ` + up + `old.ar FROM old WHERE old.repo_id = r.repo_id`:        KindAssign,
		`WITH old AS (SELECT repo_id, ar AS x FROM aveloxis_data.repos) ` + up + `old.x FROM old WHERE old.repo_id = r.repo_id`:    KindAssign,
		`WITH a AS (SELECT * FROM aveloxis_data.repos), b AS (SELECT * FROM a) ` + up + `b.ar FROM b WHERE b.repo_id = r.repo_id`:  KindAssign,
		`WITH old AS (SELECT repo_id, ar FROM aveloxis_data.repos) ` + up + `(SELECT o.ar FROM old o WHERE o.repo_id = r.repo_id)`: KindAssign,
		`WITH gone AS (DELETE FROM aveloxis_data.repos WHERE repo_id < 0 RETURNING *) ` + up + `gone.ar FROM gone`:                 KindAssign,
		// The same, with an output that is not the stored column: refused
		// since round 8 (an item that reads the target hands on nothing).
		// The store's own derived tables read OTHER tables.
		`WITH n AS (SELECT repo_id, count(*) AS c FROM aveloxis_data.repos GROUP BY repo_id) ` + up + `n.c::text FROM n WHERE n.repo_id = r.repo_id`: KindAssign,
		up + `s.x FROM (SELECT repo_id, fe AS x FROM aveloxis_data.repos) s WHERE s.repo_id = r.repo_id`:                                             KindAssign,
		`WITH n AS (SELECT repo_id, count(*) AS c FROM aveloxis_data.other GROUP BY repo_id) ` + up + `n.c::text FROM n WHERE n.repo_id = r.repo_id`: "pass",
		// A sub-select written in place.
		up + `s.x FROM (SELECT repo_id, ar AS x FROM aveloxis_data.repos) s WHERE s.repo_id = r.repo_id`:             KindAssign,
		up + `s.x FROM (SELECT repo_id, upper(ar) AS x FROM aveloxis_data.repos) s WHERE s.repo_id = r.repo_id`:      KindAssign,
		up + `x FROM (SELECT repo_id AS id, ar AS x FROM aveloxis_data.repos) s WHERE s.id = r.repo_id`:              KindAssign, // bare
		up + `s.repo_id::text FROM (SELECT repo_id, ar AS x FROM aveloxis_data.repos) s WHERE s.repo_id = r.repo_id`: KindAssign,
		up + `to_jsonb(s)::text FROM (SELECT repo_id, fe FROM aveloxis_data.repos) s`:                                KindAssign, // the whole row
		up + `s.j::text FROM (SELECT to_jsonb(r3) AS j FROM aveloxis_data.repos r3) s`:                               KindAssign, // a whole row inside
		up + `s.j::text FROM (SELECT to_jsonb(repos) AS j FROM aveloxis_data.repos) s`:                               KindAssign,
		up + `s.j::text FROM (SELECT r3.* FROM aveloxis_data.repos r3) s`:                                            KindAssign,
		// Outputs that cannot be enumerated: every column counts.
		up + `s.repo_id::text FROM (SELECT * FROM aveloxis_data.repos) s`:                                                      KindAssign,
		up + `s.upper FROM (SELECT upper(ar) FROM aveloxis_data.repos) s`:                                                      KindAssign,
		up + `s.a FROM (SELECT repo_id, fe FROM aveloxis_data.repos) s(a, b)`:                                                  KindAssign,
		up + `s.fe FROM (SELECT repo_id, fe FROM aveloxis_data.repos UNION ALL SELECT repo_id, ar FROM aveloxis_data.repos) s`: KindAssign,
		up + `s.fe FROM (SELECT v.repo_id, v.fe FROM aveloxis_data.v_repos v) s`:                                               KindAssign,
		up + `s.fe FROM (SELECT t.repo_id, t.fe FROM (SELECT * FROM aveloxis_data.repos) t) s`:                                 KindAssign,
		up + `s.c FROM (SELECT (SELECT max(ar) FROM aveloxis_data.other) AS c FROM aveloxis_data.repos) s`:                     KindAssign,
		up + `j.fe FROM (aveloxis_data.repos a JOIN aveloxis_data.other b ON a.repo_id = b.repo_id) AS j`:                      KindAssign,
		// A MERGE's source is its FROM.
		`MERGE INTO aveloxis_data.repos t USING (SELECT repo_id, ar FROM aveloxis_data.repos) s ON t.repo_id = s.repo_id WHEN MATCHED THEN UPDATE SET ar = s.ar`: KindAssign,
		`MERGE INTO aveloxis_data.repos t USING aveloxis_data.v_repos s ON t.repo_id = s.repo_id WHEN MATCHED THEN UPDATE SET ar = s.fe`:                         KindAssign,
		`MERGE INTO aveloxis_data.repos t USING aveloxis_data.other s ON t.repo_id = s.repo_id WHEN MATCHED THEN UPDATE SET ar = s.ar`:                           "pass",
		// Another relation, as before.
		up + `o.ar FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id`:                               "pass",
		up + `s.x FROM (SELECT repo_id, ar AS x FROM aveloxis_data.other) s WHERE s.repo_id = r.repo_id`: "pass",
	})
}

// What reads the target through FROM is, for the other policies, neither
// the row's own stored value nor incoming. Round 8: that holds for what a
// fill fills WITH as well (it was allowed to come from anywhere; no
// statement of the store fills from another row, and "incoming" is one
// closed list for every policy).
func TestAnotherRowsValueIsNotTheRowsOwn(t *testing.T) {
	const from = ` FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id + 1`
	r7table(t, map[string]string{
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, r2.fe)` + from:                    KindAssign,
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, o.fe) FROM aveloxis_data.other o`: "pass",
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r2.fe, $1)` + from:                      KindAssign,
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE($1, r2.pnn)` + from:                    KindAssign,
		`UPDATE aveloxis_data.repos r SET pn = COALESCE(NULLIF($1, ''), r2.pn)` + from:          KindAssign,
		`UPDATE aveloxis_data.repos r SET g = GREATEST($1, r2.g)` + from:                        KindAssign,
		`UPDATE aveloxis_data.repos r SET e = LEAST(r2.e, $1)` + from:                           KindAssign,
		`UPDATE aveloxis_data.repos r SET k = CASE WHEN TRUE THEN r2.k ELSE NOW() END` + from:   KindAssign,
		`UPDATE aveloxis_data.repos r SET fe = r2.fe` + from + ` AND r.fe IS NULL`:              KindAssign,
		`UPDATE aveloxis_data.repos r SET io = r2.io` + from:                                    KindAssign,
	})
}

// The batch forms (round 7, F13 and F26): a FROM item that holds nothing
// but values handed in is INCOMING, and its columns are incoming values:
// unnest of placeholders, VALUES of placeholders, a SELECT of them. The
// item must be named, and must read nothing: STRICTLY.
func TestAFromItemOfPlaceholdersIsIncoming(t *testing.T) {
	const tail = ` WHERE r.repo_id = v.id`
	accept := []string{
		`FROM (SELECT unnest($1::bigint[]) AS id, unnest($2::text[]) AS x) v`,
		`FROM unnest($1::bigint[], $2::text[]) AS v(id, x)`,
		`FROM (VALUES ($1::bigint, $2::text), ($3, $4)) AS v(id, x)`,
		`FROM (SELECT $1::bigint AS id, $2 AS x) v`,
		`FROM (SELECT t.id, t.x FROM unnest($1::bigint[], $2::text[]) AS t(id, x)) v`,
		`FROM (SELECT * FROM unnest($1::bigint[], $2::text[]) AS t(id, x)) v`,
		`FROM pg_catalog.unnest($1::bigint[], $2::text[]) AS v(id, x)`,
	}
	refuse := []string{
		`FROM aveloxis_data.other v`,                     // a table
		`FROM (SELECT id, x FROM aveloxis_data.other) v`, // reads a table
		`FROM (SELECT unnest($1::bigint[]) AS id, (SELECT max(x) FROM aveloxis_data.other) AS x) v`, // one output reads
		`FROM (SELECT unnest($1::bigint[]) AS id, 'k' AS x) v`,                                      // a constant is no placeholder
		`FROM (SELECT unnest($1::bigint[]) AS id, lower($2) AS x) v`,                                // nor a function of one
		`FROM (SELECT unnest($1::bigint[]) AS id, unnest(ARRAY['a']) AS x) v`,                       // nor unnest of a constant
		`FROM (SELECT unnest($1::bigint[]) AS id, $2 AS x WHERE EXISTS (SELECT 1)) v`,               // a WHERE
		`FROM (SELECT unnest($1::bigint[]) AS id, $2 AS x UNION ALL SELECT repo_id, ar FROM aveloxis_data.repos) v`,
		`FROM (WITH w AS (SELECT 1) SELECT unnest($1::bigint[]) AS id, $2 AS x) v`,
		`FROM (VALUES ($1::bigint, 'k')) AS v(id, x)`,
		`FROM generate_series($1::int, $2::int) AS v(id), (SELECT 1) q(x)`,
		`FROM myschema.unnest($1::bigint[], $2::text[]) AS v(id, x)`,
		`FROM unnest($1::bigint[], $2::text[]) WITH ORDINALITY AS v(id, x, n), aveloxis_data.other v`, // two things of one name
		`FROM aveloxis_data.other v, unnest($1::bigint[], $2::text[]) AS v(id, x)`,                    // the same, the other way round
		`FROM (SELECT t.id, o.x FROM unnest($1::bigint[]) AS t(id), aveloxis_data.other o) v`,
	}
	shapes := map[string]string{
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(v.x, r.pnn) `:               tail,
		`UPDATE aveloxis_data.repos r SET pn = COALESCE(NULLIF(v.x, ''), r.pn) `:     tail,
		`UPDATE aveloxis_data.repos r SET g = GREATEST(r.g, v.x::int) `:              tail,
		`UPDATE aveloxis_data.repos r SET fe = v.x `:                                 tail + ` AND r.fe IS NULL`,
		`UPDATE aveloxis_data.repos r SET k = CASE WHEN TRUE THEN v.x ELSE r.k END `: tail,
	}
	for head, where := range shapes {
		for _, from := range accept {
			if got := r7verdict(t, head+from+where); got != "pass" {
				t.Errorf("an incoming item is refused (%s):\n  %s", got, head+from+where)
			}
		}
		for _, from := range refuse {
			if got := r7verdict(t, head+from+where); got == "pass" {
				t.Errorf("an item that is not incoming is taken for one:\n  %s", head+from+where)
			}
		}
	}
	// The column must be qualified by the item: a bare name could be the
	// target's. And the target's own alias, a variable, EXCLUDED are not it.
	r7table(t, map[string]string{
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(x, r.pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, x) WHERE r.repo_id = v.id`: KindAssign,
		`UPDATE aveloxis_data.repos v SET pnn = COALESCE(v.x, v.pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, x)`:                      KindAssign,
		`UPDATE aveloxis_data.repos v SET pnn = COALESCE(v.x, pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, x)`:                        KindAssign,
		// The item's whole row, and a longer name that begins with the
		// item's, are not a column of it.
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(v::text, r.pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, x) WHERE r.repo_id = v.id`: KindAssign,
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(v.y.x, r.pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, x) WHERE r.repo_id = v.id`:   KindAssign,
		// The guard is still required of a bare fill.
		`UPDATE aveloxis_data.repos r SET fe = v.x FROM unnest($1::bigint[], $2::text[]) AS v(id, x) WHERE r.repo_id = v.id`: KindAssign,
		// An incoming value is not the stored one.
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE($1, v.pnn) FROM unnest($1::bigint[], $2::text[]) AS v(id, pnn) WHERE r.repo_id = v.id`: KindAssign,
		// AlwaysRefresh from it, of course.
		`UPDATE aveloxis_data.repos r SET ar = v.x FROM unnest($1::bigint[], $2::text[]) AS v(id, x) WHERE r.repo_id = v.id`: "pass",
	})
}

// The other forms a correct statement was refused in (round 7), each with
// its nearest neighbour that is NOT the shape.
func TestTheFormsRound7Accepts(t *testing.T) {
	const up = `UPDATE aveloxis_data.repos SET `
	r7table(t, map[string]string{
		// F29: plain GREATEST / LEAST of the two (GREATEST ignores a NULL
		// argument: PostgreSQL 18, GREATEST(NULL, 5) = 5).
		`INSERT INTO aveloxis_data.repos (repo_id, g) VALUES ($1, $2) ON CONFLICT (repo_id) DO UPDATE SET g = GREATEST(repos.g, EXCLUDED.g)`: "pass",
		up + `g = GREATEST($1, g)`:     "pass",
		up + `g = GREATEST(g, g)`:      KindAssign,
		up + `g = GREATEST($1, $2)`:    KindAssign,
		up + `g = GREATEST(g, $1, 0)`:  KindAssign,
		up + `g = LEAST(g, $1)`:        KindAssign,
		up + `g = GREATEST(g + 1, $1)`: KindAssign,
		up + `e = LEAST($1, e)`:        "pass",
		up + `e = GREATEST($1, e)`:     KindAssign,
		// F22: TRIM of an incoming value is incoming; of the stored value
		// it is a new value written back.
		up + `pn = COALESCE(NULLIF(TRIM($1), ''), pn)`:           "pass",
		up + `pn = COALESCE(NULLIF(btrim($1::text), ''), pn)`:    "pass",
		up + `pn = COALESCE(NULLIF(TRIM($1), ''), TRIM(pn))`:     KindAssign,
		up + `pn = COALESCE(NULLIF(TRIM(pn), ''), $1)`:           KindAssign,
		up + `pn = COALESCE(NULLIF(TRIM(TRIM($1)), ''), pn)`:     KindAssign,
		up + `pn = COALESCE(NULLIF(lower($1), ''), pn)`:          KindAssign,
		up + `pn = COALESCE(NULLIF(TRIM($1, 'x'), ''), pn)`:      KindAssign,
		up + `pn = COALESCE(NULLIF(myschema.btrim($1), ''), pn)`: KindAssign,
		up + `fe = COALESCE(NULLIF(TRIM(fe), ''), $1)`:           KindAssign,
		// F43, F44: the transaction's time, in its three spellings, and
		// COALESCE(incoming, that).
		up + `k = CASE WHEN $1 THEN k ELSE COALESCE($2::timestamptz, NOW()) END`: "pass",
		up + `k = CASE WHEN $1 THEN k ELSE CURRENT_TIMESTAMP END`:                "pass",
		up + `k = CASE WHEN $1 THEN k ELSE transaction_timestamp() END`:          "pass",
		up + `k = CASE WHEN $1 THEN k ELSE clock_timestamp() END`:                KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE statement_timestamp() END`:            KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE CURRENT_TIMESTAMP(0) END`:             KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE CURRENT_DATE END`:                     KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE LOCALTIMESTAMP END`:                   KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE pg_catalog.now() END`:                 KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE COALESCE(NOW(), $2) END`:              KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE COALESCE(k, NOW()) END`:               KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE COALESCE($2, NOW(), k) END`:           KindAssign,
		up + `k = CASE WHEN $1 THEN k ELSE COALESCE($2, clock_timestamp()) END`:  KindAssign,
		// F08: an equality either way round; F09: length is not the test.
		up + `fe = $2 WHERE repo_id = $1 AND '' = COALESCE(fe, '')`:          "pass",
		up + `fe = $2 WHERE repo_id = $1 AND (fe IS NULL OR '' = fe)`:        "pass",
		up + `fe = $2 WHERE repo_id = $1 AND '' = COALESCE(fe, 'x')`:         KindAssign,
		up + `fe = $2 WHERE repo_id = $1 AND 'x' = COALESCE(fe, '')`:         KindAssign,
		up + `fe = $2 WHERE repo_id = $1 AND '' <> COALESCE(fe, '')`:         KindAssign,
		up + `fe = $2 WHERE repo_id = $1 AND (fe IS NULL OR length(fe) = 0)`: KindAssign,
		up + `fe = $2 WHERE repo_id = $1 AND (fe IS NULL OR '' = $3)`:        KindAssign,
		// F18: the fill as a CASE, that tree exactly.
		up + `fe = CASE WHEN fe IS NULL THEN $1 ELSE fe END`:                   "pass",
		up + `fe = CASE WHEN COALESCE(fe, '') = '' THEN $1 ELSE fe END`:        "pass",
		up + `fe = CASE WHEN (fe IS NULL OR fe = '') THEN $1 ELSE fe END`:      "pass",
		up + `fe = CASE WHEN fe IS NOT NULL THEN $1 ELSE fe END`:               KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN fe ELSE $1 END`:                   KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN $1 END`:                           KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN $1 ELSE NULL END`:                 KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN $1 WHEN TRUE THEN $2 ELSE fe END`: KindAssign,
		up + `fe = CASE WHEN fe IS NULL OR TRUE THEN $1 ELSE fe END`:           KindAssign,
		up + `fe = CASE WHEN io IS NULL THEN $1 ELSE fe END`:                   KindAssign,
		up + `fe = CASE fe WHEN NULL THEN $1 ELSE fe END`:                      KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN $1 ELSE upper(fe) END`:            KindAssign,
		up + `fe = CASE WHEN fe IS NULL THEN fe || $1 ELSE fe END`:             KindAssign,
		// F04 stays refused: a cast of the whole value (the judge's catalog
		// holds no column types). The way out is a cast of the operand.
		up + `fe = COALESCE(NULLIF(fe, ''), $1)::text`: KindAssign,
		up + `fe = COALESCE(NULLIF(fe, ''), $1::text)`: "pass",
	})
}

// Q7-3: a foreign key is read wherever a column or a table can declare
// one. (LIKE ... INCLUDING ALL copies no foreign key: PostgreSQL never
// copies one.)
func TestAForeignKeysActionIsReadWhereverItIsDeclared(t *testing.T) {
	r7table(t, map[string]string{
		`ALTER TABLE aveloxis_data.repos ADD COLUMN IF NOT EXISTS fe text REFERENCES aveloxis_data.other (id) ON DELETE SET NULL`:           KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD COLUMN fe text CONSTRAINT c REFERENCES aveloxis_data.other (id) ON UPDATE CASCADE`:             KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD COLUMN fe text DEFAULT 'x' NOT NULL REFERENCES aveloxis_data.other (id) ON DELETE SET DEFAULT`: KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD CONSTRAINT c FOREIGN KEY (fe) REFERENCES aveloxis_data.other (id) ON DELETE SET NULL`:          KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD COLUMN x int, ADD COLUMN ar text REFERENCES aveloxis_data.other (id) ON UPDATE SET NULL`:       KindFKAction,
		// No action that writes: nothing to hold.
		`ALTER TABLE aveloxis_data.repos ADD COLUMN IF NOT EXISTS fe text REFERENCES aveloxis_data.other (id)`:                   "pass",
		`ALTER TABLE aveloxis_data.repos ADD COLUMN IF NOT EXISTS fe text REFERENCES aveloxis_data.other (id) ON DELETE CASCADE`: "pass",
		// A column that is not registered.
		`ALTER TABLE aveloxis_data.repos ADD COLUMN zz text REFERENCES aveloxis_data.other (id) ON DELETE SET NULL`: "pass",
		// The other places a column's constraints stand.
		`CREATE TABLE aveloxis_data.repos (fe text REFERENCES aveloxis_data.other (id) ON DELETE SET NULL)`:                                     KindFKAction,
		`CREATE TABLE aveloxis_data.repos OF aveloxis_data.rtype (fe WITH OPTIONS REFERENCES aveloxis_data.other (id) ON DELETE SET NULL)`:      KindFKAction,
		`CREATE TABLE aveloxis_data.repos PARTITION OF aveloxis_data.lonely (fe REFERENCES aveloxis_data.other (id) ON UPDATE CASCADE) DEFAULT`: KindFKAction,
	})
}

// Q7-4, Q7-7: statements that reach a registered table without naming it.
func TestStatementsThatReachATableWithoutNamingIt(t *testing.T) {
	r7table(t, map[string]string{
		// TRUNCATE ... CASCADE empties what references the table, and what
		// references that (the catalog read the foreign keys).
		`TRUNCATE aveloxis_data.repo_groups CASCADE`:                       KindTruncate,
		`TRUNCATE aveloxis_data.repo_groups`:                               "pass",
		`TRUNCATE aveloxis_data.repo_groups RESTRICT`:                      "pass",
		`TRUNCATE aveloxis_data.child CASCADE`:                             "pass", // nothing registered references it
		`TRUNCATE aveloxis_data.lonely CASCADE`:                            "pass",
		`TRUNCATE aveloxis_data.lonely, aveloxis_data.repo_groups CASCADE`: KindTruncate,
		`TRUNCATE aveloxis_data.repos CASCADE`:                             KindTruncate,
		// A foreign key added after the table was made, as a column's
		// own or as a constraint.
		`TRUNCATE aveloxis_data.late CASCADE`:  KindTruncate,
		`TRUNCATE aveloxis_data.late2 CASCADE`: KindTruncate,
		// Seven tables away: c1 <- c2 <- ... <- c6 <- repo_groups <- repos.
		`TRUNCATE aveloxis_data.c1 CASCADE`: KindTruncate,
		// The schema, renamed away or another renamed onto its name.
		`ALTER SCHEMA aveloxis_data RENAME TO aveloxis_old`: KindRenameSchema,
		`ALTER SCHEMA staging RENAME TO aveloxis_data`:      KindRenameSchema,
		`ALTER SCHEMA staging RENAME TO staging2`:           "pass",
		// What a role owns.
		`DROP OWNED BY aveloxis`:              KindOwned,
		`REASSIGN OWNED BY aveloxis TO other`: KindOwned,
		// A type dropped with every column of it.
		`DROP TYPE aveloxis_data.kind CASCADE`:  KindDropType,
		`DROP DOMAIN aveloxis_data.d CASCADE`:   KindDropType,
		`DROP TYPE aveloxis_data.kind`:          "pass",
		`DROP TYPE aveloxis_data.kind RESTRICT`: "pass",
	})
	// A table the catalog was only TOLD of (a catalog made from a live
	// database, which reads no DDL) has foreign keys nobody read: CASCADE
	// is then held against every registered table.
	told := NewCatalog().Table("aveloxis_data.repos", "aveloxis_data.repo_groups")
	st := []Stmt{{File: "f.go", SQL: `TRUNCATE aveloxis_data.repo_groups CASCADE`}}
	rep := CheckIn(told, st, round6Registry, nil)
	if len(rep.Violations) != 1 || rep.Violations[0].Table != AnyRegisteredTable || rep.Violations[0].Kind != KindTruncate {
		t.Errorf("with foreign keys unknown: %+v", rep.Violations)
	}
	// ... until they are declared.
	known := NewCatalog().Table("aveloxis_data.repos", "aveloxis_data.repo_groups", "aveloxis_data.lonely").
		Reference("aveloxis_data.repos", "aveloxis_data.repo_groups").Defined("aveloxis_data.repos", "aveloxis_data.repo_groups", "aveloxis_data.lonely")
	if rep := CheckIn(known, st, round6Registry, nil); len(rep.Violations) != 1 || rep.Violations[0].Table != "aveloxis_data.repos" {
		t.Errorf("with the foreign key declared: %+v", rep.Violations)
	}
	lonely := []Stmt{{File: "f.go", SQL: `TRUNCATE aveloxis_data.lonely CASCADE`}}
	if rep := CheckIn(known, lonely, round6Registry, nil); len(rep.Violations) != 0 {
		t.Errorf("a table nothing references: %+v", rep.Violations)
	}
	// An Exception names the table the violation names.
	ex := []Exception{{Table: AnyRegisteredTable, Kind: KindOwned, File: "f.go", Statement: `DROP OWNED BY aveloxis`, Reason: "r"}}
	owned := []Stmt{{File: "f.go", SQL: `DROP OWNED BY aveloxis`}}
	if rep := CheckIn(known, owned, round6Registry, ex); len(rep.Violations)+len(rep.StaleExceptions)+len(rep.BadExceptions) != 0 {
		t.Errorf("an excepted DROP OWNED: %+v", rep)
	}
}

// Q7-5: the two conditions round 7's mutants showed unpinned, and the two
// that only SQL PostgreSQL refuses can tell apart.
func TestTheConditionsRound7FoundUnpinned(t *testing.T) {
	r7table(t, map[string]string{
		// m36: a table that is NOT temporary, of an unqualified name, is
		// not this text's own session table.
		`CREATE TABLE IF NOT EXISTS repos (io int); UPDATE repos SET io = 2`: KindUnqualifiedTarget,
		`CREATE TEMP TABLE repos (io int); UPDATE repos SET io = 2`:          "pass",
		`CREATE UNLOGGED TABLE repos (io int); UPDATE repos SET io = 2`:      KindUnqualifiedTarget,
		// m53: a child of the registered table named without its schema.
		`CREATE TABLE aveloxis_data.repos_2026 PARTITION OF repos FOR VALUES FROM (1) TO (2)`: KindPartition,
		`CREATE TABLE aveloxis_data.repos_c () INHERITS (repos)`:                              KindPartition,
		`CREATE TABLE aveloxis_data.repos_c () INHERITS (aveloxis_data.repos)`:                KindPartition,
		`CREATE TABLE aveloxis_data.lonely_c () INHERITS (aveloxis_data.lonely)`:              "pass",
		`ALTER TABLE aveloxis_data.other INHERIT repos`:                                       KindPartition,
		// m20: a target aliased EXCLUDED. PostgreSQL refuses the statement
		// ("table reference "excluded" is ambiguous"); the judge must not
		// take the alias's column for the incoming value all the same.
		`INSERT INTO aveloxis_data.repos AS excluded (repo_id, pnn) VALUES ($1, $2) ON CONFLICT (repo_id) DO UPDATE SET ar = excluded.ar`: KindAssign,
		// m24: schema.table.col beside an alias. PostgreSQL refuses it
		// ("invalid reference to FROM-clause entry for table "repos"").
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(aveloxis_data.repos.fe, $1)`: KindAssign,
		`UPDATE aveloxis_data.repos SET fe = COALESCE(aveloxis_data.repos.fe, $1)`:   "pass",
	})
}

// Q7-8: PL/pgSQL scoping as PostgreSQL has it.
func TestBodiesScopeNamesAsPostgreSQLDoes(t *testing.T) {
	fn := func(directives, body string) string {
		return "CREATE FUNCTION aveloxis_data.f(fe text) RETURNS void LANGUAGE plpgsql AS $$\n" + directives + "BEGIN " + body + " END $$"
	}
	const guarded = `UPDATE aveloxis_data.repos SET fe = $1 WHERE fe IS NULL;`
	r7table(t, map[string]string{
		// The LAST #variable_conflict decides (PostgreSQL 18: with
		// use_variable last, the WHERE read the parameter and a stored
		// value was overwritten).
		fn("#variable_conflict use_column\n#variable_conflict use_variable\n", guarded): KindAssign,
		fn("#variable_conflict use_variable\n#variable_conflict use_column\n", guarded): "pass",
		fn("#variable_conflict use_column\n", guarded):                                  "pass",
		fn("#variable_conflict use_variable\n", guarded):                                KindAssign,
		fn("", guarded): KindAssign, // ambiguous: PostgreSQL stops it
		fn("#variable_conflict use_column\n#variable_conflict error\n", guarded): KindAssign,
		// Text run by EXECUTE has none of the body's variables: ar there is
		// the column (it was read as the variable, and so as not stored).
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN EXECUTE 'UPDATE aveloxis_data.repos SET ar = ar || ''x'''; END $$": KindAssign,
		// (In the body itself ar IS the variable: not the stored value,
		// and since round 8 not an incoming leaf either: what a variable
		// holds is not known.)
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN UPDATE aveloxis_data.repos SET ar = ar || 'x'; END $$":        KindAssign,
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN UPDATE aveloxis_data.repos SET fe = COALESCE(fe, $1); END $$": "pass",
		// The same told apart the other way, by a fill: in the body fe is
		// the variable, so the shape's "stored" operand is not the column;
		// in the EXECUTEd text it is the column, and the fill conforms.
		"DO $$\n#variable_conflict use_variable\nDECLARE fe text;\nBEGIN UPDATE aveloxis_data.repos SET fe = COALESCE(fe, 'x'); END $$":             KindAssign,
		"DO $$\n#variable_conflict use_variable\nDECLARE fe text;\nBEGIN EXECUTE 'UPDATE aveloxis_data.repos SET fe = COALESCE(fe, ''x'')'; END $$": "pass",
		// Nor has a function created inside a body, by a statement of it
		// or by its EXECUTE.
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN CREATE FUNCTION aveloxis_data.g() RETURNS void LANGUAGE sql AS 'UPDATE aveloxis_data.repos SET ar = ar || 1'; END $$":                KindAssign,
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN CREATE FUNCTION aveloxis_data.g() RETURNS void LANGUAGE plpgsql AS 'BEGIN UPDATE aveloxis_data.repos SET ar = ar || 1; END'; END $$": KindAssign,
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN CREATE FUNCTION aveloxis_data.g() RETURNS void LANGUAGE sql BEGIN ATOMIC UPDATE aveloxis_data.repos SET ar = ar || 1; END; END $$":   KindAssign,
		"DO $$\n#variable_conflict use_variable\nDECLARE ar text;\nBEGIN EXECUTE 'CREATE FUNCTION aveloxis_data.g() RETURNS void LANGUAGE sql AS ''UPDATE aveloxis_data.repos SET ar = ar || 1'''; END $$":    KindAssign,
	})
}

// Q7-9: a temporary table is this text's own only where the text made it
// EARLIER, at the same level, and has not dropped it.
func TestATemporaryTableIsTheTextsOwnOnlyWhereItStands(t *testing.T) {
	r7table(t, map[string]string{
		`CREATE TEMP TABLE repos (io int); UPDATE repos SET io = 2`:                            "pass",
		`CREATE TEMP TABLE repos (io int); UPDATE pg_temp.repos SET io = 2`:                    "pass",
		`CREATE TABLE pg_temp.repos (io int); UPDATE pg_temp.repos SET io = 2`:                 "pass",
		`CREATE TEMP TABLE repos AS SELECT 1 AS io; UPDATE repos SET io = 2; DROP TABLE repos`: "pass",
		`SELECT 1 AS io INTO TEMP repos; UPDATE repos SET io = 2`:                              "pass",
		"DO $$ BEGIN CREATE TEMP TABLE repos (io int); UPDATE repos SET io = 2; END $$":        "pass",
		// The SCHEMA's table of the same name is not the temporary one.
		`CREATE TEMP TABLE repos (io int); UPDATE aveloxis_data.repos SET io = 2`: KindAssign,
		`CREATE TEMP TABLE repos (io int); UPDATE public.repos SET io = 2`:        KindUnknownTarget,
		// Made LATER.
		`UPDATE repos SET io = 2; CREATE TEMP TABLE repos (io int)`: KindUnqualifiedTarget,
		// Dropped before the write.
		`CREATE TEMP TABLE repos (io int); DROP TABLE repos; UPDATE repos SET io = 2`:         KindUnqualifiedTarget,
		`CREATE TEMP TABLE repos (io int); DROP TABLE pg_temp.repos; UPDATE repos SET io = 2`: KindUnqualifiedTarget,
		// Made in a function's body that nothing here calls.
		"CREATE FUNCTION aveloxis_data.mk() RETURNS void LANGUAGE plpgsql AS $$ BEGIN CREATE TEMP TABLE repos (io int); END $$; UPDATE repos SET io = 2": KindUnqualifiedTarget,
		"CREATE FUNCTION aveloxis_data.mk() RETURNS void LANGUAGE sql AS 'CREATE TEMP TABLE repos (io int)'; UPDATE repos SET io = 2":                    KindUnqualifiedTarget,
		// Made at the top, written in a body (another level).
		"CREATE TEMP TABLE repos (io int); DO $$ BEGIN UPDATE repos SET io = 2; END $$": KindUnqualifiedTarget,
		// Made by one EXECUTE, written by another.
		"DO $$ BEGIN EXECUTE 'CREATE TEMP TABLE repos (io int)'; EXECUTE 'UPDATE repos SET io = 2'; END $$": KindUnqualifiedTarget,
		// A temporary VIEW is no table of its own.
		`CREATE TEMP VIEW repos AS SELECT * FROM aveloxis_data.repos; UPDATE repos SET io = 2`: KindUnqualifiedTarget,
		// pg_temp, of a table nothing here made.
		`UPDATE pg_temp.work SET io = 2`: KindUnknownTarget,
	})
	// Made by one text and written by another: not this text's own, and
	// not a table of the catalog either (a pg_temp name is no schema's).
	two := []Stmt{{File: "f.go", SQL: `CREATE TABLE pg_temp.repos (io int)`}, {File: "f.go", SQL: `UPDATE pg_temp.repos SET io = 2`}}
	if rep := CheckIn(r7catalog(t, two), two, round6Registry, nil); len(rep.Violations) != 1 || rep.Violations[0].Kind != KindUnknownTarget {
		t.Errorf("a pg_temp table of another text: %+v", rep.Violations)
	}
	// A session table that ANOTHER text made: declared in the catalog, and
	// written with pg_temp (an unqualified name is the search path's).
	cat := NewCatalog().Table("aveloxis_data.repos").Temp("work")
	for sql, want := range map[string]int{
		`UPDATE pg_temp.work SET io = 2`:      0,
		`INSERT INTO pg_temp.work VALUES (1)`: 0,
		`UPDATE work SET io = 2`:              1,
		`UPDATE pg_temp.other SET io = 2`:     1,
	} {
		if rep := CheckIn(cat, []Stmt{{File: "f.go", SQL: sql}}, round6Registry, nil); len(rep.Violations) != want {
			t.Errorf("%d violations, want %d: %s\n  %+v", len(rep.Violations), want, sql, rep.Violations)
		}
	}
}

// Q7-10: what the catalog knows of a materialized view, and that a WITH
// query's name is no relation.
func TestTheCatalogsSourcesAreRelations(t *testing.T) {
	c := NewCatalog()
	if err := c.Add(r7schema); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"aveloxis_data.m_repos":  "aveloxis_data.repos",
		"aveloxis_data.v_repos":  "aveloxis_data.repos",
		"aveloxis_data.vv_repos": "aveloxis_data.repos aveloxis_data.v_repos",
		"aveloxis_data.v_other":  "aveloxis_data.other", // not the WITH query's name, w
	} {
		if got := strings.Join(c.behind(name), " "); got != want {
			t.Errorf("behind %s: %q, want %q", name, got, want)
		}
	}
	if got := c.Kind("aveloxis_data.m_repos"); got != "matview" {
		t.Errorf("m_repos is a %q", got)
	}
	if got := strings.Join(c.dependents("aveloxis_data.repo_groups"), " "); got != "aveloxis_data.child aveloxis_data.repos" {
		t.Errorf("what references repo_groups, and that: %q", got)
	}
	// Every remove of a long chain, whatever order the tables are met in.
	for i := 0; i < 20; i++ {
		if got := strings.Join(c.dependents("aveloxis_data.c1"), " "); got != "aveloxis_data.c2 aveloxis_data.c3 aveloxis_data.c4 aveloxis_data.c5 aveloxis_data.c6 aveloxis_data.child aveloxis_data.repo_groups aveloxis_data.repos" {
			t.Fatalf("what references c1, through seven tables: %q", got)
		}
	}
	// A write through the materialized view over the table is view-target.
	if got := r7verdict(t, `UPDATE aveloxis_data.m_repos SET ar = 'x'`); got != KindViewTarget {
		t.Errorf("a write of the materialized view: %s", got)
	}
}
