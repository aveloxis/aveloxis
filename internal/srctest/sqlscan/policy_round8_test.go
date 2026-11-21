// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// Round 8 (0.29.215). Rounds 6 to 8 each found a route by which a stored
// value reached an AlwaysRefresh value, because the rule listed routes. It
// now says what a value may be MADE OF (policy.go: "Incoming: the closed
// list"), and these are its tests. The fixture schema and columns are
// round 7's (r7schema; ar is AlwaysRefresh).

const r8up = `UPDATE aveloxis_data.repos r SET ar = `

// What an AlwaysRefresh value may be made of.
func TestAnAlwaysRefreshValueIsMadeOfIncomingLeaves(t *testing.T) {
	r7table(t, map[string]string{
		// A placeholder, a constant, a function over them, DEFAULT.
		r8up + `$1`:                                 "pass",
		r8up + `COALESCE($1::text, 'x')`:            "pass",
		r8up + `lower($1) || NOW()::text`:           "pass",
		r8up + `DEFAULT`:                            "pass",
		r8up + `CASE WHEN $2 THEN $1 ELSE NULL END`: "pass",
		// A function of a schema: what it reads is not known.
		r8up + `aveloxis_data.f($1)`:  KindAssign,
		r8up + `pg_catalog.lower($1)`: "pass",
		// A column of a clean item: another base table, a view over one,
		// an incoming batch, a WITH query or a sub-select over those.
		r8up + `o.ar FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id`:                                          "pass",
		r8up + `v.ar FROM aveloxis_data.v_other v WHERE v.repo_id = r.repo_id`:                                        "pass",
		r8up + `u.v FROM unnest($1::text[], $2::int[]) AS u(v, id) WHERE u.id = r.repo_id`:                            "pass",
		r8up + `s.v FROM (SELECT o.repo_id, max(o.ar) AS v FROM aveloxis_data.other o GROUP BY o.repo_id) s`:          "pass",
		r8up + `(SELECT max(o.ar) FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id)`:                            "pass",
		`WITH n AS (SELECT repo_id, ar FROM aveloxis_data.other) ` + r8up + `n.ar FROM n WHERE n.repo_id = r.repo_id`: "pass",
		`WITH n(a, b) AS (SELECT repo_id, ar FROM aveloxis_data.other) ` + r8up + `n.b FROM n WHERE n.a = r.repo_id`:  "pass",
		// A relation the catalog does not know, one with no schema, a
		// function in FROM that is no batch of placeholders.
		r8up + `z.ar FROM aveloxis_data.nowhere z`:                                  KindAssign,
		r8up + `m.ar FROM v_other m WHERE m.repo_id = r.repo_id`:                    KindAssign, // S8-9
		r8up + `f.ar FROM aveloxis_data.read_repos() f WHERE f.repo_id = r.repo_id`: KindAssign, // S8-9
		r8up + `j.ar FROM jsonb_to_recordset($1::jsonb) AS j(ar text)`:              KindAssign,
		r8up + `g::text FROM generate_series(1, 2) g`:                               KindAssign,
		// S8-1: a LATERAL item that carries a sibling's column. The
		// sibling is the target again, so no column of the FROM is taken.
		r8up + `k.prev FROM aveloxis_data.repos l CROSS JOIN LATERAL (SELECT l.ar AS prev) k WHERE l.repo_id = $2`:                                                                        KindAssign,
		r8up + `s.v FROM aveloxis_data.repos r2, LATERAL (SELECT r2.ar AS v) s WHERE r2.repo_id = r.repo_id`:                                                                              KindAssign,
		r8up + `u.v FROM aveloxis_data.repos r2, unnest(ARRAY[r2.ar]) AS u(v) WHERE r2.repo_id = r.repo_id`:                                                                               KindAssign,
		r8up + `j.ar FROM aveloxis_data.repos r2, jsonb_to_record(to_jsonb(r2)) AS j(ar text) WHERE r2.repo_id = r.repo_id`:                                                               KindAssign,
		r8up + `s.v FROM (SELECT x.repo_id, l.v FROM aveloxis_data.repos x, LATERAL (SELECT x.ar AS v) l) s WHERE s.repo_id = r.repo_id`:                                                  KindAssign,
		`MERGE INTO aveloxis_data.repos r USING (aveloxis_data.repos l CROSS JOIN LATERAL (SELECT l.ar AS prev) k) ON l.repo_id = r.repo_id + 1 WHEN MATCHED THEN UPDATE SET ar = k.prev`: KindAssign,
		// The same LATERAL over a clean sibling is a clean item.
		r8up + `k.v FROM aveloxis_data.other o CROSS JOIN LATERAL (SELECT o.ar AS v) k WHERE o.repo_id = r.repo_id`: "pass",
		// An item of the FROM that is not clean spoils every column of
		// the list, the clean items' too: a bare name or a LATERAL item
		// could carry its value.
		r8up + `o.ar FROM aveloxis_data.other o, aveloxis_data.repos r2 WHERE o.repo_id = r.repo_id`: KindAssign,
		// S8-2: a WITH query's column list cannot rename a stored value
		// into an incoming one: a WITH that reads the target is not clean.
		`WITH old(a, b) AS (SELECT repo_id, ar FROM aveloxis_data.repos) ` + r8up + `old.b FROM old WHERE old.a = r.repo_id`:               KindAssign,
		`WITH old(repo_id, fe) AS (SELECT repo_id, ar FROM aveloxis_data.repos) ` + r8up + `old.fe FROM old WHERE old.repo_id = r.repo_id`: KindAssign,
		`WITH a AS (SELECT * FROM aveloxis_data.repos), b(x, y) AS (SELECT repo_id, fe FROM a) ` + r8up + `b.y FROM b`:                     KindAssign,
		`WITH gone AS (DELETE FROM aveloxis_data.other RETURNING *) ` + r8up + `gone.ar FROM gone`:                                         KindAssign,
		// S8-4: a bare name in a sub-select of the value, when an item of
		// the FROM is not clean; and the column's own name, bare, always.
		r8up + `(SELECT keep) FROM (SELECT repo_id AS rid, ar AS keep FROM aveloxis_data.repos) s WHERE s.rid = r.repo_id`: KindAssign,
		r8up + `(SELECT fe) FROM aveloxis_data.v_repos v WHERE v.repo_id = r.repo_id`:                                      KindAssign,
		r8up + `(SELECT ar FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id)`:                                        KindAssign,
		r8up + `(SELECT o.ar FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id)`:                                      "pass",
		// Another column of the row being written is the one leaf that is
		// not incoming and passes; the column itself and the whole row do
		// not.
		r8up + `lower(fe)`:         "pass",
		r8up + `r.fe`:              "pass",
		r8up + `(SELECT fe)`:       "pass",
		r8up + `r.ar`:              KindAssign,
		r8up + `ar`:                KindAssign,
		r8up + `to_jsonb(r)::text`: KindAssign,
		r8up + `(r).fe`:            KindAssign,
		// A variable of a body, under any name: what it holds is not known
		// (a loop's record can hold the stored value).
		"DO $$ DECLARE v text; BEGIN UPDATE aveloxis_data.repos SET ar = v; END $$":                                                                KindAssign,
		"DO $$ DECLARE v text; BEGIN UPDATE aveloxis_data.repos SET ar = lower(v) || $1; END $$":                                                   KindAssign,
		"DO $$ DECLARE q record; BEGIN FOR q IN SELECT * FROM aveloxis_data.repos LOOP UPDATE aveloxis_data.repos SET ar = q.ar; END LOOP; END $$": KindAssign,
		// A condition picks rows; it is not the value, and may read
		// anything (as the write's own WHERE may).
		r8up + `o.ar FROM aveloxis_data.other o WHERE o.x = r.ar`: "pass",
		r8up + `s.v FROM (SELECT o.repo_id, o.ar AS v FROM aveloxis_data.other o WHERE EXISTS (SELECT 1 FROM aveloxis_data.repos x WHERE x.repo_id = o.repo_id)) s WHERE s.repo_id = r.repo_id`: "pass",
		// But a relation a JOIN names is an item, whatever its ON says.
		r8up + `s.v FROM (SELECT o.repo_id, o.ar AS v FROM aveloxis_data.other o JOIN aveloxis_data.repos x ON x.repo_id = o.repo_id) s WHERE s.repo_id = r.repo_id`: KindAssign,
		// A table the text filled from the target holds its stored values.
		`CREATE TABLE aveloxis_data.scratch AS SELECT repo_id, ar FROM aveloxis_data.repos; ` + r8up + `s.ar FROM aveloxis_data.scratch s WHERE s.repo_id = r.repo_id`: KindAssign,
		`CREATE TABLE aveloxis_data.scratch AS SELECT repo_id, ar FROM aveloxis_data.other; ` + r8up + `s.ar FROM aveloxis_data.scratch s WHERE s.repo_id = r.repo_id`: "pass",
		// A WITH query that names itself is judged once.
		`WITH RECURSIVE t(n, v) AS (SELECT o.repo_id, o.ar FROM aveloxis_data.other o UNION ALL SELECT t.n + 1, t.v FROM t WHERE t.n < 3) ` + r8up + `t.v FROM t WHERE t.n = r.repo_id`:   "pass",
		`WITH RECURSIVE t(n, v) AS (SELECT o.repo_id, o.ar FROM aveloxis_data.repos o UNION ALL SELECT t.n + 1, t.v FROM t WHERE t.n < 3) ` + r8up + `t.v FROM t WHERE t.n = r.repo_id`:   KindAssign,
		`WITH RECURSIVE t(n, v) AS (SELECT 1, $1::text UNION ALL SELECT t.n + 1, x.ar FROM t JOIN aveloxis_data.repos x ON x.repo_id = t.n) ` + r8up + `t.v FROM t WHERE t.n = r.repo_id`: KindAssign,
		// Each arm of a set operation is the query.
		r8up + `u.x FROM (SELECT $1 AS x UNION ALL SELECT o.ar FROM aveloxis_data.other o) u`: "pass",
		r8up + `u.x FROM (SELECT $1 AS x UNION ALL SELECT o.ar FROM aveloxis_data.repos o) u`: KindAssign,
		r8up + `u.x FROM (SELECT o.ar AS x FROM aveloxis_data.repos o UNION ALL SELECT $1) u`: KindAssign,
		// A VALUES list is walked as outputs are.
		r8up + `u.x FROM (VALUES ($1::text), ((SELECT max(o.ar) FROM aveloxis_data.repos o))) AS u(x)`: KindAssign,
		// A view the catalog knows nothing behind is not known to be clean.
		`CREATE VIEW aveloxis_data.v_one AS SELECT 1 AS x; ` + r8up + `v.x::text FROM aveloxis_data.v_one v`: KindAssign,
		// Two items of one name: PostgreSQL refuses it; it names neither.
		r8up + `v.ar FROM aveloxis_data.other v, aveloxis_data.other v`: KindAssign,
		// A session table: pg_temp.<name> only when declared (below).
		r8up + `s.ar FROM pg_temp.stage s WHERE s.repo_id = r.repo_id`: KindAssign,
		// A session table named with no schema: what it holds is not
		// followed (S8-8).
		`CREATE TEMP TABLE tk AS SELECT repo_id, ar FROM aveloxis_data.repos; ` + r8up + `tk.ar FROM tk WHERE tk.repo_id = r.repo_id`: KindAssign,
	})
}

// A declared session table (Catalog.Temp; writejudge.TempTables) is a
// clean item as pg_temp.<name>, and only so.
func TestADeclaredSessionTableIsACleanItem(t *testing.T) {
	const sql = r8up + `s.ar FROM pg_temp.stage s WHERE s.repo_id = r.repo_id`
	stmts := []Stmt{{File: "f.go", SQL: sql}}
	if rep := CheckIn(r7catalog(t, stmts).Temp("stage"), stmts, round6Registry, nil); len(rep.Violations)+len(rep.Unparsed) != 0 {
		t.Errorf("a declared session table read as pg_temp.stage: %+v", rep.Violations)
	}
	if rep := CheckIn(r7catalog(t, stmts).Temp("other"), stmts, round6Registry, nil); len(rep.Violations) != 1 {
		t.Errorf("a session table that is not the declared one: %+v", rep.Violations)
	}
}

// S8-3: EXCLUDED holds what the INSERT's own source reads, and is incoming
// only when that source is: for every policy.
func TestExcludedIsIncomingOnlyWhenTheInsertsSourceIs(t *testing.T) {
	const tail = ` ON CONFLICT (repo_id) DO UPDATE SET `
	r7table(t, map[string]string{
		`INSERT INTO aveloxis_data.repos (repo_id, ar) VALUES ($1, $2)` + tail + `ar = EXCLUDED.ar`:                                                 "pass",
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT u.id, u.v FROM unnest($1::int[], $2::text[]) AS u(id, v)` + tail + `ar = EXCLUDED.ar`: "pass",
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT o.repo_id, o.ar FROM aveloxis_data.other o` + tail + `ar = EXCLUDED.ar`:               "pass",
		`INSERT INTO aveloxis_data.repos DEFAULT VALUES` + tail + `ar = EXCLUDED.ar`:                                                                "pass",
		// The source reads the target: one row's stored value over another's.
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT $2, l.ar FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `ar = EXCLUDED.ar`:                                 KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) VALUES ($1, (SELECT ar FROM aveloxis_data.repos WHERE repo_id = $2))` + tail + `ar = EXCLUDED.ar`:                            KindAssign,
		`WITH old AS (SELECT repo_id, ar AS keep FROM aveloxis_data.repos) INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT repo_id, keep FROM old` + tail + `ar = EXCLUDED.ar`: KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT v.repo_id, v.ar FROM aveloxis_data.v_repos v` + tail + `ar = EXCLUDED.ar`:                                             KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, ar) SELECT z.repo_id, z.ar FROM aveloxis_data.nowhere z` + tail + `ar = EXCLUDED.ar`:                                             KindAssign,
		// The other policies' "incoming" is the same EXCLUDED.
		`INSERT INTO aveloxis_data.repos (repo_id, pnn) VALUES ($1, $2)` + tail + `pnn = COALESCE(EXCLUDED.pnn, repos.pnn)`:                                                  "pass",
		`INSERT INTO aveloxis_data.repos (repo_id, pnn) SELECT $2, l.pnn FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `pnn = COALESCE(EXCLUDED.pnn, repos.pnn)`: KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, fe) VALUES ($1, $2)` + tail + `fe = COALESCE(repos.fe, EXCLUDED.fe)`:                                                      "pass",
		`INSERT INTO aveloxis_data.repos (repo_id, fe) SELECT $2, l.fe FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `fe = COALESCE(repos.fe, EXCLUDED.fe)`:      KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, g) SELECT $2, l.g FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `g = GREATEST(repos.g, EXCLUDED.g)`:           KindAssign,
		`INSERT INTO aveloxis_data.repos (repo_id, e) SELECT $2, l.e FROM aveloxis_data.repos l WHERE l.repo_id = $1` + tail + `e = LEAST(repos.e, EXCLUDED.e)`:              KindAssign,
	})
}

// What a fill fills WITH is incoming by the same list (it could come from
// anywhere that was not the row's own stored value).
func TestAFillFillsWithAnIncomingValue(t *testing.T) {
	r7table(t, map[string]string{
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, $1)`:                                                          "pass",
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, o.x) FROM aveloxis_data.other o WHERE o.repo_id = r.repo_id`:  "pass",
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, r2.fe) FROM aveloxis_data.repos r2 WHERE r2.repo_id = $1`:     KindAssign,
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, v.fe) FROM aveloxis_data.v_repos v WHERE v.repo_id = $1`:      KindAssign,
		`UPDATE aveloxis_data.repos r SET fe = COALESCE(NULLIF(r.fe, ''), z.fe) FROM aveloxis_data.nowhere z`:               KindAssign,
		`UPDATE aveloxis_data.repos r SET fe = CASE WHEN r.fe IS NULL THEN r2.fe ELSE r.fe END FROM aveloxis_data.repos r2`: KindAssign,
	})
}

// S8-12: the edges of an incoming batch. (a) WITH ORDINALITY's counter is
// the position of a value handed in: it reads no stored value and is taken
// as one. (b) A LATERAL sub-select must qualify what it hands on: a bare
// name in it can be a column of the item to its left.
func TestTheEdgesOfAnIncomingBatch(t *testing.T) {
	r7table(t, map[string]string{
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(v.ord::text, r.pnn) FROM unnest($1::int[]) WITH ORDINALITY AS v(id, ord) WHERE v.id = r.repo_id`:                                    "pass",
		`UPDATE aveloxis_data.repos r SET pn = COALESCE(NULLIF(v.c, ''), r.pn) FROM aveloxis_data.other o, LATERAL (SELECT x AS c FROM unnest($1::int[]) AS t(id)) v WHERE o.id = r.repo_id`: KindAssign,
		`UPDATE aveloxis_data.repos r SET pnn = COALESCE(v.c, r.pnn) FROM aveloxis_data.repos r2, LATERAL (SELECT pnn AS c FROM unnest($1::int[]) AS t(id)) v WHERE r2.repo_id = $2`:         KindAssign,
		`UPDATE aveloxis_data.repos r SET pn = COALESCE(NULLIF(v.c, ''), r.pn) FROM LATERAL (SELECT t.c FROM unnest($1::text[]) AS t(c)) v`:                                                  "pass",
		`UPDATE aveloxis_data.repos r SET pn = COALESCE(NULLIF(v.c, ''), r.pn) FROM (SELECT c FROM unnest($1::text[]) AS t(c)) v`:                                                            "pass",
	})
}

// S8-5: the conditions round 8 found that no test held.
func TestTheConditionsRound8FoundUnpinned(t *testing.T) {
	r7table(t, map[string]string{
		// m34b: the fill spelled as CASE is a SEARCHED case. With an
		// operand, CASE false WHEN fe IS NULL ... takes the THEN arm when
		// the column is NOT empty, and overwrites every established value.
		`UPDATE aveloxis_data.repos SET fe = CASE WHEN fe IS NULL THEN $1 ELSE fe END`:       "pass",
		`UPDATE aveloxis_data.repos SET fe = CASE false WHEN fe IS NULL THEN $1 ELSE fe END`: KindAssign,
		`UPDATE aveloxis_data.repos SET fe = CASE true WHEN fe IS NULL THEN $1 ELSE fe END`:  KindAssign,
		// m15 and m21 were conditions of the enumeration that is gone;
		// what they held is held by the list.
		`WITH repos AS (SELECT 1 AS x) ` + r8up + `r2.ar FROM aveloxis_data.repos r2 WHERE r2.repo_id = r.repo_id`:                                                                 KindAssign,
		r8up + `(s.v).ar FROM (SELECT x.repo_id, (SELECT t FROM aveloxis_data.repos t WHERE t.repo_id = x.repo_id) AS v FROM aveloxis_data.repos x) s WHERE s.repo_id = r.repo_id`: KindAssign,
		r8up + `(s.v).ar FROM (SELECT x.repo_id, (SELECT t FROM aveloxis_data.repos t WHERE t.repo_id = x.repo_id) AS v FROM aveloxis_data.other x) s WHERE s.repo_id = r.repo_id`: KindAssign,
	})
	// m53: TRUNCATE x CASCADE names the tables it empties only when the
	// catalog READ every registered table's foreign keys. Here the
	// registered table is only declared: it may reference x for all the
	// catalog knows.
	cat := NewCatalog().Table("aveloxis_data.repos")
	if err := cat.Add("CREATE TABLE aveloxis_data.lone (id int)"); err != nil {
		t.Fatal(err)
	}
	rep := CheckIn(cat, []Stmt{{File: "f.go", SQL: "TRUNCATE aveloxis_data.lone CASCADE"}}, round6Registry, nil)
	if len(rep.Violations) != 1 || rep.Violations[0].Kind != KindTruncate || rep.Violations[0].Table != AnyRegisteredTable {
		t.Errorf("TRUNCATE CASCADE of a defined table, the registered one only declared: %+v", rep.Violations)
	}
	rep = CheckIn(cat, []Stmt{{File: "f.go", SQL: "TRUNCATE aveloxis_data.lone"}}, round6Registry, nil)
	if len(rep.Violations) != 0 {
		t.Errorf("TRUNCATE without CASCADE of another table: %+v", rep.Violations)
	}
}

// r8family is a schema in which the registered table has a parent, a
// partition and a child.
const r8family = `
CREATE TABLE aveloxis_data.anc (id int);
CREATE TABLE aveloxis_data.repos (repo_id int, fe text, ar text) INHERITS (aveloxis_data.anc) PARTITION BY LIST (repo_id);
CREATE TABLE aveloxis_data.repos_part PARTITION OF aveloxis_data.repos DEFAULT;
CREATE TABLE aveloxis_data.kidc () INHERITS (aveloxis_data.repos);
CREATE TABLE aveloxis_data.lone (id int);
CREATE TABLE aveloxis_data.other (id int, ar text);
`

func r8familyVerdict(t *testing.T, sql string) string {
	t.Helper()
	cat := NewCatalog()
	if err := cat.Add(r8family); err != nil {
		t.Fatal(err)
	}
	rep := CheckIn(cat, []Stmt{{File: "f.go", SQL: sql}}, round6Registry, nil)
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
	return strings.Join(sortedSet(set), "+")
}

// S8-6: what is done to a partition, a child or a parent of a registered
// table is done to that table's rows. (Confirmed on PostgreSQL 18 by the
// reviewer: DROP of a parent CASCADE drops the child; TRUNCATE of a parent
// empties it; NO INHERIT takes the child's rows out of the parent.)
func TestWhatIsDoneToARegisteredTablesFamilyIsHeld(t *testing.T) {
	for sql, want := range map[string]string{
		`DROP TABLE aveloxis_data.repos_part`:                                                           KindChildTarget,
		`DROP TABLE aveloxis_data.anc CASCADE`:                                                          KindChildTarget,
		`TRUNCATE aveloxis_data.anc`:                                                                    KindChildTarget,
		`TRUNCATE aveloxis_data.repos_part`:                                                             KindChildTarget,
		`UPDATE aveloxis_data.anc SET id = 1`:                                                           KindChildTarget,
		`DELETE FROM aveloxis_data.anc`:                                                                 KindChildTarget,
		`UPDATE aveloxis_data.kidc SET ar = ar`:                                                         KindChildTarget,
		`ALTER TABLE aveloxis_data.kidc NO INHERIT aveloxis_data.repos`:                                 KindChildTarget + "+" + KindPartition,
		`ALTER TABLE aveloxis_data.repos NO INHERIT aveloxis_data.anc`:                                  KindPartition,
		`ALTER TABLE aveloxis_data.repos_part ALTER COLUMN ar TYPE varchar(3)`:                          KindChildTarget,
		`ALTER TABLE aveloxis_data.anc ALTER COLUMN id TYPE bigint`:                                     KindChildTarget,
		`ALTER TABLE aveloxis_data.repos_part RENAME TO gone`:                                           KindChildTarget,
		`CREATE TRIGGER tg BEFORE UPDATE ON aveloxis_data.repos_part FOR EACH ROW EXECUTE FUNCTION f()`: KindChildTarget,
		`CREATE RULE ru AS ON UPDATE TO aveloxis_data.kidc DO INSTEAD NOTHING`:                          KindChildTarget,
		`ALTER TABLE aveloxis_data.repos_part ADD CONSTRAINT fk FOREIGN KEY (ar) REFERENCES aveloxis_data.other (ar) ON DELETE SET NULL`: KindChildTarget,
		// A value read from one of them is a stored value.
		`UPDATE aveloxis_data.repos r SET ar = a.id::text FROM aveloxis_data.anc a`:  KindAssign,
		`UPDATE aveloxis_data.repos r SET ar = p.ar FROM aveloxis_data.repos_part p`: KindAssign,
		// A table of no relation to it is not held.
		`DROP TABLE aveloxis_data.lone`:                                                                "pass",
		`TRUNCATE aveloxis_data.lone`:                                                                  "pass",
		`ALTER TABLE aveloxis_data.lone ALTER COLUMN id TYPE bigint`:                                   "pass",
		`UPDATE aveloxis_data.repos r SET ar = o.ar FROM aveloxis_data.other o WHERE o.id = r.repo_id`: "pass",
	} {
		if got := r8familyVerdict(t, sql); got != want {
			t.Errorf("%s, want %s:\n  %s", got, want, sql)
		}
	}
}

// S8-7: DROP ... CASCADE of anything a column can depend on drops columns
// (or their defaults) in tables the statement does not name.
func TestDropCascadeOfWhatAColumnCanDependOn(t *testing.T) {
	r7table(t, map[string]string{
		`DROP FUNCTION aveloxis_data.f() CASCADE`:          KindDropType,
		`DROP PROCEDURE aveloxis_data.p() CASCADE`:         KindDropType,
		`DROP ROUTINE aveloxis_data.f() CASCADE`:           KindDropType,
		`DROP AGGREGATE aveloxis_data.a(int) CASCADE`:      KindDropType,
		`DROP EXTENSION citext CASCADE`:                    KindDropType,
		`DROP COLLATION aveloxis_data.c CASCADE`:           KindDropType,
		`DROP OPERATOR aveloxis_data.+ (int, int) CASCADE`: KindDropType,
		`DROP CAST (int AS text) CASCADE`:                  KindDropType,
		`DROP SEQUENCE aveloxis_data.s CASCADE`:            KindDropType,
		`DROP TYPE aveloxis_data.t CASCADE`:                KindDropType,
		`DROP DOMAIN aveloxis_data.d CASCADE`:              KindDropType,
		// Without CASCADE PostgreSQL refuses when a column depends on it.
		`DROP FUNCTION aveloxis_data.f()`:    "pass",
		`DROP EXTENSION citext`:              "pass",
		`DROP SEQUENCE aveloxis_data.s`:      "pass",
		`DROP INDEX aveloxis_data.i CASCADE`: "pass",
	})
}

// S8-8: a session table is the text's own only while it is a PLAIN table
// that still stands.
func TestASessionTableIsPassedOverOnlyWhileItIsPlainAndStands(t *testing.T) {
	r7table(t, map[string]string{
		`CREATE TEMP TABLE tk (id int); UPDATE tk SET id = 2`:                               "pass",
		`BEGIN; CREATE TEMP TABLE tk (id int) ON COMMIT DROP; UPDATE tk SET id = 2; COMMIT`: "pass",
		`BEGIN; CREATE TEMP TABLE tk (id int); COMMIT; UPDATE tk SET id = 2`:                "pass",
		// A child of a registered table holds its rows: its writes are not
		// passed over as a session table's (they are held as writes of a
		// name with no schema).
		`CREATE TEMP TABLE tk () INHERITS (aveloxis_data.repos); UPDATE tk SET ar = ar`:                                  KindUnqualifiedTarget,
		`CREATE TEMP TABLE tk (repo_id int, ar text); ALTER TABLE tk INHERIT aveloxis_data.repos; UPDATE tk SET ar = ar`: KindUnqualifiedTarget,
		// Past the end of its transaction the name means another relation.
		`BEGIN; CREATE TEMP TABLE tk (id int) ON COMMIT DROP; COMMIT; UPDATE tk SET id = 2`:      KindUnqualifiedTarget,
		`BEGIN; CREATE TEMP TABLE tk (id int); ROLLBACK; UPDATE tk SET id = 2`:                   KindUnqualifiedTarget,
		`BEGIN; SAVEPOINT s; CREATE TEMP TABLE tk (id int); ROLLBACK TO s; UPDATE tk SET id = 2`: KindUnqualifiedTarget,
		// Renamed away, the old name is not it.
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk RENAME TO tk2; UPDATE tk SET id = 2`: KindUnqualifiedTarget,
	})
}

// The statement that makes a session table a child of a registered table,
// and the write of it after, are EACH held (two rules that one verdict
// would hide behind each other), and what is done to a plain one is not.
func TestASessionTableMadeAChildIsHeldTwice(t *testing.T) {
	for sql, want := range map[string]int{
		`CREATE TEMP TABLE tk () INHERITS (aveloxis_data.repos); UPDATE tk SET ar = ar`:                                  2,
		`CREATE TEMP TABLE tk (repo_id int, ar text); ALTER TABLE tk INHERIT aveloxis_data.repos; UPDATE tk SET ar = ar`: 2,
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk ADD COLUMN x int; CREATE INDEX ON tk (x); UPDATE tk SET x = 1`:    0,
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk ALTER COLUMN id TYPE bigint; DELETE FROM tk; TRUNCATE tk`:         0,
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk RENAME TO tk2`:                                                    0,
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk SET SCHEMA aveloxis_data`:                                         1,
		`CREATE TEMP TABLE tk (id int); ALTER TABLE tk SET SCHEMA aveloxis_data; UPDATE tk SET id = 2`:                   2,
	} {
		stmts := []Stmt{{File: "f.go", SQL: sql}}
		rep := CheckIn(r7catalog(t, nil), stmts, round6Registry, nil)
		if len(rep.Unparsed) != 0 || len(rep.Violations) != want {
			t.Errorf("%d violations, want %d (%+v):\n  %s", len(rep.Violations), want, rep.Unparsed, sql)
			for _, v := range rep.Violations {
				t.Logf("  %s %s: %s", v.Kind, v.Table, v.Statement)
			}
		}
	}
}

// S8-10: an Exception written for a statement at the top of a text does
// not excuse the same statement inside a body, nor the reverse: when it
// runs is then the body's to say (IF false THEN ... ELSE <it>).
func TestAnExceptionSaysWhetherItsStatementStandsInABody(t *testing.T) {
	const stmt = `UPDATE aveloxis_data.repos SET io = $1 WHERE repo_id = $2 AND io IS NULL`
	const inDo = "DO $$ BEGIN IF false THEN NULL; ELSE UPDATE aveloxis_data.repos SET io = $1 WHERE repo_id = $2 AND io IS NULL; END IF; END $$"
	ex := func(inBody bool) []Exception {
		return []Exception{{Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, File: "f.go", Decl: "d", Fragment: "$1", Statement: stmt, InBody: inBody, Reason: "x"}}
	}
	judge := func(sql string, inBody bool) (violations, stale int) {
		stmts := []Stmt{{File: "f.go", Decl: "d", SQL: sql}}
		rep := CheckIn(r7catalog(t, stmts), stmts, round6Registry, ex(inBody))
		if len(rep.BadExceptions)+len(rep.Unparsed) > 0 {
			t.Fatalf("%+v %+v", rep.BadExceptions, rep.Unparsed)
		}
		return len(rep.Violations), len(rep.StaleExceptions)
	}
	for _, tc := range []struct {
		sql               string
		inBody            bool
		violations, stale int
	}{
		{stmt, false, 0, 0},
		{inDo, true, 0, 0},
		{inDo, false, 1, 1},
		{stmt, true, 1, 1},
	} {
		if v, s := judge(tc.sql, tc.inBody); v != tc.violations || s != tc.stale {
			t.Errorf("InBody %v: %d violations, %d stale; want %d, %d:\n  %s", tc.inBody, v, s, tc.violations, tc.stale, tc.sql)
		}
	}
}

// The reviewer's cases that passed and must not: each is held now, by the
// one list, whatever kind it is held as.
func TestTheRound8ProbesThatPassedAreHeld(t *testing.T) {
	must := map[string][]string{
		"a1.txt": {"A01", "A02", "A03", "A04", "A05", "A19", "A20", "A28", "A29", "A46", "A55", "A56"},
		"a2.txt": {"A60", "A62", "A63", "A64", "A65", "A66", "A67", "A68", "A69", "A70", "A71", "A72"},
		"a3.txt": {"A80", "A81", "A82"},
		"a4.txt": {"M15"},
		"d1.txt": {"D17", "D18", "D24", "D25", "D26", "D27", "D38", "D40", "D41", "D44", "D51", "D52"},
		"d2.txt": {"P01", "P03", "P04", "P07", "P09", "P13", "P14", "P16"},
		"sv.txt": {"S21", "S34b", "S53"},
	}
	n := 0
	for file, tags := range must {
		verdict := map[string]string{}
		for _, c := range round8Cases(t, file) {
			verdict[tag8(c.head)] = round8Verdict(t, c)
		}
		for _, tag := range tags {
			n++
			switch v := verdict[tag]; v {
			case "":
				t.Errorf("%s has no case %s", file, tag)
			case "pass":
				t.Errorf("%s %s passes", file, tag)
			}
		}
	}
	if n != 51 {
		t.Fatalf("%d cases checked, the list holds 51", n)
	}
}
