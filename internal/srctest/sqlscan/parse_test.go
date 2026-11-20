// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

var repoGit = []Registered{{Table: "aveloxis_data.repos", Column: "repo_git", Policy: FillEmptyOnly, Reason: "r"}}

// Every spelling of a write of a registered column with a value its policy
// forbids is a violation: the rows reviewers found passing in rounds 3 to
// 5 (the old matchers read text with patterns), and the forms a reader of
// statements adds. Deny by default: what is not understood fails too.
func TestAForbiddenWriteIsAViolationHoweverItIsSpelled(t *testing.T) {
	for name, sql := range map[string]string{
		"plain":                            `UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = 1`,
		"no schema (search_path)":          `UPDATE repos SET repo_git = $1 WHERE repo_id = $2`,
		"spaces around the dot":            `UPDATE aveloxis_data . repos SET repo_git = 'x'`,
		"a comment in the name":            "UPDATE aveloxis_data./* c */repos SET repo_git = 'x'",
		"a line comment before SET":        "UPDATE aveloxis_data.repos -- note\n SET repo_git = 'x'",
		"ONLY with brackets":               `UPDATE ONLY (aveloxis_data.repos) SET repo_git = 'x'`,
		"ONLY":                             `UPDATE ONLY aveloxis_data.repos SET repo_git = 'x'`,
		"star":                             `UPDATE aveloxis_data.repos * SET repo_git = 'x'`,
		"alias AS":                         `UPDATE aveloxis_data.repos AS r SET repo_git = $1 WHERE r.repo_id = $2`,
		"quoted alias":                     `UPDATE aveloxis_data.repos AS "r" SET repo_git = $1`,
		"all quoted":                       `UPDATE "aveloxis_data"."repos" SET "repo_git" = $1`,
		"a Unicode-escaped column":         `UPDATE aveloxis_data.repos SET U&"repo\005fgit" = 'x'`,
		"a Unicode-escaped table":          `UPDATE aveloxis_data.U&"\0072epos" SET repo_git = 'x'`,
		"a Unicode escape with UESCAPE":    `UPDATE aveloxis_data.repos SET U&"repo!005fgit" UESCAPE '!' = 'x'`,
		"upper case":                       `UPDATE AVELOXIS_DATA.REPOS SET REPO_GIT = 'x'`,
		"no space before SET":              `UPDATE aveloxis_data."repos"SET repo_git = 'x'`,
		"database-qualified":               `UPDATE aveloxis.aveloxis_data.repos SET repo_git = 'x'`,
		"a guard only in a string":         `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE 'repo_git IS NULL' <> ''`,
		"a guard under OR":                 `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_git IS NULL OR TRUE`,
		"a guard beside an OR":             `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_git IS NULL AND repo_id = 1 OR TRUE`,
		"a guard under NOT":                `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE NOT (repo_git IS NULL)`,
		"a guard in a subquery":            `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_id IN (SELECT 1 WHERE repo_git IS NULL)`,
		"a guard in a comment":             "UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_id = 1 -- AND repo_git IS NULL",
		"a guard of another alias":         `UPDATE aveloxis_data.repos r SET repo_git = $1 FROM aveloxis_data.repos o WHERE o.repo_git IS NULL`,
		"a guard behind BETWEEN's AND":     `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_id BETWEEN 1 AND repo_git IS NULL`,
		"a guard in an E string":           `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE note = E'\' AND repo_git IS NULL --'`,
		"the guard of the other statement": `UPDATE aveloxis_data.repos SET repo_git = $1; UPDATE aveloxis_data.repos SET repo_name = $1 WHERE repo_git IS NULL`,
		"in a CTE":                         `WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT * FROM w`,
		"in a CTE two deep":                `WITH a AS (WITH b AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT * FROM b) SELECT * FROM a`,
		"a conflict's update":              `INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = EXCLUDED.repo_git`,
		"a conflict's update, aliased":     `INSERT INTO aveloxis_data.repos AS t (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = 'x' WHERE EXCLUDED.repo_git IS NULL`,
		"MERGE":                            `MERGE INTO aveloxis_data.repos r USING s ON r.repo_id = s.id WHEN MATCHED THEN UPDATE SET repo_git = s.g WHEN NOT MATCHED THEN INSERT (repo_id) VALUES (s.id)`,
		"a column list":                    `UPDATE aveloxis_data.repos SET (repo_name, repo_git) = ($1, $2)`,
		"the second of two statements":     `SELECT 1; UPDATE aveloxis_data.repos SET repo_git = 'x'`,
		"a DO block":                       `DO $$ BEGIN UPDATE aveloxis_data.repos SET repo_git = 'x'; END $$`,
		"a DO block with a tag":            `DO $do$ BEGIN IF TRUE THEN UPDATE aveloxis_data.repos SET repo_git = 'x'; END IF; END $do$`,
		"a function's plain-string body":   `CREATE FUNCTION f() RETURNS void AS 'BEGIN UPDATE aveloxis_data.repos SET repo_git = ''x''; END' LANGUAGE plpgsql`,
		"EXECUTE of a string in a body":    `DO $$ BEGIN EXECUTE 'UPDATE aveloxis_data.repos SET repo_git = ''x'''; END $$`,
		"a trigger's NEW":                  `CREATE FUNCTION t() RETURNS trigger AS $$ BEGIN NEW.repo_git := 'x'; RETURN NEW; END $$ LANGUAGE plpgsql`,
		"ALTER TYPE USING":                 `ALTER TABLE aveloxis_data.repos ALTER repo_git TYPE text USING 'x'`,
		"ALTER COLUMN SET DATA TYPE":       `ALTER TABLE aveloxis_data.repos ALTER COLUMN repo_git SET DATA TYPE text`,
		"DROP COLUMN":                      `ALTER TABLE IF EXISTS aveloxis_data.repos DROP COLUMN IF EXISTS repo_git`,
		"DROP without the word COLUMN":     `ALTER TABLE aveloxis_data.repos ADD x int, DROP repo_git`,
		"RENAME COLUMN":                    `ALTER TABLE aveloxis_data.repos RENAME repo_git TO old_git`,
		"COPY FROM":                        `COPY aveloxis_data.repos (repo_id, repo_git) FROM STDIN`,
		"COPY FROM, every column":          `COPY aveloxis_data.repos FROM STDIN`,
		"through a view":                   `CREATE VIEW aveloxis_augur_data.repo AS SELECT repo_id, repo_git FROM aveloxis_data.repos; UPDATE aveloxis_augur_data.repo SET repo_git = 'x'`,
	} {
		rep := Check([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
		if len(rep.Violations) == 0 && len(rep.Unparsed) == 0 {
			t.Errorf("%s: passed\n  %s", name, sql)
		}
	}
}

// What is not a statement the reader understands is not read, and says so.
func TestTextThatIsNotReadToItsEndIsReported(t *testing.T) {
	for name, sql := range map[string]string{
		"an open string":             `UPDATE aveloxis_data.repos SET repo_name = 'x`,
		"an open comment":            `SELECT 1 /* UPDATE`,
		"an open bracket":            `SELECT (1`,
		"an open dollar quote":       `DO $$ BEGIN`,
		"UPDATE of nothing":          `UPDATE (SELECT 1) SET a = 1`,
		"UPDATE with no SET":         `UPDATE aveloxis_data.repos`,
		"an assignment with no =":    `UPDATE aveloxis_data.repos SET repo_git`,
		"run-time EXECUTE in a body": `DO $$ BEGIN EXECUTE format('UPDATE %I SET repo_git = 1', t); END $$`,
		"INSERT with no INTO":        `INSERT aveloxis_data.repos VALUES (1)`,
		"a name ending in a dot":     `UPDATE aveloxis_data. SET repo_git = 1`,
		"a character SQL lacks":      "SELECT 1 \x01",
	} {
		if _, err := ParseWrites(sql); err == nil {
			t.Errorf("%s: read without complaint\n  %s", name, sql)
		}
		if rep := Check([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil); len(rep.Unparsed) != 1 {
			t.Errorf("%s: Check reported %d unparsed statements, want 1", name, len(rep.Unparsed))
		}
	}
}

// What conforms, or is not a write of the column, passes: the reader must
// not cry wolf, or people stop reading what it says.
func TestAConformingWriteAndAReadPass(t *testing.T) {
	for name, sql := range map[string]string{
		"the guarded fill":                `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_id = $2 AND COALESCE(repo_git, '') = ''`,
		"the guard by alias":              `UPDATE aveloxis_data.repos r SET repo_git = $1 WHERE r.repo_git IS NULL AND r.repo_id = $2`,
		"the guard in brackets":           `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE (repo_git IS NULL) AND repo_id = $2`,
		"the guard by table name":         `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repos.repo_git IS NULL`,
		"COALESCE of stored":              `UPDATE aveloxis_data.repos SET repo_git = COALESCE(NULLIF(repo_git, ''), $1)`,
		"another column":                  `UPDATE aveloxis_data.repos SET repo_name = 'x' WHERE repo_id = 1`,
		"another table's column":          `WITH a AS (UPDATE aveloxis_data.repos SET repo_name = 1 RETURNING repo_id) UPDATE aveloxis_data.other SET repo_git = $1 FROM a`,
		"the word in a string":            `INSERT INTO aveloxis_ops.notes (t) VALUES ('UPDATE aveloxis_data.repos SET repo_git = 1')`,
		"the word in a comment":           "SELECT 1 -- UPDATE aveloxis_data.repos SET repo_git = 1",
		"the word in a dollar string":     `SELECT $x$UPDATE aveloxis_data.repos SET repo_git = 1$x$`,
		"a row lock":                      `SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1 FOR NO KEY UPDATE`,
		"FOR UPDATE OF":                   `SELECT 1 FROM aveloxis_data.repos r FOR UPDATE OF r SKIP LOCKED`,
		"a foreign key's action":          `ALTER TABLE aveloxis_data.repos ADD CONSTRAINT fk FOREIGN KEY (g) REFERENCES aveloxis_data.g (id) ON UPDATE CASCADE ON DELETE SET NULL`,
		"a trigger's events":              `CREATE TRIGGER tr BEFORE INSERT OR UPDATE OR DELETE ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION t()`,
		"a grant":                         `GRANT SELECT, INSERT, UPDATE, DELETE ON aveloxis_data.repos TO r`,
		"an insert":                       `INSERT INTO aveloxis_data.repos (repo_id, repo_git) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		"a delete":                        `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		"SET DEFAULT":                     `ALTER TABLE aveloxis_data.repos ALTER COLUMN repo_git SET DEFAULT ''`,
		"ADD COLUMN":                      `ALTER TABLE aveloxis_data.repos ADD COLUMN IF NOT EXISTS repo_git text`,
		"COPY TO":                         `COPY aveloxis_data.repos (repo_git) TO STDOUT`,
		"IS DISTINCT FROM in a value":     `UPDATE aveloxis_data.repos SET repo_name = $1, changed = (repo_name IS DISTINCT FROM $1) WHERE repo_id = $2`,
		"CASE in a conflict's value":      `INSERT INTO aveloxis_data.m (a) VALUES ($1) ON CONFLICT (a) DO UPDATE SET b = CASE WHEN m.a IS NULL THEN 1 ELSE 2 END, c = 3`,
		"a quoted name with capitals":     `UPDATE aveloxis_data."Repos" SET repo_git = 'x'`,
		"an unqualified other column":     `UPDATE tmp_pairs SET keep = TRUE`,
		"a view over another table":       `CREATE VIEW aveloxis_augur_data.v AS SELECT * FROM aveloxis_data.other; UPDATE aveloxis_augur_data.v SET repo_git = 'x'`,
		"a materialized view is no door":  `CREATE MATERIALIZED VIEW aveloxis_data.mv AS SELECT * FROM aveloxis_data.repos`,
		"a body that only reads":          `DO $$ BEGIN PERFORM 1 FROM aveloxis_data.repos; END $$`,
		"a Unicode-escaped other column":  `UPDATE aveloxis_data.repos SET U&"repo\005fname" = 'x'`,
		"a nested comment":                "SELECT /* a /* UPDATE x SET */ b */ 1",
		"operators next to a comment":     "SELECT 1 +-- UPDATE\n 2",
		"a parameter and a dollar string": `SELECT $1, $$a$$, $t$ b $t$`,
	} {
		rep := Check([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
		if len(rep.Violations) != 0 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %+v %+v\n  %s", name, rep.Violations, rep.Unparsed, sql)
		}
	}
}

// The reader's own results, for the shapes Check builds on.
func TestParseWritesSaysWhatAStatementWrites(t *testing.T) {
	ws, err := ParseWrites(`WITH a AS (INSERT INTO s1.t AS x (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET n = x.n + 1, m = CASE WHEN TRUE THEN 1 END WHERE x.m IS NULL RETURNING id)
		UPDATE ONLY s2."U" u SET (a, b) = (1, 2), c = (SELECT max(c) FROM s2.z WHERE z.k = u.k) FROM a WHERE u.c IS NULL AND u.id = a.id; TRUNCATE TABLE s3.p, s3.q; DELETE FROM s4.d USING s4.e WHERE d.i = e.i`)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, w := range ws {
		line := w.Kind + " " + w.Qualified() + " as " + w.Alias + ":"
		for _, it := range w.Sets {
			line += " " + it.Column + "=" + strings.Join(strings.Fields(it.Expr), " ")
			if it.Multi {
				line += "(multi)"
			}
		}
		got = append(got, line)
	}
	want := []string{
		"insert s1.t as x:",
		"conflict-update s1.t as x: n=x.n + 1 m=CASE WHEN TRUE THEN 1 END",
		"update s2.U as u: a=(1, 2)(multi) b=(1, 2)(multi) c=(subquery)",
		"truncate s3.p as :", "truncate s3.q as :", "delete s4.d as :",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if !ws[1].GuardsEmpty("m") || ws[1].GuardsEmpty("n") || !ws[2].GuardsEmpty("c") || ws[2].GuardsEmpty("a") {
		t.Error("GuardsEmpty: the conflict's WHERE guards m alone, the update's guards c alone")
	}
}

// PostgreSQL's scanner, on the spellings it has for one thing: the words
// of a fragment are the names it names, however each is written, and what
// is in a string or a comment is not a word.
func TestFragmentWordsAreTheScannersNames(t *testing.T) {
	got := FragmentWords(`Sel "A""b" U&"d\0061t\+000061" 'it''s' E'a\'b' $1 $q$x'y$q$ 1.5e-3 a::int /* c /* d */ */ -- e` + "\n" + `x.y <> !=- 3 B'01' N'n'`)
	// (N'n' is the key word NCHAR and a string to the scanner.)
	if want := `sel A"b data a int x y n`; strings.Join(got, " ") != want {
		t.Errorf("got:\n%s\nwant:\n%s", strings.Join(got, " "), want)
	}
	// Text the scanner refuses is read word by word up to the trouble.
	if got := FragmentWords(`UPDATE t SET a = 'open`); strings.Join(got, " ") != "update t set a open" {
		t.Errorf("an open string: %q", got)
	}
}

// The token numbers pgtok.go names are the scanner's: one of each is
// scanned, so a parser whose grammar numbers them otherwise fails here.
func TestTheScannersTokenNumbersAreTheOnesNamed(t *testing.T) {
	toks, err := scanSQL("a \"b\" U&\"c\" := -- d\n/* e */ select ( = [")
	if err != nil {
		t.Fatal(err)
	}
	var got []int
	for _, tk := range toks {
		got = append(got, tk.token)
	}
	want := []int{tokIdent, tokIdent, tokUIdent, tokColonEquals, tokSQLComment, tokBlockComment, 0, '(', '=', '['}
	if len(got) != len(want) {
		t.Fatalf("tokens %v, want %d of them", got, len(want))
	}
	for i := range want {
		if want[i] != 0 && got[i] != want[i] {
			t.Errorf("token %d is %d, want %d", i, got[i], want[i])
		}
	}
	if !toks[6].keyword || toks[0].keyword {
		t.Error("SELECT is a key word and a is not")
	}
	if toks[1].start != 2 || toks[1].end != 5 {
		t.Errorf("the quoted name stands at %d to %d, want 2 to 5", toks[1].start, toks[1].end)
	}
	for _, open := range []string{`'open`, `"open`, `/* open`, `$t$ open`} {
		if _, err := scanSQL(open); err == nil {
			t.Errorf("%s: scanned without complaint", open)
		}
	}
}
