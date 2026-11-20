// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// Review round 6. The reader found every write; the JUDGING decided two
// things by a local look at one tree node: whose column a name is, and what
// counts as a write of a table. Each is now decided once (the resolver,
// Write.classify; the Catalog), and an Exception excuses one violation.

var r6fe = []Registered{
	{Table: "aveloxis_data.repos", Column: "fe", Policy: FillEmptyOnly, Reason: "r"},
	{Table: "aveloxis_data.repos", Column: "ar", Policy: AlwaysRefresh, Reason: "r"},
	{Table: "aveloxis_data.repos", Column: "pnn", Policy: PreferNonNullIncoming, Reason: "r"},
	{Table: "aveloxis_data.repos", Column: "k", Policy: KeepCarryOrNow, Reason: "r"},
}

func r6judge(sql string) Report {
	st := []Stmt{{File: "f.go", SQL: sql}}
	return CheckIn(CatalogOf(st).Table("aveloxis_data.repos", "s.stg"), st, r6fe, nil)
}

// "Stored" is a reference RESOLVED to the target's own column.
func TestStoredIsTheTargetsOwnColumn(t *testing.T) {
	for name, tc := range map[string]struct {
		sql string
		ok  bool
	}{
		// The target's column, however it is named.
		"bare":                    {`UPDATE aveloxis_data.repos SET fe = COALESCE(fe, $1)`, true},
		"by the alias":            {`UPDATE aveloxis_data.repos r SET fe = COALESCE(r.fe, $1)`, true},
		"by the table's name":     {`UPDATE aveloxis_data.repos SET fe = COALESCE(repos.fe, $1)`, true},
		"by schema and table":     {`UPDATE aveloxis_data.repos SET fe = COALESCE(aveloxis_data.repos.fe, $1)`, true},
		"in a conflict update":    {`INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET fe = COALESCE(repos.fe, EXCLUDED.fe)`, true},
		"an alias named excluded": {`UPDATE aveloxis_data.repos AS excluded SET fe = COALESCE(excluded.fe, $1)`, true},
		"use_column says column":  {"CREATE FUNCTION f(fe text) RETURNS void LANGUAGE plpgsql AS $$ #variable_conflict use_column\nBEGIN UPDATE aveloxis_data.repos SET fe = COALESCE(fe, 'x'); END $$", true},
		"an SQL function's parameter yields to the column": {`CREATE FUNCTION f(fe text) RETURNS void LANGUAGE sql AS $$ UPDATE aveloxis_data.repos SET fe = COALESCE(fe, 'x') $$`, true},
		// Not the target's column, though it ends in the column's name.
		"a FROM item's column":           {`UPDATE aveloxis_data.repos r SET fe = COALESCE(s.fe, $1) FROM s.stg s WHERE s.id = r.id`, false},
		"a FROM item named in full":      {`UPDATE aveloxis_data.repos r SET fe = COALESCE(s.stg.fe, $1) FROM s.stg WHERE s.stg.id = r.id`, false},
		"the name, when an alias hides":  {`UPDATE aveloxis_data.repos r SET fe = COALESCE(repos.fe, $1)`, false},
		"another schema's table":         {`UPDATE aveloxis_data.repos SET fe = COALESCE(other.repos.fe, $1)`, false},
		"a qualifier that names nothing": {`UPDATE aveloxis_data.repos SET fe = COALESCE(zz.fe, $1)`, false},
		"a MERGE's source":               {`MERGE INTO aveloxis_data.repos t USING s.stg s ON t.id = s.id WHEN MATCHED THEN UPDATE SET fe = COALESCE(s.fe, 'x')`, false},
		"a record of the body":           {`DO $$ DECLARE r record; BEGIN FOR r IN SELECT * FROM s.stg LOOP UPDATE aveloxis_data.repos SET fe = COALESCE(r.fe, 'x') WHERE id = r.id; END LOOP; END $$`, false},
		"a parameter, by the function":   {`CREATE FUNCTION f(fe text) RETURNS void LANGUAGE sql AS $$ UPDATE aveloxis_data.repos SET fe = COALESCE(f.fe, 'x') $$`, false},
		"a parameter in BEGIN ATOMIC":    {`CREATE FUNCTION f(fe text) RETURNS void LANGUAGE sql BEGIN ATOMIC UPDATE aveloxis_data.repos SET fe = COALESCE(f.fe, 'x'); END`, false},
		"use_variable says variable":     {"CREATE FUNCTION f(fe text) RETURNS void LANGUAGE plpgsql AS $$ #variable_conflict use_variable\nBEGIN UPDATE aveloxis_data.repos SET fe = COALESCE(fe, 'x'); END $$", false},
		"a variable, and nothing said":   {`CREATE FUNCTION f(fe text) RETURNS void LANGUAGE plpgsql AS $$ BEGIN UPDATE aveloxis_data.repos SET fe = COALESCE(fe, 'x'); END $$`, false},
		"EXCLUDED outside a conflict":    {`UPDATE aveloxis_data.repos SET pnn = COALESCE(excluded.pnn, pnn) FROM s.stg excluded`, false},
		"NULLIF of another column":       {`UPDATE aveloxis_data.repos SET fe = COALESCE(NULLIF(repo_name, ''), $1)`, false},
		"NULLIF of EXCLUDED's other":     {`INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET pnn = COALESCE(EXCLUDED.other, repos.pnn)`, false},
		// The guard is the target's own column too.
		"a guard on the variable": {"CREATE FUNCTION f(fe text) RETURNS void LANGUAGE plpgsql AS $$ #variable_conflict use_variable\nBEGIN UPDATE aveloxis_data.repos SET fe = $1 WHERE fe IS NULL; END $$", false},
		"a guard on a FROM item":  {`UPDATE aveloxis_data.repos r SET fe = $1 FROM s.stg s WHERE s.fe IS NULL`, false},
		// The guards that are the same condition, written otherwise.
		"guard: IS NULL OR = ''":         {`UPDATE aveloxis_data.repos SET fe = $1 WHERE (fe IS NULL OR fe = '') AND id = $2`, true},
		"guard: = '' OR IS NULL":         {`UPDATE aveloxis_data.repos r SET fe = $1 WHERE (r.fe = '' OR r.fe IS NULL)`, true},
		"guard: NULLIF IS NULL":          {`UPDATE aveloxis_data.repos SET fe = $1 WHERE NULLIF(fe, '') IS NULL`, true},
		"guard: schema-qualified":        {`UPDATE aveloxis_data.repos SET fe = $1 WHERE aveloxis_data.repos.fe IS NULL`, true},
		"guard: a cast placeholder":      {`UPDATE aveloxis_data.repos SET fe = $1::text WHERE fe IS NULL`, true},
		"guard: OR of three":             {`UPDATE aveloxis_data.repos SET fe = $1 WHERE (fe IS NULL OR fe = '' OR id = 1)`, false},
		"guard: OR with another column":  {`UPDATE aveloxis_data.repos SET fe = $1 WHERE (fe IS NULL OR repo_name = '')`, false},
		"guard: OR against a blank":      {`UPDATE aveloxis_data.repos SET fe = $1 WHERE (fe IS NULL OR fe = ' ')`, false},
		"guard: NULLIF of another value": {`UPDATE aveloxis_data.repos SET fe = $1 WHERE NULLIF(fe, 'x') IS NULL`, false},
		"guard: COALESCE against 'x'":    {`UPDATE aveloxis_data.repos SET fe = $1 WHERE COALESCE(fe, '') = 'x'`, false},
		"guard: ROW(col) IS NULL":        {`UPDATE aveloxis_data.repos SET fe = $1 WHERE ROW(fe) IS NULL`, false},
		// A cast is allowed on an incoming value, and on that only.
		"keep: a cast placeholder": {`UPDATE aveloxis_data.repos SET k = CASE WHEN id = 1 THEN k ELSE $1::timestamptz END`, true},
		"keep: a cast of stored":   {`UPDATE aveloxis_data.repos SET k = CASE WHEN id = 1 THEN k::date ELSE NOW() END`, false},
		"keep: a cast of a cast":   {`UPDATE aveloxis_data.repos SET k = CASE WHEN id = 1 THEN k ELSE $1::text::timestamptz END`, false},
		// A part of the column is no shape.
		"a subscripted target": {`UPDATE aveloxis_data.repos SET fe[1] = COALESCE(fe, $1)`, false},
	} {
		rep := r6judge(tc.sql)
		if got := len(rep.Violations)+len(rep.Unparsed) == 0; got != tc.ok {
			t.Errorf("%s: conforms = %v, want %v\n  %s\n  %+v %+v", name, got, tc.ok, tc.sql, rep.Violations, rep.Unparsed)
		}
	}
}

// AlwaysRefresh: the value may not read the stored value by ANY route, and
// what cannot be told to be something else counts.
func TestAlwaysRefreshReadsNothingStored(t *testing.T) {
	for name, tc := range map[string]struct {
		sql string
		ok  bool
	}{
		"a placeholder":                {`UPDATE aveloxis_data.repos SET ar = $1`, true},
		"another column":               {`UPDATE aveloxis_data.repos SET ar = lower(repo_name)`, true},
		"a FROM item's same name":      {`UPDATE aveloxis_data.repos r SET ar = s.ar FROM s.stg s WHERE s.id = r.id`, true},
		"a sub-select's own alias":     {`UPDATE aveloxis_data.repos r SET ar = (SELECT o.ar FROM s.stg o WHERE o.id = r.id)`, true},
		"EXCLUDED's":                   {`INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET ar = EXCLUDED.ar`, true},
		"a variable, by use_variable":  {"DO $$ #variable_conflict use_variable\nDECLARE ar text; BEGIN UPDATE aveloxis_data.repos SET ar = ar; END $$", true},
		"a parameter, by the function": {`CREATE FUNCTION f(ar text) RETURNS void LANGUAGE sql AS $$ UPDATE aveloxis_data.repos SET ar = f.ar $$`, true},
		"a record's field":             {`DO $$ DECLARE r record; BEGIN FOR r IN SELECT * FROM s.stg LOOP UPDATE aveloxis_data.repos SET ar = r.ar; END LOOP; END $$`, true},
		"the stored value":             {`UPDATE aveloxis_data.repos SET ar = ar || 'x'`, false},
		"by the alias":                 {`UPDATE aveloxis_data.repos r SET ar = r.ar || 'x'`, false},
		"the row's field":              {`UPDATE aveloxis_data.repos r SET ar = (r).ar || 'x'`, false},
		"the whole row":                {`UPDATE aveloxis_data.repos r SET ar = to_jsonb(r)->>'ar'`, false},
		"the whole row, starred":       {`UPDATE aveloxis_data.repos r SET ar = (SELECT x FROM (SELECT r.*) y(x))`, false},
		"the whole row, by its name":   {`UPDATE aveloxis_data.repos SET ar = to_jsonb(repos)->>'ar'`, false},
		"an alias named excluded":      {`UPDATE aveloxis_data.repos AS excluded SET ar = excluded.ar || 'x'`, false},
		"the table again, renamed":     {`UPDATE aveloxis_data.repos r SET ar = (SELECT q.z FROM aveloxis_data.repos AS q(id, z) WHERE q.id = r.id)`, false},
		"the table again, unqualified": {`UPDATE aveloxis_data.repos r SET ar = (SELECT max(q.other) FROM repos q)`, false},
		"bare, in a sub-select":        {`UPDATE aveloxis_data.repos r SET ar = (SELECT ar FROM s.stg o WHERE o.id = r.id)`, false},
		"an unknown qualifier":         {`UPDATE aveloxis_data.repos SET ar = zz.ar`, false},
		"an unknown row, starred":      {`UPDATE aveloxis_data.repos SET ar = (SELECT x FROM (SELECT zz.*) y(x))`, false},
	} {
		rep := r6judge(tc.sql)
		if got := len(rep.Violations)+len(rep.Unparsed) == 0; got != tc.ok {
			t.Errorf("%s: conforms = %v, want %v\n  %s\n  %+v %+v", name, got, tc.ok, tc.sql, rep.Violations, rep.Unparsed)
		}
	}
}

// The catalog is built from DDL, transitively, and is what says a target is
// a base table. A catalog that was given the schema knows its views in
// every call (round 6: a package outside the store was judged knowing only
// its own statements, so a write through a view of the schema passed).
func TestTheCatalogSaysWhatATargetIs(t *testing.T) {
	cat := NewCatalog()
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS aveloxis_data.repos (id bigint, fe text)`,
		`CREATE TABLE aveloxis_data.plain (id bigint)`,
		`CREATE OR REPLACE VIEW aveloxis_augur_data.repo AS SELECT id, fe AS repo_language FROM aveloxis_data.repos`,
		`CREATE VIEW aveloxis_augur_data.repo2 AS SELECT * FROM aveloxis_augur_data.repo`,
		`CREATE MATERIALIZED VIEW aveloxis_data.mv AS SELECT 1 AS a`,
		`DO $$ BEGIN CREATE TABLE aveloxis_data.made_in_a_body (id bigint); END $$`,
		`CREATE TEMP TABLE not_kept (id bigint)`,
		`CREATE TABLE aveloxis_data.part PARTITION OF aveloxis_data.repos DEFAULT`,
		`ALTER TABLE aveloxis_data.plain RENAME TO plain2`,
	} {
		if err := cat.Add(ddl); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]string{
		"aveloxis_data.repos": "table", "aveloxis_data.plain": "table", "aveloxis_data.plain2": "table",
		"aveloxis_augur_data.repo": "view", "aveloxis_augur_data.repo2": "view", "aveloxis_data.mv": "matview",
		"aveloxis_data.made_in_a_body": "table", "not_kept": "", "aveloxis_data.nobody": "",
	} {
		if got := cat.Kind(name); got != want {
			t.Errorf("%s is %q to the catalog, want %q", name, got, want)
		}
	}
	if got := strings.Join(cat.behind("aveloxis_augur_data.repo2"), ","); got != "aveloxis_augur_data.repo,aveloxis_data.repos" {
		t.Errorf("behind the view over the view: %s", got)
	}
	if c := cat.Counts(); c["table"] != 5 || c["view"] != 2 || c["matview"] != 1 {
		t.Errorf("counts %v", c)
	}
	reg := []Registered{{Table: "aveloxis_data.repos", Column: "fe", Policy: FillEmptyOnly, Reason: "r"}}
	for sql, kind := range map[string]string{
		`UPDATE aveloxis_augur_data.repo SET repo_language = $1`:                                                    KindViewTarget,
		`UPDATE aveloxis_augur_data.repo2 SET repo_language = $1`:                                                   KindViewTarget,
		`INSERT INTO aveloxis_data.mv VALUES (1)`:                                                                   KindViewTarget,
		`DELETE FROM aveloxis_data.nobody`:                                                                          KindUnknownTarget,
		`UPDATE repo SET repo_language = $1`:                                                                        KindUnqualifiedTarget,
		`UPDATE not_kept SET id = 1`:                                                                                KindUnqualifiedTarget,
		`UPDATE aveloxis_data.part SET fe = 'x'`:                                                                    KindChildTarget,
		`UPDATE aveloxis_data.plain2 SET id = 1`:                                                                    "",
		`UPDATE aveloxis_data.made_in_a_body SET id = 1`:                                                            "",
		`UPDATE aveloxis_data.repos SET fe = COALESCE(fe, $1)`:                                                      "",
		`COPY aveloxis_augur_data.repo (repo_language) FROM STDIN`:                                                  KindViewTarget,
		`TRUNCATE aveloxis_augur_data.repo`:                                                                         KindViewTarget,
		`MERGE INTO aveloxis_augur_data.repo r USING aveloxis_data.plain s ON r.id = s.id WHEN MATCHED THEN DELETE`: KindViewTarget,
	} {
		rep := CheckIn(cat, []Stmt{{File: "other/pkg.go", SQL: sql}}, reg, nil)
		got := ""
		if len(rep.Violations) == 1 {
			got = rep.Violations[0].Kind
		}
		if got != kind || len(rep.Violations) > 1 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %q, want %q (%+v)", sql, got, kind, rep.Violations)
		}
	}
	// A registered table the catalog does not know as a base table.
	rep := CheckIn(NewCatalog(), nil, reg, nil)
	if len(rep.Violations) != 1 || rep.Violations[0].Kind != KindUnknownTable {
		t.Errorf("a registered table no DDL creates: %+v", rep.Violations)
	}
}

// What changes a registered table without assigning a column is named by
// kind, once per statement, and an Exception of that kind and statement
// takes it.
func TestWhatChangesATableIsAViolationOfItsKind(t *testing.T) {
	reg := []Registered{
		{Table: "aveloxis_data.repos", Column: "fe", Policy: FillEmptyOnly, Reason: "r"},
		{Table: "aveloxis_data.repos", Column: "ar", Policy: AlwaysRefresh, Reason: "r"},
	}
	for sql, kind := range map[string]string{
		`TRUNCATE aveloxis_data.repos`:                                                                                             KindTruncate,
		`DROP TABLE aveloxis_data.repos`:                                                                                           KindDrop,
		`DROP SCHEMA aveloxis_data CASCADE`:                                                                                        KindDropSchema,
		`ALTER TABLE aveloxis_data.repos RENAME TO r2`:                                                                             KindRenameTable,
		`ALTER TABLE aveloxis_data.other RENAME TO repos`:                                                                          KindRenameTable,
		`ALTER TABLE aveloxis_data.repos SET SCHEMA elsewhere`:                                                                     KindSetSchema,
		`ALTER TABLE elsewhere.repos SET SCHEMA aveloxis_data`:                                                                     KindRenameTable,
		`CREATE TABLE s.p PARTITION OF aveloxis_data.repos DEFAULT`:                                                                KindPartition,
		`CREATE TABLE s.c () INHERITS (aveloxis_data.repos)`:                                                                       KindPartition,
		`ALTER TABLE aveloxis_data.repos ATTACH PARTITION s.p DEFAULT`:                                                             KindPartition,
		`ALTER TABLE s.parent ATTACH PARTITION aveloxis_data.repos DEFAULT`:                                                        KindPartition,
		`ALTER TABLE s.c INHERIT aveloxis_data.repos`:                                                                              KindPartition,
		`ALTER TABLE aveloxis_data.repos DETACH PARTITION s.p`:                                                                     KindPartition,
		`CREATE RULE r AS ON UPDATE TO aveloxis_data.repos DO ALSO NOTHING`:                                                        KindRule,
		`CREATE TRIGGER t BEFORE UPDATE ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION f()`:                                  KindTrigger,
		`CREATE TRIGGER t BEFORE UPDATE ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()`: KindTrigger,
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (fe) REFERENCES s.o (id) ON UPDATE CASCADE`:                               KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (fe) REFERENCES s.o (id) ON UPDATE SET NULL`:                              KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (fe) REFERENCES s.o (id) ON DELETE SET DEFAULT`:                           KindFKAction,
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (x, y) REFERENCES s.o (a, b) ON DELETE SET NULL (fe)`:                     KindFKAction,
		`ALTER TABLE aveloxis_data.repos RENAME zz TO fe`:                                                                          KindAlterColumn,
		`ALTER TABLE aveloxis_data.repos RENAME fe TO zz`:                                                                          KindAlterColumn,
		// What does not write the column.
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (fe) REFERENCES s.o (id)`:                             "",
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (fe) REFERENCES s.o (id) ON DELETE CASCADE`:           "",
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (x) REFERENCES s.o (id) ON UPDATE CASCADE`:            "",
		`ALTER TABLE aveloxis_data.repos ADD FOREIGN KEY (x, fe) REFERENCES s.o (a, b) ON DELETE SET NULL (x)`: "",
		`ALTER TABLE s.o ADD FOREIGN KEY (x) REFERENCES aveloxis_data.repos (fe) ON UPDATE CASCADE`:            "",
		`CREATE TRIGGER t BEFORE UPDATE ON aveloxis_data.other FOR EACH ROW EXECUTE FUNCTION f()`:              "",
		`ALTER TABLE aveloxis_data.other RENAME TO other2`:                                                     "",
		`DROP SCHEMA scratch`: "",
	} {
		st := []Stmt{{File: "f.go", SQL: sql}}
		cat := CatalogOf(st).Table("aveloxis_data.repos", "aveloxis_data.other", "s.o", "s.parent", "s.c", "elsewhere.repos")
		rep := CheckIn(cat, st, reg, nil)
		got := ""
		if len(rep.Violations) > 0 {
			got = rep.Violations[0].Kind
		}
		if got != kind || len(rep.Violations) > 1 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %q (%d violations), want one %q: %+v %+v", sql, got, len(rep.Violations), kind, rep.Violations, rep.Unparsed)
			continue
		}
		if kind == "" {
			continue
		}
		v := rep.Violations[0]
		ex := []Exception{{Table: v.Table, Column: v.Column, Kind: v.Kind, Statement: "/* reviewed */ " + strings.ToLower(sql[:5]) + sql[5:], Reason: "r"}}
		if rep := CheckIn(cat, st, reg, ex); len(rep.Violations)+len(rep.StaleExceptions)+len(rep.BadExceptions) != 0 {
			t.Errorf("%s: an Exception of the kind and the statement does not take it: %+v stale %+v bad %v", sql, rep.Violations, rep.StaleExceptions, rep.BadExceptions)
		}
	}
}

// A trigger function's body: every form that changes the row otherwise than
// by a read assignment to one column of NEW is held against it.
func TestATriggerFunctionsFormsAreHeldAgainstIt(t *testing.T) {
	fn := func(body string) string {
		return "CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ " + body + " $$"
	}
	for name, tc := range map[string]struct{ sql, kind string }{
		"FOREACH into NEW.col":     {fn(`DECLARE a text[]; BEGIN FOREACH NEW.fe IN ARRAY a LOOP NULL; END LOOP; RETURN NEW; END`), KindTriggerAssign},
		"a copy of NEW, changed":   {fn(`DECLARE r record; BEGIN r := NEW; r.fe := 'x'; RETURN r; END`), KindTriggerForm},
		"OLD.col assigned":         {fn(`BEGIN OLD.fe := 'x'; RETURN OLD; END`), KindTriggerForm},
		"OLD assigned whole":       {fn(`BEGIN OLD := NEW; RETURN OLD; END`), KindTriggerForm},
		"SELECT INTO OLD.col":      {fn(`BEGIN SELECT 'x' INTO OLD.fe; RETURN OLD; END`), KindTriggerForm},
		"a returned expression":    {fn(`BEGIN RETURN jsonb_populate_record(NEW, '{"fe":"x"}'); END`), KindTriggerForm},
		"NEW.col to a procedure":   {fn(`BEGIN CALL setit(NEW.fe); RETURN NEW; END`), KindTriggerForm},
		"NEW to a function":        {fn(`DECLARE j jsonb; BEGIN j := to_jsonb(NEW); RETURN NEW; END`), KindTriggerForm},
		"NEW.* to a function":      {fn(`BEGIN PERFORM f(NEW.*); RETURN NEW; END`), KindTriggerForm},
		"a variable named new.col": {fn(`BEGIN FOR NEW.fe IN 1..3 LOOP NULL; END LOOP; RETURN NEW; END`), KindTriggerForm},
		"GET DIAGNOSTICS into NEW": {fn(`BEGIN GET DIAGNOSTICS NEW.fe = ROW_COUNT; RETURN NEW; END`), KindTriggerAssign},
		"NEW.col := v":             {fn(`BEGIN NEW.fe := 'x'; RETURN NEW; END`), KindTriggerAssign},
		"reads only":               {fn(`DECLARE g text; BEGIN g := NEW.fe; IF OLD.fe IS DISTINCT FROM NEW.fe THEN RAISE NOTICE 'x'; END IF; RETURN NEW; END`), ""},
		"returns NULL":             {fn(`BEGIN RETURN NULL; END`), ""},
		"returns old":              {fn(`BEGIN RETURN old; END`), ""},
		"another column of NEW":    {fn(`BEGIN NEW.other := lower(NEW.fe); RETURN NEW; END`), ""},
		"not a trigger function":   {`CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ DECLARE new int := 1; BEGIN RETURN new + 1; END $$`, ""},
	} {
		rep := r6judge(tc.sql)
		got := ""
		if len(rep.Violations) > 0 {
			got = rep.Violations[0].Kind
		}
		if got != tc.kind || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %q, want %q: %+v %+v", name, got, tc.kind, rep.Violations, rep.Unparsed)
		}
	}
}

// An Exception excuses ONE violation: of its table, column and kind, in its
// file, whose assigned expression (or statement) is exactly its Fragment
// (or Statement) as the parser reads both. Round 6: a needle anywhere in
// the argument's text excused every violation of the column in that text.
func TestAnExceptionExcusesOneViolation(t *testing.T) {
	reg := []Registered{{Table: "aveloxis_data.repos", Column: "io", Policy: InsertOnly, Reason: "r"}}
	ex := []Exception{{Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, File: "f.go", Fragment: "$2", Reason: "reviewed"}}
	judge := func(sql string, ex []Exception) Report {
		st := []Stmt{{File: "f.go", SQL: sql}}
		return CheckIn(CatalogOf(st).Table("aveloxis_data.repos"), st, reg, ex)
	}
	const excused = `UPDATE aveloxis_data.repos SET io = $2 WHERE id = $1`
	for what, sql := range map[string]string{
		"spacing and a comment": "UPDATE aveloxis_data.repos\n  SET io = /* the forge's */ $2\n WHERE id = $1",
		"in a conflict update":  `INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET io = $2`,
	} {
		if rep := judge(sql, ex); len(rep.Violations)+len(rep.StaleExceptions)+len(rep.BadExceptions) != 0 {
			t.Errorf("%s: %+v stale %+v", what, rep.Violations, rep.StaleExceptions)
		}
	}
	for what, sql := range map[string]string{
		"a second statement":           excused + `; UPDATE aveloxis_data.repos SET io = NULL`,
		"the needle in a comment":      `UPDATE aveloxis_data.repos SET io = NULL -- was: SET io = $2`,
		"the needle in a string":       `UPDATE aveloxis_data.repos SET io = NULL, note = 'SET io = $2'`,
		"a longer placeholder":         `UPDATE aveloxis_data.repos SET io = $20`,
		"the value and more":           `UPDATE aveloxis_data.repos SET io = $2 || io`,
		"another kind of violation":    excused + `; ALTER TABLE aveloxis_data.repos DROP COLUMN io`,
		"a COPY beside it":             excused + `; COPY aveloxis_data.repos FROM STDIN`,
		"a TRUNCATE beside it":         excused + `; TRUNCATE aveloxis_data.repos`,
		"the same value, twice":        excused + `; UPDATE aveloxis_data.repos SET io = $2`,
		"twice, in one conflict chain": `WITH a AS (UPDATE aveloxis_data.repos SET io = $2 RETURNING 1) UPDATE aveloxis_data.repos SET io = $2`,
	} {
		rep := judge(sql, ex)
		if len(rep.Violations) != 1 {
			t.Errorf("%s: %d violations, want the one the exception does not excuse: %+v\n  %s", what, len(rep.Violations), rep.Violations, sql)
		}
	}
	// Two violations of the same value in one text take two Exceptions.
	if rep := judge(excused+`; UPDATE aveloxis_data.repos SET io = $2`, append(ex, ex[0])); len(rep.Violations)+len(rep.StaleExceptions) != 0 {
		t.Errorf("two exceptions for two violations: %+v stale %+v", rep.Violations, rep.StaleExceptions)
	}
	// Another file's, another kind's, another column's exception excuses
	// nothing, and is stale.
	for what, change := range map[string]func(*Exception){
		"another file":   func(e *Exception) { e.File = "g.go" },
		"another column": func(e *Exception) { e.Column = "other" },
		"another table":  func(e *Exception) { e.Table = "aveloxis_data.other" },
		"another value":  func(e *Exception) { e.Fragment = "$3" },
		"another statement": func(e *Exception) {
			e.Statement = `UPDATE aveloxis_data.repos SET io = $2 WHERE id = $1 AND TRUE`
		},
		"another kind": func(e *Exception) { e.Kind, e.Fragment, e.Statement = KindCopy, "", excused },
	} {
		e := ex[0]
		change(&e)
		if rep := judge(excused, []Exception{e}); len(rep.Violations) != 1 || len(rep.StaleExceptions) != 1 {
			t.Errorf("%s: %d violations, %d stale; want the violation and the exception stale", what, len(rep.Violations), len(rep.StaleExceptions))
		}
	}
	// With a Statement, that statement and no other.
	withStmt := []Exception{{Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, Fragment: "$2", Statement: excused, Reason: "r"}}
	if rep := judge("update aveloxis_data.repos set io=$2 where id=$1", withStmt); len(rep.Violations)+len(rep.StaleExceptions) != 0 {
		t.Errorf("the statement itself: %+v", rep.Violations)
	}
	if rep := judge(`UPDATE aveloxis_data.repos SET io = $2`, withStmt); len(rep.Violations) != 1 {
		t.Error("the statement without its WHERE was excused by an exception that gives the statement whole")
	}
	// An exception that is not well formed is said to be, and excuses
	// nothing.
	for what, e := range map[string]Exception{
		"no kind":                      {Table: "aveloxis_data.repos", Column: "io", Fragment: "$2", Reason: "r"},
		"an assignment, no fragment":   {Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, Statement: excused, Reason: "r"},
		"a fragment that is not one":   {Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, Fragment: "$2, $3", Reason: "r"},
		"a fragment that is a prefix":  {Table: "aveloxis_data.repos", Column: "io", Kind: KindAssign, Fragment: "$2 ||", Reason: "r"},
		"a table kind, no statement":   {Table: "aveloxis_data.repos", Kind: KindTruncate, Reason: "r"},
		"a table kind with a fragment": {Table: "aveloxis_data.repos", Kind: KindTruncate, Fragment: "$2", Statement: "TRUNCATE aveloxis_data.repos", Reason: "r"},
		"two statements":               {Table: "aveloxis_data.repos", Kind: KindTruncate, Statement: "TRUNCATE aveloxis_data.repos; SELECT 1", Reason: "r"},
	} {
		rep := judge(excused+`; TRUNCATE aveloxis_data.repos`, []Exception{e})
		if len(rep.BadExceptions) != 1 || len(rep.Violations) != 2 {
			t.Errorf("%s: %d said bad, %d violations; want 1 and 2", what, len(rep.BadExceptions), len(rep.Violations))
		}
	}
}

// A text with a NUL byte is not read: the server would stop at the NUL and
// run another statement than the text shows.
func TestATextWithANULIsNotRead(t *testing.T) {
	rep := r6judge("SELECT 1 --\x00\n; UPDATE aveloxis_data.repos SET fe = 'x'")
	if len(rep.Unparsed) != 1 || !strings.Contains(rep.Unparsed[0].Detail, "NUL") {
		t.Errorf("%+v", rep)
	}
}

// The write finders of the other tripwires (the users.email ban, the
// column-writer tripwire) ask the parse tree of a statement the parser
// reads, so a write spelled a way the patterns do not match is a write
// (round 6: nine of twenty-two spellings went unseen).
func TestTheWriteFindersReadTheTreeOfAWholeStatement(t *testing.T) {
	for name, tc := range map[string]struct {
		sql           string
		table, column bool
	}{
		"plain insert":           {`INSERT INTO aveloxis_ops.users (id, email) VALUES ($1, $2)`, true, true},
		"plain update":           {`UPDATE aveloxis_ops.users SET email = $1 WHERE id = $2`, true, true},
		"a spaced dot":           {`UPDATE aveloxis_ops . users SET email = $1`, true, true},
		"ONLY in brackets":       {`UPDATE ONLY (aveloxis_ops.users) SET email = $1`, true, true},
		"a quoted target":        {`UPDATE "aveloxis_ops"."users" SET "email" = $1`, true, true},
		"a U& column":            {`UPDATE aveloxis_ops.users SET U&"em\0061il" = $1`, true, true},
		"a comment in the head":  {"UPDATE /* x */ aveloxis_ops.users\nSET email = $1", true, true},
		"COPY with the column":   {`COPY aveloxis_ops.users (id, email) FROM STDIN`, true, true},
		"COPY of every column":   {`COPY aveloxis_ops.users FROM STDIN`, true, true},
		"a MERGE's INSERT arm":   {`MERGE INTO aveloxis_ops.users u USING s.stg s ON u.id = s.id WHEN NOT MATCHED THEN INSERT (id, email) VALUES (s.id, s.email)`, true, true},
		"a MERGE's UPDATE arm":   {`MERGE INTO aveloxis_ops.users u USING s.stg s ON u.id = s.id WHEN MATCHED THEN UPDATE SET email = s.email`, true, true},
		"INSERT of every column": {`INSERT INTO aveloxis_ops.users SELECT * FROM s.stg`, true, true},
		"a conflict update":      {`INSERT INTO aveloxis_ops.users (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email`, true, true},
		"in a CTE":               {`WITH w AS (UPDATE aveloxis_ops.users SET email = $1 RETURNING id) SELECT id FROM w`, true, true},
		"another column":         {`UPDATE aveloxis_ops.users SET login = $1 WHERE email = $2`, true, false},
		"COPY of other columns":  {`COPY aveloxis_ops.users (id, login) FROM STDIN`, true, false},
		"COPY TO":                {`COPY aveloxis_ops.users (email) TO STDOUT`, false, false},
		// The patterns' own looseness, as before: WritesColumn's pattern
		// does not look at the table (its callers ask FindWrites first),
		// and FindWrites' pattern reads a write's words inside a string.
		"another table":            {`UPDATE aveloxis_ops.user_groups SET email = $1`, false, true},
		"words in a string":        {`SELECT email FROM aveloxis_ops.users WHERE email = 'UPDATE aveloxis_ops.users SET email'`, true, false},
		"a read":                   {`SELECT email FROM aveloxis_ops.users WHERE email = $1`, false, false},
		"a fragment (the pattern)": {`UPDATE aveloxis_ops.users SET email = %s WHERE`, true, true},
	} {
		st := Stmt{File: "f.go", SQL: tc.sql}
		if got := len(FindWrites([]Stmt{st}, "aveloxis_ops.users")) == 1; got != tc.table {
			t.Errorf("%s: writes the table = %v, want %v", name, got, tc.table)
		}
		if got := st.WritesColumn("aveloxis_ops.users", "email"); got != tc.column {
			t.Errorf("%s: writes the column = %v, want %v", name, got, tc.column)
		}
	}
}
