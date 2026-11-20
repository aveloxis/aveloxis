// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"sort"
	"strings"
	"testing"
)

// parserStatementKinds is every statement node PostgreSQL 17's parser has
// (the names its JSON gives them), and parserAlterKinds every action of
// ALTER TABLE: the lists stmtKinds and alterKinds were classified against.
const parserStatementKinds = `AlterCollationStmt AlterDatabaseRefreshCollStmt AlterDatabaseSetStmt AlterDatabaseStmt AlterDefaultPrivilegesStmt AlterDomainStmt AlterEnumStmt AlterEventTrigStmt AlterExtensionContentsStmt AlterExtensionStmt AlterFdwStmt AlterForeignServerStmt AlterFunctionStmt AlterObjectDependsStmt AlterObjectSchemaStmt AlterOpFamilyStmt AlterOperatorStmt AlterOwnerStmt AlterPolicyStmt AlterPublicationStmt AlterRoleSetStmt AlterRoleStmt AlterSeqStmt AlterStatsStmt AlterSubscriptionStmt AlterSystemStmt AlterTSConfigurationStmt AlterTSDictionaryStmt AlterTableMoveAllStmt AlterTableSpaceOptionsStmt AlterTableStmt AlterTypeStmt AlterUserMappingStmt CallStmt CheckPointStmt ClosePortalStmt ClusterStmt CommentStmt CompositeTypeStmt ConstraintsSetStmt CopyStmt CreateAmStmt CreateCastStmt CreateConversionStmt CreateDomainStmt CreateEnumStmt CreateEventTrigStmt CreateExtensionStmt CreateFdwStmt CreateForeignServerStmt CreateForeignTableStmt CreateFunctionStmt CreateOpClassStmt CreateOpFamilyStmt CreatePLangStmt CreatePolicyStmt CreatePublicationStmt CreateRangeStmt CreateRoleStmt CreateSchemaStmt CreateSeqStmt CreateStatsStmt CreateStmt CreateSubscriptionStmt CreateTableAsStmt CreateTableSpaceStmt CreateTransformStmt CreateTrigStmt CreateUserMappingStmt CreatedbStmt DeallocateStmt DeclareCursorStmt DefineStmt DeleteStmt DiscardStmt DoStmt DropOwnedStmt DropRoleStmt DropStmt DropSubscriptionStmt DropTableSpaceStmt DropUserMappingStmt DropdbStmt ExecuteStmt ExplainStmt FetchStmt GrantRoleStmt GrantStmt ImportForeignSchemaStmt IndexStmt InsertStmt ListenStmt LoadStmt LockStmt MergeStmt NotifyStmt PLAssignStmt PrepareStmt RawStmt ReassignOwnedStmt RefreshMatViewStmt ReindexStmt RenameStmt ReplicaIdentityStmt ReturnStmt RuleStmt SecLabelStmt SelectStmt SetOperationStmt TransactionStmt TruncateStmt UnlistenStmt UpdateStmt VacuumStmt VariableSetStmt VariableShowStmt ViewStmt`

const parserAlterKinds = `AT_AddColumn AT_AddColumnToView AT_AddConstraint AT_AddIdentity AT_AddIndex AT_AddIndexConstraint AT_AddInherit AT_AddOf AT_AlterColumnGenericOptions AT_AlterColumnType AT_AlterConstraint AT_AttachPartition AT_ChangeOwner AT_CheckNotNull AT_ClusterOn AT_ColumnDefault AT_CookedColumnDefault AT_DetachPartition AT_DetachPartitionFinalize AT_DisableRowSecurity AT_DisableRule AT_DisableTrig AT_DisableTrigAll AT_DisableTrigUser AT_DropCluster AT_DropColumn AT_DropConstraint AT_DropExpression AT_DropIdentity AT_DropInherit AT_DropNotNull AT_DropOf AT_DropOids AT_EnableAlwaysRule AT_EnableAlwaysTrig AT_EnableReplicaRule AT_EnableReplicaTrig AT_EnableRowSecurity AT_EnableRule AT_EnableTrig AT_EnableTrigAll AT_EnableTrigUser AT_ForceRowSecurity AT_GenericOptions AT_NoForceRowSecurity AT_ReAddComment AT_ReAddConstraint AT_ReAddDomainConstraint AT_ReAddIndex AT_ReAddStatistics AT_ReplaceRelOptions AT_ReplicaIdentity AT_ResetOptions AT_ResetRelOptions AT_SetAccessMethod AT_SetCompression AT_SetExpression AT_SetIdentity AT_SetLogged AT_SetNotNull AT_SetOptions AT_SetRelOptions AT_SetStatistics AT_SetStorage AT_SetTableSpace AT_SetUnLogged AT_ValidateConstraint`

// The classification is closed: it holds every statement kind and every
// ALTER TABLE action of the grammar the parser reads, and nothing else. A
// parser of another PostgreSQL major version fails here until its kinds
// have been listed and each new one classified; and at run time a kind
// that is not in the table fails the statement it stands in (below).
func TestEveryStatementKindOfTheParserIsClassified(t *testing.T) {
	v, err := ParserVersion()
	if err != nil {
		t.Fatal(err)
	}
	if v/10000 != parserGrammar {
		t.Fatalf("the parser reads PostgreSQL %d's grammar (version %d); stmtKinds and alterKinds were classified for %d: list that version's kinds here, classify each new one, then change parserGrammar", v/10000, v, parserGrammar)
	}
	same := func(what, list string, have []string) {
		want := strings.Fields(list)
		sort.Strings(want)
		sort.Strings(have)
		if strings.Join(want, " ") != strings.Join(have, " ") {
			t.Errorf("%s: classified\n  %s\nthe parser has\n  %s", what, strings.Join(have, " "), strings.Join(want, " "))
		}
	}
	var stmts, alters []string
	for k, class := range stmtKinds {
		stmts = append(stmts, k)
		if class != kindReads && class != kindPasses && class != kindRefused {
			t.Errorf("%s has no class", k)
		}
	}
	for k := range alterKinds {
		alters = append(alters, k)
	}
	same("statement kinds", parserStatementKinds, stmts)
	same("ALTER TABLE actions", parserAlterKinds, alters)

	// A kind the table does not have, and one the parser never gives for
	// text, fail the reading.
	for _, kind := range []string{"NewFangledStmt", "RawStmt", "PLAssignStmt"} {
		r := &reader{}
		r.visit(kind, jnode{})
		if r.err == nil {
			t.Errorf("a %s in a tree was read without complaint", kind)
		}
	}
	r := &reader{}
	r.visit("AlterTableStmt", jnode{"relation": jnode{"schemaname": "s", "relname": "t"},
		"cmds": []any{jnode{"AlterTableCmd": jnode{"subtype": "AT_NewFangled"}}}})
	if r.err == nil {
		t.Error("an ALTER TABLE action that is not classified was read without complaint")
	}
	// A statement held inside another is classified where it stands.
	r = &reader{}
	r.walk(jnode{"ExplainStmt": jnode{"query": jnode{"NewFangledStmt": jnode{}}}})
	if r.err == nil {
		t.Error("an unclassified statement inside EXPLAIN was read without complaint")
	}
}

// What only a parser reads: the statement inside another statement, the
// body in its own language, the write that destroys.
func TestWritesTheParseTreeShows(t *testing.T) {
	for name, sql := range map[string]string{
		"EXPLAIN ANALYZE":                 `EXPLAIN ANALYZE UPDATE aveloxis_data.repos SET repo_git = 'x'`,
		"PREPARE":                         `PREPARE p AS UPDATE aveloxis_data.repos SET repo_git = 'x'`,
		"a rule's action":                 `CREATE RULE r AS ON INSERT TO aveloxis_data.other DO ALSO UPDATE aveloxis_data.repos SET repo_git = 'x'`,
		"CREATE TABLE AS over a CTE":      `CREATE TABLE aveloxis_data.c AS WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT * FROM w`,
		"a cursor over a CTE":             `DECLARE c CURSOR FOR WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT * FROM w`,
		"a subquery's CTE":                `SELECT * FROM (WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT * FROM w) q`,
		"an SQL function's body":          `CREATE FUNCTION f() RETURNS void LANGUAGE sql AS $$ UPDATE aveloxis_data.repos SET repo_git = 'x' $$`,
		"an SQL-standard body":            `CREATE FUNCTION f() RETURNS void LANGUAGE sql BEGIN ATOMIC UPDATE aveloxis_data.repos SET repo_git = 'x'; END`,
		"a procedure":                     `CREATE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN UPDATE aveloxis_data.repos SET repo_git = 'x'; END $$`,
		"a loop's query":                  `DO $$ DECLARE r record; BEGIN FOR r IN UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1 LOOP NULL; END LOOP; END $$`,
		"a CTE behind INTO":               `DO $$ DECLARE n int; BEGIN WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT count(*) INTO n FROM w; END $$`,
		"an ELSIF arm":                    `DO $$ BEGIN IF FALSE THEN NULL; ELSIF TRUE THEN UPDATE aveloxis_data.repos SET repo_git = 'x'; END IF; END $$`,
		"an exception handler":            `DO $$ BEGIN NULL; EXCEPTION WHEN OTHERS THEN UPDATE aveloxis_data.repos SET repo_git = 'x'; END $$`,
		"a function made in a DO block":   `DO $$ BEGIN EXECUTE 'CREATE FUNCTION f() RETURNS void LANGUAGE sql AS ''UPDATE aveloxis_data.repos SET repo_git = 1'''; END $$`,
		"FOR IN EXECUTE of a string":      `DO $$ DECLARE r record; BEGIN FOR r IN EXECUTE 'UPDATE aveloxis_data.repos SET repo_git = ''x'' RETURNING 1' LOOP NULL; END LOOP; END $$`,
		"FOR IN EXECUTE at run time":      `DO $$ DECLARE r record; BEGIN FOR r IN EXECUTE q LOOP NULL; END LOOP; END $$`,
		"RETURN QUERY EXECUTE":            `CREATE FUNCTION f(q text) RETURNS SETOF int LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY EXECUTE q; END $$`,
		"OPEN FOR EXECUTE":                `DO $$ DECLARE c refcursor; BEGIN OPEN c FOR EXECUTE 'UPDATE ' || 'x'; END $$`,
		"a trigger's NEW with =":          `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN new.repo_git = 'x'; RETURN NEW; END $$`,
		"a trigger's quoted NEW column":   `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW."repo_git" := lower(NEW.repo_git); RETURN NEW; END $$`,
		"SELECT INTO a column of NEW":     `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN SELECT 'x' INTO NEW.repo_git; RETURN NEW; END $$`,
		"the whole NEW row assigned":      `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW := OLD; RETURN NEW; END $$`,
		"SELECT INTO the whole NEW row":   `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN SELECT * INTO NEW FROM aveloxis_data.other; RETURN NEW; END $$`,
		"a body in another language":      `CREATE FUNCTION f() RETURNS void LANGUAGE plpython3u AS $$ plpy.execute("UPDATE aveloxis_data.repos SET repo_git = 1") $$`,
		"a DO block in another language":  `DO LANGUAGE plperl $$ spi_exec_query('UPDATE aveloxis_data.repos SET repo_git = 1'); $$`,
		"a body that does not parse":      `DO $$ BEGIN UPDATE aveloxis_data.repos SET repo_git = ; END $$`,
		"DROP TABLE":                      `DROP TABLE IF EXISTS aveloxis_data.repos`,
		"DROP TABLE, no schema":           `DROP TABLE other, repos CASCADE`,
		"SET EXPRESSION":                  `ALTER TABLE aveloxis_data.repos ALTER COLUMN repo_git SET EXPRESSION AS (lower(repo_name))`,
		"ATTACH PARTITION":                `ALTER TABLE aveloxis_data.repos ATTACH PARTITION aveloxis_data.p DEFAULT`,
		"INHERIT":                         `ALTER TABLE aveloxis_data.repos INHERIT aveloxis_data.p`,
		"a subscript":                     `UPDATE aveloxis_data.repos SET repo_git[1] = 'x'`,
		"a field":                         `UPDATE aveloxis_data.repos SET repo_git.f = 'x'`,
		"a field named as the column":     `UPDATE aveloxis_data.repos SET r.repo_git = 'x'`,
		"a MERGE whose condition guards":  `MERGE INTO aveloxis_data.repos r USING s ON r.repo_id = s.id WHEN MATCHED AND r.repo_git IS NULL THEN UPDATE SET repo_git = $1`,
		"a guard on EXCLUDED":             `INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = $2 WHERE excluded.repo_git IS NULL`,
		"a guard that is IS NOT NULL":     `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE repo_git IS NOT NULL`,
		"a guard against a blank":         `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE COALESCE(repo_git, ' ') = ''`,
		"a guard with <>":                 `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE COALESCE(repo_git, '') <> ''`,
		"a guard of three COALESCE parts": `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE COALESCE(repo_git, $2, '') = ''`,
		"EXCLUDED in quotes is a table":   `INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = COALESCE(NULLIF(repos.repo_git, ''), "EXCLUDED".repo_git)`,
		"COALESCE of three":               `UPDATE aveloxis_data.repos SET repo_git = COALESCE(NULLIF(repo_git, ''), $1, repo_name)`,
		"the stored value in a subquery":  `UPDATE aveloxis_data.repos SET repo_git = COALESCE(NULLIF(repo_git, ''), (SELECT max(o.repo_git) FROM aveloxis_data.repos o))`,
		// Valid on PostgreSQL 18 and not on 17, whose grammar the parser has
		// (round 6: the fixture this replaced was a syntax error on 18 too).
		"a grammar newer than the parser": `UPDATE aveloxis_data.repos SET repo_name = 'x' RETURNING WITH (OLD AS o) o.repo_name`,
		// Round 6: the target must be a base table the catalog knows.
		"a quoted name with capitals":     `UPDATE aveloxis_data."Repos" SET repo_git = 'x'`,
		"an unqualified target":           `UPDATE tmp_pairs SET keep = TRUE`,
		"an unqualified insert":           `INSERT INTO repos (repo_git) VALUES ($1)`,
		"an unqualified DROP":             `DROP TABLE aveloxis_data.other, tmp_pairs`,
		"a temp table of ANOTHER text":    `UPDATE tmp_made_elsewhere SET keep = TRUE`,
		"a temporary VIEW the text made":  `CREATE TEMP VIEW v AS SELECT * FROM aveloxis_data.repos; UPDATE v SET repo_git = 'x'`,
		"a view over another table":       `CREATE VIEW aveloxis_augur_data.v AS SELECT * FROM aveloxis_data.other; UPDATE aveloxis_augur_data.v SET repo_git = 'x'`,
		"a view over a view over it":      `CREATE VIEW s.v1 AS SELECT * FROM aveloxis_data.repos; CREATE VIEW s.v2 AS SELECT repo_git AS z FROM s.v1; UPDATE s.v2 SET z = 'x'`,
		"a relation nobody defined":       `UPDATE aveloxis_data.nobody SET repo_git = 'x'`,
		"a materialized view as a target": `CREATE MATERIALIZED VIEW aveloxis_data.mv AS SELECT 1 AS a; DELETE FROM aveloxis_data.mv`,
		"a foreign table as a target":     `CREATE FOREIGN TABLE s.ft (repo_git text OPTIONS (column_name 'repo_git')) SERVER x; UPDATE s.ft SET repo_git = 'x'`,
		"a table made a view by a rule":   `CREATE TABLE s.t (repo_git text); CREATE RULE "_RETURN" AS ON SELECT TO s.t DO INSTEAD SELECT * FROM aveloxis_data.repos; UPDATE s.t SET repo_git = 'x'`,
		"a view under a new name":         `CREATE VIEW s.v AS SELECT * FROM aveloxis_data.repos; ALTER VIEW s.v RENAME TO v2; UPDATE s.v2 SET repo_git = 'x'`,
		"pg_temp, by name":                `CREATE TEMP TABLE repos (repo_git text); UPDATE pg_temp.repos SET repo_git = 'x'`,
		// Round 6: what changes the table itself.
		"a trigger, with any function": `CREATE TRIGGER tr BEFORE INSERT OR UPDATE OR DELETE ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION t()`,
		"a trigger with a built-in":    `CREATE TRIGGER tg BEFORE INSERT OR UPDATE ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION tsvector_update_trigger(repo_git, 'pg_catalog.english', x)`,
		"a rule on the table":          `CREATE RULE r AS ON UPDATE TO aveloxis_data.repos DO INSTEAD NOTHING`,
		"DETACH PARTITION":             `ALTER TABLE aveloxis_data.repos DETACH PARTITION aveloxis_data.p`,
		"RENAME TO":                    `ALTER TABLE aveloxis_data.repos RENAME TO r2`,
		"the renamed table written":    `ALTER TABLE aveloxis_data.other RENAME TO r2; UPDATE aveloxis_data.r2 SET repo_git = 'x'; ALTER TABLE aveloxis_data.repos RENAME TO r3; UPDATE aveloxis_data.r3 SET repo_git = 'x'`,
		"SET SCHEMA":                   `ALTER TABLE aveloxis_data.repos SET SCHEMA other`,
		"PARTITION OF":                 `CREATE TABLE s.p PARTITION OF aveloxis_data.repos DEFAULT`,
		"INHERITS":                     `CREATE TABLE s.c () INHERITS (aveloxis_data.repos)`,
		"a write of a child":           `CREATE TABLE s.c () INHERITS (aveloxis_data.repos); UPDATE s.c SET repo_git = 'x'`,
		"DROP SCHEMA":                  `DROP SCHEMA aveloxis_data CASCADE`,
		"TRUNCATE":                     `TRUNCATE aveloxis_data.other, aveloxis_data.repos`,
		"a foreign key that cascades":  `ALTER TABLE aveloxis_data.repos ADD CONSTRAINT fk FOREIGN KEY (repo_git) REFERENCES s.o (id) ON UPDATE CASCADE`,
		"a foreign key that sets NULL": `ALTER TABLE aveloxis_data.repos ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES s.o (id) ON DELETE SET NULL (repo_git)`,
		"a column's own foreign key":   `CREATE TABLE aveloxis_data.repos (repo_git text REFERENCES s.o (id) ON DELETE SET DEFAULT)`,
		"two texts as an SQL body":     `CREATE FUNCTION f() RETURNS void LANGUAGE sql AS 'SELECT 1', 'UPDATE aveloxis_data.repos SET repo_git = 1'`,
		"a NUL in the text":            "SELECT 1 --\x00\n; UPDATE aveloxis_data.repos SET repo_git = 'x'",
	} {
		rep := fixtureCheck([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
		if len(rep.Violations) == 0 && len(rep.Unparsed) == 0 {
			t.Errorf("%s: passed\n  %s", name, sql)
		}
	}
}

// And what the parser shows is NOT a write of the column passes.
func TestReadsAndConformingWritesTheParseTreeShows(t *testing.T) {
	for name, sql := range map[string]string{
		"a guard inside a bracketed AND":     `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE (repo_id = $2 AND repo_git IS NULL) AND repo_name <> ''`,
		"a guard written E''":                `UPDATE aveloxis_data.repos SET repo_git = $1 WHERE COALESCE(repo_git, E'') = ''`,
		"EXCLUDED in any case":               `INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = COALESCE(NULLIF(repos.repo_git, ''), Excluded.repo_git)`,
		"the column's name in a string":      `UPDATE aveloxis_data.repos SET repo_git = COALESCE(NULLIF(repo_git, ''), 'repos.repo_git')`,
		"a trigger that assigns another":     `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.repo_name := lower(NEW.repo_git); RETURN NEW; END $$`,
		"a trigger that reads NEW":           `CREATE FUNCTION t() RETURNS trigger LANGUAGE plpgsql AS $$ DECLARE g text; BEGIN g := NEW.repo_git; SELECT NEW.repo_git INTO g; RETURN NEW; END $$`,
		"a body with every kind of step":     `DO $$ DECLARE n int := 0; r record; c CURSOR FOR SELECT 1; BEGIN FOR i IN 1..3 LOOP n := n + i; END LOOP; WHILE n > 0 LOOP n := n - 1; EXIT WHEN n = 1; END LOOP; FOR r IN SELECT repo_git FROM aveloxis_data.repos LOOP RAISE NOTICE '%', r.repo_git; END LOOP; GET DIAGNOSTICS n = ROW_COUNT; PERFORM pg_sleep(0); CASE n WHEN 1 THEN NULL; ELSE NULL; END CASE; EXECUTE 'SELECT 1'; RETURN; END $$`,
		"an SQL function that reads":         `CREATE OR REPLACE FUNCTION f() RETURNS text LANGUAGE sql AS 'SELECT repo_git FROM aveloxis_data.repos LIMIT 1'`,
		"a function with a RETURN body":      `CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql RETURN a + 1`,
		"DROP TABLE of another":              `DROP TABLE aveloxis_data.other`,
		"DROP INDEX":                         `DROP INDEX IF EXISTS "aveloxis_data"."repos"`,
		"RENAME of the table's constraint":   `ALTER TABLE aveloxis_data.repos RENAME CONSTRAINT a TO b`,
		"ALTER of another column's type":     `ALTER TABLE aveloxis_data.repos ALTER COLUMN repo_name TYPE text`,
		"a prepared statement run by name":   `EXECUTE p(1, 2)`,
		"a call":                             `CALL aveloxis_data.proc($1)`,
		"LOCK and SET":                       `LOCK TABLE aveloxis_data.repos IN SHARE MODE; SET LOCAL lock_timeout = '5s'; RESET ALL`,
		"an empty text and a lone comment":   " ; -- nothing\n",
		"VALUES and TABLE":                   `VALUES (1, 'UPDATE'); TABLE aveloxis_data.repos`,
		"the two statements of a rule":       `CREATE RULE r AS ON UPDATE TO aveloxis_data.other DO INSTEAD NOTHING`,
		"a refresh":                          `REFRESH MATERIALIZED VIEW CONCURRENTLY aveloxis_data.mv`,
		"INSERT ... SELECT with ON CONFLICT": `INSERT INTO aveloxis_data.repos SELECT * FROM aveloxis_data.staging ON CONFLICT (repo_id) DO UPDATE SET repo_name = EXCLUDED.repo_name`,
	} {
		rep := fixtureCheck([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
		if len(rep.Violations) != 0 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %+v %+v\n  %s", name, rep.Violations, rep.Unparsed, sql)
		}
	}
}

// A registered column has a writer when a statement of its table gives it
// a value: listed in an INSERT, written by an INSERT that lists no column,
// by a MERGE's INSERT arm, or assigned. Read from the tree, so a quoted
// table, a comment in the head and an INSERT ... SELECT are all seen.
func TestAColumnWithNoWriterIsReported(t *testing.T) {
	for name, tc := range map[string]struct {
		sql     string
		written bool
	}{
		"listed":                {`INSERT INTO aveloxis_data.repos (repo_id, repo_git) VALUES ($1, $2)`, true},
		"not listed":            {`INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1)`, false},
		"no list":               {`INSERT INTO "aveloxis_data"."repos" SELECT * FROM aveloxis_data.staging`, true},
		"a MERGE's INSERT arm":  {`MERGE INTO aveloxis_data.repos r USING s ON r.repo_id = s.id WHEN NOT MATCHED THEN INSERT (repo_id, repo_git) VALUES (s.id, s.g)`, true},
		"assigned, conforming":  {"UPDATE aveloxis_data.repos /* c */ SET repo_git = COALESCE(NULLIF(repo_git, ''), $1)", true},
		"another table":         {`INSERT INTO aveloxis_data.other (repo_git) VALUES ($1)`, false},
		"only read":             {`SELECT repo_git FROM aveloxis_data.repos WHERE repo_git = 'INSERT INTO aveloxis_data.repos (repo_git)'`, false},
		"deleted, never set":    {`DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, false},
		"a conflict's other":    {`INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_name = $2`, false},
		"a conflict's this one": {`INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = COALESCE(NULLIF(repos.repo_git, ''), $2)`, true},
	} {
		rep := fixtureCheck([]Stmt{{File: "f.go", SQL: tc.sql}}, repoGit, nil)
		if len(rep.Violations) != 0 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %+v %+v", name, rep.Violations, rep.Unparsed)
		}
		if got := len(rep.Unwritten) == 0; got != tc.written {
			t.Errorf("%s: written = %v, want %v\n  %s", name, got, tc.written, tc.sql)
		}
	}
}

// Bodies inside bodies are read a fixed number deep, and no deeper.
func TestBodiesNestedTooDeepAreNotRead(t *testing.T) {
	sql := `SELECT 1`
	for i := 0; i < maxBodyNesting+2; i++ {
		sql = `DO ` + "$b" + strings.Repeat("x", i) + "$ BEGIN EXECUTE '" + strings.ReplaceAll(sql, "'", "''") + "'; END $b" + strings.Repeat("x", i) + "$"
	}
	if _, err := ParseWrites(sql); err == nil || !strings.Contains(err.Error(), "deep") {
		t.Errorf("a body nested %d deep: %v", maxBodyNesting+2, err)
	}
	ws, err := ParseWrites(`DO $a$ BEGIN EXECUTE 'DO $b$ BEGIN UPDATE s.t SET c = 1; END $b$'; END $a$`)
	if err != nil || len(ws) != 1 || ws[0].Qualified() != "s.t" {
		t.Errorf("a body two deep: %+v %v", ws, err)
	}
}
