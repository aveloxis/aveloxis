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
		"a grammar newer than the parser": `UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING old.repo_git, new.repo_git WITH (OLD AS o)`,
	} {
		rep := Check([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
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
		"DROP TABLE of another":              `DROP TABLE aveloxis_data.other, tmp_pairs`,
		"DROP INDEX":                         `DROP INDEX IF EXISTS "aveloxis_data"."repos"`,
		"RENAME of the table's constraint":   `ALTER TABLE aveloxis_data.repos RENAME CONSTRAINT a TO b`,
		"ALTER of another column's type":     `ALTER TABLE aveloxis_data.repos ALTER COLUMN repo_name TYPE text`,
		"DETACH PARTITION":                   `ALTER TABLE aveloxis_data.repos DETACH PARTITION aveloxis_data.p`,
		"a prepared statement run by name":   `EXECUTE p(1, 2)`,
		"a call":                             `CALL aveloxis_data.proc($1)`,
		"LOCK and SET":                       `LOCK TABLE aveloxis_data.repos IN SHARE MODE; SET LOCAL lock_timeout = '5s'; RESET ALL`,
		"an empty text and a lone comment":   " ; -- nothing\n",
		"VALUES and TABLE":                   `VALUES (1, 'UPDATE'); TABLE aveloxis_data.repos`,
		"the two statements of a rule":       `CREATE RULE r AS ON UPDATE TO aveloxis_data.other DO INSTEAD NOTHING`,
		"a refresh":                          `REFRESH MATERIALIZED VIEW CONCURRENTLY aveloxis_data.mv`,
		"INSERT ... SELECT with ON CONFLICT": `INSERT INTO aveloxis_data.repos SELECT * FROM aveloxis_data.staging ON CONFLICT (repo_id) DO UPDATE SET repo_name = EXCLUDED.repo_name`,
	} {
		rep := Check([]Stmt{{File: "f.go", SQL: sql}}, repoGit, nil)
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
		"no schema":             {`INSERT INTO repos (repo_git) VALUES ($1)`, false},
		"deleted, never set":    {`DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, false},
		"a conflict's other":    {`INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_name = $2`, false},
		"a conflict's this one": {`INSERT INTO aveloxis_data.repos (repo_id) VALUES ($1) ON CONFLICT (repo_id) DO UPDATE SET repo_git = COALESCE(NULLIF(repos.repo_git, ''), $2)`, true},
	} {
		rep := Check([]Stmt{{File: "f.go", SQL: tc.sql}}, repoGit, nil)
		if len(rep.Violations) != 0 || len(rep.Unparsed) != 0 {
			t.Errorf("%s: %+v %+v", name, rep.Violations, rep.Unparsed)
		}
		if got := len(rep.Unwritten) == 0; got != tc.written {
			t.Errorf("%s: written = %v, want %v\n  %s", name, got, tc.written, tc.sql)
		}
	}
}

// Two texts are one statement but for their digits when PostgreSQL's
// parser gives them one tree but for the digits (the numbered reading of
// strict.go stands on this).
func TestTreeShapeIsTheStatementWithoutItsDigits(t *testing.T) {
	shape := func(sql string) string {
		s, ok := treeShape(sql)
		if !ok {
			return "refused"
		}
		return s
	}
	for _, same := range [][2]string{
		{`SAVEPOINT sp_1`, `SAVEPOINT sp_99999`},
		{`SELECT a FROM t LIMIT 1`, `SELECT a FROM t LIMIT 123456789012345678901234567890`},
		{`SELECT a FROM t LIMIT 0`, `SELECT a FROM t LIMIT 7`},
		{`SELECT $1, 'row 3'`, `SELECT $12, 'row 44'`},
		{`SELECT 1 -- 5`, `SELECT 2 /* 66 */`},
		{`SELECT -1`, `SELECT -22`},
	} {
		if a, b := shape(same[0]), shape(same[1]); a != b || a == "refused" {
			t.Errorf("%q and %q must be one shape:\n  %s\n  %s", same[0], same[1], a, b)
		}
	}
	for _, differ := range [][2]string{
		{`SELECT a FROM t LIMIT 1`, `SELECT a FROM t LIMIT -1`},
		{`SELECT x - 1`, `SELECT x --1`},
		{`SAVEPOINT sp_1`, `SAVEPOINT sp_-1`},
		{`SELECT 1`, `SELECT 1; DROP TABLE t`},
		{`SELECT a1`, `SELECT ab`},
		{`SELECT 1`, `SELECT '1'`},
		{`SELECT 1 FROM t`, `SELECT 1 FROM u`},
		{`SELECT 'a' || 1`, `SELECT 'a' | 1`},
		{`SELECT 1`, `SELECT 1 +`},
	} {
		if a, b := shape(differ[0]), shape(differ[1]); a == b {
			t.Errorf("%q and %q must differ, and both are\n  %s", differ[0], differ[1], a)
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
