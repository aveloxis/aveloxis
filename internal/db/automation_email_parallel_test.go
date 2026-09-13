// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// automation_email_parallel_test.go — v0.29.8: aveloxis_data.is_automation_email
// cost every query that filters on it two ways at once (diagnosed from
// a stalled aveloxis-analytics build-network-gi run, 2026-09-12):
//
//  1. Declared without a PARALLEL marker, so Postgres defaulted it to
//     PARALLEL UNSAFE (proparallel = 'u', confirmed on both chaoss.tv
//     databases) and every query calling it ran single-threaded.
//  2. Its list-address EXISTS compared lower(rgls_email) with no index
//     on that expression, so every call sequentially scanned the ~800
//     repo_groups_list_serve rows, applying lower() to each.
//
// Measured on a 3M-row synthetic table with the real function body
// (local PG 18, summary/changelog/v0.29.md): unsafe + unindexed 340 s;
// PARALLEL SAFE alone 78 s; the lower() index alone cut the serial
// per-call cost 25× (114 µs → 4.5 µs); both together 3.3 s. Either fix
// alone leaves the query one to two orders of magnitude slow, so both
// are pinned here.

package db

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// sqlFunctionStart finds the start of a CREATE [OR REPLACE] FUNCTION
// statement; group 1 is the (possibly qualified) name.
var sqlFunctionStart = regexp.MustCompile(`(?i)\bCREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([\w."]+)\s*\(`)

var (
	parallelMarker = regexp.MustCompile(`(?i)\bPARALLEL\s+(SAFE|RESTRICTED|UNSAFE)\b`)
	dollarQuoteTag = regexp.MustCompile(`^\$([A-Za-z_][A-Za-z_0-9]*)?\$`)
	beginAtomic    = regexp.MustCompile(`(?i)^BEGIN\s+ATOMIC\b`)
)

// sqlFunctionDecl is one function statement: its name and its text with
// every quoted body and string literal removed — what is left is the
// signature and the attribute list, wherever in the statement the
// attributes were written (before or after the body; both are legal).
type sqlFunctionDecl struct {
	name, attrs string
}

// sqlFunctionDecls extracts every function statement from comment-free
// SQL. Each statement runs from CREATE … FUNCTION to its terminating `;`
// outside quotes (or the end of the input): `$$` / `$tag$` bodies and
// single-quoted string bodies (doubled-quote escapes included) are skipped,
// and a BEGIN ATOMIC body ends the attribute list (the SQL-standard
// body is always last). Scanning to the real terminator is what stops
// one statement's search from running into the NEXT statement's
// attributes (fresh-context review of this gate: a lazy regex to the
// first `AS $` let an unmarked single-quoted function borrow a later
// function's marker, and missed markers written after the body).
func sqlFunctionDecls(sql string) []sqlFunctionDecl {
	var out []sqlFunctionDecl
	for _, loc := range sqlFunctionStart.FindAllStringSubmatchIndex(sql, -1) {
		name := sql[loc[2]:loc[3]]
		var attrs strings.Builder
		i := loc[0]
	scan:
		for i < len(sql) {
			switch c := sql[i]; {
			case c == ';':
				break scan
			case c == '\'':
				j := i + 1
				for j < len(sql) {
					if sql[j] == '\'' {
						if j+1 < len(sql) && sql[j+1] == '\'' {
							j += 2
							continue
						}
						break
					}
					j++
				}
				i = j + 1
			case c == '$':
				if m := dollarQuoteTag.FindString(sql[i:]); m != "" {
					if end := strings.Index(sql[i+len(m):], m); end >= 0 {
						i += len(m) + end + len(m)
					} else {
						i = len(sql)
					}
					continue
				}
				attrs.WriteByte(c)
				i++
			case beginAtomic.MatchString(sql[i:]):
				break scan
			default:
				attrs.WriteByte(c)
				i++
			}
		}
		out = append(out, sqlFunctionDecl{name: name, attrs: attrs.String()})
	}
	return out
}

// TestSQLFunctionDeclsScanner guards the gate's own scanner: every shape
// below was either legal-and-missed or a false verdict under the first
// version of the gate. A scanner that silently examines nothing makes
// the gate vacuous, so each fixture pins both the count and the verdict.
func TestSQLFunctionDeclsScanner(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		wantMarked []bool // per declaration, in order
	}{
		{"marker before a $$ body", `CREATE OR REPLACE FUNCTION a.f(x TEXT) RETURNS BOOLEAN LANGUAGE sql STABLE PARALLEL SAFE AS $$ SELECT true $$;`, []bool{true}},
		{"marker after a $$ body", `CREATE FUNCTION a.f(x TEXT) RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql PARALLEL SAFE;`, []bool{true}},
		{"no marker", `create function a.f() returns int language sql stable as $$ select 1 $$;`, []bool{false}},
		{"marker only inside the body does not count", `CREATE FUNCTION a.f() RETURNS text LANGUAGE sql AS $$ SELECT 'PARALLEL SAFE' $$;`, []bool{false}},
		{"tagged body containing $$ and ;", `CREATE FUNCTION a.f() RETURNS int LANGUAGE plpgsql PARALLEL RESTRICTED AS $fn$ BEGIN PERFORM $$x$$; RETURN 1; END $fn$;`, []bool{true}},
		{"unmarked single-quoted body does not borrow the next statement's marker",
			`CREATE FUNCTION a.q() RETURNS int LANGUAGE sql AS 'SELECT 1; SELECT ''PARALLEL SAFE''';
			 CREATE FUNCTION a.r() RETURNS int LANGUAGE sql PARALLEL SAFE AS $$ SELECT 2 $$;`, []bool{false, true}},
		{"BEGIN ATOMIC body", `CREATE FUNCTION a.f(x int) RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE BEGIN ATOMIC SELECT x; END;`, []bool{true}},
		{"BEGIN ATOMIC body without marker", `CREATE FUNCTION a.f(x int) RETURNS int LANGUAGE sql BEGIN ATOMIC SELECT 1 AS "PARALLEL SAFE"; END;`, []bool{false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decls := sqlFunctionDecls(tc.sql)
			if len(decls) != len(tc.wantMarked) {
				t.Fatalf("examined %d declarations, want %d", len(decls), len(tc.wantMarked))
			}
			for i, d := range decls {
				if got := parallelMarker.MatchString(d.attrs); got != tc.wantMarked[i] {
					t.Errorf("declaration %d (%s): marked = %v, want %v (attrs %q)", i, d.name, got, tc.wantMarked[i], d.attrs)
				}
			}
		})
	}
}

// TestSQLFunctionsDeclareParallelMarker — every SQL function aveloxis
// declares states its parallel safety explicitly. Postgres's default is
// UNSAFE, which silently forces every query that calls the function to
// a serial plan; an omitted marker is how is_automation_email shipped.
// A function that genuinely writes or touches session state says
// PARALLEL UNSAFE out loud; nothing takes the default by omission.
//
// Corpus: the three embedded DDL files migrate executes (schema.sql,
// views.sql, matviews.sql — comment-stripped) and every backtick literal
// in the comment-stripped non-test Go sources of internal/db (where
// migrations live). Comments are stripped first as in sqlscan's corpus
// (TestSQLCorpusStripsGoCommentsFirst), and srctest.BacktickLiterals
// tokenizes Go (v0.29.8), so a backtick inside a comment, an interpreted
// string or a rune literal cannot shift the pairing and silently skip
// every later literal in the file. Documented blind spot,
// shared with srctest.BacktickLiterals: DDL assembled by concatenation or
// fmt.Sprintf is invisible.
func TestSQLFunctionsDeclareParallelMarker(t *testing.T) {
	corpus := map[string][]string{}
	for _, f := range []string{"internal/db/schema.sql", "internal/db/views.sql", "internal/db/matviews.sql"} {
		corpus[f] = []string{srctest.StripSQLComments(srctest.Read(t, f))}
	}
	for path, src := range srctest.PackageFiles(t, "internal/db", 20) {
		for _, lit := range srctest.BacktickLiterals(srctest.StripGoComments(src)) {
			corpus[path] = append(corpus[path], srctest.StripSQLComments(strings.Trim(lit, "`")))
		}
	}
	examined := 0
	var automation *sqlFunctionDecl
	for path, sqls := range corpus {
		for _, sql := range sqls {
			for _, d := range sqlFunctionDecls(sql) {
				examined++
				if !parallelMarker.MatchString(d.attrs) {
					t.Errorf("%s: SQL function %s has no PARALLEL marker — Postgres defaults it to PARALLEL UNSAFE and every query calling it loses parallelism; declare PARALLEL SAFE (pure reads) or say UNSAFE explicitly", path, d.name)
				}
				if path == "internal/db/schema.sql" && d.name == "aveloxis_data.is_automation_email" {
					found := d
					automation = &found
				}
			}
		}
	}
	if automation == nil {
		t.Fatal("the scanner found no aveloxis_data.is_automation_email statement in schema.sql")
	}
	srctest.MinCount(t, "SQL function declarations examined", examined, 1)
	// STABLE stays (the repo_groups_list_serve read can change between
	// statements); SAFE is legal for a function that only reads tables.
	if !srctest.ContainsNormalized(automation.attrs, "LANGUAGE sql STABLE PARALLEL SAFE") {
		t.Errorf("is_automation_email must be declared LANGUAGE sql STABLE PARALLEL SAFE; attributes were %q", srctest.NormalizeWS(automation.attrs))
	}
}

// TestAutomationEmailListLookupIsIndexed — the list-address clause's
// lower(rgls_email) must match an expression index, built by migrate
// CONCURRENTLY like its sibling idx_rgls_group_email (SR-2 shape) and
// never declared in schema.sql. The index is only usable while the
// function body spells the comparison with the same expression, so
// both sides are pinned together.
func TestAutomationEmailListLookupIsIndexed(t *testing.T) {
	schema := srctest.StripSQLComments(srctest.Read(t, "internal/db/schema.sql"))
	if !srctest.ContainsNormalized(schema, "WHERE lower(rgls_email) = lower(addr)") {
		t.Fatal("is_automation_email's list clause must compare lower(rgls_email) = lower(addr) — the spelling idx_rgls_email_lower indexes")
	}
	if strings.Contains(schema, "idx_rgls_email_lower") {
		t.Error("idx_rgls_email_lower must be migration-owned (CREATE INDEX CONCURRENTLY in migrate.go), not declared in schema.sql (SR-2)")
	}
	migrate := srctest.StripGoComments(srctest.Read(t, "internal/db/migrate.go"))
	want := "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_rgls_email_lower ON aveloxis_data.repo_groups_list_serve (lower(rgls_email))"
	if !srctest.ContainsNormalized(migrate, want) {
		t.Errorf("migrate.go must build %q", want)
	}
	if !srctest.ContainsNormalized(migrate, `execCreateIndexConcurrently(ctx, pg, logger, errs, "aveloxis_data", "idx_rgls_email_lower",`) {
		t.Error("idx_rgls_email_lower must go through execCreateIndexConcurrently (drops an INVALID leftover of an interrupted build first)")
	}
}

// TestAutomationEmailParallelSafeAndIndexedLive — the behavior, against
// a migrated database: pg_proc says SAFE, the planner actually admits
// the function into a parallel plan (debug_parallel_query forces a
// Gather for any parallel-safe query and cannot for an unsafe one), and
// at production list-table size the planner chooses the expression
// index for the function's lookup under a generic plan (the shape a SQL
// function body is executed with).
func TestAutomationEmailParallelSafeAndIndexedLive(t *testing.T) {
	store, ctx := sbConnect(t)

	var par string
	if err := store.pool.QueryRow(ctx, `
		SELECT p.proparallel::text FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = 'aveloxis_data' AND p.proname = 'is_automation_email'`).Scan(&par); err != nil {
		t.Fatal(err)
	}
	if par != "s" {
		t.Errorf("pg_proc.proparallel for is_automation_email = %q, want 's' (SAFE) — 'u' forces every calling query to a serial plan", par)
	}

	explain := func(setup []string, sql string, args ...any) string {
		t.Helper()
		tx, err := store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, s := range setup {
			if _, err := tx.Exec(ctx, s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
		rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+sql, args...)
		if err != nil {
			t.Fatalf("EXPLAIN %s: %v", sql, err)
		}
		defer rows.Close()
		var b strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			b.WriteString(line + "\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}

	plan := explain([]string{`SET LOCAL debug_parallel_query = on`},
		`SELECT count(*) FROM aveloxis_data.repo_groups_list_serve WHERE NOT aveloxis_data.is_automation_email(rgls_email)`)
	if !strings.Contains(plan, "Gather") {
		t.Errorf("with debug_parallel_query = on a parallel-safe query gets a Gather node; none in:\n%s(is_automation_email is still forcing a serial plan)", plan)
	}

	// Production-shaped list table: ~800 rows (chaoss.tv holds 795 and
	// 813), ANALYZEd, and NO enable_* overrides — the assertion is that
	// the planner CHOOSES the expression index at production size, not
	// that it can be forced onto it. On an empty table any index-only
	// scan ties and the answer would be statistics noise.
	const seedGroup = 944147301
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_groups (repo_group_id, rg_name)
		VALUES ($1, '_avparallel-group') ON CONFLICT (repo_group_id) DO NOTHING`, seedGroup)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_groups_list_serve (repo_group_id, rgls_email)
		SELECT $1, 'List' || g || '@avparallel' || (g % 300) || '.apache.org' FROM generate_series(1, 800) g
		ON CONFLICT DO NOTHING`, seedGroup)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_groups_list_serve WHERE repo_group_id = $1`, seedGroup)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_groups WHERE repo_group_id = $1`, seedGroup)
	})
	mustExecRetry(ctx, t, store, `ANALYZE aveloxis_data.repo_groups_list_serve`)

	// PREPARE + EXECUTE under force_generic_plan: a parameter bound
	// through the extended protocol is folded into a literal (a custom
	// plan), which is not how the function body's lower(addr) is
	// planned. A dedicated connection, so the session-scoped prepared
	// statement is deallocated on the connection that made it.
	conn, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	for _, s := range []string{
		`SET plan_cache_mode = force_generic_plan`,
		`PREPARE avrgls_lookup(text) AS SELECT 1 FROM aveloxis_data.repo_groups_list_serve WHERE lower(rgls_email) = lower($1)`,
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	defer func() {
		for _, s := range []string{`DEALLOCATE avrgls_lookup`, `RESET plan_cache_mode`} {
			_, _ = conn.Exec(ctx, s)
		}
	}()
	rows, err := conn.Query(ctx, `EXPLAIN (COSTS OFF) EXECUTE avrgls_lookup('Dev@Example.Apache.org')`)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		b.WriteString(line + "\n")
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan = b.String()
	if !strings.Contains(plan, "lower($1)") {
		t.Fatalf("expected a generic plan (lower($1) in the condition); got:\n%s", plan)
	}
	if !strings.Contains(plan, "idx_rgls_email_lower") || !strings.Contains(plan, "Index Cond: (lower(rgls_email) = lower($1))") {
		t.Errorf("at production size the list-address lookup must be an index probe on idx_rgls_email_lower (Index Cond on lower(rgls_email)), not a scan with a Filter; plan:\n%s", plan)
	}
}
