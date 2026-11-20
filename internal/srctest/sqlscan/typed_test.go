// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"regexp"
	"strings"
	"testing"
)

// 0.29.211 (review round 4): the reader resolves by TYPE. These are the
// cases a reader that goes by spelling cannot decide; each is type-checked
// against the real libraries, from the module's root.

const typedRoot = "../../.."

const probePrelude = `import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	_ = fmt.Sprint
	_ = strings.Join
	_ context.Context
	_ *sql.DB
	_ pgx.Tx
	_ *pgxpool.Pool
	_ *pgconn.PgConn
	_ = os.Getenv
	_ reflect.Value
	_ = slog.Info
)
`

var probeDecls = map[string]string{
	"pool":     "type pool = P\n",
	"P":        "type P interface {\n\tExec(ctx context.Context, sql string, args ...any) error\n\tQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error)\n\tQueryRow(ctx context.Context, sql string, args ...any) pgx.Row\n\tSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults\n\tPrepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)\n\tCopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)\n}\n",
	"job":      "type job struct {\n\tsql, one, two, three, where, table, col, name string\n\tlist []string\n\tn int\n}\n",
	"S":        "type S struct{ pool *pgxpool.Pool; db *sql.DB; logger *slog.Logger }\n",
	"S.run":    "func (s *S) run(ctx context.Context, q string, args ...any) { s.pool.Exec(ctx, q, args...) }\n",
	"each":     "func each(a ...any) {}\n",
	"runner":   "type runner interface{ Run(ctx context.Context, sql string) }\n",
	"globalTx": "var globalTx P\n",
	"build":    "func build(a ...any) string { return os.Getenv(\"B\") }\n",
	"step":     "type step struct{ name, sql string }\n",
	"pick":     "func pick(s []step) []step { return s }\n",
	"Step":     "type Step struct{ SQL string }\n",
	"migStep":  "type migStep struct{ name, sql string }\n",
	"errText":  "func errText(any) string { return \"\" }\n",
	"log":      "var log = slog.Default()\n",
}

// typedFiles is src as a file of a package that compiles: the prelude's
// imports, and each stand-in declaration src does not declare itself.
func typedFiles(src string) map[string]string {
	pre := probePrelude
	for n, d := range probeDecls {
		// A stand-in is added only where src uses the name and does not
		// declare it: one that nothing uses would itself be reported (a
		// helper nobody calls), and every case would "fail" for it.
		used := regexp.MustCompile(`\b` + regexp.QuoteMeta(n) + `\b`).MatchString(src)
		switch n {
		case "P":
			used = true
		case "S.run":
			used = strings.Contains(src, "s.run(") && !strings.Contains(src, ") run(")
		}
		if used && !regexp.MustCompile(`(?m)^(type|func|var|const) `+regexp.QuoteMeta(n)+`\b`).MatchString(src) {
			pre += d
		}
	}
	return map[string]string{"internal/db/x.go": "package db\n\n" + pre + src}
}

// typedViolations runs the chain a store's test runs, typed.
func typedViolations(t *testing.T, src string) (n int, detail string) {
	t.Helper()
	files := typedFiles(src)
	whole, unread, err := AssembleTyped(typedRoot, files)
	if err != nil {
		t.Fatalf("%v\n%s", err, src)
	}
	rep := CheckUnreadable(unread, round2Registry, nil, nil, nil)
	stmts := append(append(Statements(files), whole...), rep.Statements...)
	chk := Check(stmts, round2Registry, nil)
	var b strings.Builder
	for _, u := range rep.Violations {
		b.WriteString("unread: " + strings.Join(strings.Fields(u.Text), " ") + " (" + u.Why + ")\n")
	}
	for _, v := range chk.Violations {
		b.WriteString("policy: " + v.Expr + "\n")
	}
	b.WriteString("whole: " + sqlOf(whole))
	return len(rep.Violations) + len(chk.Violations), b.String()
}

// Every one of these runs SQL nobody can read, or writes repos.repo_git
// with a forbidden value, in a way a reader by spelling passed. Each states
// the ONE thing it must be reported for (0.29.212): a case that is
// reported for another reason only (a stand-in, a second rule that happens
// to fire) fails, because it would go on passing with its own rule gone.
func TestTypedNothingUnreadPasses(t *testing.T) {
	// The harness itself is clean: a case fails for what it holds, never
	// for the stand-ins around it.
	if n, detail := typedViolations(t, "func f(ctx context.Context, tx pool, s *S, j job) {\n\ttx.Exec(ctx, `SELECT 1`)\n\ts.run(ctx, `SELECT 2`)\n\t_, _ = j, build()\n}"); n != 0 {
		t.Fatalf("the stand-in declarations are themselves reported:\n%s", detail)
	}
	const jsql = "the value j.sql, which is not declared in this package; and it is the whole SQL argument"
	const forbidden = "policy: 'x'"
	for name, c := range map[string]struct{ src, why string }{
		"a runner taken as a method value":      {"func f(ctx context.Context, tx pool, j job) {\n\trun := tx.Query\n\trun(ctx, j.sql)\n}", "tx.Query: something that runs SQL is taken as a value"},
		"a helper taken as a value":             {"func (s *S) q(ctx context.Context, text string) { s.pool.Query(ctx, text) }\nfunc (s *S) f(ctx context.Context, j job) {\n\tg := s.q\n\tg(ctx, j.sql)\n}", "s.q: something that runs SQL is taken as a value"},
		"a helper passed as a callback":         {"func (s *S) q(ctx context.Context, text string) { s.pool.Query(ctx, text) }\nfunc (s *S) f(ctx context.Context, j job) { each(j.list, s.q) }", "s.q: something that runs SQL is taken as a value"},
		"a method expression":                   {"func f(ctx context.Context, p *pgxpool.Pool, j job) { (*pgxpool.Pool).Exec(p, ctx, j.sql) }", "written as a method expression"},
		"a queued query written as a literal":   {"func f(j job) *pgx.QueuedQuery { return &pgx.QueuedQuery{SQL: j.sql} }", jsql},
		"a queued query's field assigned":       {"func f(q *pgx.QueuedQuery, j job) { q.SQL = j.sql }", jsql},
		"a forbidden write in a queued literal": {"func f() *pgx.QueuedQuery { return &pgx.QueuedQuery{SQL: \"UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = 1\"} }", forbidden},
		"the low-level connection":              {"func f(ctx context.Context, c *pgx.Conn, j job) { c.PgConn().ExecParams(ctx, j.sql, nil, nil, nil, nil) }", jsql},
		"database/sql":                          {"func f(ctx context.Context, d *sql.DB, j job) { d.ExecContext(ctx, j.sql) }", jsql},
		"database/sql without a context":        {"func f(d *sql.DB, j job) { d.Exec(j.sql) }", jsql},
		"a transaction's begin text":            {"func f(ctx context.Context, c *pgx.Conn, j job) { c.BeginTx(ctx, pgx.TxOptions{BeginQuery: j.sql}) }", jsql},
		"a one-line function that runs (S4-1)":  {"var thePool pool\nfunc setGit(v string) error { return thePool.Exec(nil, \"UPDATE aveloxis_data.repos SET repo_git = \"+v) }\nfunc g() { setGit(\"'x'\") }", forbidden},
		"the same, nothing fixed":               {"var thePool pool\nfunc run(v string) error { return thePool.Exec(nil, strings.ToUpper(v)) }\nfunc g(j job) { run(j.sql) }", "the call strings.ToUpper"},
		"a helper two calls deep":               {"func a(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }\nfunc b(ctx context.Context, tx pool, q string) { a(ctx, tx, q) }\nfunc c(ctx context.Context, tx pool, j job) { b(ctx, tx, j.sql) }", jsql},
		"a helper held in a variable":           {"var run = func(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }\nfunc f(ctx context.Context, tx pool, j job) { run(ctx, tx, j.sql) }", jsql},
		"a helper held in a field":              {"type holder struct{ run func(context.Context, string) }\nfunc mk(tx pool) holder { return holder{run: func(ctx context.Context, q string) { tx.Exec(ctx, q) }} }\nfunc f(ctx context.Context, tx pool, j job) { mk(tx).run(ctx, j.sql) }", jsql},
		"a closure a function returns":          {"func mk(tx pool) func(context.Context, string) { return func(ctx context.Context, q string) { tx.Exec(ctx, q) } }\nfunc f(ctx context.Context, tx pool, j job) { mk(tx)(ctx, j.sql) }", jsql},
		"a helper nobody in the package calls":  {"func orphan(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }", "the parameter q; and it is the whole SQL argument"},
		"an exported helper":                    {"func Run(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }\nfunc f(ctx context.Context, tx pool) { Run(ctx, tx, `SELECT 1`) }", "an exported function hands a parameter of its own"},
		"the package's own interface":           {"type doer interface{ do(ctx context.Context, q string) }\ntype impl struct{ p pool }\nfunc (r impl) do(ctx context.Context, q string) { r.p.Exec(ctx, q) }\nfunc f(ctx context.Context, r doer, j job) { r.do(ctx, j.sql) }", jsql},
		"rows copied with no SQL text":          {"func f(ctx context.Context, p *pgxpool.Pool, rows [][]any) { p.CopyFrom(ctx, pgx.Identifier{\"aveloxis_data\", \"repos\"}, []string{\"repo_git\"}, pgx.CopyFromRows(rows)) }", "CopyFrom writes rows with no SQL text"},
		"the same through an interface":         {"func f(ctx context.Context, tx pool, rows [][]any) { tx.CopyFrom(ctx, pgx.Identifier{\"aveloxis_data\", \"repos\"}, []string{\"repo_git\"}, pgx.CopyFromRows(rows)) }", "an interface's CopyFrom"},
		"a call by reflection":                  {"func f(ctx context.Context, tx pool, j job) { reflect.ValueOf(tx).MethodByName(\"Exec\").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(j.sql)}) }", "reflect.MethodByName calls a function or method this reader cannot name"},
		"an exported function handed a batch":   {"func Send(ctx context.Context, tx *pgxpool.Pool, b *pgx.Batch) { tx.SendBatch(ctx, b) }", "the exported Send takes a batch"},
		// N08 and N10b (0.29.212): closed, where they were argued.
		"a batch handed to a function nobody calls (N08)":     {"func f(ctx context.Context, tx *pgxpool.Pool, b *pgx.Batch) { tx.SendBatch(ctx, b) }", "f takes a batch, and not every call of it can be found"},
		"a batch handed to a function taken as a value":       {"func f(ctx context.Context, tx *pgxpool.Pool, b *pgx.Batch) { tx.SendBatch(ctx, b) }\nfunc g(ctx context.Context, tx *pgxpool.Pool) { f(ctx, tx, &pgx.Batch{}) }\nvar keep = f", "f takes a batch, and not every call of it can be found"},
		"text through an interface nothing implements (N10b)": {"func f(ctx context.Context, r runner, j job) { r.Run(ctx, j.sql) }", "through an interface of this package that no type of the package implements"},
		"a package constant shadowed by a local":              {"const stmt = `SELECT 1`\nfunc f(ctx context.Context, tx pool, j job) {\n\tstmt := j.sql\n\ttx.Exec(ctx, stmt)\n}", jsql},
		"a package name shadowed by a local":                  {"type fake struct{}\nfunc (fake) Sprintf(f string, a ...any) string { return a[0].(string) }\nfunc f(ctx context.Context, tx pool, j job) {\n\tfmt := fake{}\n\ttx.Exec(ctx, fmt.Sprintf(`SELECT %s`, j.sql))\n}", "the name fmt is not the package fmt here"},
		"a forbidden write in a table of steps":               {"var steps = []step{{name: \"a\", sql: \"UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = $1\"}}", forbidden},
		"a forbidden write after other text (S4-4)":           {"var note = \"-- fix\\nWITH a AS (SELECT 1) UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = $1\"", forbidden},
		"a forbidden write as a map's value":                  {"var byName = map[string]string{\"a\": \"UPDATE aveloxis_data.repos SET repo_git = 'x'\"}", forbidden},
		"a string formatted with %v is no integer":            {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, fmt.Sprintf(`SELECT repo_id FROM aveloxis_data.repos WHERE %v`, j.sql)) }", "the format verb %v"},
		"a float formatted with %v is no integer":             {"func f(ctx context.Context, tx pool, x float64) { tx.Query(ctx, fmt.Sprintf(`SELECT repo_id FROM aveloxis_data.repos LIMIT %v`, x)) }", "the format verb %v"},
		"a method named like a logger's, on another type":     {"type notes struct{}\nfunc (notes) Info(s string) {}\nfunc f(n notes) { n.Info(\"UPDATE aveloxis_data.repos SET repo_git = 'x'\") }", forbidden},
		"strings.Replace with a count that is no constant":    {"func f(ctx context.Context, tx pool, n int) { tx.Query(ctx, strings.Replace(`SELECT X, X`, `X`, `1`, n)) }", "a strings replacement one part of which is not fixed text"},
		"an argument the format has no verb for":              {"func f(ctx context.Context, tx pool) { tx.Exec(ctx, fmt.Sprintf(\"UPDATE aveloxis_data.repos SET repo_name = 'x'\", \", repo_git = 'y'\")) }", "an argument the format has no verb for"},
		// Review round 4's S4-4, and what the operator asked to see fail
		// (0.29.212): text that reaches a statement THROUGH A PARAMETER.
		// A helper's parameters are bound to what each call gives them, so
		// the statement is read whole with the caller's text in it; and
		// where the caller's text is not fixed, the part is of no verified
		// kind and nothing covers it.
		"a data-modifying CTE handed to a helper's predicate": {"func arm(ctx context.Context, tx pool, pred string) { tx.Query(ctx, `WITH a AS (SELECT 1 WHERE `+pred+`) SELECT 1`) }\nfunc g(ctx context.Context, tx pool) {\n\tarm(ctx, tx, `TRUE`)\n\tarm(ctx, tx, \"TRUE), w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1\")\n}", forbidden},
		"a column type that carries a second statement":       {"func add(ctx context.Context, tx pool, table, col, typ string) { tx.Exec(ctx, `ALTER TABLE `+table+` ADD COLUMN IF NOT EXISTS `+col+` `+typ) }\nfunc g(ctx context.Context, tx pool) {\n\tadd(ctx, tx, `t`, `a`, `TEXT`)\n\tadd(ctx, tx, `t`, `b`, \"TEXT; UPDATE aveloxis_data.repos SET repo_git = 'x'\")\n}", forbidden},
		"the same text through the caller's own parameter":    {"func add(ctx context.Context, tx pool, table, col, typ string) { tx.Exec(ctx, `ALTER TABLE `+table+` ADD COLUMN IF NOT EXISTS `+col+` `+typ) }\nfunc g(ctx context.Context, tx pool, j job) { add(ctx, tx, `t`, `b`, j.one) }", "the value j.one, which is not declared in this package"},
		"a statement a function-typed parameter returns":      {"func run(ctx context.Context, tx pool, text func(string) string) { tx.Query(ctx, text(`a`)) }\nfunc g(ctx context.Context, tx pool) {\n\trun(ctx, tx, func(col string) string { return \"WITH w AS (UPDATE aveloxis_data.repos SET repo_git = 'x' RETURNING 1) SELECT \" + col })\n}", forbidden},
		"an allowlist map that something writes":              {"var sorts = map[string]string{`a`: `x ASC`}\nfunc init() { sorts[`z`] = os.Getenv(`Z`) }\nfunc f(ctx context.Context, tx pool, key string) { tx.Query(ctx, `SELECT x FROM t ORDER BY `+sorts[key]) }", "an expression this reader does not follow (*ast.IndexExpr)"},
		"an allowlist map handed to something":                {"var sorts = map[string]string{`a`: `x ASC`}\nfunc keep(m map[string]string) {}\nfunc init() { keep(sorts) }\nfunc f(ctx context.Context, tx pool, key string) { tx.Query(ctx, `SELECT x FROM t ORDER BY `+sorts[key]) }", "an expression this reader does not follow (*ast.IndexExpr)"},
		"a parameter given another value in the helper":       {"func arm(ctx context.Context, tx pool, pred string, j job) {\n\tpred = j.one\n\ttx.Query(ctx, `SELECT 1 WHERE `+pred)\n}\nfunc g(ctx context.Context, tx pool, j job) { arm(ctx, tx, `TRUE`, j) }", "the value j.one, which is not declared in this package"},
		"a quote-doubled value outside a quoted literal":      {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT 1 WHERE a = `+strings.ReplaceAll(j.one, `'`, `''`)) }", "a value with every quote doubled"},
	} {
		n, detail := typedViolations(t, c.src)
		switch {
		case n == 0:
			t.Errorf("%s: passed with nothing reported\n%s\n%s", name, c.src, detail)
		case !strings.Contains(detail, c.why):
			t.Errorf("%s: reported, and not for its own reason (%q)\n%s\n%s", name, c.why, c.src, detail)
		}
	}
}

// And these are read whole with types, where a reader by spelling had to
// refuse them or guess.
func TestTypedWhatIsReadWhole(t *testing.T) {
	for name, src := range map[string]string{
		"an integer formatted with %d":              "func f(ctx context.Context, tx pool, n int) { tx.Exec(ctx, fmt.Sprintf(`SELECT repo_id FROM aveloxis_data.repos LIMIT %d`, n)) }",
		"a typed integer formatted with %v":         "type count int64\nfunc f(ctx context.Context, tx pool, n count) { tx.Exec(ctx, fmt.Sprintf(`SELECT repo_id FROM aveloxis_data.repos LIMIT %v`, n)) }",
		"Exec on a value that is no database":       "type shell struct{}\nfunc (shell) Exec(ctx context.Context, cmd string) {}\nfunc f(ctx context.Context, sh shell, j job) { sh.Exec(ctx, j.sql) }",
		"Queue on a value that is no batch":         "type work struct{}\nfunc (work) Queue(s string, a ...any) {}\nfunc f(w work, j job) { w.Queue(j.sql) }",
		"a constant through a typed constant":       "type stmt string\nconst a stmt = `SELECT 1`\nfunc f(ctx context.Context, tx pool) { tx.Exec(ctx, string(a)) }",
		"a call through the package's interface":    "type doer interface{ do(ctx context.Context, q string) }\ntype impl struct{ p pool }\nfunc (r impl) do(ctx context.Context, q string) { r.p.Exec(ctx, q) }\nfunc f(ctx context.Context, r doer) { r.do(ctx, `SELECT 1`) }",
		"a helper followed two calls deep":          "func a(ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }\nfunc b(ctx context.Context, tx pool, q string) { a(ctx, tx, q) }\nfunc c(ctx context.Context, tx pool) { b(ctx, tx, `SELECT 1`) }",
		"two variables of one name, each its own":   "func f(ctx context.Context, tx pool, big bool) {\n\tif big {\n\t\tq := `SELECT 1`\n\t\ttx.Exec(ctx, q)\n\t} else {\n\t\tq := `SELECT 2`\n\t\ttx.Exec(ctx, q)\n\t}\n}",
		"a logger's message that names a statement": "func f(v string) { log.Info(\"UPDATE aveloxis_data.repos SET repo_git = 'x' failed\", \"v\", v) }",
		"a panic that names a statement":            "func f() { panic(\"UPDATE aveloxis_data.repos SET repo_git = 'x' failed\") }",
		// 0.29.212: what was registered, and is read whole.
		"a column from an allowlist map, the miss refused": "var sorts = map[string]string{`a`: `x ASC`, `b`: `y DESC`}\nfunc f(ctx context.Context, tx pool, key string) error {\n\tcol, ok := sorts[key]\n\tif !ok {\n\t\treturn nil\n\t}\n\t_, err := tx.Query(ctx, `SELECT x FROM t ORDER BY `+col)\n\treturn err\n}",
		"the same, the miss given a constant":              "var sorts = map[string]string{`a`: `x ASC`, `b`: `y DESC`}\nfunc f(ctx context.Context, tx pool, key string) {\n\tcol, ok := sorts[key]\n\tif !ok {\n\t\tcol = `x ASC`\n\t}\n\ttx.Query(ctx, `SELECT x FROM t ORDER BY `+col)\n}",
		"a lookup written where it is used":                "var sorts = map[string]string{`a`: `x ASC`}\nfunc f(ctx context.Context, tx pool, key string) { tx.Query(ctx, `SELECT x FROM t ORDER BY x`+sorts[key]) }",
		"a direction chosen by an if":                      "func f(ctx context.Context, tx pool, desc bool) {\n\tdir := `ASC`\n\tif desc {\n\t\tdir = `DESC`\n\t}\n\ttx.Query(ctx, `SELECT x FROM t ORDER BY x `+dir)\n}",
		"placeholders built in a loop":                     "func f(ctx context.Context, tx pool, n int) {\n\tvar sb strings.Builder\n\tfor i := 0; i < n; i++ {\n\t\tif i > 0 {\n\t\t\tsb.WriteByte(',')\n\t\t}\n\t\tfmt.Fprintf(&sb, `$%d`, i+1)\n\t}\n\ttx.Query(ctx, `SELECT x FROM t WHERE id IN (`+sb.String()+`)`)\n}",
		"an identifier whose parts are fixed":              "func f(ctx context.Context, tx pool) { tx.Exec(ctx, `UPDATE `+pgx.Identifier{`aveloxis_data`, `repos`}.Sanitize()+` SET repo_git = COALESCE(NULLIF(repo_git, ''), $1)`) }",
		"a helper's parameter, read at each call":          "func arm(ctx context.Context, tx pool, pred string) { tx.Query(ctx, `SELECT 1 WHERE `+pred) }\nfunc g(ctx context.Context, tx pool) {\n\tarm(ctx, tx, `a`)\n\tarm(ctx, tx, `b`)\n}",
		"a function with several returns":                  "func order(by string) string {\n\tswitch by {\n\tcase `n`:\n\t\treturn `name`\n\tcase `d`:\n\t\treturn `date DESC`\n\t}\n\treturn `id`\n}\nfunc f(ctx context.Context, tx pool, by string) { tx.Query(ctx, `SELECT x FROM t ORDER BY `+order(by)) }",
		"two results of one call":                          "func two() (string, string) { return `SELECT 1`, `SELECT 2` }\nfunc f(ctx context.Context, tx pool) {\n\ta, b := two()\n\ttx.Query(ctx, a)\n\ttx.Query(ctx, b)\n}",
		"a batch built and handed on (N08)":                "func send(ctx context.Context, tx *pgxpool.Pool, b *pgx.Batch) { tx.SendBatch(ctx, b) }\nfunc g(ctx context.Context, tx *pgxpool.Pool) {\n\tb := &pgx.Batch{}\n\tb.Queue(`SELECT 1`)\n\tsend(ctx, tx, b)\n}",
		"a logger behind the package's own interface":      "type lg interface{ Info(msg string, args ...any) }\nfunc f(l lg) { l.Info(\"UPDATE aveloxis_data.repos SET repo_git = 'x' failed\") }",
		"a message that names a statement":                 "func f(v string) error { return fmt.Errorf(\"UPDATE aveloxis_data.repos SET repo_git = %s failed\", v) }",
	} {
		if n, detail := typedViolations(t, src); n != 0 {
			t.Errorf("%s: reported, and it is read whole\n%s\n%s", name, src, detail)
		}
	}
}

// The library's classification comes from its method sets: every function,
// method and field of pgx, pgxpool, pgconn and database/sql that takes or
// holds a string is one of the classes, and a NEW one (a library upgrade)
// is unclassified until someone reads it and says which.
func TestEveryLibraryMemberThatTakesTextIsClassified(t *testing.T) {
	entries, err := LibraryEntries(typedRoot)
	if err != nil {
		t.Fatal(err)
	}
	classes := map[string]int{}
	at := map[string]int{}
	for _, e := range entries {
		classes[e.Class]++
		if e.Class == LibUnclassified {
			t.Errorf("%s takes or holds a string and is not classified: read it, then add it to the runners (a parameter named sql or query is found by itself), libraryByName, librarySQLFields, libraryDescribes or libraryNotSQL in typed.go", e.Name)
		}
		if e.Class == LibRunsSQL {
			at[e.Name] = e.Index
		}
	}
	// The SQL position is the library SIGNATURE's, not a table's here
	// (round 4 moved Prepare's index in the table from 2 to 1 and every
	// test passed).
	for name, want := range map[string]int{
		"pgx.Conn.Exec": 1, "pgx.Conn.Query": 1, "pgx.Conn.QueryRow": 1, "pgx.Conn.Prepare": 2, "pgx.Tx.Prepare": 2,
		"pgx.Batch.Queue": 0, "pgxpool.Pool.Exec": 1, "pgxpool.Pool.Query": 1, "pgxpool.Pool.QueryRow": 1,
		"pgconn.PgConn.Exec": 1, "pgconn.PgConn.ExecParams": 1, "pgconn.PgConn.Prepare": 2, "pgconn.Batch.ExecParams": 0,
		"sql.DB.Exec": 0, "sql.DB.ExecContext": 1, "sql.DB.Query": 0, "sql.DB.QueryContext": 1, "sql.DB.QueryRowContext": 1,
		"sql.DB.Prepare": 0, "sql.DB.PrepareContext": 1, "sql.Tx.ExecContext": 1, "sql.Conn.ExecContext": 1,
	} {
		if got, ok := at[name]; !ok || got != want {
			t.Errorf("%s: SQL at parameter %d (known: %v), want %d", name, got, ok, want)
		}
	}
	for _, c := range []string{LibRunsSQL, LibSQLField, LibNoText, LibByName, LibNotSQL} {
		if classes[c] == 0 {
			t.Errorf("no library member is classed %q: the classification is not reading the library", c)
		}
	}
	t.Logf("library members that take or hold a string: %v", classes)
}

// A package that does not compile is not read: its names cannot be
// resolved, and a reading by spelling is what this replaces.
func TestTypedRefusesAPackageThatDoesNotCompile(t *testing.T) {
	if _, _, err := AssembleTyped(typedRoot, typedFiles("func f() { undefinedThing() }")); err == nil {
		t.Error("a package with a type error was read")
	}
}

// A call through an interface of the package is read AT THE CALL: what is
// reported is the argument it was given, by name. (Without the rule the
// implementing method is still reported, as a helper nobody calls, which
// says nothing about the statement.)
func TestTypedACallThroughThePackagesInterfaceIsReadAtTheCall(t *testing.T) {
	_, detail := typedViolations(t, "type doer interface{ do(ctx context.Context, q string) }\ntype impl struct{ p pool }\nfunc (r impl) do(ctx context.Context, q string) { r.p.Exec(ctx, q) }\nfunc f(ctx context.Context, r doer, j job) { r.do(ctx, j.sql) }")
	if !strings.Contains(detail, "j.sql") {
		t.Errorf("the argument j.sql of r.do is not what was reported:\n%s", detail)
	}
}

// `false := true` (S4-20): a condition is what the type checker says its
// name is. With the universe's false the guarded arm is dead and the
// argument is read whole; with a local variable named false it is not
// known, and the argument is reported with the arm's text.
func TestTypedAConditionIsWhatItsNameIs(t *testing.T) {
	body := "\tq := `SELECT 1`\n\tif false {\n\t\tq = `SELECT ` + v\n\t}\n\ttx.Query(ctx, q)\n}"
	if n, detail := typedViolations(t, "func f(ctx context.Context, tx pool, v string) {\n"+body); n != 0 {
		t.Errorf("under the universe's false the arm is dead, and the argument is read whole:\n%s", detail)
	}
	if n, detail := typedViolations(t, "func f(ctx context.Context, tx pool, v string) {\n\tfalse := true\n"+body); n == 0 {
		t.Errorf("under a local variable named false the arm can run, and nothing was reported:\n%s", detail)
	}
	if n, detail := typedViolations(t, "func f(ctx context.Context, tx pool, v string) {\n\ttrue := false\n\tq := `SELECT 1`\n\tif !true {\n\t\tq = `SELECT ` + v\n\t}\n\ttx.Query(ctx, q)\n}"); n == 0 {
		t.Errorf("under a local variable named true, negated: nothing was reported:\n%s", detail)
	}
}

// A shape rests on parts whose KIND the reader verified (0.29.212): an
// identifier from Sanitize, a quote-doubled value between a literal's
// quotes, text a loop repeats. A part of any other origin is covered by no
// shape, whatever the entry says; an identifier in a statement that writes
// rows is covered by none either; and an exception says where its text
// comes from.
func TestAShapeRestsOnVerifiedKinds(t *testing.T) {
	read := func(kinds string) []Shape {
		return []Shape{{Name: "read", Text: regexp.MustCompile(`(?s)^(SELECT|UPDATE)\b.*$`), Reason: "r", Args: []ShapeArg{{Decl: "f", Call: 1, Of: 1, Kinds: kinds}}}}
	}
	for name, c := range map[string]struct {
		src         string
		kinds       []string
		shape       string
		wantCovered bool
	}{
		"an identifier, in a read":                  {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM `+pgx.Identifier{j.table}.Sanitize()) }", []string{KindIdentifier}, KindIdentifier, true},
		"an identifier, in a statement that writes": {"func f(ctx context.Context, tx pool, j job) { tx.Exec(ctx, `UPDATE `+pgx.Identifier{j.table}.Sanitize()+` SET a = 1`) }", []string{KindIdentifier}, KindIdentifier, false},
		"an identifier, under another kind's entry": {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM `+pgx.Identifier{j.table}.Sanitize()) }", []string{KindIdentifier}, KindLiteral, false},
		"a literal": {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM t WHERE b = '`+strings.ReplaceAll(j.one, `'`, `''`)+`'`) }", []string{KindLiteral}, KindLiteral, true},
		"a quote-doubled value with no quotes round": {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM t WHERE b = `+strings.ReplaceAll(j.one, `'`, `''`)) }", []string{KindUnknown}, KindLiteral, false},
		"a list built in a loop":                     {"func f(ctx context.Context, tx pool, j job) {\n\tvar cols []string\n\tfor _, c := range j.list {\n\t\tcols = append(cols, pgx.Identifier{c}.Sanitize())\n\t}\n\ttx.Query(ctx, `SELECT `+strings.Join(cols, `, `)+` FROM t`)\n}", []string{KindIdentifier, KindRepeated}, KindIdentifier + "," + KindRepeated, true},
		"a list built in a loop from unread text":    {"func f(ctx context.Context, tx pool, j job) {\n\tvar cols []string\n\tfor _, c := range j.list {\n\t\tcols = append(cols, c)\n\t}\n\ttx.Query(ctx, `SELECT `+strings.Join(cols, `, `)+` FROM t`)\n}", []string{KindUnknown, KindRepeated}, KindIdentifier + "," + KindRepeated, false},
		"a list something else can change":           {"func f(ctx context.Context, tx pool, j job) {\n\tvar cols []string\n\tfor _, c := range j.list {\n\t\tcols = append(cols, pgx.Identifier{c}.Sanitize())\n\t}\n\teach(cols)\n\ttx.Query(ctx, `SELECT `+strings.Join(cols, `, `)+` FROM t`)\n}", []string{KindUnknown}, KindIdentifier + "," + KindRepeated, false},
		"fixed text a loop repeats":                  {"func f(ctx context.Context, tx pool, j job) {\n\tvar sb strings.Builder\n\tfor range j.list {\n\t\tsb.WriteString(` OR b IS NULL`)\n\t}\n\ttx.Query(ctx, `SELECT a FROM t WHERE c`+sb.String())\n}", []string{KindRepeated}, KindRepeated, true},
		"text from a parameter":                      {"func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM t WHERE `+j.where) }", []string{KindUnknown}, KindIdentifier + "," + KindLiteral + "," + KindRepeated, false},
	} {
		files := typedFiles(c.src)
		_, unread, err := AssembleTyped(typedRoot, files)
		if err != nil {
			t.Fatal(err)
		}
		if len(unread) != 1 || strings.Join(unread[0].Kinds, ",") != strings.Join(c.kinds, ",") {
			t.Errorf("%s: unread %+v, want one argument with parts of kinds %v", name, unread, c.kinds)
			continue
		}
		rep := CheckUnreadable(unread, nil, nil, nil, read(c.shape))
		if covered := len(rep.Violations) == 0 && len(rep.BadFills) == 0; covered != c.wantCovered {
			t.Errorf("%s: covered by a shape naming %q: %v, want %v (violations %d, bad %v)", name, c.shape, covered, c.wantCovered, len(rep.Violations), rep.BadFills)
		}
	}
	// No shape can name a kind the reader does not verify.
	_, unread, _ := AssembleTyped(typedRoot, typedFiles("func f(ctx context.Context, tx pool, j job) { tx.Query(ctx, `SELECT a FROM t WHERE `+j.where) }"))
	for _, kinds := range []string{KindUnknown, KindQueued, "", "anything"} {
		if rep := CheckUnreadable(unread, nil, nil, nil, read(kinds)); len(rep.BadFills) == 0 || len(rep.Violations) != 1 {
			t.Errorf("a shape naming the kind %q: bad %v, violations %d; want it refused and the argument a violation", kinds, rep.BadFills, len(rep.Violations))
		}
	}
	// An exception covers it, when it says where the text comes from.
	ex := UnreadableException{Decl: "f", Match: "SELECT a FROM t WHERE", Call: 1, Of: 1, Reason: "by design"}
	if rep := CheckUnreadable(unread, nil, nil, []UnreadableException{ex}, nil); len(rep.Violations) != 1 || len(rep.BadFills) == 0 {
		t.Errorf("an exception with no source: violations %d, bad %v; want it refused", len(rep.Violations), rep.BadFills)
	}
	ex.Source = "a rules file"
	if rep := CheckUnreadable(unread, nil, nil, []UnreadableException{ex}, nil); len(rep.Violations) != 0 || len(rep.BadFills) != 0 {
		t.Errorf("an exception with its source: violations %d, bad %v", len(rep.Violations), rep.BadFills)
	}
}

// A queued query's SQL, read back and queued again, was read where it was
// first queued: it is not reported, and the batch it came from is judged by
// where IT came from.
func TestTypedAQueuedQueryIsReadWhereItWasQueued(t *testing.T) {
	src := "func again(b *pgx.Batch) *pgx.Batch {\n\tfresh := &pgx.Batch{}\n\tfor _, q := range b.QueuedQueries {\n\t\tfresh.Queue(q.SQL, q.Arguments...)\n\t}\n\treturn fresh\n}\n"
	if n, detail := typedViolations(t, src+"func g() *pgx.Batch {\n\tb := &pgx.Batch{}\n\tb.Queue(`SELECT 1`)\n\treturn again(b)\n}"); n != 0 {
		t.Errorf("a batch of the package, copied: reported\n%s", detail)
	}
	if n, detail := typedViolations(t, src); n == 0 || !strings.Contains(detail, "again takes a batch") {
		t.Errorf("the same function with no caller in the package: want it reported for the batch it takes\n%s", detail)
	}
}

// An identifier whose parts are fixed text is computed, as Sanitize writes
// it: quoted, a quote inside doubled, parts joined by a dot.
func TestTypedAFixedIdentifierIsComputed(t *testing.T) {
	whole, unread, err := AssembleTyped(typedRoot, typedFiles("const sch = `aveloxis_data`\nfunc f(ctx context.Context, tx pool) { tx.Query(ctx, `SELECT 1 FROM `+pgx.Identifier{sch, `we\"ird`}.Sanitize()) }"))
	if err != nil {
		t.Fatal(err)
	}
	if len(unread) != 0 || sqlOf(whole) != `SELECT 1 FROM "aveloxis_data"."we""ird"` {
		t.Errorf("whole %q, unread %+v", sqlOf(whole), unread)
	}
}
