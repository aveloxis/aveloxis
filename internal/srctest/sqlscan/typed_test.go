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
