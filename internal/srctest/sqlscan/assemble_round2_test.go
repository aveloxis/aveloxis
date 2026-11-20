// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// Review round 2 found seven more spellings of the thing round 1 fixed four
// of. The rule is decided here as a class (0.29.209):
//
//	What the reader cannot prove it has read whole is REPORTED, never
//	passed as clean.
//
// A name's binding is its value only when nothing else can change it. A
// part whose place in the statement is not known makes the statement begin
// with a hole, and a statement that begins with a hole is judged as a write
// to an unknown table whenever the text that can be seen writes at all.

// The policy is one a plain assignment breaks (the stored value wins once
// set), so that a statement read WHOLE with a forbidden value fails too.
var round2Registry = []Registered{{Table: "aveloxis_data.repos", Column: "repo_git", Policy: FillEmptyOnly, Reason: "r"}}

// violations runs the whole chain a store's test runs: Assemble,
// CheckUnreadable, and Check over everything that is handed on.
func round2Violations(t *testing.T, src string) (n int, detail string) {
	t.Helper()
	files := map[string]string{"internal/db/x.go": "package db\n\n" + src}
	whole, unread, err := Assemble(files)
	if err != nil {
		t.Fatal(err)
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
	b.WriteString("whole: " + sqlOf(whole) + "\nhanded on: " + sqlOf(rep.Statements))
	return len(rep.Violations) + len(chk.Violations), b.String()
}

// Every one of these writes repos.repo_git with a value the registered
// policy forbids, or with one nobody can read. None may pass.
func TestWhatCannotBeProvedReadWholeIsReported(t *testing.T) {
	for _, c := range []struct{ what, src string }{
		// T3: a loop variable is its element only if the loop leaves it alone.
		{"a range variable appended to in its loop",
			"func f(ctx context.Context, tx pool, extra bool, v string) {\n\tfor _, q := range []string{\"UPDATE aveloxis_data.repos SET repo_name = 'x'\"} {\n\t\tif extra {\n\t\t\tq += \", repo_git = \" + v\n\t\t}\n\t\tq += \" WHERE repo_id = $1\"\n\t\ttx.Exec(ctx, q)\n\t}\n}"},
		{"a range variable reassigned in its loop",
			"func f(ctx context.Context, tx pool, col string) {\n\tfor _, q := range []string{\"UPDATE aveloxis_data.repos SET repo_name = 'x' WHERE repo_id = $1\"} {\n\t\tq = rewrite(q, col)\n\t\ttx.Exec(ctx, q)\n\t}\n}"},
		{"a field of a range variable assigned in its loop",
			"type job struct{ sql string }\n\nfunc f(ctx context.Context, tx pool, v string) {\n\tfor _, j := range []job{{sql: \"UPDATE aveloxis_data.repos SET repo_name = 'x' WHERE repo_id = $1\"}} {\n\t\tj.sql = v\n\t\ttx.Exec(ctx, j.sql)\n\t}\n}"},
		// T4: a builder that something else can write to.
		{"a builder held by pointer and passed to a function",
			"func f(ctx context.Context, tx pool, cols []string) {\n\tb := &strings.Builder{}\n\tb.WriteString(\"UPDATE aveloxis_data.repos SET repo_name = 'x'\")\n\tappendSets(b, cols)\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		{"a builder with an alias that is written to",
			"func f(ctx context.Context, tx pool, v string) {\n\tb := &strings.Builder{}\n\tb.WriteString(\"UPDATE aveloxis_data.repos SET repo_name = 'x'\")\n\tsb := b\n\tsb.WriteString(\", repo_git = \" + v)\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		{"a builder that is a parameter (what was written before is not seen)",
			"func f(ctx context.Context, tx pool, b *strings.Builder) {\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}\n\nfunc g(ctx context.Context, tx pool) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE aveloxis_data.repos SET repo_git = 'x'\")\n\tf(ctx, tx, &b)\n}"},
		{"a builder that is not this function's (a package variable other functions write to)",
			"var shared strings.Builder\n\nfunc f(ctx context.Context, tx pool) {\n\tshared.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, shared.String())\n}\n\nfunc g() { shared.WriteString(\"UPDATE aveloxis_data.repos SET repo_git = 'x'\") }"},
		// T5: where the unread write landed is not known.
		{"a builder whose address goes to a function between SET and WHERE",
			"func f(ctx context.Context, tx pool, cols []string) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE aveloxis_data.repos SET repo_name = 'x'\")\n\tappendSets(&b, cols)\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		// T8: the head is the part that cannot be read.
		{"the statement's head in a variable",
			"func f(ctx context.Context, tx pool, head string) { tx.Exec(ctx, head+\" SET repo_git = $1 WHERE repo_id = $2\") }"},
		{"the verb in a format",
			"func f(ctx context.Context, tx pool, verb string) {\n\ttx.Exec(ctx, fmt.Sprintf(\"%s aveloxis_data.repos SET repo_git = $1\", verb))\n}"},
		{"an INSERT whose head is not read",
			"func f(ctx context.Context, tx pool, head string) { tx.Exec(ctx, head+\" (repo_id, repo_git) VALUES ($1, $2)\") }"},
		{"a quoted table and schema",
			"func f(ctx context.Context, tx pool, v string) { tx.Exec(ctx, `UPDATE \"aveloxis_data\".\"repos\" SET repo_git = `+v) }"},
		{"a quoted table, fixed text",
			"func f(ctx context.Context, tx pool) { tx.Exec(ctx, \"UPDATE \\\"aveloxis_data\\\".\\\"repos\\\" SET repo_git = 'x'\") }"},
		// T9: names decided by spelling alone.
		{"a local that shares its name with an unrelated literal's parameter",
			"func f(ctx context.Context, s *S, col string) {\n\teach(func(q string) {})\n\tq := s.build(col)\n\ts.pool.Exec(ctx, q)\n}"},
		{"a helper that reassigns its parameter before running it",
			"func run(ctx context.Context, tx pool, format, table string) {\n\tformat = fmt.Sprintf(format, table)\n\ttx.Exec(ctx, format)\n}\n\nfunc g(ctx context.Context, tx pool) { run(ctx, tx, \"UPDATE %s SET repo_git = 'x'\", \"aveloxis_data.repos\") }"},
		{"two methods of one name with the SQL at different places",
			"func (a *A) run(ctx context.Context, q string) { a.pool.Exec(ctx, q) }\n\nfunc (b *B) run(q string, ctx context.Context) { b.pool.Exec(ctx, q) }\n\nfunc g(ctx context.Context, b *B) { b.run(\"UPDATE aveloxis_data.repos SET repo_git = 'x'\", ctx) }"},
		{"a generic helper called with its type argument",
			"func runT[T any](ctx context.Context, tx pool, q string) { tx.Exec(ctx, q) }\n\nfunc g(ctx context.Context, tx pool, v string) { runT[int](ctx, tx, \"UPDATE aveloxis_data.repos SET repo_git = \"+v) }"},
		{"ExecContext",
			"func f(ctx context.Context, db *sql.DB, j job) { db.ExecContext(ctx, j.sql) }"},
		{"a literal's parameter, fed a field",
			"func f(ctx context.Context, tx pool, j job) {\n\trun := func(q string) { tx.Exec(ctx, q) }\n\trun(j.sql)\n}"},
		{"a literal's parameter, the literal passed on as a callback",
			"func f(ctx context.Context, tx pool) {\n\teach(func(q string) { tx.Exec(ctx, q) })\n}"},
		{"Exec taken as a function value",
			"func f(ctx context.Context, tx pool, j job) {\n\tex := tx.Exec\n\tex(ctx, j.sql)\n}"},
		{"a helper taken as a function value",
			"func (s *S) run(ctx context.Context, q string) { s.pool.Exec(ctx, q) }\n\nfunc g(ctx context.Context, s *S, j job) {\n\tr := s.run\n\tr(ctx, j.sql)\n}"},
		{"a goto in a function that writes (the write to the builder is written after the use and runs before it)",
			"func f(ctx context.Context, tx pool, v string) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE aveloxis_data.repos SET repo_name = 'x'\")\n\tfirst := true\nrun:\n\ttx.Exec(ctx, b.String())\n\tif first {\n\t\tfirst = false\n\t\tb.WriteString(\", repo_git = \" + v)\n\t\tgoto run\n\t}\n}"},
		// A list is its elements only if nothing else can change them.
		{"a list passed to a function before it is ranged over",
			"func f(ctx context.Context, tx pool) {\n\tqs := []string{\"UPDATE aveloxis_data.repos SET repo_name = 'x' WHERE repo_id = $1\"}\n\trewriteAll(qs)\n\tfor _, q := range qs {\n\t\ttx.Exec(ctx, q)\n\t}\n}"},
		{"a list passed to a function before it is joined",
			"func f(ctx context.Context, tx pool) {\n\tsets := []string{\"repo_name = 'x'\"}\n\taddSets(sets)\n\ttx.Exec(ctx, \"UPDATE aveloxis_data.repos SET \"+strings.Join(sets, \", \")+\" WHERE repo_id = $1\")\n}"},
		{"a package list passed to a function before it is joined",
			"var sets = []string{\"repo_name = 'x'\"}\n\nfunc f(ctx context.Context, tx pool) {\n\ttx.Exec(ctx, \"UPDATE aveloxis_data.repos SET \"+strings.Join(sets, \", \")+\" WHERE repo_id = $1\")\n}\n\nfunc g() { addSets(sets) }"},
		{"a list with an alias",
			"func f(ctx context.Context, tx pool, v string) {\n\tqs := []string{\"UPDATE aveloxis_data.repos SET repo_name = 'x' WHERE repo_id = $1\"}\n\tother := qs\n\tother[0] = v\n\tfor _, q := range qs {\n\t\ttx.Exec(ctx, q)\n\t}\n}"},
	} {
		if n, detail := round2Violations(t, c.src); n == 0 {
			t.Errorf("%s: passed as clean\n%s", c.what, detail)
		}
	}
}

// And what IS read whole still is: the rule must not turn the reader into
// one that reports everything.
func TestWhatIsReadWholeStillIs(t *testing.T) {
	for _, c := range []struct{ what, src, want string }{
		{"a range variable its loop leaves alone",
			"func f(ctx context.Context, tx pool) {\n\tfor _, q := range []string{\"UPDATE t SET a = 1\", \"UPDATE t SET a = 2\"} {\n\t\ttx.Exec(ctx, q+\" WHERE id = $1\")\n\t}\n}",
			"UPDATE t SET a = 1 WHERE id = $1 ;; UPDATE t SET a = 2 WHERE id = $1"},
		{"a builder nothing else touches, read with Len and String",
			"func f(ctx context.Context, tx pool) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET a = 1\")\n\tif b.Len() > 0 {\n\t\tb.WriteString(\" WHERE id = $1\")\n\t}\n\ttx.Exec(ctx, b.String())\n}",
			"UPDATE t SET a = 1 WHERE id = $1"},
		{"a list that is only ranged over, joined and measured",
			"func f(ctx context.Context, tx pool) {\n\tcols := []string{\"a\", \"b\"}\n\tif len(cols) == 0 {\n\t\treturn\n\t}\n\ttx.Exec(ctx, \"INSERT INTO t (\"+strings.Join(cols, \", \")+\") VALUES ($1, $2)\")\n}",
			"INSERT INTO t (a, b) VALUES ($1, $2)"},
		{"a registered helper's parameter, read at its call",
			"func (s *S) run(ctx context.Context, q string) { s.pool.Exec(ctx, q) }\n\nfunc g(ctx context.Context, s *S) { s.run(ctx, \"UPDATE t SET a = 3\") }",
			"UPDATE t SET a = 3"},
		{"a local literal that runs its parameter, read at its call",
			"func f(ctx context.Context, tx pool) {\n\trun := func(q string) { tx.Exec(ctx, q) }\n\trun(\"UPDATE t SET a = 4\")\n}",
			"UPDATE t SET a = 4"},
	} {
		whole, unread := assembleOne(t, c.src)
		got := map[string]bool{}
		for _, s := range whole {
			got[strings.Join(strings.Fields(s.SQL), " ")] = true
		}
		for _, want := range strings.Split(c.want, " ;; ") {
			if !got[want] {
				t.Errorf("%s: %q is not read whole (whole %q; unread %+v)", c.what, want, sqlOf(whole), unread)
			}
		}
		if rep := CheckUnreadable(unread, round2Registry, nil, nil, nil); len(rep.Violations) != 0 {
			t.Errorf("%s: reported %+v", c.what, rep.Violations)
		}
	}
}

// A statement that begins with a part that cannot be read is a write to an
// unknown table whenever what can be seen writes; text that writes nothing
// is still nobody's business.
func TestAStatementThatBeginsUnreadIsAWriteToAnUnknownTable(t *testing.T) {
	check := func(text string) int {
		return len(CheckUnreadable([]Unreadable{{File: "x.go", Decl: "f", Line: 1, Text: text, Why: "w"}}, round2Registry, nil, nil, nil).Violations)
	}
	for _, text := range []string{
		Unknown + " SET repo_git = $1 WHERE repo_id = $2",
		Unknown + " aveloxis_data.repos SET repo_git = $1",
		Unknown + " (repo_id, repo_git) VALUES ($1, $2)",
		Unknown + " ON CONFLICT (repo_id) DO UPDATE SET repo_git = EXCLUDED.repo_git",
		Unknown + "UPDATE some_other.table SET a = 1 WHERE b", // where the unread part landed is not known
		"SELECT 1; " + Unknown + " SET a = 1",                 // the second statement begins unread
		"  -- note\n" + Unknown + " SET a = 1",                // a comment is not a beginning
		"(" + Unknown + " SET a = 1)",                         // nor is a parenthesis
	} {
		if check(text) != 1 {
			t.Errorf("%q: not a violation", text)
		}
	}
	// A table quoted where it need not be is the plain name, for text handed
	// to this check directly too.
	if check(`UPDATE "aveloxis_data"."repos" SET repo_git = `+Unknown) != 1 || check(`INSERT INTO aveloxis_data."repos" (`+Unknown+`) VALUES ($1)`) != 1 {
		t.Error("a registered table written with quotes it does not need is not seen as registered")
	}
	// A name quoted because it NEEDS the quotes is another table.
	if check(`UPDATE "aveloxis_data"."Repos" SET repo_git = `+Unknown) != 0 {
		t.Error("a quoted name with a capital was taken for the registered table")
	}
	for _, text := range []string{
		Unknown + ": could not read the row",
		Unknown + " SELECT a FROM t WHERE b = $1",
		Unknown + " rows were set aside",
		"SELECT a FROM t WHERE b = " + Unknown,
		"UPDATE some_other.table SET a = 1 WHERE b = " + Unknown,
	} {
		if check(text) != 0 {
			t.Errorf("%q: a violation; it writes nothing that can be seen, or its table is read and not registered", text)
		}
	}
}

// Text that is the argument of a call that logs or makes an error does not
// reach the database: it is not judged as SQL.
func TestAMessageIsNotAStatement(t *testing.T) {
	for what, src := range map[string]string{
		"a log line":   "func f(l *slog.Logger, n int) { l.Warn(\"UPDATE aveloxis_data.repos SET row \" + strconv.Itoa(n)) }",
		"an error":     "func f(n int) error { return fmt.Errorf(\"UPDATE aveloxis_data.repos SET repo_git failed for \" + name(n)) }",
		"a log's attr": "func f(l *slog.Logger, v string) { l.Info(\"ran\", \"sql\", \"UPDATE aveloxis_data.repos SET repo_git = \"+v) }",
	} {
		if n, detail := round2Violations(t, src); n != 0 {
			t.Errorf("%s: reported\n%s", what, detail)
		}
	}
	// The same text anywhere else is judged; and a function of the package
	// that RUNS its argument is no logger, whatever it is called.
	for what, src := range map[string]string{
		"returned":                     "func f(v string) string { return \"UPDATE aveloxis_data.repos SET repo_git = \" + v }",
		"a helper named like a logger": "func (s *S) Warn(ctx context.Context, q string) { s.pool.Exec(ctx, q) }\n\nfunc g(ctx context.Context, s *S, v string) { s.Warn(ctx, \"UPDATE aveloxis_data.repos SET repo_git = \"+v) }",
	} {
		if n, detail := round2Violations(t, src); n == 0 {
			t.Errorf("%s: passed as clean\n%s", what, detail)
		}
	}
}
