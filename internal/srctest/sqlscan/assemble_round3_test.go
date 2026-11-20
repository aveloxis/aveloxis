// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// Review round 3 (0.29.210): DENY BY DEFAULT. Three rounds each found a
// place where the reader passed, as clean, SQL it had not read: a part it
// judged harmless, a builder it read as less than it was, a name it took
// for another. Nothing is judged harmless now, and every case below is one
// the round's reviewer showed passing.
const bad = "UPDATE aveloxis_data.repos SET repo_name = 'x'"

func TestRound3NothingUnreadPasses(t *testing.T) {
	for _, c := range []struct{ what, src string }{
		// F1: a replacement's unread part was put at the END of the text.
		{"ReplaceAll with an unread replacement",
			"func f(ctx context.Context, tx pool, col string) {\n\ttx.Exec(ctx, strings.ReplaceAll(\"UPDATE aveloxis_data.repos SET {col} = NOW() WHERE repo_id = $1\", \"{col}\", col))\n}"},
		{"ReplaceAll with an unread needle",
			"func f(ctx context.Context, tx pool, old string) {\n\ttx.Exec(ctx, strings.ReplaceAll(\"UPDATE aveloxis_data.repos SET repo_name = NOW() /*more*/ WHERE repo_id = $1\", old, \", repo_git = 'x'\"))\n}"},
		{"Replace with a count",
			"func f(ctx context.Context, tx pool) {\n\ttx.Exec(ctx, strings.Replace(\"UPDATE aveloxis_data.repos SET a = 1 WHERE a = 1\", \"a = 1\", \"repo_git = 'x'\", 1))\n}"},
		// F2: fixed text that does not write, around a statement that is unread.
		{"a lock CTE and an unread body",
			"const lockCTE = \"WITH locked AS (SELECT 1 FROM aveloxis_data.repos FOR UPDATE) \"\n\nfunc f(ctx context.Context, tx pool, j job) {\n\ttx.Exec(ctx, lockCTE+j.body())\n}"},
		{"a wrapper CTE", "func f(ctx context.Context, tx pool, q string) {\n\ttx.Exec(ctx, \"WITH x AS (\"+q+\") SELECT 1\")\n}"},
		{"a DO block", "func f(ctx context.Context, tx pool, q string) {\n\ttx.Exec(ctx, \"DO $$ BEGIN \"+q+\" END $$\")\n}"},
		{"EXPLAIN ANALYZE", "func f(ctx context.Context, tx pool, q string) {\n\ttx.Exec(ctx, \"EXPLAIN ANALYZE \"+q)\n}"},
		{"PREPARE in a batch", "func f(b *pgx.Batch, q string) {\n\tb.Queue(\"PREPARE p AS \"+q)\n}"},
		// F3: a builder read as less than it is.
		{"a write wrapped in a call",
			"func f(ctx context.Context, tx pool, c string) {\n\tvar b strings.Builder\n\tb.WriteString(\"" + bad + "\")\n\tmust(b.WriteString(\", repo_git = \" + c))\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		{"a deferred write",
			"func f(ctx context.Context, tx pool, v string) {\n\tvar b strings.Builder\n\tdefer b.WriteString(v)\n\tb.WriteString(\"" + bad + " WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		{"a write through a method value",
			"func f(ctx context.Context, tx pool, v string) {\n\tvar b strings.Builder\n\tw := b.WriteString\n\tb.WriteString(\"" + bad + "\")\n\tw(v)\n\ttx.Exec(ctx, b.String())\n}"},
		{"a buffer declared with content",
			"func f(ctx context.Context, tx pool, j job) {\n\tb := bytes.NewBufferString(j.head)\n\tb.WriteString(\" WHERE repo_id = $1\")\n\ttx.Exec(ctx, b.String())\n}"},
		// F4: SQL that leaves the package.
		{"an exported function that runs its parameter",
			"func (s *PostgresStore) ExecRaw(ctx context.Context, q string, args ...any) error {\n\t_, err := s.pool.Exec(ctx, q, args...)\n\treturn err\n}"},
		{"a quoted statement handed to another package",
			"func f(ctx context.Context, p pool) {\n\tdbutil.Run(ctx, p, \"UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = $1\")\n}"},
		{"a quoted statement handed to a function held in a field",
			"func (s *S) f(ctx context.Context) {\n\ts.run(ctx, \"UPDATE aveloxis_data.repos SET repo_git = 'x' WHERE repo_id = $1\")\n}"},
		// F5: a part that ends an INSERT or a MERGE.
		{"an INSERT ... SELECT with an unread tail",
			"func f(ctx context.Context, tx pool, tail string) {\n\ttx.Exec(ctx, \"INSERT INTO aveloxis_data.repos (repo_id) SELECT id FROM src WHERE \"+tail)\n}"},
		{"a MERGE with an unread source",
			"func f(ctx context.Context, tx pool, tail string) {\n\ttx.Exec(ctx, \"MERGE INTO aveloxis_data.repos t USING \"+tail)\n}"},
		{"an unread ORDER BY", "func f(ctx context.Context, tx pool, o string) {\n\ttx.Exec(ctx, \"INSERT INTO aveloxis_data.repos (repo_id) SELECT id FROM src ORDER BY \"+o)\n}"},
		// F7: a read with no fixed text.
		{"a read of a field", "func f(ctx context.Context, p pool, j job) {\n\tp.QueryRow(ctx, j.sql)\n}"},
		{"a read of a builder's result", "func f(ctx context.Context, p pool, j job) {\n\tp.Query(ctx, j.build())\n}"},
		// F8: names taken by spelling.
		{"a declaration under a true condition",
			"func f(ctx context.Context, tx pool) {\n\tq := \"UPDATE aveloxis_data.repos SET repo_git = 'x'\"\n\tif true {\n\t\tq := \"SELECT 1\"\n\t\t_ = q\n\t}\n\ttx.Exec(ctx, q)\n}"},
		{"a closure parameter named like the range variable",
			"func f(ctx context.Context, tx pool, j job) {\n\tfor _, q := range []string{\"SELECT 1\"} {\n\t\tfunc(q string) { tx.Exec(ctx, q) }(j.sql)\n\t\t_ = q\n\t}\n}"},
		{"a local named like a package builder",
			"func where(a string) string { return \" WHERE repo_id = \" + a }\n\nfunc f(ctx context.Context, tx pool, where func(string) string) {\n\ttx.Exec(ctx, \"" + bad + "\"+where(\"$1\"))\n}"},
		// F9: a package value something else can change.
		{"a package list handed out by a function",
			"var qs = []string{\"SELECT 1\"}\n\nfunc g() []string { return qs }\n\nfunc f(ctx context.Context, tx pool) {\n\tfor _, q := range qs {\n\t\ttx.Exec(ctx, q)\n\t}\n}"},
		{"an exported package variable",
			"var Stmt = \"SELECT 1\"\n\nfunc f(ctx context.Context, tx pool) {\n\ttx.Exec(ctx, Stmt)\n}"},
		// F10: a value printed by its String method.
		{"a typed constant in a format",
			"type col string\n\nfunc (col) String() string { return \"repo_git\" }\n\nconst nameCol col = \"repo_name\"\n\nfunc f(ctx context.Context, tx pool) {\n\ttx.Exec(ctx, fmt.Sprintf(\"UPDATE aveloxis_data.repos SET %s = 'x' WHERE repo_id = $1\", nameCol))\n}"},
	} {
		if n, detail := round2Violations(t, c.src); n == 0 {
			t.Errorf("%s: passed as clean\n%s", c.what, detail)
		}
	}
}

// What is read whole still is, and is judged: the rule reports what it
// cannot read, it does not stop reading.
func TestRound3WhatIsReadWholeStillIs(t *testing.T) {
	for _, c := range []struct{ what, src, want string }{
		// F13: a statement chosen in the arms of an if under `var q string`.
		{"var q string and two arms",
			"func f(ctx context.Context, tx pool, x bool) {\n\tvar q string\n\tif x {\n\t\tq = \"UPDATE aveloxis_data.other SET a = 1\"\n\t} else {\n\t\tq = \"UPDATE aveloxis_data.other SET a = 2\"\n\t}\n\ttx.Exec(ctx, q)\n}",
			"UPDATE aveloxis_data.other SET a = 1 ;; UPDATE aveloxis_data.other SET a = 2"},
		{"var q string and a switch",
			"func f(ctx context.Context, tx pool, x int) {\n\tvar q string\n\tswitch x {\n\tcase 1:\n\t\tq = \"UPDATE aveloxis_data.other SET a = 1\"\n\tdefault:\n\t\tq = \"UPDATE aveloxis_data.other SET a = 2\"\n\t}\n\ttx.Exec(ctx, q)\n}",
			"UPDATE aveloxis_data.other SET a = 1 ;; UPDATE aveloxis_data.other SET a = 2"},
		{"a builder used only where it is followed",
			"func f(ctx context.Context, tx pool) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE aveloxis_data.other SET a = 1\")\n\tfmt.Fprintf(&b, \" WHERE %s\", \"id = $1\")\n\tif b.Len() > 0 {\n\t\ttx.Exec(ctx, b.String())\n\t}\n}",
			"UPDATE aveloxis_data.other SET a = 1 WHERE id = $1"},
		{"a typed string constant and an unexported package variable",
			"const q string = \"UPDATE aveloxis_data.other SET a = 1\"\n\nvar w = \" WHERE id = $1\"\n\nfunc f(ctx context.Context, tx pool) {\n\ttx.Exec(ctx, q+w)\n}",
			"UPDATE aveloxis_data.other SET a = 1 WHERE id = $1"},
	} {
		whole, unread := assembleOne(t, c.src)
		if len(unread) != 0 || sqlOf(whole) != c.want {
			t.Errorf("%s:\n whole  %q\n want   %q\n unread %+v", c.what, sqlOf(whole), c.want, unread)
		}
	}
}

// F6: every call that runs SQL, from the reader's own table. For each: an
// argument that cannot be read, at that call's SQL position, is reported as
// an SQL argument and with whether the call writes; and a quoted statement
// there is read whole. An entry removed from the table fails here (two
// mutants that deleted one survived every test of both packages).
func TestEveryRunnerIsReadAtItsSQLArgument(t *testing.T) {
	runners := Runners()
	want := map[string]bool{"Exec": true, "ExecContext": true, "Prepare": true, "PrepareContext": true, "Query": false,
		"QueryContext": false, "QueryRow": false, "QueryRowContext": false, "Queue": true}
	if len(runners) != len(want) {
		t.Fatalf("the reader knows %d calls that run SQL, this test names %d: add the new one here with whether it writes", len(runners), len(want))
	}
	for _, r := range runners {
		writes, known := want[r.Name]
		if !known || writes != r.Writes {
			t.Errorf("%s: writes %v in the reader's table; this test says %v (known %v)", r.Name, r.Writes, writes, known)
		}
		call := func(arg string) string {
			args := make([]string, r.Index+1)
			for i := range args {
				args[i] = "nil"
			}
			args[r.Index] = arg
			return "func f(x runner, j job) {\n\tx." + r.Name + "(" + strings.Join(args, ", ") + ")\n}"
		}
		_, unread := assembleOne(t, call("j.sql"))
		if len(unread) != 1 || !unread[0].SQLArg || unread[0].WriteArg != r.Writes || unread[0].Arg != "j.sql" {
			t.Errorf("%s: an unread argument at position %d gave %+v; want it reported as this call's SQL argument", r.Name, r.Index, unread)
		}
		// (A statement the quoted-literal pass does not take for a write,
		// so that it is this call's entry that reads it.)
		whole, unread := assembleOne(t, call("\"SELECT repo_git FROM aveloxis_data.repos\""))
		if len(unread) != 0 || sqlOf(whole) != "SELECT repo_git FROM aveloxis_data.repos" {
			t.Errorf("%s: a quoted statement at position %d: whole %q, unread %+v", r.Name, r.Index, sqlOf(whole), unread)
		}
		// One position earlier is not the SQL argument.
		if r.Index > 0 {
			args := make([]string, r.Index+1)
			for i := range args {
				args[i] = "nil"
			}
			args[r.Index-1] = "j.sql"
			args[r.Index] = "\"SELECT 1\""
			if _, unread := assembleOne(t, "func f(x runner, j job) {\n\tx."+r.Name+"("+strings.Join(args, ", ")+")\n}"); len(unread) != 0 {
				t.Errorf("%s: an argument before the SQL one was reported: %+v", r.Name, unread)
			}
		}
	}
}

// F6: a parameter that is given a value keeps "what came in" as one of its
// values (a round-2 rule whose removal passed every test).
func TestAParameterGivenAValueKeepsWhatCameIn(t *testing.T) {
	src := "func f(ctx context.Context, tx pool, q string, x bool) {\n\tif x {\n\t\tq = \"SELECT 1\"\n\t}\n\ttx.Exec(ctx, q)\n}\n\nfunc g(ctx context.Context, tx pool, v string) {\n\tf(ctx, tx, \"UPDATE aveloxis_data.repos SET repo_git = \"+v, false)\n}"
	if n, detail := round2Violations(t, src); n == 0 {
		t.Errorf("a parameter reassigned under a condition was read as the new value alone\n%s", detail)
	}
	_, unread := assembleOne(t, "func f(ctx context.Context, tx pool, q string, x bool) {\n\tif x {\n\t\tq = \"SELECT 1\"\n\t}\n\ttx.Exec(ctx, q)\n}")
	found := false
	for _, u := range unread {
		found = found || (u.SQLArg && strings.Contains(u.Text, Unknown))
	}
	if !found {
		t.Errorf("unread %+v; want the argument reported with what came in unread", unread)
	}
}

// An exception for an argument with no fixed text names the argument, and
// covers that argument only.
func TestAWholeArgumentExceptionNamesItsArgument(t *testing.T) {
	_, unread := assembleOne(t, "func f(ctx context.Context, p pool, j job) {\n\tp.Query(ctx, j.one)\n\tp.Exec(ctx, j.two)\n}")
	if len(unread) != 2 {
		t.Fatalf("unread %+v", unread)
	}
	for what, c := range map[string]struct {
		ex   UnreadableException
		left int
	}{
		"names one": {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one", Call: 1, Of: 2, Reason: "r", Source: "a rules file"}, 1},
		// 0.29.211: an SQL argument is excepted by its PLACE too, in a
		// declaration of that many arguments: one added to f later makes
		// the exception stale instead of leaving it standing unread.
		"no source":           {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one", Call: 1, Of: 2, Reason: "r"}, 2},
		"no place":            {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one", Reason: "r"}, 2},
		"another place":       {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one", Call: 2, Of: 2, Reason: "r", Source: "a rules file"}, 2},
		"another count":       {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one", Call: 1, Of: 3, Reason: "r", Source: "a rules file"}, 2},
		"names none":          {UnreadableException{Decl: "f", Match: Unknown, Reason: "r"}, 2},
		"names another":       {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.three", Reason: "r"}, 2},
		"no reason":           {UnreadableException{Decl: "f", Match: Unknown, Arg: "j.one"}, 2},
		"another declaration": {UnreadableException{Decl: "g", Match: Unknown, Arg: "j.one", Reason: "r"}, 2},
	} {
		if rep := CheckUnreadable(unread, nil, nil, []UnreadableException{c.ex}, nil); len(rep.Violations) != c.left {
			t.Errorf("%s: %d violations, want %d", what, len(rep.Violations), c.left)
		}
	}
}
