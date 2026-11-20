// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// Review round 1 of the vulnerability program's first part found three ways
// a write could still pass unread (0.29.203's reader). Each is closed here.

// (2) A write whose TABLE is not fixed text is a write to an unknown table,
// however its head is spelled: with a schema before the hole, quoted, in
// lower case, with a comment between. A message is still not a write.
func TestAWriteToATableThatCannotBeReadIsAViolationHoweverSpelled(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: AlwaysRefresh, Reason: "r"}}
	for _, text := range []string{
		"UPDATE aveloxis_data." + Unknown + " SET a = 1",
		"UPDATE aveloxis_data.repo" + Unknown + " SET a = 1",
		`UPDATE "` + Unknown + `" SET a = 1`,
		`UPDATE aveloxis_data."` + Unknown + `" SET a = 1`,
		"update " + Unknown + " set a = 1",
		"Update Only " + Unknown + " Set a = 1",
		"UPDATE /* which */ " + Unknown + " SET a = 1",
		"UPDATE -- which\n " + Unknown + " SET a = 1",
		"insert into aveloxis_data." + Unknown + " (a) values (1)",
		"INSERT INTO " + Unknown + " SELECT * FROM u",
		"INSERT INTO " + Unknown + " DEFAULT VALUES",
		"merge into " + Unknown + " t using u on t.id = u.id when matched then update set a = 1",
		"WITH c AS (SELECT 1) UPDATE " + Unknown + "." + Unknown + " SET a = 1",
	} {
		rep := CheckUnreadable([]Unreadable{{File: "x.go", Decl: "f", Line: 1, Text: text, Why: "the parameter tb"}}, registry, nil, nil, nil)
		if len(rep.Violations) != 1 || len(rep.Statements) != 0 {
			t.Errorf("%q: violations %d, handed on %q; want one violation", text, len(rep.Violations), sqlOf(rep.Statements))
		}
	}
	for _, message := range []string{
		"could not update " + Unknown + ": " + Unknown,
		"failed to insert into " + Unknown,
		"update " + Unknown + " failed after " + Unknown + " attempts",
		"merge into " + Unknown + " refused",
		"UPDATE aveloxis_data.other SET a = " + Unknown, // a spelled table that is not registered
	} {
		if rep := CheckUnreadable([]Unreadable{{File: "x.go", Decl: "f", Line: 1, Text: message, Why: "w"}}, registry, nil, nil, nil); len(rep.Violations)+len(rep.Statements) != 0 {
			t.Errorf("%q is not a write to an unknown or registered table: %+v %q", message, rep.Violations, sqlOf(rep.Statements))
		}
	}
}

// (3) The ways SQL is put together that the reader did not follow.
func TestAssembleReadsEveryWayTheSQLArgumentIsBuilt(t *testing.T) {
	for _, c := range []struct{ what, src, want string }{
		{"a double-quoted literal passed straight to Exec",
			"func f(ctx context.Context, p pool) { p.Exec(ctx, \"UPDATE t SET a = 1\") }",
			"UPDATE t SET a = 1"},
		{"a package variable with a double-quoted value passed to Queue",
			"var q = \"UPDATE t SET a = 2\"\n\nfunc f(b *pgx.Batch) { b.Queue(q, 1) }",
			"UPDATE t SET a = 2"},
		{"a local passed to QueryRow",
			"func f(ctx context.Context, p pool) {\n\tq := \"UPDATE t SET a = 3 RETURNING id\"\n\tp.QueryRow(ctx, q)\n}",
			"UPDATE t SET a = 3 RETURNING id"},
		{"x += y, in order",
			"func f(ctx context.Context, p pool) {\n\tq := \"UPDATE t SET a = 4\"\n\tq += \" WHERE id = $1\"\n\tp.Exec(ctx, q)\n}",
			"UPDATE t SET a = 4 WHERE id = $1"},
		{"x += y under a condition: with and without",
			"func f(ctx context.Context, p pool, b bool) string {\n\tq := \"UPDATE t SET a = 5\"\n\tif b {\n\t\tq += \" WHERE x\"\n\t}\n\treturn q + \" RETURNING id\"\n}",
			"UPDATE t SET a = 5 RETURNING id ;; UPDATE t SET a = 5 WHERE x ;; UPDATE t SET a = 5 WHERE x RETURNING id"},
		{"x += y returned, never passed to a call here",
			"func f() string {\n\tq := `UPDATE t SET a = 6`\n\tq += ` WHERE y`\n\treturn q\n}",
			"UPDATE t SET a = 6 WHERE y"},
		{"a strings.Builder",
			"func f(ctx context.Context, p pool) {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET a = 7\")\n\tb.WriteString(\" WHERE id = $1\")\n\tp.Exec(ctx, b.String())\n}",
			"UPDATE t SET a = 7 WHERE id = $1"},
		{"a bytes.Buffer written with Fprintf, returned",
			"func f() string {\n\tvar b bytes.Buffer\n\tfmt.Fprintf(&b, \"UPDATE %s SET a = 8\", \"t\")\n\tb.WriteString(\" WHERE z\")\n\treturn b.String()\n}",
			"UPDATE t SET a = 8 WHERE z"},
		{"fmt.Sprint",
			"func f() string { return fmt.Sprint(\"UPDATE t SET a = 9\", \" WHERE k\") }",
			"UPDATE t SET a = 9 WHERE k"},
		{"strings.ReplaceAll over fixed text",
			"const tmpl = `UPDATE T_ SET a = 10`\n\nfunc f() string { return strings.ReplaceAll(tmpl, \"T_\", \"t\") }",
			"UPDATE t SET a = 10"},
		{"a helper that hands its parameter to Exec, called with a literal",
			"func (s *S) run(ctx context.Context, q string, args ...any) error {\n\t_, err := s.pool.Exec(ctx, q, args...)\n\treturn err\n}\n\nfunc g(ctx context.Context, s *S) { s.run(ctx, \"UPDATE t SET a = 11\") }",
			"UPDATE t SET a = 11"},
		{"a helper of a helper",
			"func (s *S) run(ctx context.Context, q string) { s.pool.Exec(ctx, q) }\n\nfunc (s *S) twice(ctx context.Context, n int, q string) { s.run(ctx, q) }\n\nfunc g(ctx context.Context, s *S) { s.twice(ctx, 2, \"UPDATE t SET a = 12\") }",
			"UPDATE t SET a = 12"},
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
	}
}

// What cannot be read is REPORTED with the fixed text it does have, so
// that the table it writes is seen.
func TestAssembleReportsEveryWayTheSQLArgumentIsBuilt(t *testing.T) {
	for _, c := range []struct{ what, src, want string }{
		{"x += a parameter",
			"func f(ctx context.Context, p pool, w string) {\n\tq := \"UPDATE t SET a = 1 WHERE \"\n\tq += w\n\tp.Exec(ctx, q)\n}",
			"UPDATE t SET a = 1 WHERE " + Unknown},
		{"x += y in a loop",
			"func f(cols []string) string {\n\tq := \"UPDATE t SET \"\n\tfor _, c := range cols {\n\t\tq += c + \" = NULL,\"\n\t}\n\treturn q\n}",
			"UPDATE t SET " + Unknown},
		{"a builder written in a loop",
			"func f(cols []string) string {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET \")\n\tfor _, c := range cols {\n\t\tb.WriteString(c)\n\t}\n\treturn b.String()\n}",
			"UPDATE t SET " + Unknown},
		{"a builder whose address also goes to a function this reader does not follow",
			"func f(cols []string) string {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET a = 1\")\n\taddColumns(&b, cols)\n\treturn b.String()\n}",
			Unknown + "UPDATE t SET a = 1"}, // 0.29.209: where the other write lands is not known, so the hole is first
		{"a builder with a byte written that is no literal", // (a literal one is read, 0.29.212)
			"func f(c byte) string {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET a = 1\")\n\tb.WriteByte(c)\n\treturn b.String()\n}",
			"UPDATE t SET a = 1" + Unknown},
		{"a builder that is reset",
			"func f() string {\n\tvar b strings.Builder\n\tb.WriteString(\"UPDATE t SET a = 1\")\n\tb.Reset()\n\treturn b.String()\n}",
			"UPDATE t SET a = 1" + Unknown},
		{"fmt.Sprint with a value",
			"func f(v int) string { return fmt.Sprint(\"UPDATE t SET a = \", v) }",
			"UPDATE t SET a = " + Unknown},
		{"strings.ReplaceAll with a part that is not fixed",
			"func f(tb string) string { return strings.ReplaceAll(\"UPDATE T_ SET a = 1\", \"T_\", tb) }",
			"UPDATE T_ SET a = 1" + Unknown},
		{"strings.Repeat",
			"func f(n int) string { return \"INSERT INTO t VALUES \" + strings.Repeat(\"($1),\", n) }",
			"INSERT INTO t VALUES ($1)," + Unknown},
		{"a sum inside a call this reader does not follow, inside another sum",
			"func f(v string) string { return \"wrapped: \" + wrap2(1, \"UPDATE t SET a = \"+v) }",
			"UPDATE t SET a = " + Unknown},
	} {
		_, unread := assembleOne(t, c.src)
		found := false
		for _, u := range unread {
			found = found || strings.Join(strings.Fields(u.Text), " ") == c.want
		}
		if !found {
			t.Errorf("%s: %q is not reported (unread %+v)", c.what, c.want, unread)
		}
	}
}

// An SQL argument NO part of which can be read (a map's value, a field, a
// method's result) passed to Exec or Queue is reported as it is, and is a
// violation: it may write anything. A parameter is not reported where it is
// passed on; it is read where the helper is called.
func TestAnSQLArgumentWithNoFixedTextIsReported(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: AlwaysRefresh, Reason: "r"}}
	for what, src := range map[string]string{
		"a map's value":     "func f(ctx context.Context, p pool, k string) { p.Exec(ctx, queries[k]) }",
		"a field":           "func f(ctx context.Context, p pool, j job) { p.Exec(ctx, j.sql) }",
		"a method's result": "func f(ctx context.Context, p pool, j job) { p.Exec(ctx, j.SQL()) }",
		"a queued field":    "func f(b *pgx.Batch, j job) { b.Queue(j.sql, 1) }",
		// Two values, neither with any fixed text: reported once.
		"a local with two unreadable values": "func f(ctx context.Context, p pool, j job, b bool) {\n\tq := j.sql\n\tif b {\n\t\tq = j.other + j.more\n\t}\n\tp.Exec(ctx, q)\n}",
	} {
		_, unread := assembleOne(t, src)
		if len(unread) != 1 || unread[0].Text != Unknown {
			t.Errorf("%s: unread %+v, want the one argument reported with no fixed text", what, unread)
			continue
		}
		if rep := CheckUnreadable(unread, registry, nil, nil, nil); len(rep.Violations) != 1 {
			t.Errorf("%s: not a violation (%+v)", what, rep)
		}
		if rep := CheckUnreadable(unread, registry, nil, []UnreadableException{{Decl: "f", Match: Unknown, Arg: unread[0].Arg, Call: unread[0].Call, Of: unread[0].Of, Reason: "read by a person", Source: "a rules file"}}, nil); len(rep.Violations) != 0 {
			t.Errorf("%s: an exception with a reason did not excuse it", what)
		}
	}
	for what, src := range map[string]string{
		"a parameter passed on": "func (s *S) run(ctx context.Context, q string) { s.pool.Exec(ctx, q) }",
		"the context":           "func f(ctx context.Context, p pool) { p.Exec(ctx, `UPDATE t SET a = 1`) }",
	} {
		if _, unread := assembleOne(t, src); len(unread) != 0 {
			t.Errorf("%s: reported %+v", what, unread)
		}
	}
	// A READ with no fixed text is reported too (0.29.210): `UPDATE ...
	// RETURNING` runs through Query, and the store's own libyear replace
	// does. It was left out "so as not to bury the writes"; counting them
	// added eight reports in the whole store.
	_, unread := assembleOne(t, "func f(ctx context.Context, p pool, j job) { p.Query(ctx, j.sql) }")
	if len(unread) != 1 || !unread[0].SQLArg || unread[0].WriteArg || unread[0].Arg != "j.sql" {
		t.Errorf("a read with no fixed text: %+v; want it reported as an SQL argument", unread)
	} else if rep := CheckUnreadable(unread, registry, nil, nil, nil); len(rep.Violations) != 1 {
		t.Errorf("a read with no fixed text is not a violation: %+v", rep)
	}
}

// A statement Statements already reads (a backtick literal, a constant
// whose value is one) is not handed on a second time because it is an
// argument.
func TestAnArgumentAlreadyReadIsNotReadTwice(t *testing.T) {
	whole, unread := assembleOne(t, "const q = `UPDATE t SET a = 1`\n\nfunc f(ctx context.Context, p pool) {\n\tp.Exec(ctx, q)\n\tp.Exec(ctx, `UPDATE t SET a = 2`)\n}")
	if len(whole) != 0 || len(unread) != 0 {
		t.Errorf("whole %q, unread %+v; want neither (Statements reads both literals)", sqlOf(whole), unread)
	}
}

// A fill for an argument with no fixed text (Match is the hole itself)
// names the argument, as written, and matches that argument only: never
// another statement of the declaration that merely contains a hole, and
// never another argument of the same declaration.
func TestAFillForAWholeArgumentMatchesOnlyThat(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: AlwaysRefresh, Reason: "r"}}
	_, unread := assembleOne(t, "func f(ctx context.Context, p pool, j job, tb string) {\n\tp.Exec(ctx, j.sql)\n\tp.Exec(ctx, j.other)\n\tp.Exec(ctx, `UPDATE `+tb+` SET a = 1`)\n}")
	if len(unread) != 3 || unread[0].Arg != "j.sql" || unread[1].Arg != "j.other" || unread[2].Text == Unknown {
		t.Fatalf("unread %+v; want the two arguments named as written, and the sum with its fixed text", unread)
	}
	fill := Fill{Decl: "f", Match: Unknown, Arg: "j.sql", Reason: "the statements the field takes", Values: [][]string{{"UPDATE aveloxis_data.repos SET a = NOW()"}}}
	rep := CheckUnreadable(unread, registry, []Fill{fill}, nil, nil)
	if len(rep.BadFills) != 0 || sqlOf(rep.Statements) != "UPDATE aveloxis_data.repos SET a = NOW()" {
		t.Errorf("bad fills %v, statements %q; want the one argument filled once", rep.BadFills, sqlOf(rep.Statements))
	}
	if len(rep.Violations) != 2 {
		t.Errorf("violations %+v; want the other argument and the other statement still violations", rep.Violations)
	}
	// 0.29.211 (review round 4, two behaviours no test held). A fill for a
	// whole argument does not match one that has fixed text, though it
	// names it as written; and a fill that matches nothing is stale.
	partial := Fill{Decl: "f", Match: Unknown, Arg: unread[2].Arg, Reason: "r", Values: [][]string{{"UPDATE aveloxis_data.repos SET a = NOW()"}}}
	if rep = CheckUnreadable(unread, registry, []Fill{partial}, nil, nil); len(rep.Statements) != 0 || len(rep.Violations) != 3 || len(rep.StaleFills) != 1 {
		t.Errorf("a whole-argument fill naming an argument with fixed text: statements %q, violations %d, stale %d; want nothing filled and the fill stale", sqlOf(rep.Statements), len(rep.Violations), len(rep.StaleFills))
	}
	absent := Fill{Decl: "f", Match: Unknown, Arg: "j.none", Reason: "r", Values: [][]string{{"SELECT 1"}}}
	if rep = CheckUnreadable(unread, registry, []Fill{fill, absent}, nil, nil); len(rep.StaleFills) != 1 || rep.StaleFills[0].Arg != "j.none" {
		t.Errorf("a fill for an argument the declaration does not have: stale %+v, want that one", rep.StaleFills)
	}
	// One that names no argument is refused: it would cover any.
	fill.Arg = ""
	if rep = CheckUnreadable(unread, registry, []Fill{fill}, nil, nil); len(rep.BadFills) != 1 || len(rep.Statements) != 0 || len(rep.Violations) != 3 {
		t.Errorf("a whole-argument fill that names no argument: bad fills %v, statements %q, violations %d", rep.BadFills, sqlOf(rep.Statements), len(rep.Violations))
	}
}
