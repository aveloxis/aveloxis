// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"regexp"
	"strings"
	"testing"
)

// A statement assembled in Go is read whole when every part of it is fixed
// text, and reported when a part is not (v0.29.203; summary/71's 0.29.166
// item). Until then such a statement reached the write-policy check in
// pieces, or not at all.

func assembleOne(t *testing.T, src string) ([]Stmt, []Unreadable) {
	t.Helper()
	whole, unread, err := Assemble(map[string]string{"internal/db/x.go": "package db\n\n" + src})
	if err != nil {
		t.Fatal(err)
	}
	return whole, unread
}

func sqlOf(stmts []Stmt) string {
	var out []string
	for _, s := range stmts {
		out = append(out, strings.Join(strings.Fields(s.SQL), " "))
	}
	return strings.Join(out, " ;; ")
}

func TestAssembleReadsFixedText(t *testing.T) {
	for _, c := range []struct{ what, src, want string }{
		{"a constant and a tail", "const stamp = `UPDATE t SET a = NOW()`\n\nfunc f() string { return stamp + ` WHERE id = $1` }",
			"UPDATE t SET a = NOW() WHERE id = $1"},
		{"a constant made of constants", "const a = `UPDATE t SET `\nconst b = `x = 1`\nconst c = a + b + ` WHERE y`",
			"UPDATE t SET x = 1 WHERE y"},
		{"an interpreted string beside a raw one", "func f() string { return \"UPDATE t \" + `SET a = 1` }",
			"UPDATE t SET a = 1"},
		{"a local assigned once", "func f() string {\n\tsub := `SELECT id FROM u`\n\treturn `UPDATE t SET a = 1 WHERE id IN (` + sub + `)`\n}",
			"UPDATE t SET a = 1 WHERE id IN (SELECT id FROM u)"},
		{"a format string with fixed arguments", "const q = `UPDATE %s SET a = 1 WHERE b = %s`\n\nfunc f() string { return fmt.Sprintf(q, \"t\", \"$1\") }",
			"UPDATE t SET a = 1 WHERE b = $1"},
		{"a joined list of fixed columns", "var cols = []string{\"a\", \"b\"}\n\nfunc f() string { return `INSERT INTO t (` + strings.Join(cols, \", \") + `) VALUES ($1, $2)` }",
			"INSERT INTO t (a, b) VALUES ($1, $2)"},
		{"a one-line builder called with fixed text", "func wrap(inner string) string { return `WITH w AS (` + inner + `) UPDATE t SET a = 1` }\n\nfunc f() string { return wrap(`SELECT 1`) }",
			"WITH w AS (SELECT 1) UPDATE t SET a = 1"},
		{"a builder of a builder", "const tail = ` WHERE z`\n\nfunc pred(col string) string { return col + ` = 1` }\n\nfunc f() string { return `UPDATE t SET a = 2 WHERE ` + pred(`b`) + tail }",
			"UPDATE t SET a = 2 WHERE b = 1 WHERE z"},
		{"a package variable nothing assigns", "var table = `t`\n\nfunc f() string { return `UPDATE ` + table + ` SET a = 1` }",
			"UPDATE t SET a = 1"},
		// Every value a local is given, when each is fixed text: one
		// statement per value.
		{"a local assigned twice", "func f(b bool) string {\n\ttail := ` WHERE x`\n\tif b {\n\t\ttail = ` WHERE y`\n\t}\n\treturn `UPDATE t SET a = 1` + tail\n}",
			"UPDATE t SET a = 1 WHERE x ;; UPDATE t SET a = 1 WHERE y"},
		// A statement in a loop over a fixed list: one per element.
		{"a loop over an inline list of strings", "func f() (out []string) {\n\tfor _, tb := range []string{`t`, `u`} {\n\t\tout = append(out, `UPDATE `+tb+` SET a = 1`)\n\t}\n\treturn\n}",
			"UPDATE t SET a = 1 ;; UPDATE u SET a = 1"},
		{"a loop over a local list of structs, positional", "func f() (out []string) {\n\tfixes := []struct {\n\t\ttable, column string\n\t\tnow           bool\n\t}{\n\t\t{`t`, `a`, false},\n\t\t{`u`, `b`, true},\n\t}\n\tfor _, x := range fixes {\n\t\tout = append(out, fmt.Sprintf(`UPDATE %s SET %s = 1`, x.table, x.column))\n\t}\n\treturn\n}",
			"UPDATE t SET a = 1 ;; UPDATE u SET b = 1"},
		{"a loop over a package list of a named struct, keyed", "type target struct{ Table, PK string }\n\nvar targets = []target{{Table: `t`, PK: `id`}, {PK: `uid`, Table: `u`}}\n\nfunc win(table, pk string) string { return `UPDATE ` + table + ` SET a = 1 WHERE ` + pk + ` > $1` }\n\nfunc f() (out []string) {\n\tfor _, x := range targets {\n\t\tout = append(out, win(x.Table, x.PK))\n\t}\n\treturn\n}",
			"UPDATE t SET a = 1 WHERE id > $1 ;; UPDATE u SET a = 1 WHERE uid > $1"},
		{"a loop and a local with two values: every pairing", "func f() (out []string) {\n\tfor _, x := range []struct {\n\t\tc   string\n\t\tnow bool\n\t}{{`a`, false}, {`b`, true}} {\n\t\tv := `NULL`\n\t\tif x.now {\n\t\t\tv = `NOW()`\n\t\t}\n\t\tout = append(out, fmt.Sprintf(`UPDATE t SET %s = %s`, x.c, v))\n\t}\n\treturn\n}",
			"UPDATE t SET a = NULL ;; UPDATE t SET b = NOW()"},
		{"the flag negated", "func f() (out []string) {\n\tfor _, x := range []struct {\n\t\tc    string\n\t\tkeep bool\n\t}{{`a`, false}, {`b`, true}} {\n\t\tv := `NOW()`\n\t\tif !x.keep {\n\t\t\tv = `NULL`\n\t\t}\n\t\tout = append(out, `UPDATE t SET `+x.c+` = `+v)\n\t}\n\treturn\n}",
			"UPDATE t SET a = NULL ;; UPDATE t SET b = NOW()"},
		// With an else arm: the then arm follows the flag, the else arm is
		// not read for it and stays a possible value.
		{"the flag with an else arm", "func f() (out []string) {\n\tfor _, x := range []struct {\n\t\tc   string\n\t\tnow bool\n\t}{{`a`, false}, {`b`, true}} {\n\t\tv := `0`\n\t\tif x.now {\n\t\t\tv = `NOW()`\n\t\t} else {\n\t\t\tv = `NULL`\n\t\t}\n\t\tout = append(out, `UPDATE t SET `+x.c+` = `+v)\n\t}\n\treturn\n}",
			"UPDATE t SET a = 0 ;; UPDATE t SET a = NULL ;; UPDATE t SET b = NOW() ;; UPDATE t SET b = NULL"},
		// Where which value holds cannot be told, every value is read.
		{"a value given after the statement is built", "func f() (string, string) {\n\tv := `1`\n\tq := `UPDATE t SET a = ` + v\n\tv = `2`\n\treturn q, v\n}",
			"UPDATE t SET a = 1 ;; UPDATE t SET a = 2"},
		{"a value captured before it is replaced", "func f() string {\n\tv := `1`\n\tw := v\n\tv = `2`\n\treturn `UPDATE t SET a = ` + w + `, b = ` + v\n}",
			"UPDATE t SET a = 1, b = 2 ;; UPDATE t SET a = 2, b = 2"},
		{"a value given in a function literal", "func f(run func(func())) string {\n\tv := `1`\n\trun(func() { v = `2` })\n\tv = `3`\n\treturn `UPDATE t SET a = ` + v\n}",
			"UPDATE t SET a = 2 ;; UPDATE t SET a = 3"},
		{"a value given in both arms", "func f(b bool) string {\n\tv := `0`\n\tif b {\n\t\tv = `1`\n\t} else {\n\t\tv = `2`\n\t}\n\treturn `UPDATE t SET a = ` + v\n}",
			"UPDATE t SET a = 0 ;; UPDATE t SET a = 1 ;; UPDATE t SET a = 2"},
		{"a later plain value replaces the earlier ones", "func f(b bool) string {\n\tv := `0`\n\tif b {\n\t\tv = `1`\n\t}\n\tv = `2`\n\treturn `UPDATE t SET a = ` + v\n}",
			"UPDATE t SET a = 2"},
		{"a later value in an inner block does not replace for a use outside it", "func f(b bool) string {\n\tv := `0`\n\t{\n\t\tv = `1`\n\t\t_ = v\n\t}\n\tfor b {\n\t\tv = `2`\n\t}\n\treturn `UPDATE t SET a = ` + v\n}",
			"UPDATE t SET a = 0 ;; UPDATE t SET a = 1 ;; UPDATE t SET a = 2"},
		{"numbered format arguments", "const q = `UPDATE t SET a = %[2]s, b = %[1]s, c = %s`\n\nfunc f() string { return fmt.Sprintf(q, `1`, `2`) }",
			"UPDATE t SET a = 2, b = 1, c = 2"},
		{"two statements in one assembled text", "const a = `UPDATE t SET x = 1; `\n\nfunc f() string { return a + `UPDATE u SET y = 2` }",
			"UPDATE t SET x = 1 ;; UPDATE u SET y = 2"},
	} {
		whole, unread := assembleOne(t, c.src)
		if got := sqlOf(whole); got != c.want {
			t.Errorf("%s:\n got %q\nwant %q", c.what, got, c.want)
		}
		if len(unread) != 0 {
			t.Errorf("%s: reported unreadable: %+v", c.what, unread)
		}
	}
}

// What is not fixed text is reported with what stands in its way, and with
// the fixed parts around it, so that it can be asked which table it writes.
func TestAssembleReportsWhatItCannotRead(t *testing.T) {
	for _, c := range []struct{ what, src, why, textHas string }{
		{"a parameter", "func f(where string) string { return `UPDATE t SET a = 1 WHERE ` + where }", "where", "UPDATE t SET a = 1 WHERE"},
		{"a local assigned from a parameter on one path", "func f(b bool, w string) string {\n\ttail := ` WHERE x`\n\tif b {\n\t\ttail = w\n\t}\n\treturn `UPDATE t SET a = 1` + tail\n}", "w", "UPDATE t SET a = 1"},
		{"a local defined in terms of itself", "func f(b bool) string {\n\ttail := ` WHERE x`\n\tif b {\n\t\ttail = tail + ` AND y`\n\t}\n\treturn `UPDATE t SET a = 1` + tail\n}", "tail", "UPDATE t SET a = 1"},
		{"a loop over a list that is not fixed", "func f(tables []string) (out []string) {\n\tfor _, tb := range tables {\n\t\tout = append(out, `UPDATE `+tb+` SET a = 1`)\n\t}\n\treturn\n}", "tb", "SET a = 1"},
		// (A local built up with += is READ since 0.29.208: assemble_round1_test.go.)
		{"a local changed through its address", "func f(g func(*string)) string {\n\tq := `UPDATE t SET a = 1`\n\tg(&q)\n\treturn q + ` AND y`\n}", "q", "AND y"},
		{"a package variable that is assigned after its declaration", "var table = `t`\n\nfunc g() { table = `u` }\n\nfunc f() string { return `UPDATE ` + table + ` SET a = 1` }", "table", "SET a = 1"},
		{"a call that is not a one-line builder", "func pick(b bool) string {\n\tif b {\n\t\treturn `x`\n\t}\n\treturn `y`\n}\n\nfunc f() string { return `UPDATE t SET a = ` + pick(true) }", "pick", "UPDATE t SET a ="},
		{"a format string with an argument that is not fixed", "func f(n string) string { return fmt.Sprintf(`UPDATE %s SET a = 1`, n) }", "n", "UPDATE"},
		{"a format string with a verb that is not %s", "func f() string { return fmt.Sprintf(`UPDATE t SET a = %d`, 3) }", "%d", "UPDATE t SET a ="},
		{"a joined list that is appended to", "var cols = []string{\"a\"}\n\nfunc g() { cols = append(cols, \"b\") }\n\nfunc f() string { return `INSERT INTO t (` + strings.Join(cols, \", \") + `)` }", "cols", "INSERT INTO t ("},
		{"a builder called with something not fixed", "func wrap(inner string) string { return `WITH w AS (` + inner + `) UPDATE t SET a = 1` }\n\nfunc f(x string) string { return wrap(x) }", "x", "UPDATE t SET a = 1"},
	} {
		whole, unread := assembleOne(t, c.src)
		// Reported once, with the reason and the fixed text around the
		// hole. (An inner sum of the same function may be reported beside
		// it; that one does not hold the statement's text.)
		n := 0
		for _, u := range unread {
			if strings.Contains(u.Why, c.why) && strings.Contains(u.Text, c.textHas) && u.File == "internal/db/x.go" && u.Line != 0 {
				n++
			}
		}
		if n != 1 {
			t.Errorf("%s: unreadable %+v, read %q; want one report whose reason names %q and whose text holds %q", c.what, unread, sqlOf(whole), c.why, c.textHas)
		}
	}
}

// A lone string is not this function's business (Statements reads those).
// A one-line builder's body is judged where it is called, when every use of
// it is a call that can be seen; one nobody here calls is reported, because
// its callers cannot be seen.
func TestAssembleLeavesLoneStringsAndJudgesBuildersAtTheirCalls(t *testing.T) {
	whole, unread := assembleOne(t, "const q = `UPDATE t SET a = 1`\n\nfunc f() string { return q }")
	if len(whole) != 0 || len(unread) != 0 {
		t.Errorf("read %q, unreadable %+v; want neither", sqlOf(whole), unread)
	}
	const wrap = "func wrap(inner string) string { return `WITH w AS (` + inner + `) UPDATE t SET a = 1` }"
	if whole, unread = assembleOne(t, wrap); len(whole) != 0 || len(unread) != 1 || !strings.Contains(unread[0].Why, "inner") {
		t.Errorf("a builder nobody calls: read %q, unreadable %+v; want it reported for its parameter", sqlOf(whole), unread)
	}
	// Handed about as a value: its callers cannot all be seen either.
	if _, unread = assembleOne(t, wrap+"\n\nvar keep = wrap\n\nfunc f() string { return wrap(`SELECT 1`) }"); len(unread) != 1 {
		t.Errorf("a builder also used as a value: unreadable %+v; want its body reported", unread)
	}
	// An arithmetic sum and a message with no fixed text are nobody's SQL.
	if whole, unread = assembleOne(t, "func f(a, b int, s, u string) (int, string) { return a + b, s + u }"); len(whole) != 0 || len(unread) != 0 {
		t.Errorf("sums with no fixed text: read %q, unreadable %+v; want neither", sqlOf(whole), unread)
	}
	// A statement inside a function literal inside an assembled expression.
	whole, _ = assembleOne(t, "func f(run func(func() string) string) string {\n\treturn `x: ` + run(func() string { return `UPDATE t SET ` + `a = 1` })\n}")
	if !strings.Contains(sqlOf(whole), "UPDATE t SET a = 1") {
		t.Errorf("a nested function literal's statement was not read: %q", sqlOf(whole))
	}
	// Text in a comment is not code.
	whole, unread = assembleOne(t, "// const q = `UPDATE t SET a = 1` + x\nfunc f() {}")
	if len(whole) != 0 || len(unread) != 0 {
		t.Errorf("a commented-out concatenation was read: %q %+v", sqlOf(whole), unread)
	}
	if _, _, err := Assemble(map[string]string{"internal/db/bad.go": "package db\n\nfunc {"}); err == nil {
		t.Error("a file that does not parse must be an error, not an empty result")
	}
}

// The write-policy check over what could not be read whole. Nothing is
// trusted: a statement is checked on its fixed text when its holes cannot
// matter, or checked once per real value its holes take; anything else is a
// violation.
func TestCheckUnreadable(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: AlwaysRefresh, Reason: "r"}}

	// (1) NOTHING is judged harmless (0.29.210). A hole in a WHERE, or
	// inside the parentheses of another column's value, used to be handed
	// on as one that "cannot matter"; three review rounds each found a
	// place where it could. A statement that shows a write to a registered
	// table and has a hole is a violation wherever the hole stands.
	_, unread := assembleOne(t, "func f(w string) string { return `UPDATE aveloxis_data.repos SET a = $1 WHERE id = $2 AND ` + w }\n\n"+
		"func g(w string) string { return fmt.Sprintf(`WITH c AS (UPDATE aveloxis_data.repos SET a = $1, b = COALESCE(%s, 0) WHERE %s RETURNING id) SELECT COUNT(*) FROM c`, w, w) }\n\n"+
		"func other(w string) string { return `UPDATE aveloxis_data.other SET a = 1 WHERE ` + w }\n\n"+
		"func read(w string) string { return `SELECT a FROM aveloxis_data.repos WHERE ` + w }")
	rep := CheckUnreadable(unread, registry, nil, nil, nil)
	if len(rep.Violations) != 2 || len(rep.Statements) != 0 {
		t.Fatalf("violations %+v, statements %q; want the two writes to the registered table as violations and nothing handed on (the other table's and the read are fragments)", rep.Violations, sqlOf(rep.Statements))
	}
	// The same four as SQL ARGUMENTS: every one is a violation, the other
	// table's write and the read included. An argument is never passed on
	// what its fixed text looks like.
	_, unread = assembleOne(t, "func f(ctx context.Context, p pool, w string) {\n"+
		"\tp.Exec(ctx, `UPDATE aveloxis_data.repos SET a = $1 WHERE id = $2 AND `+w)\n"+
		"\tp.Exec(ctx, `UPDATE aveloxis_data.other SET a = 1 WHERE `+w)\n"+
		"\tp.Query(ctx, `SELECT a FROM aveloxis_data.repos WHERE `+w)\n"+
		"\tp.Exec(ctx, `SAVEPOINT sp_`+w)\n}")
	// This test is of CheckUnreadable's bookkeeping, on text read by name
	// (no kind is verified there): the parts are marked as placeholders by
	// hand. What a kind is, and that an unverified one is covered by no
	// shape, is TestAShapeRestsOnVerifiedKinds.
	asPlaceholders := func(us []Unreadable) {
		for i := range us {
			for j := range us[i].Kinds {
				us[i].Kinds[j] = KindRepeated
			}
		}
	}
	asPlaceholders(unread)
	if rep = CheckUnreadable(unread, registry, nil, nil, nil); len(rep.Violations) != 4 || len(rep.Statements) != 0 {
		t.Fatalf("four arguments with a hole: violations %+v, statements %q; want four violations", rep.Violations, sqlOf(rep.Statements))
	}
	// A registered SHAPE covers the declarations it lists, for arguments
	// whose whole text matches it; they are handed on with their holes.
	read := Shape{Name: "read", Args: []ShapeArg{{Decl: "f", Call: 3, Of: 4, Kinds: KindRepeated}}, Text: regexp.MustCompile(`^SELECT [^;]*$`), Forbid: regexp.MustCompile(`(?i)\b(INSERT|UPDATE|DELETE|MERGE)\b`), Reason: "filters built from validated input"}
	savepoint := Shape{Name: "savepoint", Args: []ShapeArg{{Decl: "f", Call: 4, Of: 4, Kinds: KindRepeated}}, Text: regexp.MustCompile(`^SAVEPOINT sp_` + regexp.QuoteMeta(Unknown) + `$`), Reason: "a counter"}
	rep = CheckUnreadable(unread, registry, nil, nil, []Shape{read, savepoint})
	if len(rep.Violations) != 2 || len(rep.Statements) != 2 || len(rep.StaleShapes) != 0 {
		t.Errorf("with two shapes: violations %+v, statements %q, stale %v; want the two UPDATEs still violations", rep.Violations, sqlOf(rep.Statements), rep.StaleShapes)
	}
	// A shape lists declarations: one it lists with nothing of that shape
	// is stale, another declaration's argument is not covered, and a text
	// that matches Forbid is not covered.
	other := Shape{Name: "read", Args: []ShapeArg{{Decl: "g", Call: 3, Of: 4, Kinds: KindRepeated}}, Text: read.Text, Reason: "r"}
	if rep = CheckUnreadable(unread, registry, nil, nil, []Shape{other}); len(rep.Violations) != 4 || len(rep.StaleShapes) != 1 {
		t.Errorf("a shape for another declaration: violations %d, stale %v", len(rep.Violations), rep.StaleShapes)
	}
	// 0.29.211: a shape covers an ARGUMENT, by its place in a declaration
	// of that many SQL arguments. The read is the third of four: an entry
	// for another place, or for a declaration of another size (what a
	// statement added to f makes of every entry), covers nothing and is
	// stale; and an entry that does not say what its unread parts are is
	// refused.
	for what, entry := range map[string]ShapeArg{
		"another place": {Decl: "f", Call: 2, Of: 4, Kinds: KindRepeated},
		"another count": {Decl: "f", Call: 3, Of: 5, Kinds: KindRepeated},
	} {
		sh := Shape{Name: "read", Args: []ShapeArg{entry}, Text: read.Text, Forbid: read.Forbid, Reason: "r"}
		if rep = CheckUnreadable(unread, registry, nil, nil, []Shape{sh}); len(rep.Violations) != 4 || len(rep.StaleShapes) != 1 {
			t.Errorf("a shape entry for %s: violations %d, stale %v; want all four violations and the entry stale", what, len(rep.Violations), rep.StaleShapes)
		}
	}
	noPart := Shape{Name: "read", Args: []ShapeArg{{Decl: "f", Call: 3, Of: 4}}, Text: read.Text, Forbid: read.Forbid, Reason: "r"}
	if rep = CheckUnreadable(unread, registry, nil, nil, []Shape{noPart}); len(rep.BadFills) == 0 {
		t.Error("a shape entry that does not say what its unread parts are was accepted")
	}
	wide := Shape{Name: "anything", Args: []ShapeArg{{Decl: "f", Call: 1, Of: 4, Kinds: KindRepeated}, {Decl: "f", Call: 2, Of: 4, Kinds: KindRepeated}, {Decl: "f", Call: 3, Of: 4, Kinds: KindRepeated}, {Decl: "f", Call: 4, Of: 4, Kinds: KindRepeated}}, Text: regexp.MustCompile(`.*`), Forbid: regexp.MustCompile(`(?i)\bUPDATE\b`), Reason: "r"}
	if rep = CheckUnreadable(unread, registry, nil, nil, []Shape{wide}); len(rep.Violations) != 2 {
		t.Errorf("a forbidden word: violations %d, want the two UPDATEs", len(rep.Violations))
	}
	if rep = CheckUnreadable(unread, registry, nil, nil, []Shape{{Name: "no reason", Args: []ShapeArg{{Decl: "f", Call: 3, Of: 4, Kinds: KindRepeated}}, Text: read.Text}}); len(rep.BadFills) == 0 {
		t.Error("a shape with no reason was accepted")
	}

	// (2) A hole that can matter is a violation: in the SET list itself
	// (it could assign the column), in the column list or the values of an
	// INSERT, or as the table.
	for what, src := range map[string]string{
		"the SET list":               "func f(s string) string { return `UPDATE aveloxis_data.repos SET b = 1, ` + s + ` WHERE id = $1` }",
		"a value at the top of SET":  "func f(s string) string { return `UPDATE aveloxis_data.repos SET b = ` + s + ` WHERE id = $1` }",
		"an INSERT's column list":    "func f(s string) string { return `INSERT INTO aveloxis_data.repos (` + s + `) VALUES ($1)` }",
		"a conflict's SET":           "func f(s string) string { return `INSERT INTO aveloxis_data.repos (a) VALUES ($1) ON CONFLICT (id) DO UPDATE SET ` + s }",
		"after DISTINCT FROM in SET": "func f(s string) string { return `UPDATE aveloxis_data.repos SET b = c IS DISTINCT FROM ` + s + ` WHERE id = $1` }",
		"the table":                  "func f(tb string) string { return `UPDATE ` + tb + ` SET a = 1` }",
		"the table, formatted":       "func f(tb string) string { return fmt.Sprintf(`INSERT INTO %s (a) VALUES (1)`, tb) }",
	} {
		_, unread = assembleOne(t, src)
		if rep = CheckUnreadable(unread, registry, nil, nil, nil); len(rep.Violations) != 1 || len(rep.Statements) != 0 {
			t.Errorf("a hole in %s: violations %+v, statements %q; want one violation and nothing handed on", what, rep.Violations, sqlOf(rep.Statements))
		}
	}
	// A message is not a write.
	_, unread = assembleOne(t, "func h(err string) string { return `could not update ` + err }")
	if rep = CheckUnreadable(unread, registry, nil, nil, nil); len(rep.Violations) != 0 || len(rep.Statements) != 0 {
		t.Errorf("a message: %+v %q", rep.Violations, sqlOf(rep.Statements))
	}

	// (3) A shaped argument is handed on with its holes; a hole inside a
	// registered column's own value reaches Check, which cannot judge it
	// and says so.
	_, unread = assembleOne(t, "func f(ctx context.Context, p pool, v string) { p.Exec(ctx, `UPDATE aveloxis_data.repos SET a = COALESCE(`+v+`, a) WHERE id = $1`) }")
	asPlaceholders(unread)
	rep = CheckUnreadable(unread, registry, nil, nil, []Shape{{Name: "s", Args: []ShapeArg{{Decl: "f", Call: 1, Of: 1, Kinds: KindRepeated}}, Text: regexp.MustCompile(`^UPDATE aveloxis_data\.repos SET a = COALESCE\(`), Reason: "r"}})
	if len(rep.Statements) != 1 {
		t.Fatalf("statements %q, violations %+v", sqlOf(rep.Statements), rep.Violations)
	}
	if r := Check(rep.Statements, registry, nil); len(r.Violations) != 1 || !strings.Contains(r.Violations[0].Detail, "not fixed text") {
		t.Errorf("a hole in the registered column's value: Check says %+v; want one violation that names the hole", r.Violations)
	}

	// (4) A fill gives the real values the holes take, one row per
	// instance; each instance is a whole statement.
	_, unread = assembleOne(t, "func rot(tb, hist string) string { return fmt.Sprintf(`INSERT INTO %s SELECT * FROM %s WHERE repo_id = $1`, hist, tb) }")
	fill := Fill{Decl: "rot", Match: "SELECT * FROM", Reason: "the allowlist", Values: [][]string{{"aveloxis_data.h1", "aveloxis_data.t1"}, {"aveloxis_data.repos", "aveloxis_data.t2"}}}
	rep = CheckUnreadable(unread, registry, []Fill{fill}, nil, nil)
	if len(rep.Violations) != 0 || sqlOf(rep.Statements) != "INSERT INTO aveloxis_data.h1 SELECT * FROM aveloxis_data.t1 WHERE repo_id = $1 ;; INSERT INTO aveloxis_data.repos SELECT * FROM aveloxis_data.t2 WHERE repo_id = $1" {
		t.Errorf("filled: violations %+v, statements %q", rep.Violations, sqlOf(rep.Statements))
	}
	// A row with the wrong number of values, no rows, no reason, another
	// declaration, a fill that matches nothing: each is reported, none excuses.
	for what, f := range map[string]Fill{
		"a row too short":     {Decl: "rot", Match: "SELECT * FROM", Reason: "r", Values: [][]string{{"only-one"}}},
		"no rows":             {Decl: "rot", Match: "SELECT * FROM", Reason: "r"},
		"no reason":           {Decl: "rot", Match: "SELECT * FROM", Values: fill.Values},
		"another declaration": {Decl: "other", Match: "SELECT * FROM", Reason: "r", Values: fill.Values},
		"no match":            {Decl: "rot", Match: "no such statement", Reason: "r", Values: fill.Values},
	} {
		rep = CheckUnreadable(unread, registry, []Fill{f}, nil, nil)
		if len(rep.Violations)+len(rep.BadFills)+len(rep.StaleFills) == 0 {
			t.Errorf("%s: accepted (statements %q)", what, sqlOf(rep.Statements))
		}
		if what != "no reason" && len(rep.Statements) != 0 && what != "a row too short" {
			t.Errorf("%s: handed on %q", what, sqlOf(rep.Statements))
		}
	}

	// (5) The last resort: an exception with a reason, for its own declaration;
	// one that covers nothing is stale.
	_, unread = assembleOne(t, "func f(tb string) string { return `UPDATE ` + tb + ` SET a = 1` }")
	ex := []UnreadableException{{Decl: "f", Match: "SET a = 1", Reason: "why"}, {Decl: "f", Match: "no such statement", Reason: "stale"}}
	rep = CheckUnreadable(unread, registry, nil, ex, nil)
	if len(rep.Violations) != 0 || len(rep.StaleExceptions) != 1 || rep.StaleExceptions[0].Match != "no such statement" {
		t.Errorf("with exceptions: %+v, stale %+v", rep.Violations, rep.StaleExceptions)
	}
	for what, e := range map[string]UnreadableException{"another declaration": {Decl: "g", Match: "SET a = 1", Reason: "r"}, "no reason": {Decl: "f", Match: "SET a = 1"}} {
		if rep = CheckUnreadable(unread, registry, nil, []UnreadableException{e}, nil); len(rep.Violations) != 1 {
			t.Errorf("an exception with %s excused the statement", what)
		}
	}
}

// The stored value may be named in full: schema.table.column (the Jira
// comment upsert, read whole for the first time in 0.29.203, names it so).
func TestStoredValueNamedInFull(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.m", Column: "u", Policy: GreatestNonNull, Reason: "r"}}
	ok := Stmt{File: "f.go", SQL: "INSERT INTO aveloxis_data.m (id, u) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET u = GREATEST(COALESCE(EXCLUDED.u, aveloxis_data.m.u), COALESCE(aveloxis_data.m.u, EXCLUDED.u))"}
	if r := Check([]Stmt{ok}, registry, nil); len(r.Violations) != 0 {
		t.Errorf("the full name of the stored value was not recognised: %+v", r.Violations)
	}
	// Another table's column of that name is not the stored value, and
	// EXCLUDED in any case is the incoming one.
	for _, bad := range []string{
		"GREATEST(COALESCE(EXCLUDED.u, aveloxis_data.other.x), COALESCE(aveloxis_data.other.x, EXCLUDED.u))",
		"GREATEST(COALESCE(EXCLUDED.u, a.b.c.u), COALESCE(a.b.c.u, EXCLUDED.u))",
	} {
		s := Stmt{File: "f.go", SQL: "INSERT INTO aveloxis_data.m (id, u) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET u = " + bad}
		if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != 1 {
			t.Errorf("%s: %d violations, want 1", bad, len(r.Violations))
		}
	}
}

// One statement can write two tables (a CTE clears a repository's gone mark
// and the statement proper upserts its queue row; read whole for the first
// time in 0.29.203). An assignment belongs to the write it stands in: the
// queue's `updated_at = NOW()` is not the repository's updated_at.
func TestAnAssignmentBelongsToItsOwnWrite(t *testing.T) {
	const two = `WITH cleared AS (
		UPDATE aveloxis_data.repos SET gone = NULL, updated_at = GREATEST(COALESCE($2, repos.updated_at), COALESCE(repos.updated_at, $2))
		WHERE repo_id = $1 RETURNING repo_id)
	INSERT INTO aveloxis_ops.queue (repo_id, note) SELECT $1, 'UPDATE aveloxis_data.repos SET x' WHERE EXISTS (SELECT 1 FROM cleared)
	ON CONFLICT (repo_id) DO UPDATE SET status = 'queued', updated_at = NOW()`
	s := Stmt{File: "f.go", SQL: two}
	if got := s.SetExprsOf("aveloxis_data.repos", "updated_at"); len(got) != 1 || !strings.HasPrefix(got[0], "GREATEST(") {
		t.Errorf("the repository's updated_at: %q, want its own one assignment", got)
	}
	if got := s.SetExprsOf("aveloxis_ops.queue", "updated_at"); len(got) != 1 || got[0] != "NOW()" {
		t.Errorf("the queue's updated_at: %q, want NOW()", got)
	}
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "updated_at", Policy: GreatestNonNull, Reason: "r"}}
	if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != 0 {
		t.Errorf("the queue's assignment was judged as the repository's: %+v", r.Violations)
	}
	// The repository's own assignment is still judged, wherever it stands.
	for what, sql := range map[string]string{
		"in the CTE":            strings.Replace(two, "GREATEST(COALESCE($2, repos.updated_at), COALESCE(repos.updated_at, $2))", "NOW()", 1),
		"a plain update":        "UPDATE aveloxis_data.repos SET updated_at = NOW() WHERE repo_id = $1",
		"UPDATE ONLY":           "UPDATE ONLY aveloxis_data.repos SET a = 1, updated_at = NOW()",
		"lower case":            "update aveloxis_data.repos set updated_at = NOW()",
		"a conflict's update":   "INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET updated_at = NOW()",
		"after another's write": "WITH q AS (INSERT INTO aveloxis_ops.queue (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET updated_at = NOW() RETURNING id) UPDATE aveloxis_data.repos SET updated_at = NOW() FROM q",
		// Where the write cannot be told, the assignment counts (fail closed).
		"a table by alias only": "WITH w AS (UPDATE aveloxis_data.repos SET a = 1 RETURNING id) UPDATE r SET updated_at = NOW() FROM w",
		"MERGE":                 "MERGE INTO aveloxis_data.repos r USING s ON r.id = s.id WHEN MATCHED THEN UPDATE SET updated_at = NOW()",
	} {
		if r := Check([]Stmt{{File: "f.go", SQL: sql}}, registry, nil); len(r.Violations) != 1 {
			t.Errorf("%s: %d violations, want 1", what, len(r.Violations))
		}
	}
}

// A column assigned by its quoted name is that column (the pre-1970 repair
// writes `SET "col" = ...` for each row of a list; a mutant that added a
// protected column to the list passed until 0.29.203). A quoted name is
// exact: "Updated_At" is another column.
func TestAQuotedColumnIsAssigned(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "updated_at", Policy: GreatestNonNull, Reason: "r"}}
	for sql, want := range map[string]int{
		`UPDATE aveloxis_data.repos SET "updated_at" = NOW() WHERE "updated_at" IS NOT NULL`: 1,
		`UPDATE aveloxis_data.repos SET a = 1, "updated_at" = NOW()`:                         1,
		`UPDATE aveloxis_data.repos SET "Updated_At" = NOW()`:                                0,
		`UPDATE aveloxis_data.repos SET a = 1 WHERE "updated_at" = NOW()`:                    0,
		`UPDATE aveloxis_data.repos SET "not_updated_at" = NOW()`:                            0,
	} {
		s := Stmt{File: "f.go", SQL: sql}
		if got := len(s.SetExprsOf("aveloxis_data.repos", "updated_at")); got != want {
			t.Errorf("%s: %d assignments read, want %d", sql, got, want)
		}
		if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != want {
			t.Errorf("%s: %d violations, want %d", sql, len(r.Violations), want)
		}
		if want == 1 && !s.WritesColumn("aveloxis_data.repos", "updated_at") {
			t.Errorf("%s: WritesColumn says no", sql)
		}
	}
}

// An unreadable expression names the declaration it stands in.
func TestUnreadableNamesItsDeclaration(t *testing.T) {
	_, unread := assembleOne(t, "type T struct{}\n\nfunc (t *T) M(w string) string { return `UPDATE t SET a = 1 WHERE ` + w }\n\n"+
		"func F(w string) string { return `UPDATE u SET a = 1 WHERE ` + w }\n\n"+
		"var (\n\tfirst = 1\n\tV     = func(w string) string { return `UPDATE v SET a = 1 WHERE ` + w }\n)")
	got := map[string]string{}
	for _, u := range unread {
		got[u.Decl] = strings.Fields(u.Text)[1]
	}
	if len(got) != 3 || got["T.M"] != "t" || got["F"] != "u" || got["V"] != "v" {
		t.Errorf("declarations = %v, want T.M, F and V", got)
	}
}

// An exception that names no file is for ONE statement, whole: its Match
// is the statement's entire text (whitespace aside), not a needle in it. A
// change to the statement (its guard removed, say) ends the exception.
func TestAnExceptionWithNoFileMatchesTheWholeStatement(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: FillEmptyOnly, Reason: "r"}}
	const guarded = `UPDATE aveloxis_data.repos SET "a" = NULL WHERE "a" IS NOT NULL AND EXTRACT(YEAR FROM "a") < 1970`
	ex := []Exception{{Table: "aveloxis_data.repos", Column: "a", Match: guarded, Reason: "garbage only"}}
	if r := Check([]Stmt{{File: "f.go", SQL: "UPDATE aveloxis_data.repos\n\tSET \"a\" = NULL WHERE \"a\" IS NOT NULL AND EXTRACT(YEAR FROM \"a\") < 1970"}}, registry, ex); len(r.Violations) != 0 || len(r.StaleExceptions) != 0 {
		t.Errorf("the statement itself: %+v, stale %+v", r.Violations, r.StaleExceptions)
	}
	for what, sql := range map[string]string{
		"its guard removed":  `UPDATE aveloxis_data.repos SET "a" = NULL WHERE "a" IS NOT NULL`,
		"a longer statement": guarded + ` OR TRUE`,
		"a prefix":           `UPDATE aveloxis_data.repos SET "a" = NULL`,
	} {
		if r := Check([]Stmt{{File: "f.go", SQL: sql}}, registry, ex); len(r.Violations) != 1 || len(r.StaleExceptions) != 1 {
			t.Errorf("%s: %d violations, %d stale; want the violation, and the exception stale", what, len(r.Violations), len(r.StaleExceptions))
		}
	}
}
