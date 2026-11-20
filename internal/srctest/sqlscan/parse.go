// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"errors"
	"fmt"
	"strings"
)

// The statement reader of the write-policy check (0.29.212). It works on
// tokens (lex.go), and it denies by default:
//
//   - text that does not tokenise is not read, and ParseWrites fails;
//   - every word UPDATE, INSERT, DELETE, MERGE, TRUNCATE, COPY and ALTER
//     TABLE that is not in one of the listed places where the word is not
//     a statement (FOR UPDATE, ON DELETE, a trigger's or a grant's list)
//     must be followed by a target this reader understands, at any depth
//     (a data-modifying WITH, a subquery), or ParseWrites fails;
//   - the body of a DO block or of CREATE FUNCTION / PROCEDURE is read as
//     statements too; EXECUTE in such a body is read when it is given one
//     string and fails otherwise (the text is made at run time).
//
// What it does NOT do: it is not a grammar of SQL. It reads the head of
// each write (which table), the SET list (which columns, each with its
// value's text) and the WHERE that follows; everything else in a statement
// is tokens it steps over.

// Write is one place a statement changes rows or a table's stored values.
type Write struct {
	// Kind: "update", "conflict-update" (INSERT ... ON CONFLICT DO UPDATE),
	// "merge-update", "insert", "delete", "merge", "truncate", "copy",
	// "alter".
	Kind string
	// Schema and Table name the target. Schema is "" when the statement
	// does not say one: which table that is depends on the session's
	// search_path, which no reader of text can know.
	Schema, Table string
	// Alias is the target's alias, "" when it has none.
	Alias string
	// Sets are the assignments of an update's SET list.
	Sets []SetItem
	// Alters are the actions of ALTER TABLE that change or destroy a
	// column's stored values.
	Alters []AlterAction
	// CopyColumns are COPY ... FROM's columns; CopyAll when it names none.
	CopyColumns []string
	CopyAll     bool
	where       []sqlTok
	depth       int
}

// SetItem is one assignment.
type SetItem struct {
	// Column is the column assigned. For SET (a, b) = ..., one item per
	// column, Multi set, and Expr the whole right side.
	Column string
	Expr   string
	Multi  bool
}

// AlterAction is one value-changing action on a column.
type AlterAction struct {
	Column string
	// Action: "type" (ALTER [COLUMN] c [SET DATA] TYPE, with or without
	// USING: every stored value is rewritten), "drop" (DROP [COLUMN] c),
	// "rename" (RENAME [COLUMN] c TO).
	Action string
}

// Qualified is "schema.table", or "" when the target names no schema.
func (w Write) Qualified() string {
	if w.Schema == "" {
		return ""
	}
	return w.Schema + "." + w.Table
}

// notAStatement are the words before UPDATE, INSERT, DELETE or TRUNCATE
// that make it a clause of something else: a row lock (FOR UPDATE, FOR NO
// KEY UPDATE), a foreign key's or a rule's action (ON UPDATE, ON DELETE), a
// trigger's event list (BEFORE, AFTER, OF, OR), a grant's list.
var notAStatement = map[string]bool{"for": true, "key": true, "on": true, "before": true, "after": true, "of": true, "or": true,
	"grant": true, "revoke": true}

const errNoInsert = "DO UPDATE with no INSERT it belongs to"

// ParseWrites is every write of one statement's text (or of several
// statements'), or an error when the text is not read to its end.
func ParseWrites(sql string) ([]Write, error) {
	toks, err := lexSQL(sql)
	if err != nil {
		return nil, err
	}
	return parseWrites(toks, sql, 0)
}

func parseWrites(toks []sqlTok, sql string, nesting int) ([]Write, error) {
	if nesting > 8 {
		return nil, fmt.Errorf("bodies nested more than 8 deep are not read")
	}
	var out []Write
	// owners: the INSERT or MERGE target open at each depth, for the ON
	// CONFLICT DO UPDATE or WHEN MATCHED THEN UPDATE that belongs to it.
	owners := map[int]*Write{}
	grant := false
	body := false
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.isPunct(";") && t.depth == 0 {
			owners = map[int]*Write{}
			grant, body = false, false
			continue
		}
		prev := sqlTok{kind: tPunct}
		if i > 0 {
			prev = toks[i-1]
		}
		next := func(k int) sqlTok {
			if i+k < len(toks) {
				return toks[i+k]
			}
			return sqlTok{kind: tPunct}
		}
		// A body is the dollar-quoted string of a DO block or a function,
		// or the plain string that follows DO or AS there.
		if t.kind == tString && body && (t.dollar || prev.is("as") || prev.is("do")) {
			inner, err := lexSQL(t.val)
			if err != nil {
				return nil, fmt.Errorf("in a body: %w", err)
			}
			ws, err := parseWrites(inner, t.val, nesting+1)
			if err != nil {
				return nil, fmt.Errorf("in a body: %w", err)
			}
			out = append(out, ws...)
			continue
		}
		if t.kind != tWord {
			continue
		}
		// In a body, NEW.col := v (a trigger changing the row being
		// written). Which table the trigger is on is said elsewhere, so
		// this is a write of that column of a table nobody can name.
		if nesting > 0 && t.val == "new" && next(1).isPunct(".") && next(2).isName() && next(3).kind == tOp && (next(3).raw == ":=" || next(3).raw == "=") {
			j := i + 4
			for j < len(toks) && !toks[j].isPunct(";") {
				j++
			}
			expr := ""
			if j > i+4 {
				expr = strings.TrimSpace(sql[toks[i+4].pos:toks[j-1].end])
			}
			out = append(out, Write{Kind: "trigger-assign", Table: "new", Sets: []SetItem{{Column: next(2).val, Expr: expr}}, depth: t.depth})
			continue
		}
		clause := prev.kind == tWord && notAStatement[prev.val] || (grant && prev.isPunct(","))
		switch t.val {
		case "grant", "revoke":
			if stmtStart(toks, i) {
				grant = true
			}
		case "do":
			if stmtStart(toks, i) {
				body = true
			}
		case "function", "procedure":
			if prev.is("create") || prev.is("replace") {
				body = true
			}
		case "execute":
			// In a body, EXECUTE runs text. (Outside one it runs a
			// prepared statement by name, or is a trigger's EXECUTE
			// FUNCTION.)
			if nesting == 0 || next(1).is("function") || next(1).is("procedure") {
				continue
			}
			if s := next(1); s.kind == tString && (next(2).isPunct(";") || next(2).is("using") || i+2 >= len(toks)) {
				inner, err := lexSQL(s.val)
				if err != nil {
					return nil, fmt.Errorf("in EXECUTE: %w", err)
				}
				ws, err := parseWrites(inner, s.val, nesting+1)
				if err != nil {
					return nil, fmt.Errorf("in EXECUTE: %w", err)
				}
				out = append(out, ws...)
				i++
				continue
			}
			return nil, fmt.Errorf("EXECUTE of text made at run time is not read")
		case "update":
			if clause && !next(1).is("set") {
				continue
			}
			w := Write{depth: t.depth}
			j := i + 1
			switch {
			case prev.is("do") && next(1).is("set"):
				o := owners[t.depth]
				if o == nil || o.Kind != "insert" {
					return nil, errors.New(errNoInsert)
				}
				w.Kind, w.Schema, w.Table, w.Alias = "conflict-update", o.Schema, o.Table, o.Alias
			case prev.is("then") && next(1).is("set"):
				o := owners[t.depth]
				if o == nil || o.Kind != "merge" {
					return nil, fmt.Errorf("THEN UPDATE with no MERGE it belongs to")
				}
				w.Kind, w.Schema, w.Table, w.Alias = "merge-update", o.Schema, o.Table, o.Alias
			default:
				w.Kind = "update"
				var err error
				if j, err = target(toks, j, &w, true); err != nil {
					return nil, fmt.Errorf("UPDATE: %w", err)
				}
			}
			if j >= len(toks) || !toks[j].is("set") {
				return nil, fmt.Errorf("UPDATE of %s: no SET where one must stand", w.Table)
			}
			end, err := setList(toks, sql, j+1, &w)
			if err != nil {
				return nil, err
			}
			out = append(out, w)
			// The values and the WHERE are read again by the loop: a
			// subquery in them can hold a write of its own.
			_ = end
			i = j
		case "insert":
			if !next(1).is("into") {
				if clause || prev.is("then") {
					// A trigger's event, a grant's privilege, MERGE's
					// THEN INSERT (whose target is the MERGE's).
					continue
				}
				return nil, fmt.Errorf("INSERT with no INTO")
			}
			w := Write{Kind: "insert", depth: t.depth}
			if _, err := target(toks, i+2, &w, false); err != nil {
				return nil, fmt.Errorf("INSERT: %w", err)
			}
			out = append(out, w)
			o := w
			owners[t.depth] = &o
			i++
		case "merge":
			if !next(1).is("into") {
				continue
			}
			w := Write{Kind: "merge", depth: t.depth}
			if _, err := target(toks, i+2, &w, true); err != nil {
				return nil, fmt.Errorf("MERGE: %w", err)
			}
			out = append(out, w)
			o := w
			owners[t.depth] = &o
			i++
		case "delete":
			if !next(1).is("from") {
				if clause || prev.is("then") {
					continue
				}
				return nil, fmt.Errorf("DELETE with no FROM")
			}
			w := Write{Kind: "delete", depth: t.depth}
			if _, err := target(toks, i+2, &w, true); err != nil {
				return nil, fmt.Errorf("DELETE: %w", err)
			}
			out = append(out, w)
			i++
		case "truncate":
			if clause {
				continue
			}
			j := i + 1
			if j < len(toks) && toks[j].is("table") {
				j++
			}
			for {
				w := Write{Kind: "truncate", depth: t.depth}
				var err error
				if j, err = target(toks, j, &w, true); err != nil {
					return nil, fmt.Errorf("TRUNCATE: %w", err)
				}
				out = append(out, w)
				if j < len(toks) && toks[j].isPunct(",") && toks[j].depth == t.depth {
					j++
					continue
				}
				break
			}
		case "copy":
			if !stmtStart(toks, i) || next(1).isPunct("(") {
				continue
			}
			w := Write{Kind: "copy", depth: t.depth}
			j, err := target(toks, i+1, &w, false)
			if err != nil {
				return nil, fmt.Errorf("COPY: %w", err)
			}
			w.Alias = ""
			if j < len(toks) && toks[j].isPunct("(") {
				for j++; j < len(toks) && !(toks[j].isPunct(")") && toks[j].depth == t.depth); j++ {
					if toks[j].isName() {
						w.CopyColumns = append(w.CopyColumns, toks[j].val)
					}
				}
				j++
			} else {
				w.CopyAll = true
			}
			if j >= len(toks) || !(toks[j].is("from") || toks[j].is("to")) {
				return nil, fmt.Errorf("COPY of %s: neither FROM nor TO", w.Table)
			}
			if toks[j].is("from") {
				out = append(out, w)
			}
		case "alter":
			if !next(1).is("table") {
				continue
			}
			j := i + 2
			if j+1 < len(toks) && toks[j].is("if") && toks[j+1].is("exists") {
				j += 2
			}
			w := Write{Kind: "alter", depth: t.depth}
			j, err := target(toks, j, &w, true)
			if err != nil {
				return nil, fmt.Errorf("ALTER TABLE: %w", err)
			}
			w.Alias = ""
			alterActions(toks, j, &w)
			out = append(out, w)
		}
	}
	return out, nil
}

// stmtStart: the token begins a statement.
func stmtStart(toks []sqlTok, i int) bool {
	return i == 0 || (toks[i-1].isPunct(";") && toks[i-1].depth == 0) ||
		// In a body, after BEGIN, THEN, ELSE, LOOP.
		toks[i-1].is("begin") || toks[i-1].is("then") || toks[i-1].is("else") || toks[i-1].is("loop")
}

// target reads [ONLY] [(] name [)] [*] [[AS] alias] at j into w, and
// returns where it ends. only: the forms ONLY and * are allowed here.
func target(toks []sqlTok, j int, w *Write, only bool) (int, error) {
	at := func(k int) sqlTok {
		if k < len(toks) {
			return toks[k]
		}
		return sqlTok{kind: tPunct}
	}
	paren := false
	if only && at(j).is("only") {
		j++
		if at(j).isPunct("(") {
			paren = true
			j++
		}
	}
	if !at(j).isName() {
		return j, fmt.Errorf("no table name where one must stand")
	}
	parts := []string{at(j).val}
	j++
	for at(j).isPunct(".") && at(j+1).isName() {
		parts = append(parts, at(j+1).val)
		j += 2
	}
	if at(j).isPunct(".") {
		return j, fmt.Errorf("a table name that ends in a dot")
	}
	switch len(parts) {
	case 1:
		w.Table = parts[0]
	case 2:
		w.Schema, w.Table = parts[0], parts[1]
	case 3:
		// database.schema.table
		w.Schema, w.Table = parts[1], parts[2]
	default:
		return j, fmt.Errorf("a table name of %d parts", len(parts))
	}
	if paren {
		if !at(j).isPunct(")") {
			return j, fmt.Errorf("ONLY ( with no closing bracket after the name")
		}
		j++
	}
	if only && at(j).kind == tOp && at(j).raw == "*" {
		j++
	}
	if at(j).is("as") && at(j+1).isName() {
		w.Alias = at(j + 1).val
		return j + 2, nil
	}
	if a := at(j); a.kind == tQuoted || (a.kind == tWord && !targetEnd[a.val]) {
		w.Alias = a.val
		j++
	}
	return j, nil
}

// targetEnd are the words that can follow a target and are not its alias.
var targetEnd = map[string]bool{"set": true, "using": true, "where": true, "returning": true, "select": true, "values": true,
	"default": true, "on": true, "from": true, "to": true, "with": true, "overriding": true, "table": true, "restart": true,
	"continue": true, "cascade": true, "restrict": true, "add": true, "alter": true, "drop": true, "rename": true, "enable": true,
	"disable": true, "validate": true, "owner": true, "cluster": true, "inherit": true, "no": true, "attach": true, "detach": true,
	"replica": true, "force": true, "reset": true, "when": true, "not": true, "of": true}

// setList reads the assignments that follow SET at j into w, then the
// WHERE that belongs to the same statement.
func setList(toks []sqlTok, sql string, j int, w *Write) (int, error) {
	d := w.depth
	ends := func(k int) bool {
		t := toks[k]
		if t.depth < d || (t.isPunct(";") && t.depth == 0) {
			return true
		}
		if t.depth != d || t.kind != tWord {
			return false
		}
		switch t.val {
		case "where", "returning":
			return true
		case "when":
			// MERGE's next WHEN clause. (In any other update a WHEN at
			// this depth is a CASE's.)
			return w.Kind == "merge-update"
		case "from":
			// IS [NOT] DISTINCT FROM is a comparison.
			return !(k > 0 && toks[k-1].is("distinct"))
		}
		return false
	}
	for j < len(toks) && !ends(j) {
		// One assignment: to the next comma at this depth.
		from := j
		for j < len(toks) && !ends(j) && !(toks[j].isPunct(",") && toks[j].depth == d) {
			j++
		}
		item := toks[from:j]
		if j < len(toks) && toks[j].isPunct(",") && toks[j].depth == d {
			j++
		}
		if len(item) == 0 {
			return j, fmt.Errorf("UPDATE of %s: an empty assignment", w.Table)
		}
		eq := -1
		for k, t := range item {
			if t.depth == d && t.kind == tOp && t.raw == "=" {
				eq = k
				break
			}
		}
		if eq < 1 || eq == len(item)-1 {
			return j, fmt.Errorf("UPDATE of %s: an assignment with no = or with nothing after it", w.Table)
		}
		expr := strings.TrimSpace(sql[item[eq+1].pos:item[len(item)-1].end])
		if item[0].isPunct("(") {
			n := 0
			for _, t := range item[1:eq] {
				if t.depth == d+1 && t.isName() {
					w.Sets = append(w.Sets, SetItem{Column: t.val, Expr: expr, Multi: true})
					n++
				}
			}
			if n == 0 {
				return j, fmt.Errorf("UPDATE of %s: a column list with no column", w.Table)
			}
			continue
		}
		if !item[0].isName() {
			return j, fmt.Errorf("UPDATE of %s: an assignment that does not begin with a column", w.Table)
		}
		col := item[0].val
		w.Sets = append(w.Sets, SetItem{Column: col, Expr: expr})
		// a.b = v assigns field b of column a. Written with the target's
		// own name or alias in front (which PostgreSQL refuses) it would
		// name column b: both are recorded.
		if eq >= 3 && item[1].isPunct(".") && item[2].isName() {
			w.Sets = append(w.Sets, SetItem{Column: item[2].val, Expr: expr})
		}
	}
	// The WHERE of this statement: after an optional FROM.
	k := j
	if k < len(toks) && toks[k].is("from") && toks[k].depth == d {
		for k++; k < len(toks); k++ {
			t := toks[k]
			if t.depth < d || (t.isPunct(";") && t.depth == 0) || (t.depth == d && (t.is("where") || t.is("returning"))) {
				break
			}
		}
	}
	if k < len(toks) && toks[k].is("where") && toks[k].depth == d {
		from := k + 1
		for k = from; k < len(toks); k++ {
			t := toks[k]
			if t.depth < d || (t.isPunct(";") && t.depth == 0) || (t.depth == d && (t.is("returning") || (t.is("when") && w.Kind == "merge-update"))) {
				break
			}
		}
		w.where = toks[from:k]
	}
	return k, nil
}

// alterActions reads ALTER TABLE's actions at j.
func alterActions(toks []sqlTok, j int, w *Write) {
	d := w.depth
	start := true
	for ; j < len(toks); j++ {
		t := toks[j]
		if t.depth < d || (t.isPunct(";") && t.depth == 0) {
			return
		}
		if t.depth != d {
			continue
		}
		if t.isPunct(",") {
			start = true
			continue
		}
		if !start {
			continue
		}
		start = false
		at := func(k int) sqlTok {
			if k < len(toks) {
				return toks[k]
			}
			return sqlTok{kind: tPunct}
		}
		k := j + 1
		switch {
		case t.is("alter"):
			if at(k).is("column") {
				k++
			}
			if at(k).is("constraint") || !at(k).isName() {
				continue
			}
			col := at(k).val
			// ALTER c TYPE t, ALTER c SET DATA TYPE t: each with or
			// without USING rewrites every stored value.
			if at(k+1).is("type") || (at(k+1).is("set") && at(k+2).is("data") && at(k+3).is("type")) {
				w.Alters = append(w.Alters, AlterAction{Column: col, Action: "type"})
			}
		case t.is("drop"):
			if at(k).is("constraint") {
				continue
			}
			if at(k).is("column") {
				k++
			}
			if at(k).is("if") && at(k+1).is("exists") {
				k += 2
			}
			if at(k).isName() {
				w.Alters = append(w.Alters, AlterAction{Column: at(k).val, Action: "drop"})
			}
		case t.is("rename"):
			if at(k).is("to") || at(k).is("constraint") {
				continue
			}
			if at(k).is("column") {
				k++
			}
			if at(k).isName() && at(k+1).is("to") {
				w.Alters = append(w.Alters, AlterAction{Column: at(k).val, Action: "rename"})
			}
		}
	}
}

// GuardsEmpty reports whether the write's WHERE lets it touch only rows
// whose col is empty: the WHERE has, AS ONE OF THE CONDITIONS JOINED BY
// AND at its top level, exactly
//
//	[q.]col IS NULL          or          COALESCE([q.]col, '') = ''
//
// where q is the target's alias or name. A WHERE with OR or BETWEEN at its
// top level has no such condition (OR binds looser than AND, so nothing
// beside it is required; BETWEEN's own AND is not a joint). A condition
// under NOT, in a string, or in a subquery is not this condition.
func (w Write) GuardsEmpty(col string) bool {
	if w.Kind != "update" && w.Kind != "conflict-update" && w.Kind != "merge-update" {
		return false
	}
	d := w.depth
	var parts [][]sqlTok
	from := 0
	for k, t := range w.where {
		if t.depth != d || t.kind != tWord {
			continue
		}
		switch t.val {
		case "or", "between":
			return false
		case "and":
			parts = append(parts, w.where[from:k])
			from = k + 1
		}
	}
	parts = append(parts, w.where[from:])
	for _, p := range parts {
		if w.isEmptyGuard(p, col) {
			return true
		}
	}
	return false
}

func (w Write) isEmptyGuard(p []sqlTok, col string) bool {
	// Brackets around the whole condition.
	for len(p) >= 2 && p[0].isPunct("(") && p[len(p)-1].isPunct(")") && p[0].depth == p[len(p)-1].depth {
		inner := true
		for _, t := range p[1 : len(p)-1] {
			if t.depth <= p[0].depth {
				inner = false
			}
		}
		if !inner {
			break
		}
		p = p[1 : len(p)-1]
	}
	// stored reads [q.]col at k and returns what follows.
	stored := func(k int) (int, bool) {
		if k+2 < len(p) && p[k].isName() && p[k+1].isPunct(".") {
			if q := p[k].val; q != w.Alias && !(w.Alias == "" && q == w.Table) {
				return k, false
			}
			k += 2
		}
		if k < len(p) && p[k].isName() && p[k].val == col {
			return k + 1, true
		}
		return k, false
	}
	if k, ok := stored(0); ok && k+2 == len(p) && p[k].is("is") && p[k+1].is("null") {
		return true
	}
	if len(p) >= 2 && p[0].is("coalesce") && p[1].isPunct("(") {
		k, ok := stored(2)
		empty := func(t sqlTok) bool { return t.kind == tString && !t.dollar && t.val == "" }
		if ok && k+5 == len(p) && p[k].isPunct(",") && empty(p[k+1]) && p[k+2].isPunct(")") &&
			p[k+3].kind == tOp && p[k+3].raw == "=" && empty(p[k+4]) {
			return true
		}
	}
	return false
}

// ViewSources are the relations each plain view defined in sql reads
// from ("schema.view" to the "schema.table" names after FROM and JOIN in
// its definition). A write through such a view is a write of the tables
// behind it, under column names the view chooses.
func ViewSources(sql string) map[string][]string {
	toks, err := lexSQL(sql)
	if err != nil {
		return nil
	}
	out := map[string][]string{}
	qualified := func(j int) (string, int) {
		if j+2 < len(toks) && toks[j].isName() && toks[j+1].isPunct(".") && toks[j+2].isName() {
			return toks[j].val + "." + toks[j+2].val, j + 3
		}
		return "", j
	}
	for i := 0; i < len(toks); i++ {
		if !toks[i].is("view") || i == 0 || toks[i-1].is("materialized") || toks[i-1].is("drop") || toks[i-1].is("alter") {
			continue
		}
		create := false
		for k := i - 1; k >= 0 && k >= i-5; k-- {
			create = create || toks[k].is("create")
		}
		name, j := qualified(i + 1)
		if !create || name == "" {
			continue
		}
		for ; j < len(toks) && !(toks[j].isPunct(";") && toks[j].depth == 0); j++ {
			if toks[j].is("from") || toks[j].is("join") {
				if src, _ := qualified(j + 1); src != "" {
					out[name] = append(out[name], src)
				}
			}
		}
		i = j
	}
	return out
}
