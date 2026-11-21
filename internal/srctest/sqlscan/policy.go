// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Write-policy engine (regression-infrastructure Phase 3, v0.27.126).
//
// The founding incident (PR #184 round 11, v0.27.117): two writers held
// CONFLICTING policies for repos.platform_repo_id — the intended policy
// was fill-empty-only, but UpsertRepo's ON CONFLICT and
// UpdateRepoMetadata both shipped prefer-nonempty-INCOMING, so a
// non-empty incoming ID overwrote a different stored one and silently
// destroyed the round-10 delete-and-recreate mismatch detector. The
// registry (internal/db/column_write_policy_test.go) declares each
// protected column's ONE policy; this engine verifies every UPDATE SET
// assignment conforms.
//
// A registered column assigned in a multiple-column SET, (a, col) = (...),
// is a violation: no matcher reads that form (closed-world, as below). So
// is a subscripted or field target (`col[1] =`, `col.f =`): a part of the
// column is written, which no shape describes. A quoted target (`SET "col"
// =`) is the column it names, by PostgreSQL's parser (pgread.go).
//
// The matchers read the value's PARSE TREE (0.29.212), not its text: what
// is a call of COALESCE, which name is a column's and which a string's, is
// the parser's reading.
//
// WHOSE COLUMN A NAME IS (0.29.213) is decided once, by the resolver below
// (Write.classify), for every matcher and for the guard: "stored" is a
// reference RESOLVED TO THE TARGET's own column, never "anything EXCLUDED
// does not qualify". WHAT IS A WRITE OF A TABLE is decided once too, by
// the Catalog of the schema's relations: a write is accepted only on a
// base table the catalog knows.
//
// Matchers are ORDER-SENSITIVE and closed-world per registered column:
// an RHS matching no recognized shape for the policy FAILS
// ("unrecognized write shape") — new idioms must conform, extend the
// tested shape set, or take a reviewed Exception WITH a reason. Never
// weaken a matcher to get green.

package sqlscan

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Policy is a column's declared write discipline.
type Policy int

const (
	// FillEmptyOnly: the stored value wins once set. Shapes:
	// COALESCE(NULLIF(stored, ''), incoming) — text columns;
	// COALESCE(stored, incoming) — nullable columns; or a bare
	// incoming assignment guarded by WhereGuardsEmpty (the
	// SetPlatformRepoIDIfEmptySeen form).
	FillEmptyOnly Policy = iota + 1
	// PreferNonemptyIncoming: a non-empty incoming value overwrites;
	// empty incoming preserves stored. COALESCE(NULLIF(incoming, ''), stored).
	PreferNonemptyIncoming
	// PreferNonNullIncoming: a non-NULL incoming overwrites.
	// COALESCE(incoming, stored).
	PreferNonNullIncoming
	// GreatestNonNull: the value only increases, nil-safe both ways.
	// GREATEST(COALESCE(a, b), COALESCE(b, a)) with {a, b} =
	// {incoming, stored} (the v0.27.122 updated_at shape).
	GreatestNonNull
	// AlwaysRefresh: incoming is authoritative; the RHS must not
	// reference the stored value at all.
	AlwaysRefresh
	// InsertOnly: the column is written at INSERT time only — any
	// UPDATE SET assignment is a violation.
	InsertOnly
	// EarliestOfStoredAndIncoming: the value only moves EARLIER (or lower).
	// LEAST(stored, incoming), in either order, for a NOT NULL column: the
	// first-detection shape, where a successor may hand in an older date
	// and a plain re-observation, which offers NOW(), never moves it.
	EarliestOfStoredAndIncoming
	// KeepCarryOrNow: a searched CASE whose every result is the stored
	// value, an incoming placeholder (a cast allowed on it, and on it
	// only) or NOW(): the column
	// can keep what it has, take a value handed in, or be stamped now, and
	// nothing else. The conditions are not judged; they are behaviour, and
	// behaviour is pinned by tests. The entered-exposure shape.
	KeepCarryOrNow
)

func (p Policy) String() string {
	switch p {
	case FillEmptyOnly:
		return "FillEmptyOnly"
	case PreferNonemptyIncoming:
		return "PreferNonemptyIncoming"
	case PreferNonNullIncoming:
		return "PreferNonNullIncoming"
	case GreatestNonNull:
		return "GreatestNonNull"
	case AlwaysRefresh:
		return "AlwaysRefresh"
	case InsertOnly:
		return "InsertOnly"
	case EarliestOfStoredAndIncoming:
		return "EarliestOfStoredAndIncoming"
	case KeepCarryOrNow:
		return "KeepCarryOrNow"
	}
	return fmt.Sprintf("Policy(%d)", int(p))
}

// Registered declares one protected column. Reason is REQUIRED — the
// registry is documentation (the documentedEmpty convention).
type Registered struct {
	Table  string // schema-qualified
	Column string
	Policy Policy
	Reason string
}

// Violation kinds. An Exception names the kind it excuses.
const (
	// KindAssign: an UPDATE SET (or ON CONFLICT DO UPDATE SET, or a MERGE's
	// UPDATE) gives the column a value its policy does not allow.
	KindAssign = "assign"
	// KindTriggerAssign: a trigger function's body gives NEW.col a value.
	KindTriggerAssign = "trigger-assign"
	// KindTriggerForm: a trigger function's body does something with NEW or
	// OLD that is not read (Table is TriggerFunction, Column "").
	KindTriggerForm = "trigger-form"
	// KindAlterColumn: ALTER TABLE rewrites, renames or drops the column.
	KindAlterColumn = "alter-column"
	// KindCopy: COPY ... FROM writes the column.
	KindCopy = "copy"
	// KindFKAction: a foreign key's referential action writes the column.
	KindFKAction = "fk-action"
	// The kinds below are of a TABLE (Column is ""): the statement changes
	// what the table is or holds without assigning a column.
	KindDrop        = "drop"
	KindTruncate    = "truncate"
	KindRenameTable = "rename-table"
	KindSetSchema   = "set-schema"
	KindPartition   = "partition" // ATTACH, DETACH, INHERIT; CREATE TABLE ... PARTITION OF / INHERITS
	KindDropSchema  = "drop-schema"
	KindRule        = "rule"
	KindTrigger     = "trigger"
	// KindRenameSchema: ALTER SCHEMA ... RENAME of, or onto, a schema that
	// holds the table.
	KindRenameSchema = "rename-schema"
	// KindOwned (Table is AnyRegisteredTable): DROP OWNED BY drops, and
	// REASSIGN OWNED BY hands over, whatever the role owns: which tables,
	// no reader of text can know.
	KindOwned = "owned"
	// KindDropType (Table is AnyRegisteredTable): DROP TYPE / DOMAIN ...
	// CASCADE drops every column of the type.
	KindDropType = "drop-type"
	// The target of a write is not a base table the catalog knows.
	KindUnqualifiedTarget = "unqualified-target"
	KindUnknownTarget     = "unknown-target"
	KindViewTarget        = "view-target"
	KindChildTarget       = "child-target"
	// KindUnknownTable: a registered column's table is not a base table of
	// the catalog (registry rot, or a catalog that was not given the DDL).
	KindUnknownTable = "unknown-table"
)

// TriggerFunction is the Table of a KindTriggerForm violation: a trigger
// function does not say which table its trigger is on.
const TriggerFunction = "(trigger function)"

// AnyRegisteredTable is the Table of a violation that may reach any
// registered table and names none: DROP OWNED, DROP TYPE ... CASCADE, a
// TRUNCATE ... CASCADE whose dependents the catalog does not know.
const AnyRegisteredTable = "(any registered table)"

// Exception carves ONE violation out, WITH a reason.
//
// It PINS THE STATEMENT it excuses, whole (0.29.214). It names the table,
// the column ("" for a kind that is of a table), the violation's Kind, the
// File and the declaration the statement stands in, and Statement: the one
// statement that holds the violation, as SQL. Statement is compared as
// PostgreSQL's parse tree with the places removed: the target, the SET
// list, FROM, WHERE, ON CONFLICT, RETURNING, everything; spacing, comments
// and the case of keywords do not matter, and nothing else is overlooked.
// A statement whose WHERE was changed or dropped is therefore another
// statement: its violation is reported AND the Exception is stale.
//
// For an assignment, Fragment is the assigned expression, whole: it says
// WHICH assignment of the statement is the excused one, and must be an
// assignment of Column in Statement (an Exception whose Fragment is not is
// malformed and excuses nothing).
//
// Why the whole statement (round 7): 0.29.213 matched the expression alone,
// so an Exception whose reason rested on the statement's WHERE ("WHERE
// added_at IS NULL makes it self-disabling") went on excusing the
// statement after the WHERE was removed. That very edit, on the v0.27.50
// step, is the 0.29.140 regression that un-archived 426 gone repositories.
//
// An Exception excuses its one statement and no other: two statements that
// need excusing need two Exceptions, in one text or across the texts of a
// declaration (0.29.213 counted per text, so one expression-only Exception
// excused every statement of its declaration that assigned the
// expression). The SAME statement met again is that statement: a builder
// that gives it for two inputs, a text judged each time it is sent. An
// Exception that excuses nothing in a run is reported STALE.
type Exception struct {
	Table  string
	Column string
	Kind   string
	File   string
	// Decl is the declaration the excused statement stands in
	// ("PostgresStore.AdoptForgeID"): the same violation written in
	// another function of the file is not excused. "" excuses it anywhere
	// in the file (the store's own registry requires a Decl).
	Decl string
	// Fragment: the assigned expression (KindAssign, KindTriggerAssign),
	// which must be an assignment of Column in Statement.
	Fragment string
	// Statement: the one statement that holds the violation, whole.
	// REQUIRED for every kind.
	Statement string
	Reason    string
}

// Pins reports whether sql holds the Exception's statement: some statement
// of it (at any depth: a statement inside a body or a WITH counts) is,
// as a parse tree, the Exception's Statement. It is the answer to "does
// this Exception still describe this text", for a caller that wants it
// without a whole Check; CheckIn applies the same rule itself.
func (ex Exception) Pins(sql string) (bool, error) {
	t, problem := exceptionTree(ex)
	if problem != "" {
		return false, errors.New(problem)
	}
	ws, err := ParseWrites(sql)
	if err != nil {
		return false, err
	}
	for _, w := range ws {
		if canon(w.top) == t.statement {
			return true, nil
		}
	}
	return false, nil
}

// Violation is one non-conforming write.
type Violation struct {
	Table  string
	Column string
	Kind   string
	File   string
	// Decl is the declaration the statement stands in (Stmt.Decl).
	Decl string
	// Expr is the assigned expression, or the statement, as written (cut
	// from the text where that could be done exactly, written out from the
	// tree otherwise): what an Exception's Fragment would be.
	Expr string
	// Statement is the statement the violation stands in, as written (as
	// Expr is): what an Exception's Statement would be. Every violation
	// that comes from a statement carries it; KindUnknownTable, which
	// comes from the registry, has none. SQL is the whole text the
	// statement was read from.
	Statement string
	SQL       string
	Want      Policy
	Detail    string
	// fragment and statement are the trees an Exception is compared with.
	fragment, statement string
}

// Unparsed is one statement not read.
type Unparsed struct {
	File, SQL, Detail string
}

// Report is Check's result.
type Report struct {
	Violations      []Violation
	StaleExceptions []Exception
	// BadExceptions: exceptions that are not well formed (no kind, no
	// fragment, text the parser does not read), each with what is wrong.
	BadExceptions []string
	// Unwritten: registered columns with NO writer statement at all —
	// registry rot (a renamed column) or a genuinely dark column (the
	// column-writer tripwire's territory).
	Unwritten []Registered
	// Unparsed are the statements the reader did not read to their end
	// (pgread.go says what that means: PostgreSQL's parser refused the
	// text, or a statement or a body is of a kind that is not read). Each
	// is a failure until a person has read it: nothing was checked on it.
	Unparsed []Unparsed
}

// Catalog is what relations exist: the base tables, and the views, the
// materialized views and foreign tables with what stands behind each; and
// which tables are partitions or children of which. It is built from the
// schema's own DDL (Add, CatalogOf), by the same reader as everything else.
// A write is judged against it: the target must be a base table it knows.
type Catalog struct {
	kind    map[string]string
	sources map[string][]string
	parents map[string][]string
	// refs: the tables each table's foreign keys reference; defined: the
	// tables whose CREATE TABLE the catalog read (so that their foreign
	// keys are known, where a table only declared by Table is not).
	refs    map[string][]string
	defined map[string]bool
	// temp: session tables a write may name as pg_temp.<name> (Temp).
	temp map[string]bool
}

// NewCatalog is an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{kind: map[string]string{}, sources: map[string][]string{}, parents: map[string][]string{},
		refs: map[string][]string{}, defined: map[string]bool{}, temp: map[string]bool{}}
}

// Temp declares temporary tables that some text creates and ANOTHER text
// writes (two Exec calls of one session): a write of pg_temp.<name> is then
// accepted as a write of that session's own table. A temporary table made
// and written in ONE text needs no declaration. The name must be written
// with pg_temp: an unqualified name is whatever the search path makes it.
func (c *Catalog) Temp(names ...string) *Catalog {
	for _, n := range names {
		c.temp[n] = true
	}
	return c
}

// Reference declares that the table from has a foreign key that references
// the table to (for a catalog built from a live database, which reads no
// DDL). Defined then says that every foreign key of from has been declared.
func (c *Catalog) Reference(from, to string) *Catalog {
	c.refs[from] = append(c.refs[from], to)
	return c
}

// Defined declares that the tables' foreign keys are all known to the
// catalog (through Reference).
func (c *Catalog) Defined(names ...string) *Catalog {
	for _, n := range names {
		c.defined[n] = true
	}
	return c
}

// dependents are the tables that reference name through a foreign key,
// and those that reference them: what TRUNCATE name CASCADE empties.
func (c *Catalog) dependents(name string) []string {
	seen := map[string]bool{name: true}
	var out []string
	for changed := true; changed; {
		changed = false
		for from, tos := range c.refs {
			if seen[from] {
				continue
			}
			for _, to := range tos {
				if seen[to] {
					seen[from], changed = true, true
					out = append(out, from)
					break
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// CatalogOf is the catalog of everything the statements define. A statement
// that is not read defines nothing (Check reports it).
func CatalogOf(stmts []Stmt) *Catalog {
	c := NewCatalog()
	for _, st := range stmts {
		_ = c.Add(st.SQL)
	}
	return c
}

// Table declares base tables by name ("schema.table").
func (c *Catalog) Table(names ...string) *Catalog {
	for _, n := range names {
		if c.kind[n] == "" {
			c.kind[n] = "table"
		}
	}
	return c
}

// Add reads the relations one text defines into the catalog. Temporary
// relations are not added: they exist for the session that made them.
func (c *Catalog) Add(sql string) error {
	defs, err := Defs(sql)
	if err != nil {
		return err
	}
	c.add(defs)
	return nil
}

func (c *Catalog) add(defs []Def) {
	for _, d := range defs {
		if d.Temp || strings.HasPrefix(d.Name, "pg_temp.") {
			continue
		}
		switch d.Kind {
		case "ref":
			c.refs[d.Name] = append(c.refs[d.Name], d.Refs...)
		case "rename":
			// The relation is known under its new name as what it was
			// under the old one.
			old := d.Sources[0]
			if k := c.kind[old]; k != "" && c.kind[d.Name] == "" {
				c.kind[d.Name] = k
				c.sources[d.Name] = c.sources[old]
				c.parents[d.Name] = append([]string{old}, c.parents[old]...)
			}
		case "view":
			// CREATE OR REPLACE and a rule ON SELECT make a view of the
			// name whatever it was.
			c.kind[d.Name] = "view"
			c.sources[d.Name] = append(c.sources[d.Name], d.Sources...)
		default:
			if c.kind[d.Name] == "" {
				c.kind[d.Name] = d.Kind
			}
			c.parents[d.Name] = append(c.parents[d.Name], d.Parents...)
			if d.Kind == "matview" {
				// What a materialized view was made from stands behind it
				// as a view's sources do.
				c.sources[d.Name] = append(c.sources[d.Name], d.Sources...)
			}
			if d.Kind == "table" {
				c.defined[d.Name] = true
				c.refs[d.Name] = append(c.refs[d.Name], d.Refs...)
			}
		}
	}
}

// Kind is what the catalog knows the relation as: "table", "view",
// "matview", "foreign", or "" for one it does not know.
func (c *Catalog) Kind(name string) string { return c.kind[name] }

// Counts are how many relations of each kind the catalog holds.
func (c *Catalog) Counts() map[string]int {
	out := map[string]int{}
	for _, k := range c.kind {
		out[k]++
	}
	return out
}

// behind is every relation that stands behind name: a view's sources and
// theirs, a table's parents and theirs.
func (c *Catalog) behind(name string) []string {
	seen := map[string]bool{}
	var out []string
	var walk func(string)
	walk = func(n string) {
		for _, next := range append(append([]string{}, c.sources[n]...), c.parents[n]...) {
			if !seen[next] {
				seen[next] = true
				out = append(out, next)
				walk(next)
			}
		}
	}
	walk(name)
	sort.Strings(out)
	return out
}

// Check judges the statements against a catalog made of what they define
// themselves, with the registered tables taken as base tables. The store's
// own test gives its catalog (CheckIn), so that a registered table the
// schema does not create is a failure there.
func Check(stmts []Stmt, registry []Registered, exceptions []Exception) Report {
	cat := CatalogOf(stmts)
	for _, reg := range registry {
		cat.Table(reg.Table)
	}
	return CheckIn(cat, stmts, registry, exceptions)
}

// dmlKinds are the writes of rows, whose target must be a base table.
var dmlKinds = map[string]bool{"update": true, "insert": true, "delete": true, "merge": true, "copy": true, "truncate": true}

// CheckIn verifies every write of the statements: that its target is a base
// table of the catalog, that no statement changes what a registered table
// is or holds by another route, and that every assignment of a registered
// column conforms to the column's policy.
func CheckIn(cat *Catalog, stmts []Stmt, registry []Registered, exceptions []Exception) Report {
	var rep Report
	exs := make([]exceptionTrees, len(exceptions))
	for i, ex := range exceptions {
		t, problem := exceptionTree(ex)
		if problem != "" {
			rep.BadExceptions = append(rep.BadExceptions, fmt.Sprintf("exception %s.%s (%s): %s", ex.Table, ex.Column, ex.Kind, problem))
		}
		exs[i] = t
	}
	used := make([]bool, len(exceptions))
	regTables := map[string]bool{}
	for _, reg := range registry {
		regTables[reg.Table] = true
	}
	for t := range regTables {
		if cat.Kind(t) != "table" {
			rep.Violations = append(rep.Violations, Violation{Table: t, Kind: KindUnknownTable,
				Detail: "a registered column's table is not a base table the catalog knows (it knows it as \"" + cat.Kind(t) + "\"): the registry names a table the schema does not create, or the catalog was not given the schema"})
		}
	}
	seenWriter := map[Registered]bool{}
	for _, st := range stmts {
		ws, defs, err := readWrites(st.SQL, 0, nil)
		if err != nil {
			rep.Unparsed = append(rep.Unparsed, Unparsed{File: st.File, SQL: st.SQL, Detail: err.Error()})
			continue
		}
		// tempHere: the write's target is a temporary table this text
		// made EARLIER, at the same body level, and has not dropped since
		// (round 7: a temporary definition anywhere in the text counted,
		// one dropped before the write and one in a function body that is
		// never called included).
		tempHere := func(w *Write) bool {
			if w.scope == nil || (w.Schema != "" && w.Schema != "pg_temp") {
				return false
			}
			for _, d := range defs {
				named := (d.Temp && d.Name == w.Table) || d.Name == "pg_temp."+w.Table
				if !named || d.Kind != "table" || d.env != w.scope.env || d.seq >= w.seq {
					continue
				}
				dropped := false
				for j := range ws {
					x := &ws[j]
					if x.Kind == "drop" && (x.Schema == "" || x.Schema == "pg_temp") && x.Table == w.Table && x.scope != nil && x.scope.env == d.env && x.seq > d.seq && x.seq < w.seq {
						dropped = true
					}
				}
				if !dropped {
					return true
				}
			}
			return false
		}
		var stmtText, stmtSource string
		violate := func(v Violation) {
			v.File, v.Decl, v.SQL = st.File, st.Decl, st.SQL
			if v.Statement == "" {
				v.Statement = stmtSource
			}
			for i, ex := range exceptions {
				// An Exception is the violation's when it names its table,
				// column, kind, file and declaration, and PINS ITS
				// STATEMENT, whole (and, for an assignment, is of this
				// assigned expression).
				if !exs[i].ok || ex.Table != v.Table || ex.Column != v.Column || ex.Kind != v.Kind || (ex.File != "" && ex.File != st.File) || (ex.Decl != "" && ex.Decl != st.Decl) {
					continue
				}
				if exs[i].statement != v.statement || exs[i].fragment != v.fragment {
					continue
				}
				used[i] = true
				return
			}
			rep.Violations = append(rep.Violations, v)
		}
		for i := range ws {
			w := &ws[i]
			w.cat = cat
			// The statement an Exception pins: the whole one the write
			// stands in (a WITH and all under it), not the innermost.
			stmtTree := canon(w.top)
			stmtSource = w.src
			stmtText = strings.Join(strings.Fields(w.src), " ")
			table := func(kind, name, detail string) {
				violate(Violation{Table: name, Kind: kind, Expr: stmtText, Detail: detail, statement: stmtTree})
			}
			switch {
			case w.Kind == "rename-schema":
				for t := range regTables {
					if strings.HasPrefix(t, w.Schema+".") || strings.HasPrefix(t, w.NewName+".") {
						table(KindRenameSchema, t, "ALTER SCHEMA "+w.Schema+" RENAME TO "+w.NewName+": every write that names the table then names another relation, or none")
					}
				}
				continue
			case w.Kind == "owned":
				table(KindOwned, AnyRegisteredTable, w.Note+" drops, or hands to another role, whatever that role owns: which tables that is, the text does not say")
				continue
			case w.Kind == "drop-type":
				table(KindDropType, AnyRegisteredTable, w.Note+" drops every column of the type, in whatever table has one: which, the text does not say")
				continue
			case w.Kind == "trigger-form":
				table(KindTriggerForm, TriggerFunction, "a trigger function's body does something with NEW or OLD that is not read ("+w.Note+"): the row it returns may differ from the one written in any column, and no policy can be checked on that")
				continue
			case w.Kind == "trigger-assign":
				// Which table the trigger is on is said elsewhere, so the
				// assignment is one of every registered column of the name.
				for _, reg := range registry {
					for _, it := range w.Sets {
						if it.Column == reg.Column {
							violate(Violation{Table: reg.Table, Column: reg.Column, Kind: KindTriggerAssign, Want: reg.Policy, Expr: "NEW." + reg.Column + " := " + it.Expr,
								Detail:   "a trigger's body assigns the column of the row being written; which table the trigger is on is said elsewhere, so it may be " + reg.Table + " and nothing can tell",
								fragment: canon(it.val), statement: stmtTree})
						}
					}
				}
				continue
			case w.Kind == "drop-schema":
				for t := range regTables {
					if strings.HasPrefix(t, w.Schema+".") {
						table(KindDropSchema, t, "DROP SCHEMA "+w.Schema+" destroys the table and every stored value of its columns")
					}
				}
				continue
			case tempHere(w):
				// A temporary table this text made: not a table of the
				// schema, whatever it is called.
				continue
			case w.Schema == "pg_temp" && cat.temp[w.Table]:
				// A session table another text of the session made, and
				// the registry declares (Catalog.Temp).
				continue
			case w.Schema == "":
				table(KindUnqualifiedTarget, w.Table, "the target of "+strings.ToUpper(w.Kind)+" names no schema: which relation that is depends on the session's search_path (a temporary relation of the name comes first), and nothing can tell. Write the schema")
				continue
			}
			q := w.Qualified()
			if dmlKinds[w.Kind] {
				switch kind := cat.Kind(q); kind {
				case "table":
					for _, b := range cat.behind(q) {
						if regTables[b] {
							violate(Violation{Table: b, Kind: KindChildTarget, Expr: stmtText, statement: stmtTree,
								Detail: q + " is a partition or a child of " + b + " (or that table under another name): a write of it writes that table's rows, which no policy is checked on"})
						}
					}
				case "":
					table(KindUnknownTarget, q, "the target of "+strings.ToUpper(w.Kind)+" is not a relation the catalog knows: it may be a view, a partition or a foreign table over a registered table, and nothing can tell. Create it in the schema, or take a reviewed Exception")
				default:
					table(KindViewTarget, q, "the target of "+strings.ToUpper(w.Kind)+" is a "+kind+", not a base table (behind it: "+strings.Join(cat.behind(q), ", ")+"): a write through it writes those tables under the names it chooses, which no policy can be checked on")
				}
			}
			if w.Kind == "truncate" && w.Cascade {
				// CASCADE empties every table that references this one
				// (and theirs). Which those are is known when the catalog
				// read this table's and every registered table's CREATE
				// TABLE; otherwise it is not, and the statement is held
				// against them all.
				known := cat.defined[q]
				for t := range regTables {
					known = known && cat.defined[t]
				}
				if !known {
					table(KindTruncate, AnyRegisteredTable, "TRUNCATE "+q+" CASCADE empties every table that references it, and the catalog was not given the foreign keys to say which")
				} else {
					for _, dep := range cat.dependents(q) {
						if regTables[dep] {
							table(KindTruncate, dep, "TRUNCATE "+q+" CASCADE empties "+dep+" too (it references "+q+" through a foreign key): every stored value of its columns is destroyed")
						}
					}
				}
			}
			if w.Kind == "create-child" {
				for _, parent := range w.Parents {
					for t := range regTables {
						if parent == t || (!strings.Contains(parent, ".") && strings.HasSuffix(t, "."+parent)) {
							table(KindPartition, t, q+" is made a partition or a child of "+t+": its rows are that table's, written under another name")
						}
					}
				}
			}
			// Another relation takes a registered table's name, or a
			// registered table is made part of another.
			if (w.Kind == "rename-table" || w.Kind == "set-schema") && regTables[w.NewName] {
				table(KindRenameTable, w.NewName, q+" is given the registered table's name: every write that names "+w.NewName+" then writes another relation")
			}
			for _, a := range w.Alters {
				if a.Column == "" && a.Other != q {
					for t := range regTables {
						if a.Other == t || (!strings.Contains(a.Other, ".") && strings.HasSuffix(t, "."+a.Other)) {
							table(KindPartition, t, "ALTER TABLE "+q+" ... "+strings.ToUpper(a.Action)+" "+a.Other+": the registered table's rows become another table's (or that table's become its)")
						}
					}
				}
			}
			if !regTables[q] {
				continue
			}
			// Statements that change what a registered table is or holds.
			switch w.Kind {
			case "drop":
				table(KindDrop, q, "the statement destroys every stored value of the table's columns: no policy allows that; take a reviewed Exception")
			case "truncate":
				table(KindTruncate, q, "TRUNCATE destroys every stored value of the table's columns: no policy allows that; take a reviewed Exception")
			case "rename-table":
				table(KindRenameTable, q, "the table is renamed ("+w.Note+"): every later write names it otherwise, and no policy follows it there")
			case "set-schema":
				table(KindSetSchema, q, "the table is moved ("+w.Note+"): every later write names it otherwise, and no policy follows it there")
			case "rule":
				table(KindRule, q, "a rule on the table ("+w.Note+") rewrites statements that name it into others")
			case "trigger":
				table(KindTrigger, q, "a trigger on the table ("+w.Note+") can change any column of a row being written, whatever function it runs")
			}
			for _, a := range w.Alters {
				if a.Column == "" {
					table(KindPartition, q, "ALTER TABLE ... "+strings.ToUpper(a.Action)+" makes another table's rows this table's (or stops them being so), by no assignment")
				}
			}
			for _, reg := range registry {
				if reg.Table != q {
					continue
				}
				if w.writesColumn(reg.Column) {
					seenWriter[reg] = true
				}
				column := func(kind, expr, detail string) {
					violate(Violation{Table: reg.Table, Column: reg.Column, Kind: kind, Want: reg.Policy, Expr: expr, Detail: detail, statement: stmtTree})
				}
				for _, a := range w.Alters {
					if a.Column == reg.Column {
						column(KindAlterColumn, stmtText, "the statement rewrites, renames or destroys the column's stored values ("+a.Action+"): no policy allows that; take a reviewed Exception")
					}
				}
				for _, fk := range w.FKs {
					if fk.Column == reg.Column {
						column(KindFKAction, stmtText, "a foreign key on the column acts "+fk.Action+": the database then writes the column itself, with a value no policy is checked on")
					}
				}
				if w.Kind == "copy" {
					all := w.CopyAll
					for _, c := range w.CopyColumns {
						all = all || c == reg.Column
					}
					if all {
						column(KindCopy, stmtText, "COPY ... FROM writes the column with values no policy can be checked on")
					}
				}
				for _, it := range w.Sets {
					if it.Column != reg.Column {
						continue
					}
					var ok bool
					var detail string
					switch {
					case it.Multi:
						// SET (a, col) = (...): the right side is one row,
						// not a value per column.
						ok, detail = false, "multiple-column SET: the policy cannot be checked on this form; assign the column on its own, or take a reviewed Exception"
					case it.Indirect:
						ok, detail = false, "the target is a part of the column (a subscript or a field): no policy shape describes writing a part; assign the whole column, or take a reviewed Exception"
					case it.val == nil:
						ok, detail = false, "the value is not an expression that was read, so the policy cannot be checked on it"
					case holdsUnknown(it.val):
						ok, detail = false, "the value has a part that is not fixed text ("+Unknown+"), so the policy cannot be checked on it"
					case reg.Policy == InsertOnly:
						ok, detail = false, "column is InsertOnly but this statement assigns it in an UPDATE SET"
					default:
						ok, detail = valueConforms(w, it.val, reg.Column, reg.Policy)
					}
					if !ok {
						violate(Violation{Table: reg.Table, Column: reg.Column, Kind: KindAssign, Want: reg.Policy, Expr: it.Expr, Detail: detail,
							fragment: canon(it.val), statement: stmtTree})
					}
				}
			}
		}
	}
	for _, reg := range registry {
		if !seenWriter[reg] {
			rep.Unwritten = append(rep.Unwritten, reg)
		}
	}
	for i, ex := range exceptions {
		if !used[i] {
			rep.StaleExceptions = append(rep.StaleExceptions, ex)
		}
	}
	return rep
}

// writesColumn: the write, of the table it names, gives col a value: an
// INSERT (or a MERGE's INSERT arm) that lists it or lists no column, or an
// assignment of it.
func (w Write) writesColumn(col string) bool {
	if w.InsertAll && (w.Kind == "insert" || w.Kind == "merge") {
		return true
	}
	for _, c := range w.InsertColumns {
		if c == col {
			return true
		}
	}
	for _, it := range w.Sets {
		if it.Column == col {
			return true
		}
	}
	return false
}

// canon is a tree as text, without the places things stood at in the
// source: two trees are the same statement or expression exactly when
// their canons are equal.
func canon(v any) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(placeless(v))
	if err != nil {
		return ""
	}
	return string(b)
}

func placeless(v any) any {
	switch t := v.(type) {
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = placeless(e)
		}
		return out
	case jnode:
		out := make(jnode, len(t))
		for k, e := range t {
			if k == "location" || k == "stmt_location" || k == "stmt_len" {
				continue
			}
			out[k] = placeless(e)
		}
		return out
	}
	return v
}

// exceptionTrees are an Exception's fragment and statement as trees.
type exceptionTrees struct {
	fragment, statement string
	ok                  bool
}

// exceptionTree reads an Exception's SQL; problem says what is wrong with
// one that is not well formed (it then excuses nothing).
func exceptionTree(ex Exception) (t exceptionTrees, problem string) {
	if ex.Kind == "" {
		return t, "it names no Kind"
	}
	assign := ex.Kind == KindAssign || ex.Kind == KindTriggerAssign
	switch {
	case ex.Statement == "":
		return t, "an exception pins the one statement it excuses, whole, as Statement (its WHERE included): an expression alone excuses the statement with any WHERE, or none"
	case assign && ex.Fragment == "":
		return t, "an assignment's exception gives the assigned expression, whole, as Fragment"
	case !assign && ex.Fragment != "":
		return t, "Fragment is an assigned expression; this kind has none"
	}
	if ex.Fragment != "" {
		val, err := exprTree(ex.Fragment)
		if err != nil {
			return t, "Fragment is not one expression: " + err.Error()
		}
		t.fragment = canon(val)
	}
	p := parseSQL(ex.Statement)
	if p.err != nil || len(p.stmts) != 1 {
		return t, "Statement is not one statement the parser reads"
	}
	rs, _ := p.stmts[0].(jnode)
	t.statement = canon(rs["stmt"])
	if ex.Kind == KindAssign {
		// The fragment says which assignment of the statement is excused:
		// it must be one, of this column.
		found := false
		ws, err := ParseWrites(ex.Statement)
		if err != nil {
			return t, "Statement is not read: " + err.Error()
		}
		for _, w := range ws {
			for _, it := range w.Sets {
				found = found || (it.Column == ex.Column && canon(it.val) == t.fragment)
			}
		}
		if !found {
			return t, "Fragment is not an assignment of " + ex.Column + " in Statement"
		}
	}
	t.ok = true
	return t, ""
}

// holdsUnknown: some name or string of the value holds a mark of the
// Unknown placeholder, which stands for text that was not read.
func holdsUnknown(v any) bool {
	switch t := v.(type) {
	case string:
		return strings.ContainsAny(t, Unknown)
	case []any:
		for _, e := range t {
			if holdsUnknown(e) {
				return true
			}
		}
	case jnode:
		for _, e := range t {
			if holdsUnknown(e) {
				return true
			}
		}
	}
	return false
}

// exprConforms reports whether one SET RHS, given as text, conforms to the
// policy for col, as the value assigned by an INSERT ... ON CONFLICT DO
// UPDATE of the table stmt's first write names (so that both the stored
// value and EXCLUDED's can be named), under that write's WHERE. Text
// PostgreSQL's parser does not read as one expression does not conform.
func exprConforms(stmt Stmt, expr, col string, want Policy) (bool, string) {
	val, err := exprTree(expr)
	if err != nil {
		return false, "the value is not read as one expression: " + err.Error()
	}
	ws, err := ParseWrites(stmt.SQL)
	if err != nil || len(ws) == 0 {
		return false, "the statement the value stands in is not read"
	}
	w := ws[0]
	w.Kind = "conflict-update"
	return valueConforms(&w, val, col, want)
}

// valueConforms reports whether a value's tree, assigned by the write w,
// conforms to the policy for col. On failure the detail classifies what the
// expression ACTUALLY matches so the mismatch reads at a glance (the
// round-11 postmortem hinged on recognizing "this is
// prefer-nonempty-INCOMING").
func valueConforms(w *Write, val any, col string, want Policy) (bool, string) {
	if matchPolicy(w, val, col, want) {
		return true, ""
	}
	var also []string
	for _, p := range []Policy{FillEmptyOnly, PreferNonemptyIncoming, PreferNonNullIncoming, GreatestNonNull, AlwaysRefresh} {
		if p != want && matchPolicy(w, val, col, p) {
			also = append(also, p.String())
		}
	}
	if len(also) > 0 {
		return false, fmt.Sprintf("expression matches %s — registered policy is %s", strings.Join(also, "+"), want)
	}
	return false, fmt.Sprintf("unrecognized write shape for %s — conform, extend the tested shape set, or take a reviewed Exception", want)
}

// twoArgs: v is a node of the given kind with exactly two arguments.
func twoArgs(v any, kind string) (jnode, [2]any, bool) {
	k, b := wrapped(v)
	args := jlist(b["args"])
	if k != kind || len(args) != 2 {
		return nil, [2]any{}, false
	}
	return b, [2]any{args[0], args[1]}, true
}

func matchPolicy(w *Write, val any, col string, p Policy) bool {
	// What an operand is, by the one resolver.
	operand := func(v any) string { return w.operand(v, col) }
	switch p {
	case FillEmptyOnly:
		if _, args, ok := twoArgs(val, "CoalesceExpr"); ok {
			// Text form: COALESCE(NULLIF(stored, ''), incoming)
			if inner, ok := nullifEmpty(args[0]); ok && operand(inner) == "stored" && !w.mentionsStored(args[1], col) {
				return true
			}
			// Nullable form: COALESCE(stored, incoming)
			return operand(args[0]) == "stored" && !w.mentionsStored(args[1], col)
		}
		// The same fill spelled as a CASE (round 7): CASE WHEN <the column
		// is empty> THEN incoming ELSE stored END, the condition being one
		// of the forms a guard is read in, and nothing else in the CASE.
		if k, c := wrapped(val); k == "CaseExpr" && c["arg"] == nil && len(jlist(c["args"])) == 1 {
			_, when := wrapped(jlist(c["args"])[0])
			return when != nil && w.isEmptyGuard(when["expr"], col) && when["result"] != nil &&
				!w.mentionsStored(when["result"], col) && operand(c["defresult"]) == "stored"
		}
		// Guarded bare form: SET col = $N ... WHERE COALESCE(col,'')=''.
		return operand(val) == "incoming" && w.GuardsEmpty(col)
	case PreferNonemptyIncoming:
		if _, args, ok := twoArgs(val, "CoalesceExpr"); ok {
			inner, ok := nullifEmpty(args[0])
			return ok && operand(inner) == "incoming" && operand(args[1]) == "stored"
		}
		return false
	case PreferNonNullIncoming:
		_, args, ok := twoArgs(val, "CoalesceExpr")
		return ok && operand(args[0]) == "incoming" && operand(args[1]) == "stored"
	case GreatestNonNull:
		b, args, ok := twoArgs(val, "MinMaxExpr")
		if !ok || jstr(b["op"]) != "IS_GREATEST" {
			return false
		}
		// GREATEST(stored, incoming), in either order (round 7):
		// PostgreSQL's GREATEST ignores a NULL argument and is NULL only
		// when both are, so it is the verbose shape below already.
		if a, b := operand(args[0]), operand(args[1]); (a == "stored" && b == "incoming") || (a == "incoming" && b == "stored") {
			return true
		}
		aargs, aok := w.coalescePair(args[0], col)
		bargs, bok := w.coalescePair(args[1], col)
		if !aok || !bok {
			return false
		}
		// One side (incoming, stored), the other (stored, incoming) —
		// same-order-twice loses a fallback and does NOT conform.
		return (aargs == [2]string{"incoming", "stored"} && bargs == [2]string{"stored", "incoming"}) ||
			(aargs == [2]string{"stored", "incoming"} && bargs == [2]string{"incoming", "stored"})
	case AlwaysRefresh:
		// Nothing of the stored value, of this row or another: a column
		// that reaches the value through a FROM item which reads the
		// target counts (round 7).
		return !w.storedIn(val, col, nil, true)
	case EarliestOfStoredAndIncoming:
		mm, args, ok := twoArgs(val, "MinMaxExpr")
		if !ok || jstr(mm["op"]) != "IS_LEAST" {
			return false
		}
		a, b := operand(args[0]), operand(args[1])
		return (a == "stored" && b == "incoming") || (a == "incoming" && b == "stored")
	case KeepCarryOrNow:
		results, ok := caseResults(val)
		if !ok {
			return false
		}
		for _, r := range results {
			if isNow(r) {
				continue
			}
			// COALESCE(incoming, NOW()): the value handed in, or now when
			// none was (round 7).
			if _, args, ok := twoArgs(r, "CoalesceExpr"); ok && operand(args[0]) == "incoming" && isNow(args[1]) {
				continue
			}
			// operand allows a cast on an incoming value, and on that
			// only: a cast of the stored value would change what is kept.
			if k := operand(r); k != "stored" && k != "incoming" {
				return false
			}
		}
		return true
	}
	return false
}

// isNow: v is the time the transaction began, in one of its three
// spellings: NOW(), transaction_timestamp() (each with no argument and
// nothing else about the call), or CURRENT_TIMESTAMP with no precision.
// They are one value. clock_timestamp() and statement_timestamp() are
// other values (the wall clock; the statement's start) and are not this:
// two columns stamped in one transaction would then disagree.
func isNow(v any) bool {
	k, f := wrapped(v)
	if k == "SQLValueFunction" {
		return jstr(f["op"]) == "SVFOP_CURRENT_TIMESTAMP"
	}
	names := jlist(f["funcname"])
	if k != "FuncCall" || len(names) == 0 || len(names) > 2 || len(jlist(f["args"])) != 0 {
		return false
	}
	for _, extra := range []string{"agg_order", "agg_filter", "over", "agg_star", "agg_distinct", "func_variadic", "agg_within_group"} {
		if f[extra] != nil {
			return false
		}
	}
	if len(names) == 2 {
		if schema, _ := sval(names[0]); schema != "pg_catalog" {
			return false
		}
	}
	name, _ := sval(names[len(names)-1])
	return len(names) == 1 && (name == "now" || name == "transaction_timestamp")
}

// caseResults returns the result expressions of `CASE WHEN c THEN r ... ELSE
// r END` that is the WHOLE value: what follows each THEN and the ELSE. It
// refuses (false) a CASE with an operand (CASE x WHEN ...), and a CASE with
// no ELSE (an unmatched row would be NULLed). A nested CASE as a result is
// a result like any other, and matches nothing its caller allows.
func caseResults(v any) ([]any, bool) {
	k, c := wrapped(v)
	if k != "CaseExpr" || c["arg"] != nil || c["defresult"] == nil || len(jlist(c["args"])) == 0 {
		return nil, false
	}
	var out []any
	for _, e := range jlist(c["args"]) {
		wk, w := wrapped(e)
		if wk != "CaseWhen" || w["result"] == nil {
			return nil, false
		}
		out = append(out, w["result"])
	}
	return append(out, c["defresult"]), true
}

// nullifEmpty matches NULLIF of an operand and the empty string, and
// returns the operand.
func nullifEmpty(v any) (operand any, ok bool) {
	k, e := wrapped(v)
	if k != "A_Expr" || jstr(e["kind"]) != "AEXPR_NULLIF" {
		return nil, false
	}
	if s, isString := constString(e["rexpr"]); !isString || s != "" {
		return nil, false
	}
	return e["lexpr"], true
}

// coalescePair matches `COALESCE(a, b)` and returns the two operands'
// kinds for col.
func (w *Write) coalescePair(v any, col string) ([2]string, bool) {
	_, args, ok := twoArgs(v, "CoalesceExpr")
	if !ok {
		return [2]string{}, false
	}
	return [2]string{w.operand(args[0], col), w.operand(args[1], col)}, true
}

// ---------------------------------------------------------------------
// The resolver: whose column a name is.

// refClass is what a column reference whose last part is the column's name
// resolves to, where the write stands.
type refClass int

const (
	// refStored: the target relation's own column, which holds the stored
	// value: bare (PostgreSQL stops a statement in which a bare name could
	// also be another relation's column), or qualified by the target's
	// alias, or by its name (or schema.name) when it has no alias.
	refStored refClass = iota + 1
	// refIncoming: EXCLUDED.col, in ON CONFLICT DO UPDATE and nowhere else.
	refIncoming
	// refOther: a column of another relation in scope (a FROM item, a
	// MERGE's source, a relation of the sub-select the reference stands in).
	refOther
	// refVariable: a PL/pgSQL variable or record's field, or a function's
	// parameter.
	refVariable
	// refUnresolved: none of these can be told: the qualifier names nothing
	// in scope, or two things; a body has a variable of the name and does
	// not say which wins.
	refUnresolved
)

// target is the qualifier that names the write's target: its alias, or
// with none its name.
func (w *Write) targetName() string {
	if w.Alias != "" {
		return w.Alias
	}
	return w.Table
}

// fromNamed: an item of the list is named q by a qualifier.
func fromNamed(items []fromItem, q string) bool {
	for _, it := range items {
		if it.alias == q || (it.alias == "" && it.name == q) {
			return true
		}
	}
	return false
}

// variable: name is a variable of a body the write stands in, and how that
// body settles a name that is both a variable and a column ("" when it does
// not: PL/pgSQL then stops the statement; an SQL function's parameter always
// yields to a column, "use_column").
func (w *Write) variable(name string) (is bool, conflict string) {
	if w.scope == nil {
		return false, ""
	}
	for env := w.scope.env; env != nil; env = env.outer {
		if env.vars[name] {
			if env.lang == "sql" {
				return true, "use_column"
			}
			return true, env.conflict
		}
	}
	return false, ""
}

// qualifiesVariable: q qualifies a variable's field or a parameter (a
// record's name, a function's name).
func (w *Write) qualifiesVariable(q string) bool {
	if w.scope == nil {
		return false
	}
	for env := w.scope.env; env != nil; env = env.outer {
		if env.vars[q] || (env.fn != "" && env.fn == q) {
			return true
		}
	}
	return false
}

// classify resolves a column reference whose last part is col. inner are
// the FROM lists of the sub-selects the reference stands in, innermost last
// (none for a reference in the assignment's own expression or WHERE).
func (w *Write) classify(parts []string, inner [][]fromItem) refClass {
	var own []fromItem
	if w.scope != nil {
		own = w.scope.from
	}
	switch len(parts) {
	case 1:
		// In a sub-select the sub-select's own relations come first, and
		// what columns they have is not known here: a bare name there is
		// taken for the stored value all the same (the only reader of a
		// reference inside a sub-select, storedIn, denies on it).
		if is, conflict := w.variable(parts[0]); is {
			switch conflict {
			case "use_variable":
				return refVariable
			case "use_column":
				return refStored
			}
			return refUnresolved
		}
		return refStored
	case 2:
		q := parts[0]
		for i := len(inner) - 1; i >= 0; i-- {
			if fromNamed(inner[i], q) {
				return refOther
			}
		}
		if w.Kind == "conflict-update" && q == "excluded" && w.Alias != "excluded" {
			return refIncoming
		}
		isTarget, isFrom, isVar := q == w.targetName(), fromNamed(own, q), w.qualifiesVariable(q)
		switch {
		case isTarget && !isFrom && !isVar:
			return refStored
		case isTarget:
			return refUnresolved
		case isFrom:
			return refOther
		case isVar:
			return refVariable
		}
		return refUnresolved
	case 3:
		s, t := parts[0], parts[1]
		named := func(items []fromItem) bool {
			for _, it := range items {
				if it.alias == "" && it.schema == s && it.name == t {
					return true
				}
			}
			return false
		}
		for i := len(inner) - 1; i >= 0; i-- {
			if named(inner[i]) {
				return refOther
			}
		}
		if w.Alias == "" && s == w.Schema && t == w.Table && w.Schema != "" {
			if named(own) {
				return refUnresolved
			}
			return refStored
		}
		if named(own) {
			return refOther
		}
	}
	return refUnresolved
}

// operand classifies a single operand of a policy shape for col:
//   - "incoming": a value handed in: a $N placeholder; EXCLUDED.<col> (the
//     same column) in ON CONFLICT DO UPDATE; a column of an INCOMING FROM
//     item (unnest of placeholders, VALUES of placeholders: incomingQuery),
//     qualified by that item's name. Any of them may carry one cast
//     ($1::text), and may stand in TRIM(...) / btrim(...) of one argument
//     (a value handed in with its outer spaces removed is still the value
//     handed in);
//   - "stored": a reference the resolver gives to the target's own <col>;
//   - "other": anything else, a column of another relation, a variable and
//     an unresolved reference included. TRIM of the STORED value is
//     "other": the shapes keep or compare the stored value AS IT IS, and a
//     trimmed one written back is a new value no policy describes.
func (w *Write) operand(v any, col string) string {
	if k, tc := wrapped(v); k == "TypeCast" {
		if w.operand(tc["arg"], col) == "incoming" {
			if ak, _ := wrapped(tc["arg"]); ak != "TypeCast" {
				return "incoming"
			}
		}
		return "other"
	}
	if k, _ := wrapped(v); k == "ParamRef" {
		return "incoming"
	}
	if arg, ok := trimOf(v); ok {
		if w.operand(arg, col) == "incoming" {
			if ak, _ := wrapped(arg); ak != "FuncCall" {
				return "incoming"
			}
		}
		return "other"
	}
	parts, ok := columnRef(v)
	if !ok {
		return "other"
	}
	if len(parts) == 2 && w.incomingItem(parts[0]) {
		return "incoming"
	}
	if parts[len(parts)-1] != col {
		return "other"
	}
	switch w.classify(parts, nil) {
	case refStored:
		return "stored"
	case refIncoming:
		return "incoming"
	}
	return "other"
}

// trimOf: v is TRIM(x) or btrim(x), of one argument and nothing else, and
// that argument.
func trimOf(v any) (any, bool) {
	k, f := wrapped(v)
	names, args := jlist(f["funcname"]), jlist(f["args"])
	if k != "FuncCall" || len(names) == 0 || len(names) > 2 || len(args) != 1 {
		return nil, false
	}
	for _, extra := range []string{"agg_order", "agg_filter", "over", "agg_star", "agg_distinct", "func_variadic", "agg_within_group"} {
		if f[extra] != nil {
			return nil, false
		}
	}
	if len(names) == 2 {
		if schema, _ := sval(names[0]); schema != "pg_catalog" {
			return nil, false
		}
	}
	if name, _ := sval(names[len(names)-1]); name != "btrim" {
		return nil, false
	}
	return args[0], true
}

// incomingItem: the qualifier q names exactly one thing where the write
// stands, and that is an INCOMING item of its FROM (fromItem.incoming).
func (w *Write) incomingItem(q string) bool {
	if w.scope == nil || q == w.targetName() || w.qualifiesVariable(q) || (w.Kind == "conflict-update" && q == "excluded") {
		return false
	}
	n, incoming := 0, false
	for _, it := range w.scope.from {
		if it.alias == q || (it.alias == "" && it.name == q) {
			n++
			incoming = it.incoming
		}
	}
	return n == 1 && incoming
}

// ---------------------------------------------------------------------
// Provenance: what a FROM item reads.

// itemRead is what one FROM item can hand the assignment of the target's
// stored values.
type itemRead struct {
	// reads: the item reads the target table: it IS the target under
	// another alias, or a view over it (the catalog's, transitively), or a
	// sub-select or WITH query that reads it by any of these.
	reads bool
	// self: the item is the target table itself, written in place. Its
	// columns are the table's, under their own names.
	self bool
	// known: the item's output columns are enumerated; tainted are those
	// that may hold the protected column's stored value. When not known,
	// every column of the item may.
	known   bool
	tainted map[string]bool
}

// may: a column of the item named c may hold the stored value of col.
func (ir itemRead) may(c, col string) bool {
	switch {
	case !ir.reads:
		return false
	case ir.self:
		return c == col
	case !ir.known:
		return true
	}
	return ir.tainted[c]
}

// isTarget: the relation name (schema.name, or a bare name) is the write's
// target table, or may be (a bare name of the table's is taken for it).
func (w *Write) isTarget(rel string) bool {
	return rel == w.Qualified() || (!strings.Contains(rel, ".") && rel == w.Table)
}

// relReadsTarget: the relation named rel reads the target: it is it, a view
// (or materialized view, or partition) over it by the catalog, or a WITH
// query of the statement that reads it. seen stops a WITH that names itself.
func (w *Write) relReadsTarget(rel string, seen map[string]bool) bool {
	if w.scope != nil && !strings.Contains(rel, ".") {
		if q, ok := w.scope.ctes[rel]; ok {
			if seen[rel] {
				return false
			}
			seen[rel] = true
			for _, inner := range relsIn(q) {
				if w.relReadsTarget(inner, seen) {
					return true
				}
			}
			return false
		}
	}
	if w.isTarget(rel) {
		return true
	}
	if w.cat != nil {
		for _, b := range w.cat.behind(rel) {
			if w.isTarget(b) {
				return true
			}
		}
	}
	return false
}

// read says what a FROM item can hand over of col's stored value.
func (w *Write) read(it fromItem, col string) itemRead {
	var ir itemRead
	for _, rel := range it.rels {
		ir.reads = ir.reads || w.relReadsTarget(rel, map[string]bool{})
	}
	if !ir.reads {
		return ir
	}
	var ctes map[string]any
	if w.scope != nil {
		ctes = w.scope.ctes
	}
	_, isCTE := ctes[it.name]
	isCTE = isCTE && it.schema == ""
	if it.direct && !it.colnames && !isCTE && w.isTarget(relName(jnode{"schemaname": it.schema, "relname": it.name})) {
		ir.self = true
		return ir
	}
	if it.colnames {
		return ir
	}
	// A sub-select written in place, or a WITH query named: its outputs
	// are read when it is one plain SELECT that reads the target as itself.
	q := it.sub
	if it.direct && isCTE {
		q = ctes[it.name]
	}
	ir.tainted, ir.known = w.taintedOutputs(q, col)
	return ir
}

// taintedOutputs enumerates the output columns of a query that reads the
// target, and says which may hold col's stored value. known is false when
// that cannot be told: the query is not one plain SELECT (a set operation,
// VALUES, a WITH of its own, a data-modifying statement), one of its FROM
// items reads the target other than as the target itself written in place
// (through a view, a sub-select, a WITH query, a join's alias), an output
// is `*`, or an output that may hold the value has no name of its own.
func (w *Write) taintedOutputs(q any, col string) (tainted map[string]bool, known bool) {
	k, sel := wrapped(q)
	if k != "SelectStmt" || sel["larg"] != nil || sel["rarg"] != nil || sel["withClause"] != nil || sel["valuesLists"] != nil {
		return nil, false
	}
	from := fromItems(sel["fromClause"])
	names := map[string]bool{}
	for _, it := range from {
		reads := false
		for _, rel := range it.rels {
			reads = reads || w.relReadsTarget(rel, map[string]bool{})
		}
		if reads && !(it.direct && !it.colnames && w.isTarget(relName(jnode{"schemaname": it.schema, "relname": it.name}))) {
			return nil, false
		}
		if it.alias != "" {
			names[it.alias] = true
		} else {
			names[it.name] = true
		}
	}
	// carries: an output's expression may hold the stored value: it names
	// a column called col, a whole row, `*`, or holds a query of its own.
	var carries func(v any) bool
	carries = func(v any) bool {
		switch t := v.(type) {
		case []any:
			for _, e := range t {
				if carries(e) {
					return true
				}
			}
		case jnode:
			kind, body := wrapped(t)
			switch kind {
			case "SubLink", "SelectStmt":
				return true
			case "ColumnRef":
				parts, ok := columnRef(t)
				if !ok {
					return true // a `*`
				}
				return parts[len(parts)-1] == col || (len(parts) == 1 && names[parts[0]])
			}
			if kind != "" {
				t = body
			}
			for _, key := range sortedKeys(t) {
				if carries(t[key]) {
					return true
				}
			}
		}
		return false
	}
	// Every output that may hold the value has a name here, so a column of
	// the item holds it exactly when its name is one of those.
	tainted = map[string]bool{}
	for _, e := range jlist(sel["targetList"]) {
		tk, rt := wrapped(e)
		if tk != "ResTarget" {
			return nil, false
		}
		if !carries(rt["val"]) {
			continue
		}
		name := jstr(rt["name"])
		if name == "" {
			parts, ok := columnRef(rt["val"])
			if !ok {
				// A `*`, or an expression PostgreSQL names after what it
				// is (a function's name, "?column?"): not a name to go by.
				return nil, false
			}
			name = parts[len(parts)-1]
		}
		tainted[name] = true
	}
	return tainted, true
}

// mentionsStored reports whether the value can read the STORED value of
// col OF THE ROW BEING WRITTEN: it holds, at any depth, a reference the
// resolver gives to the target's column or cannot resolve; the target's
// whole row (r, r.*, its name); or the target TABLE itself, in a
// sub-select. It denies: what cannot be told to be something else counts.
//
// AlwaysRefresh asks more (storedIn with wide): the stored value of ANY
// row, by any route: a column of a FROM item that reads the target (the
// table again under another alias, a view over it, a sub-select or WITH
// query that reads it), unless that item's outputs are enumerated and the
// column is not one that may hold it; and a sub-select of the value that
// reads the target through a view or a WITH query.
func (w *Write) mentionsStored(v any, col string) bool {
	return w.storedIn(v, col, nil, false)
}

func (w *Write) storedIn(v any, col string, inner [][]fromItem, wide bool) bool {
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			if w.storedIn(e, col, inner, wide) {
				return true
			}
		}
	case jnode:
		kind, body := wrapped(t)
		switch kind {
		case "RangeVar":
			if jstr(body["relname"]) == w.Table && (jstr(body["schemaname"]) == "" || jstr(body["schemaname"]) == w.Schema) {
				return true
			}
			if wide && w.relReadsTarget(relName(body), map[string]bool{}) {
				return true
			}
		case "SelectStmt":
			inner = append(append([][]fromItem{}, inner...), fromItems(body["fromClause"]))
		case "ColumnRef":
			return w.refReadsStored(body, col, inner, wide)
		}
		for _, k := range sortedKeys(t) {
			if w.storedIn(t[k], col, inner, wide) {
				return true
			}
		}
	}
	return false
}

// refReadsStored: one column reference can read the stored value of col.
func (w *Write) refReadsStored(ref jnode, col string, inner [][]fromItem, wide bool) bool {
	fields := jlist(ref["fields"])
	var parts []string
	star := false
	for _, f := range fields {
		if s, ok := sval(f); ok {
			parts = append(parts, s)
		} else {
			star = true
		}
	}
	shadowed := func(q string) bool {
		for _, items := range inner {
			if fromNamed(items, q) {
				return true
			}
		}
		return false
	}
	var own []fromItem
	if w.scope != nil {
		own = w.scope.from
	}
	// reading: the own FROM items named q that read the target (wide only).
	reading := func(q string) []itemRead {
		var out []itemRead
		if !wide || shadowed(q) {
			return nil
		}
		for _, it := range own {
			if it.alias == q || (it.alias == "" && it.name == q) {
				if ir := w.read(it, col); ir.reads {
					out = append(out, ir)
				}
			}
		}
		return out
	}
	// wholeRow: the qualifier parts name the target's row.
	wholeRow := func(q []string) bool {
		switch len(q) {
		case 0:
			// A bare * at the top of the assignment is no expression; in a
			// sub-select it is that sub-select's rows.
			return len(inner) == 0
		case 1:
			if shadowed(q[0]) {
				return false
			}
			if q[0] == w.targetName() {
				return true
			}
			// The whole row of an item that reads the target holds
			// whatever the item reads of it.
			if len(reading(q[0])) > 0 {
				return true
			}
			// A name in scope as another relation, or a variable, is that.
			return !fromNamed(own, q[0]) && !w.qualifiesVariable(q[0]) && !(w.Kind == "conflict-update" && q[0] == "excluded")
		case 2:
			return q[0] == w.Schema && q[1] == w.Table
		}
		return true
	}
	if star {
		return wholeRow(parts)
	}
	if len(parts) == 0 {
		return true
	}
	last := parts[len(parts)-1]
	if wide {
		switch len(parts) {
		case 1:
			// A bare name, where the assignment stands, may be a column of
			// any FROM item: of one that reads the target, when that item
			// may hold the stored value under this name.
			if len(inner) == 0 {
				for _, it := range own {
					if ir := w.read(it, col); ir.reads && !ir.self && ir.may(last, col) {
						return true
					}
				}
			}
		case 2:
			for _, ir := range reading(parts[0]) {
				if ir.may(last, col) {
					return true
				}
			}
		case 3:
			if !shadowed(parts[1]) {
				for _, it := range own {
					if it.alias == "" && it.schema == parts[0] && it.name == parts[1] {
						if ir := w.read(it, col); ir.may(last, col) {
							return true
						}
					}
				}
			}
		}
	}
	if last == col {
		switch w.classify(parts, inner) {
		case refStored, refUnresolved:
			return true
		}
		return false
	}
	// A reference that is not to a column of this name can still be the
	// target's whole row, written as its name: to_jsonb(r), (r).col; or,
	// for AlwaysRefresh, the whole row of an item that reads the target.
	switch len(parts) {
	case 1:
		return !shadowed(parts[0]) && (parts[0] == w.targetName() || len(reading(parts[0])) > 0)
	case 2:
		return w.Alias == "" && parts[0] == w.Schema && parts[1] == w.Table
	}
	return false
}
