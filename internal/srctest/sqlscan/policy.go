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
// is a violation: no matcher reads that form (closed-world, as below). A
// quoted, subscripted or field target (`SET "col" =`, `col[1] =`, `col.f
// =`) is the column it names, by PostgreSQL's parser (pgread.go).
//
// The matchers read the value's PARSE TREE (0.29.212), not its text: what
// is a call of COALESCE, which name is a column's and which a string's, is
// the parser's reading.
//
// Matchers are ORDER-SENSITIVE and closed-world per registered column:
// an RHS matching no recognized shape for the policy FAILS
// ("unrecognized write shape") — new idioms must conform, extend the
// tested shape set, or take a reviewed Exception WITH a reason. Never
// weaken a matcher to get green.

package sqlscan

import (
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/srctest"
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
	// value, an incoming placeholder (a cast allowed) or NOW(): the column
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

// Exception carves one statement out of its column's policy, WITH a
// reason. Match is a whitespace-normalized needle that must appear in
// the excepted statement. An exception that suppresses nothing in a
// run is reported STALE (the allowlist-rot reverse check).
type Exception struct {
	Table  string
	Column string
	// File, with Match a needle in the statement; or no File, with Match
	// the statement's WHOLE text (0.29.203), which names no source file
	// and ends with any change to the statement.
	File   string
	Match  string
	Reason string
}

// Violation is one non-conforming write.
type Violation struct {
	Table  string
	Column string
	File   string
	Expr   string
	Want   Policy
	Detail string
}

// Unparsed is one statement not read.
type Unparsed struct {
	File, SQL, Detail string
}

// Report is Check's result.
type Report struct {
	Violations      []Violation
	StaleExceptions []Exception
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

// Check verifies every registered column's UPDATE SET assignments
// against its policy across the statement corpus.
func Check(stmts []Stmt, registry []Registered, exceptions []Exception) Report {
	var rep Report
	used := make([]bool, len(exceptions))
	// Every statement is read once, through PostgreSQL's parser. One that
	// is not read to its end is reported whatever it is.
	type read struct {
		stmt   Stmt
		writes []Write
	}
	reads := make([]read, 0, len(stmts))
	for _, st := range stmts {
		ws, err := ParseWrites(st.SQL)
		if err != nil {
			rep.Unparsed = append(rep.Unparsed, Unparsed{File: st.File, SQL: st.SQL, Detail: err.Error()})
			continue
		}
		reads = append(reads, read{st, ws})
	}
	// The views defined anywhere in what was read: a write through one is
	// a write of the tables behind it.
	views := map[string][]string{}
	for _, st := range stmts {
		for v, srcs := range ViewSources(st.SQL) {
			views[v] = append(views[v], srcs...)
		}
	}
	for _, reg := range registry {
		bare := reg.Table[strings.LastIndex(reg.Table, ".")+1:]
		seenWriter := false
		for _, rd := range reads {
			for i := range rd.writes {
				w := rd.writes[i]
				st := rd.stmt
				st.write = &rd.writes[i]
				violate := func(expr, detail string) {
					if i := findException(exceptions, reg, st); i >= 0 {
						used[i] = true
						return
					}
					rep.Violations = append(rep.Violations, Violation{
						Table: reg.Table, Column: reg.Column, File: st.File,
						Expr: expr, Want: reg.Policy, Detail: detail,
					})
				}
				if w.Schema == "" {
					// Deny by default: which table an unqualified name is
					// depends on the session's search_path, so a write of a
					// column of this NAME through one is judged a write of
					// the registered column that nothing can check.
					touches := (w.Kind == "copy" && (w.CopyAll || w.Table == bare)) || (w.Kind == "drop" && w.Table == bare)
					for _, it := range w.Sets {
						touches = touches || it.Column == reg.Column
					}
					for _, a := range w.Alters {
						touches = touches || a.Column == reg.Column
					}
					for _, c := range w.CopyColumns {
						touches = touches || (w.Kind == "copy" && c == reg.Column)
					}
					switch {
					case touches && w.Kind == "trigger-assign":
						violate("NEW."+reg.Column+" := "+w.Sets[0].Expr, "a trigger's body assigns the column of the row being written; which table the trigger is on is said elsewhere, so it may be "+
							reg.Table+" and nothing can tell")
					case touches:
						violate(w.Kind+" "+w.Table, "the target names no schema, so it may be "+reg.Table+
							" and nothing can tell: write the schema")
					}
					continue
				}
				if w.Qualified() != reg.Table {
					for _, src := range views[w.Qualified()] {
						if src == reg.Table && (len(w.Sets) > 0 || w.Kind == "copy") {
							violate(w.Kind+" "+w.Qualified(), "the target is a view over "+reg.Table+": a write through it writes that table under the view's column names, which no policy can be checked on")
							break
						}
					}
					continue
				}
				if w.writesColumn(reg.Column) {
					seenWriter = true
				}
				if w.Kind == "drop" {
					violate("DROP TABLE "+reg.Table, "the statement destroys every stored value of the column: no policy allows that; take a reviewed Exception")
				}
				for _, a := range w.Alters {
					if a.Column == reg.Column {
						violate("ALTER TABLE ... "+a.Action+" "+a.Column, "the statement rewrites, renames or destroys the column's stored values ("+
							a.Action+"): no policy allows that; take a reviewed Exception")
					}
				}
				if w.Kind == "copy" {
					all := w.CopyAll
					for _, c := range w.CopyColumns {
						all = all || c == reg.Column
					}
					if all {
						violate("COPY "+reg.Table, "COPY ... FROM writes the column with values no policy can be checked on")
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
					case it.val == nil:
						ok, detail = false, "the value is not an expression that was read, so the policy cannot be checked on it"
					case holdsUnknown(it.val):
						ok, detail = false, "the value has a part that is not fixed text ("+Unknown+"), so the policy cannot be checked on it"
					case reg.Policy == InsertOnly:
						ok, detail = false, "column is InsertOnly but this statement assigns it in an UPDATE SET"
					default:
						ok, detail = valueConforms(it.val, reg.Column, reg.Policy, func() bool { return w.GuardsEmpty(reg.Column) })
					}
					if !ok {
						violate(it.Expr, detail)
					}
				}
			}
		}
		if !seenWriter {
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

func findException(exceptions []Exception, reg Registered, w Stmt) int {
	for i, ex := range exceptions {
		if ex.Table != reg.Table || ex.Column != reg.Column {
			continue
		}
		// With no file, the exception is for one statement, whole.
		if ex.File == "" && ex.Match != "" && srctest.NormalizeWS(w.SQL) == srctest.NormalizeWS(ex.Match) {
			return i
		}
		if ex.File != "" && ex.File == w.File && srctest.ContainsNormalized(w.SQL, ex.Match) {
			return i
		}
	}
	return -1
}

// exprConforms reports whether one SET RHS, given as text, conforms to the
// policy for col; stmt is the statement it stands in (for the guard of a
// bare fill). Text PostgreSQL's parser does not read as one expression
// does not conform.
func exprConforms(stmt Stmt, expr, col string, want Policy) (bool, string) {
	val, err := exprTree(expr)
	if err != nil {
		return false, "the value is not read as one expression: " + err.Error()
	}
	return valueConforms(val, col, want, func() bool { return stmt.WhereGuardsEmpty(col) })
}

// valueConforms reports whether a value's tree conforms to the policy for
// col. On failure the detail classifies what the expression ACTUALLY
// matches so the mismatch reads at a glance (the round-11 postmortem
// hinged on recognizing "this is prefer-nonempty-INCOMING").
func valueConforms(val any, col string, want Policy, guarded func() bool) (bool, string) {
	if matchPolicy(val, col, want, guarded) {
		return true, ""
	}
	var also []string
	for _, p := range []Policy{FillEmptyOnly, PreferNonemptyIncoming, PreferNonNullIncoming, GreatestNonNull, AlwaysRefresh} {
		if p != want && matchPolicy(val, col, p, guarded) {
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

func matchPolicy(val any, col string, p Policy, guarded func() bool) bool {
	switch p {
	case FillEmptyOnly:
		if _, args, ok := twoArgs(val, "CoalesceExpr"); ok {
			// Text form: COALESCE(NULLIF(stored, ''), incoming)
			if inner, ok := nullifEmpty(args[0]); ok && inner == "stored" && !containsStoredRef(args[1], col) {
				return true
			}
			// Nullable form: COALESCE(stored, incoming)
			return operandKind(args[0], col) == "stored" && !containsStoredRef(args[1], col)
		}
		// Guarded bare form: SET col = $N ... WHERE COALESCE(col,'')=''.
		return operandKind(val, col) == "incoming" && guarded()
	case PreferNonemptyIncoming:
		if _, args, ok := twoArgs(val, "CoalesceExpr"); ok {
			inner, ok := nullifEmpty(args[0])
			return ok && inner == "incoming" && operandKind(args[1], col) == "stored"
		}
		return false
	case PreferNonNullIncoming:
		_, args, ok := twoArgs(val, "CoalesceExpr")
		return ok && operandKind(args[0], col) == "incoming" && operandKind(args[1], col) == "stored"
	case GreatestNonNull:
		b, args, ok := twoArgs(val, "MinMaxExpr")
		if !ok || jstr(b["op"]) != "IS_GREATEST" {
			return false
		}
		aargs, aok := coalescePair(args[0], col)
		bargs, bok := coalescePair(args[1], col)
		if !aok || !bok {
			return false
		}
		// One side (incoming, stored), the other (stored, incoming) —
		// same-order-twice loses a fallback and does NOT conform.
		return (aargs == [2]string{"incoming", "stored"} && bargs == [2]string{"stored", "incoming"}) ||
			(aargs == [2]string{"stored", "incoming"} && bargs == [2]string{"incoming", "stored"})
	case AlwaysRefresh:
		return !containsStoredRef(val, col)
	case EarliestOfStoredAndIncoming:
		mm, args, ok := twoArgs(val, "MinMaxExpr")
		if !ok || jstr(mm["op"]) != "IS_LEAST" {
			return false
		}
		a, b := operandKind(args[0], col), operandKind(args[1], col)
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
			// One cast is allowed on a result ($23::timestamptz).
			if k, tc := wrapped(r); k == "TypeCast" {
				r = tc["arg"]
			}
			if k := operandKind(r, col); k != "stored" && k != "incoming" {
				return false
			}
		}
		return true
	}
	return false
}

// isNow: v is the call NOW(), with no argument.
func isNow(v any) bool {
	k, f := wrapped(v)
	if k != "FuncCall" || len(jlist(f["funcname"])) != 1 || len(jlist(f["args"])) != 0 {
		return false
	}
	for _, extra := range []string{"agg_order", "agg_filter", "over", "agg_star", "agg_distinct", "func_variadic", "agg_within_group"} {
		if f[extra] != nil {
			return false
		}
	}
	name, _ := sval(jlist(f["funcname"])[0])
	return name == "now"
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

// operandKind classifies a single operand for col:
//   - "incoming": EXCLUDED.<col> or a $N placeholder
//   - "stored": bare <col>, or <qualifier>.<col> with a non-EXCLUDED
//     qualifier (in UPDATE SET RHS both reference the OLD row value); the
//     qualifier an alias or a table, or the table with its schema
//     (0.29.203: the Jira comment upsert names the stored value in full)
//   - "other": anything else
//
// Names are compared as PostgreSQL reads them: an unquoted name folded, a
// quoted one exactly (so "EXCLUDED" in quotes is a table of that name).
func operandKind(v any, col string) string {
	if k, _ := wrapped(v); k == "ParamRef" {
		return "incoming"
	}
	parts, ok := columnRef(v)
	if !ok || len(parts) > 3 || parts[len(parts)-1] != col {
		return "other"
	}
	if len(parts) == 1 {
		return "stored"
	}
	if parts[len(parts)-2] == "excluded" {
		if len(parts) == 2 {
			return "incoming"
		}
		return "other"
	}
	return "stored"
}

// nullifEmpty matches NULLIF of an operand and the empty string, and
// returns the operand's kind.
func nullifEmpty(v any) (kind string, ok bool) {
	k, e := wrapped(v)
	if k != "A_Expr" || jstr(e["kind"]) != "AEXPR_NULLIF" {
		return "", false
	}
	if s, isString := constString(e["rexpr"]); !isString || s != "" {
		return "", false
	}
	return operandKindName(e["lexpr"]), true
}

// operandKindName is operandKind without the column bound — the NULLIF
// helpers carry their own column reference, so classify by shape alone.
func operandKindName(v any) string {
	if k, _ := wrapped(v); k == "ParamRef" {
		return "incoming"
	}
	parts, ok := columnRef(v)
	switch {
	case !ok:
		return "other"
	case len(parts) >= 2 && parts[0] == "excluded":
		return "incoming"
	case len(parts) <= 2:
		return "stored"
	}
	return "other"
}

// coalescePair matches `COALESCE(a, b)` and returns the two operands'
// kinds for col.
func coalescePair(v any, col string) ([2]string, bool) {
	_, args, ok := twoArgs(v, "CoalesceExpr")
	if !ok {
		return [2]string{}, false
	}
	return [2]string{operandKind(args[0], col), operandKind(args[1], col)}, true
}

// containsStoredRef reports whether the value references the STORED value
// of col anywhere, a subquery included: a column reference whose last part
// is col, bare or qualified by anything but EXCLUDED.
func containsStoredRef(v any, col string) bool {
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			if containsStoredRef(e, col) {
				return true
			}
		}
	case jnode:
		if parts, ok := columnRef(t); ok && parts[len(parts)-1] == col &&
			!(len(parts) >= 2 && parts[len(parts)-2] == "excluded") {
			return true
		}
		for _, e := range t {
			if containsStoredRef(e, col) {
				return true
			}
		}
	}
	return false
}
