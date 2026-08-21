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
// Matchers are ORDER-SENSITIVE and closed-world per registered column:
// an RHS matching no recognized shape for the policy FAILS
// ("unrecognized write shape") — new idioms must conform, extend the
// tested shape set, or take a reviewed Exception WITH a reason. Never
// weaken a matcher to get green.

package sqlscan

import (
	"fmt"
	"regexp"
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
	// SetPlatformRepoIDIfEmpty form).
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

// Report is Check's result.
type Report struct {
	Violations      []Violation
	StaleExceptions []Exception
	// Unwritten: registered columns with NO writer statement at all —
	// registry rot (a renamed column) or a genuinely dark column (the
	// column-writer tripwire's territory).
	Unwritten []Registered
}

// Check verifies every registered column's UPDATE SET assignments
// against its policy across the statement corpus.
func Check(stmts []Stmt, registry []Registered, exceptions []Exception) Report {
	var rep Report
	used := make([]bool, len(exceptions))
	for _, reg := range registry {
		writes := FindWrites(stmts, reg.Table)
		seenWriter := false
		for _, w := range writes {
			if w.WritesColumn(reg.Table, reg.Column) {
				seenWriter = true
			}
			for _, expr := range w.SetExprs(reg.Column) {
				var ok bool
				var detail string
				if reg.Policy == InsertOnly {
					ok, detail = false, "column is InsertOnly but this statement assigns it in an UPDATE SET"
				} else {
					ok, detail = exprConforms(w, expr, reg.Column, reg.Policy)
				}
				if ok {
					continue
				}
				if i := findException(exceptions, reg, w); i >= 0 {
					used[i] = true
					continue
				}
				rep.Violations = append(rep.Violations, Violation{
					Table: reg.Table, Column: reg.Column, File: w.File,
					Expr: expr, Want: reg.Policy, Detail: detail,
				})
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

func findException(exceptions []Exception, reg Registered, w Stmt) int {
	for i, ex := range exceptions {
		if ex.Table == reg.Table && ex.Column == reg.Column && ex.File == w.File &&
			srctest.ContainsNormalized(w.SQL, ex.Match) {
			return i
		}
	}
	return -1
}

// exprConforms reports whether one SET RHS conforms to the policy for
// col. On failure the detail classifies what the expression ACTUALLY
// matches so the mismatch reads at a glance (the round-11 postmortem
// hinged on recognizing "this is prefer-nonempty-INCOMING").
func exprConforms(stmt Stmt, expr, col string, want Policy) (bool, string) {
	expr = srctest.NormalizeWS(expr)
	if matchPolicy(stmt, expr, col, want) {
		return true, ""
	}
	var also []string
	for _, p := range []Policy{FillEmptyOnly, PreferNonemptyIncoming, PreferNonNullIncoming, GreatestNonNull, AlwaysRefresh} {
		if p != want && matchPolicy(stmt, expr, col, p) {
			also = append(also, p.String())
		}
	}
	if len(also) > 0 {
		return false, fmt.Sprintf("expression matches %s — registered policy is %s", strings.Join(also, "+"), want)
	}
	return false, fmt.Sprintf("unrecognized write shape for %s — conform, extend the tested shape set, or take a reviewed Exception", want)
}

func matchPolicy(stmt Stmt, expr, col string, p Policy) bool {
	name, args, isCall := parseCall(expr)
	switch p {
	case FillEmptyOnly:
		if isCall && name == "COALESCE" && len(args) == 2 {
			// Text form: COALESCE(NULLIF(stored, ''), incoming)
			if inner, _, ok := nullifEmpty(args[0]); ok && inner == "stored" && !containsStoredRef(args[1], col) {
				return true
			}
			// Nullable form: COALESCE(stored, incoming)
			if operandKind(args[0], col) == "stored" && !containsStoredRef(args[1], col) {
				return true
			}
			return false
		}
		// Guarded bare form: SET col = $N ... WHERE COALESCE(col,'')=''.
		return operandKind(expr, col) == "incoming" && stmt.WhereGuardsEmpty(col)
	case PreferNonemptyIncoming:
		if isCall && name == "COALESCE" && len(args) == 2 {
			if inner, _, ok := nullifEmpty(args[0]); ok && inner == "incoming" && operandKind(args[1], col) == "stored" {
				return true
			}
		}
		return false
	case PreferNonNullIncoming:
		return isCall && name == "COALESCE" && len(args) == 2 &&
			operandKind(args[0], col) == "incoming" && operandKind(args[1], col) == "stored"
	case GreatestNonNull:
		if !isCall || name != "GREATEST" || len(args) != 2 {
			return false
		}
		na, aargs, aok := coalescePair(args[0], col)
		nb, bargs, bok := coalescePair(args[1], col)
		_ = na
		_ = nb
		if !aok || !bok {
			return false
		}
		// One side (incoming, stored), the other (stored, incoming) —
		// same-order-twice loses a fallback and does NOT conform.
		return (aargs == [2]string{"incoming", "stored"} && bargs == [2]string{"stored", "incoming"}) ||
			(aargs == [2]string{"stored", "incoming"} && bargs == [2]string{"incoming", "stored"})
	case AlwaysRefresh:
		return !containsStoredRef(expr, col)
	}
	return false
}

// parseCall decomposes `NAME( ... )` spanning the WHOLE expression into
// its upper-cased name and depth-0-split arguments.
func parseCall(expr string) (string, []string, bool) {
	open := strings.IndexByte(expr, '(')
	if open <= 0 || !strings.HasSuffix(expr, ")") {
		return "", nil, false
	}
	name := strings.TrimSpace(expr[:open])
	if !identRe.MatchString(name) {
		return "", nil, false
	}
	inner := expr[open+1 : len(expr)-1]
	// The parens must balance across the inner span (reject `f(a) + g(b`).
	depth := 0
	for i := 0; i < len(inner); i++ {
		switch inner[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return "", nil, false
			}
		}
	}
	if depth != 0 {
		return "", nil, false
	}
	return strings.ToUpper(name), splitArgs(inner), true
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func splitArgs(inner string) []string {
	var out []string
	depth := 0
	inQuote := false
	start := 0
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		switch {
		case inQuote:
			if c == '\'' {
				inQuote = false
			}
		case c == '\'':
			inQuote = true
		case c == '(':
			depth++
		case c == ')':
			depth--
		case c == ',' && depth == 0:
			out = append(out, strings.TrimSpace(inner[start:i]))
			start = i + 1
		}
	}
	return append(out, strings.TrimSpace(inner[start:]))
}

// operandKind classifies a single operand for col:
//   - "incoming": EXCLUDED.<col> or a $N placeholder
//   - "stored": bare <col>, or <qualifier>.<col> with a non-EXCLUDED
//     qualifier (in UPDATE SET RHS both reference the OLD row value)
//   - "other": anything else
func operandKind(op, col string) string {
	op = strings.TrimSpace(op)
	if op == "EXCLUDED."+col {
		return "incoming"
	}
	if placeholderRe.MatchString(op) {
		return "incoming"
	}
	if op == col {
		return "stored"
	}
	if q, rest, ok := strings.Cut(op, "."); ok && rest == col && identRe.MatchString(q) && !strings.EqualFold(q, "EXCLUDED") {
		return "stored"
	}
	return "other"
}

var placeholderRe = regexp.MustCompile(`^\$\d+$`)

// nullifEmpty matches `NULLIF(<operand>, '')` and returns the
// operand's kind plus its text.
func nullifEmpty(expr string) (kind, operand string, ok bool) {
	name, args, isCall := parseCall(strings.TrimSpace(expr))
	if !isCall || name != "NULLIF" || len(args) != 2 || args[1] != "''" {
		return "", "", false
	}
	return operandKindName(args[0]), args[0], true
}

// operandKindName is operandKind without the column bound — the NULLIF
// helpers carry their own column reference, so classify by shape alone.
func operandKindName(op string) string {
	op = strings.TrimSpace(op)
	if placeholderRe.MatchString(op) {
		return "incoming"
	}
	if strings.HasPrefix(op, "EXCLUDED.") {
		return "incoming"
	}
	if identRe.MatchString(op) {
		return "stored"
	}
	if q, rest, ok := strings.Cut(op, "."); ok && identRe.MatchString(q) && identRe.MatchString(rest) {
		return "stored"
	}
	return "other"
}

// coalescePair matches `COALESCE(a, b)` and returns the two operands'
// kinds for col.
func coalescePair(expr, col string) (string, [2]string, bool) {
	name, args, isCall := parseCall(strings.TrimSpace(expr))
	if !isCall || name != "COALESCE" || len(args) != 2 {
		return "", [2]string{}, false
	}
	return name, [2]string{operandKind(args[0], col), operandKind(args[1], col)}, true
}

// containsStoredRef reports whether expr references the STORED value
// of col anywhere — a bare or qualified (non-EXCLUDED) occurrence.
func containsStoredRef(expr, col string) bool {
	re := regexp.MustCompile(`(?:\b([A-Za-z_][A-Za-z0-9_]*)\.)?\b` + regexp.QuoteMeta(col) + `\b`)
	for _, m := range re.FindAllStringSubmatch(expr, -1) {
		if !strings.EqualFold(m[1], "EXCLUDED") {
			return true
		}
	}
	return false
}
