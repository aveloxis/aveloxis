// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package sqlscan is the SQL write scanner underneath the house
// source-contract tests (regression-infrastructure Phase 2, v0.27.124;
// plan: the PR #184 review-round meta-pattern of writers with
// CONFLICTING policies silently clobbering each other's columns —
// round 11's platform_repo_id incident is the founding case).
//
// It promotes the machinery of internal/db's column-writer tripwire
// (v0.27.105–116) into a shared engine with three hardening fixes the
// design review demanded:
//
//   - STATEMENT SPLITTING at quote-aware, paren-depth-0 semicolons
//     (multi-statement literals in migrate.go previously counted as one
//     block, so a `SET col =` in an UNRELATED trailing statement could
//     satisfy "this table writes col");
//   - PER-FILE ATTRIBUTION (violations can name their file — the old
//     concatenated corpus could not);
//   - the Stmt.SetExprs / Stmt.WhereGuardsEmpty micro-parses that the
//     Phase 3 write-policy matchers stand on.
//
// TEST-ONLY, like its parent: production code must never import it —
// srctest's selfguard scan bans the `internal/srctest` import-path
// prefix from every non-test file, which covers this subpackage
// transitively.
//
// Known blind spots (documented, not hidden):
//   - SQL assembled by string concatenation is invisible
//     (srctest.BacktickLiterals only sees single backtick literals);
//   - tagged dollar-quotes ($tag$...$tag$) are not tracked — the corpus
//     uses only anonymous $$ blocks (migrate.go's DO blocks);
//   - the SET-LHS anchor inherits the flagship's documented assumption
//     that WHERE predicates join with AND (never commas), so `, col =`
//     cannot false-match a predicate.
package sqlscan

import (
	"regexp"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Stmt is one comment-stripped SQL statement attributed to the Go
// source file whose backtick literal carried it.
type Stmt struct {
	File string
	SQL  string
}

// Statements extracts every SQL statement from the backtick string
// literals of a Go source corpus (the map shape srctest.PackageFiles
// returns). Per file: pull backtick literals, strip SQL comments
// (literal-aware — the v0.27.89 `);`-inside-comment incident is a
// fixture), split at top-level semicolons, drop empty pieces. Files
// are processed in sorted-name order so output is deterministic.
func Statements(files map[string]string) []Stmt {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	var out []Stmt
	for _, name := range names {
		// v0.27.130 (Copilot round 18, suppressed — real): strip Go
		// comments BEFORE extracting backtick literals. A backticked SQL
		// statement inside a doc comment (the corpus really has one —
		// scancode_worker_store.go's operator-recovery UPDATE) would
		// otherwise count as a live writer and let a removed real write
		// pass the tripwires. StripGoComments preserves raw strings.
		for _, lit := range srctest.BacktickLiterals(srctest.StripGoComments(files[name])) {
			body := strings.Trim(lit, "`")
			clean := srctest.StripSQLComments(body)
			for _, piece := range splitStatements(clean) {
				piece = strings.TrimSpace(piece)
				if piece == "" {
					continue
				}
				out = append(out, Stmt{File: name, SQL: piece})
			}
		}
	}
	return out
}

// splitStatements splits SQL text at semicolons that sit at paren
// depth 0, outside single-quoted strings, and outside anonymous $$
// dollar-quoted blocks (migrate.go's DO $$ ... END $$ bodies contain
// semicolons that must not shred the block).
func splitStatements(sql string) []string {
	var out []string
	depth := 0
	inQuote := false
	inDollar := false
	start := 0
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case inQuote:
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // '' escape
				} else {
					inQuote = false
				}
			}
		case inDollar:
			if c == '$' && i+1 < len(sql) && sql[i+1] == '$' {
				inDollar = false
				i++
			}
		case c == '\'':
			inQuote = true
		case c == '$' && i+1 < len(sql) && sql[i+1] == '$':
			inDollar = true
			i++
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case c == ';' && depth == 0:
			out = append(out, sql[start:i])
			start = i + 1
		}
	}
	return append(out, sql[start:])
}

// FindWrites returns the statements that INSERT INTO or UPDATE the
// given schema-qualified table (e.g. "aveloxis_data.repos" — the
// qualification is required; unlike the old internal/db helper this
// engine serves aveloxis_ops tables too).
func FindWrites(stmts []Stmt, table string) []Stmt {
	re := regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE)\s+` + regexp.QuoteMeta(table) + `\b`)
	var out []Stmt
	for _, s := range stmts {
		if re.MatchString(s.SQL) {
			out = append(out, s)
		}
	}
	return out
}

// WritesColumn reports whether this statement writes the given column
// of the given schema-qualified table — i.e. the column appears in a
// WRITER POSITION:
//   - an INSERT INTO <table> (...) column list, or
//   - an UPDATE SET assignment's LEFT-hand side (`SET col =` or
//     `, col =` — including ON CONFLICT ... DO UPDATE SET).
//
// Appearances in WHERE predicates, RETURNING clauses, RHS expressions
// (COALESCE(col, ...), subqueries), or qualified references (v.col,
// c.col) deliberately do NOT count (the v0.27.116 round-10 contract).
// Column lists never contain parentheses, so the [^)]* capture is
// safe; SQL WHERE clauses join with AND (never commas), so the
// `, col =` LHS shape cannot false-match a predicate.
//
// Moved wholesale from internal/db's sqlStatementWritesColumn
// (v0.27.116) with its fixture table; the one contract change is that
// `table` is schema-qualified instead of assuming aveloxis_data.
func (s Stmt) WritesColumn(table, col string) bool {
	insertListRe := regexp.MustCompile(`(?is)INSERT\s+INTO\s+` + regexp.QuoteMeta(table) + `\s*\(([^)]*)\)`)
	wordRe := regexp.MustCompile(`\b` + regexp.QuoteMeta(col) + `\b`)
	for _, m := range insertListRe.FindAllStringSubmatch(s.SQL, -1) {
		if wordRe.MatchString(m[1]) {
			return true
		}
	}
	return setLHSRe(col).MatchString(s.SQL)
}

func setLHSRe(col string) *regexp.Regexp {
	return regexp.MustCompile(`(?is)(?:\bSET\s+|,\s*)` + regexp.QuoteMeta(col) + `\s*=`)
}

// SetExprs returns the right-hand-side expression of every UPDATE SET
// assignment to col in this statement (usually zero or one; ON
// CONFLICT DO UPDATE counts). The extraction is depth-counting and
// quote-aware: an expression ends at the first comma, closing paren,
// or clause keyword (WHERE / RETURNING / FROM) at depth 0. This is
// the micro-parse the Phase 3 policy matchers classify.
func (s Stmt) SetExprs(col string) []string {
	var out []string
	for _, loc := range setLHSRe(col).FindAllStringIndex(s.SQL, -1) {
		out = append(out, strings.TrimSpace(extractExpr(s.SQL[loc[1]:])))
	}
	return out
}

var exprEndKeywordRe = regexp.MustCompile(`(?i)^(WHERE|RETURNING|FROM)\b`)

func extractExpr(s string) string {
	depth := 0
	inQuote := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
				} else {
					inQuote = false
				}
			}
			continue
		}
		switch c {
		case '\'':
			inQuote = true
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return s[:i]
			}
			depth--
		case ',':
			if depth == 0 {
				return s[:i]
			}
		default:
			if depth == 0 && (i == 0 || isSpace(s[i-1])) && exprEndKeywordRe.MatchString(s[i:]) {
				return s[:i]
			}
		}
	}
	return s
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// WhereGuardsEmpty reports whether the statement's text, after a
// top-level WHERE, guards the column to its empty state — the shape
// that legitimizes a bare `SET col = $N` as fill-empty-only
// (SetPlatformRepoIDIfEmpty, internal/db/repo_forge_id.go):
//
//	WHERE ... COALESCE(col, '') = ''
//	WHERE ... col IS NULL
//
// An optional single qualifier (alias.col) is accepted. Documented
// blind spot: the search does not distinguish the outer statement's
// WHERE from a subquery's — acceptable for the house-written corpus,
// and a guard in a subquery still expresses the fill-empty intent for
// every shape the fixtures cover.
func (s Stmt) WhereGuardsEmpty(col string) bool {
	whereRe := regexp.MustCompile(`(?is)\bWHERE\b`)
	loc := whereRe.FindStringIndex(s.SQL)
	if loc == nil {
		return false
	}
	tail := s.SQL[loc[1]:]
	q := `(?:[a-z_]+\.)?` + regexp.QuoteMeta(col)
	coalesceRe := regexp.MustCompile(`(?is)\bCOALESCE\(\s*` + q + `\s*,\s*''\s*\)\s*=\s*''`)
	isNullRe := regexp.MustCompile(`(?is)\b` + q + `\s+IS\s+NULL\b`)
	return coalesceRe.MatchString(tail) || isNullRe.MatchString(tail)
}
