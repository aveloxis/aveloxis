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
//   - dollar quotes: splitStatements tracks only anonymous $$ blocks (the
//     corpus's only kind, migrate.go's DO blocks), not $tag$...$tag$;
//     extractExpr tracks no dollar quote ('…' only); the multiple-column SET
//     reader (setListMember) tracks both through quotedEnd, and reads a
//     dollar-quoted body right after DO or AS as SQL — not a body after
//     DO LANGUAGE x, a single-quoted DO or AS body, or dynamic SQL in an
//     EXECUTE (none in the corpus; round 10 of 0.29.123);
//   - the SET-LHS anchor inherits the flagship's documented assumption
//     that WHERE predicates join with AND (never commas), so `, col =`
//     cannot false-match a predicate;
//   - a MERGE INTO statement is found by FindWrites (v0.29.94), but
//     WritesColumn sees only its WHEN MATCHED THEN UPDATE SET arm; a
//     WHEN NOT MATCHED THEN INSERT (col) list is a write it cannot see.
//     The corpus has no MERGE today; the first one extends the matcher
//     in the same change, with its table's policy test as the proof.
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

// FindWrites returns the statements that INSERT INTO, UPDATE (also
// UPDATE ONLY) or MERGE INTO the given schema-qualified table (e.g.
// "aveloxis_data.repos" — the qualification is required; unlike the old
// internal/db helper this engine serves aveloxis_ops tables too). The
// ONLY and MERGE spellings were added in v0.29.94 (L10 round 2 F2): a
// writer spelled that way escaped every write-policy pin.
func FindWrites(stmts []Stmt, table string) []Stmt {
	re := regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE(?:\s+ONLY)?|MERGE\s+INTO)\s+` + regexp.QuoteMeta(table) + `\b`)
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
//     `, col =` — including ON CONFLICT ... DO UPDATE SET), or
//   - a member of a multiple-column assignment's list (`SET (a, col) =`;
//     0.29.123, read by setListMember).
//
// Appearances in WHERE predicates, RETURNING clauses, RHS expressions
// (COALESCE(col, ...), subqueries), or qualified references (v.col,
// c.col) deliberately do NOT count (the v0.27.116 round-10 contract).
// Column lists never contain parentheses, so the [^)]* capture is
// safe; SQL WHERE clauses join with AND (never commas), so the
// `, col =` LHS shape cannot false-match a predicate. The list form is
// read from the SET clause itself (setListMember), so a row expression
// elsewhere — a WHERE, a SELECT list, a function argument — never counts.
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
	if setLHSRe(col).MatchString(s.SQL) {
		return true
	}
	_, ok := setListMember(s.SQL, col)
	return ok
}

func setLHSRe(col string) *regexp.Regexp {
	return regexp.MustCompile(`(?is)(?:\bSET\s+|,\s*)` + regexp.QuoteMeta(col) + `\s*=`)
}

// setListMember reports the first member of a multiple-column assignment
// target — `SET (a, col) = (...)`, in an UPDATE, a data-modifying CTE or an
// ON CONFLICT DO UPDATE — that is col, and its text (RecordCommitBounds'
// form since round 2 of 0.29.123, which computes each value once). It READS
// the SET clause rather than matching a pattern: quoted text is skipped
// (quotedEnd: a SET or a comma inside a literal, an E'…' or dollar-quoted
// string or a quoted name is no syntax — but a dollar-quoted body right
// after DO or AS is read),
// parentheses and brackets are counted (an ARRAY[…] or a subscript may hold
// a comma), assignments and members split at their own depth's commas, and
// the clause ends at its depth's WHERE / FROM / RETURNING (not IS [NOT]
// DISTINCT FROM), a closing parenthesis or bracket below it, a ";" or the
// end. It
// does not tie a SET clause to the table its UPDATE names: a statement
// whose CTE updates another table with a list counts for both (no such
// statement exists; FindWrites already scopes by the table named). A
// member is the grammar's set_target, ColId then any indirection; its column
// is the ColId — a bare name folded to lower case in ASCII only, as
// PostgreSQL folds it, or a quoted name exactly — so `col[1]`, `col.f` and
// `"col"` are col, while `"COL"` and a table-qualified `s.col` (the grammar
// has no qualifier there: s is the column) are not. Rounds 4–7 had chased a
// regex spelling by spelling; WritesColumn and Check both read this.
// SetExprs does not read the form (its right side is one row, not a value
// per column), so Check fails closed on it.
func setListMember(sql, col string) (string, bool) {
	for _, target := range setTargets(sql) {
		t := strings.TrimSpace(target)
		if !strings.HasPrefix(t, "(") || !strings.HasSuffix(t, ")") {
			continue
		}
		for _, m := range splitDepth0(t[1 : len(t)-1]) {
			if name, quoted := memberColumn(m); (quoted && name == col) || (!quoted && asciiLower(name) == col) {
				return strings.TrimSpace(m), true
			}
		}
	}
	return "", false
}

// setTargets returns the text left of each assignment's "=" in every SET
// clause of sql, at any depth (a data-modifying CTE's SET is one), quoted
// text skipped — except a dollar-quoted body right after DO or AS, which is
// executed SQL (a DO block, a function body) and is read in turn (round 9
// of 0.29.123: skipped, it hid a list from Check) (setListMember).
func setTargets(sql string) []string {
	var out []string
	for i := 0; i < len(sql); i++ {
		if end, ok := quotedEnd(sql, i); ok {
			if w, _ := wordBefore(sql, i); sql[i] == '$' && (strings.EqualFold(w, "DO") || strings.EqualFold(w, "AS")) {
				tag := sql[i : strings.IndexByte(sql[i+1:], '$')+i+2]
				body := sql[i+len(tag) : end]
				body = strings.TrimSuffix(body, tag)
				out = append(out, setTargets(body)...)
			}
			i = end - 1
			continue
		}
		if isWordAt(sql, i, "SET") {
			out = append(out, assignmentTargets(sql, i+len("SET"))...)
		}
	}
	return out
}

// assignmentTargets reads one SET clause from start: each target up to its
// "=" at the clause's depth, each expression up to the next comma at that
// depth, and the clause's end at that depth's WHERE / FROM / RETURNING (not
// the FROM of IS [NOT] DISTINCT FROM), a closing parenthesis or bracket
// below it, a ";" or the end. Depth counts parentheses and brackets (an ARRAY[$1, $2]
// holds a comma; round 8). The RETURNING and FROM ends decide answers on
// valid SQL (a row in a RETURNING list, a FROM list's subquery), pinned by
// fixtures; WHERE is defensive once brackets count.
func assignmentTargets(sql string, start int) []string {
	var out []string
	depth, from, inTarget := 0, start, true
	for i := start; i < len(sql); i++ {
		if end, ok := quotedEnd(sql, i); ok {
			i = end - 1
			continue
		}
		c := sql[i]
		switch {
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			if depth == 0 {
				return out
			}
			depth--
		case depth > 0:
		case c == ';':
			return out
		case inTarget && c == '=':
			out = append(out, sql[from:i])
			inTarget = false
		case !inTarget && c == ',':
			from, inTarget = i+1, true
		case !inTarget && (isWordAt(sql, i, "WHERE") || isWordAt(sql, i, "RETURNING")):
			return out
		case !inTarget && isWordAt(sql, i, "FROM") && !isDistinctFrom(sql, i):
			return out
		}
	}
	return out
}

// splitDepth0 splits a SET list's inside at its commas outside quotes and
// subscripts. A set_target holds a parenthesis only inside a [subscript]
// (array_length(tags, 1) in tags[…]), so brackets are the depth to count
// (a mutant that also counted parentheses was equivalent; round 7).
func splitDepth0(s string) []string {
	var out []string
	depth, from := 0, 0
	for i := 0; i < len(s); i++ {
		if end, ok := quotedEnd(s, i); ok {
			i = end - 1
			continue
		}
		switch s[i] {
		case '[':
			depth++
		case ']':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[from:i])
				from = i + 1
			}
		}
	}
	return append(out, s[from:])
}

// memberColumn is a set_target's ColId: a quoted name (its "" unescaped,
// quoted true) or a bare identifier.
func memberColumn(m string) (name string, quoted bool) {
	m = strings.TrimSpace(m)
	if strings.HasPrefix(m, `"`) {
		end, _ := quotedEnd(m, 0)
		if end < 2 || m[end-1] != '"' {
			return "", true // never closes: no name
		}
		return strings.ReplaceAll(m[1:end-1], `""`, `"`), true
	}
	n := 0
	for n < len(m) && isIdentByte(m[n]) {
		n++
	}
	return m[:n], false
}

// quotedEnd reports whether quoted text opens at s[i] and, if so, the index
// just past it (len(s) when it never closes): a " identifier or a '
// literal, a doubled quote inside escaped; an E'…' literal, whose
// backslash also escapes the next byte; and a dollar quote, $$…$$ or
// $tag$…$tag$ — a $ followed by a digit is a parameter, and a $ inside an
// identifier is part of it (round 8 of 0.29.123: without the last two, one
// stray quote put every later byte out of step).
func quotedEnd(s string, i int) (int, bool) {
	switch c := s[i]; {
	case c == '"' || c == '\'':
		escapes := c == '\'' && i > 0 && (s[i-1] == 'E' || s[i-1] == 'e') && (i == 1 || !isIdentByte(s[i-2]))
		for j := i + 1; j < len(s); j++ {
			switch {
			case escapes && s[j] == '\\':
				j++
			case s[j] == c && j+1 < len(s) && s[j+1] == c:
				j++
			case s[j] == c:
				return j + 1, true
			}
		}
		return len(s), true
	case c == '$' && (i == 0 || !isIdentByte(s[i-1])):
		j := i + 1
		if j < len(s) && s[j] != '$' && !isTagStart(s[j]) {
			return i, false // $1: a parameter
		}
		for j < len(s) && s[j] != '$' && isIdentByte(s[j]) {
			j++
		}
		if j >= len(s) || s[j] != '$' {
			return i, false
		}
		tag := s[i : j+1]
		if k := strings.Index(s[j+1:], tag); k >= 0 {
			return j + 1 + k + len(tag), true
		}
		return len(s), true
	}
	return i, false
}

// isTagStart: a dollar-quote tag starts as an identifier does, never with a
// digit.
func isTagStart(c byte) bool {
	return c == '_' || c >= 0x80 || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// wordBefore is the identifier that ends just before s[i], spaces skipped,
// and where it starts.
func wordBefore(s string, i int) (string, int) {
	j := i
	for j > 0 && isSpace(s[j-1]) {
		j--
	}
	k := j
	for k > 0 && isIdentByte(s[k-1]) {
		k--
	}
	return s[k:j], k
}

// isDistinctFrom reports whether the FROM at s[i] is the operator's: IS
// DISTINCT FROM or IS NOT DISTINCT FROM — not a column named distinct
// (s.distinct FROM …, s . distinct FROM …: the word before DISTINCT is then
// the qualifier, never IS or NOT, which cannot name a table; rounds 9–10 of
// 0.29.123).
func isDistinctFrom(s string, i int) bool {
	w, k := wordBefore(s, i)
	if !strings.EqualFold(w, "DISTINCT") {
		return false
	}
	prev, _ := wordBefore(s, k)
	return strings.EqualFold(prev, "IS") || strings.EqualFold(prev, "NOT")
}

// isWordAt reports whether the keyword w (upper case) stands at s[i] as a
// whole word, in any case.
func isWordAt(s string, i int, w string) bool {
	if i+len(w) > len(s) || !strings.EqualFold(s[i:i+len(w)], w) {
		return false
	}
	if i > 0 && isIdentByte(s[i-1]) {
		return false
	}
	return i+len(w) == len(s) || !isIdentByte(s[i+len(w)])
}

// isIdentByte: a byte of an unquoted identifier (ASCII letters, digits, _
// and $, and any byte of a non-ASCII letter, which PostgreSQL accepts).
func isIdentByte(c byte) bool {
	return c == '_' || c == '$' || c >= 0x80 || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9')
}

// asciiLower folds ASCII letters only, as PostgreSQL folds an unquoted name
// (Go's case folding also maps the Kelvin sign to k; round 7).
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
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
// (SetPlatformRepoIDIfEmptySeen, internal/db/repo_forge_id.go):
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
