// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The cross-package shutdown-classification RATCHET — the P0 item from
// summary/26, and the enforcement the hand sweeps of passes 35-37 were
// standing in for.
//
// THE RULE: a failure log whose error came from a ctx-bound call must
// classify context.Canceled before it logs. A cancelled context is a
// `stop serve`, not a defect: logging it as one floods the operator log
// (v0.27.91: 4,011,288 lines from a single canceled job), records false
// failures, and — where the counter feeds a success predicate — reports
// a clean shutdown as a failed pass.
//
// WHY THIS EXISTS: internal/scheduler's structural analyzer enforces
// the same rule, but only inside its own package; its doc comment says
// so ("Delegates in OTHER packages … are outside this pin"). Copilot
// rounds 8-9 then found six sites sitting exactly one hop past that
// boundary, all missed by the hand sweep. A point-in-time sweep is not
// enforcement.
//
// WHY A RATCHET AND NOT AN EXEMPTION LIST: the first cut of this check
// flagged 94 sites across the two packages, most of them legitimate —
// migration steps run under a one-shot CLI where cancellation is fatal
// anyway. A 94-entry exemption list is standing permission, which is
// the shape Copilot round 7 criticised in the pass-24 ticker pin. Two
// changes make the rule precise instead:
//
//   - The producer rule: only errors whose PRODUCING statement mentions
//     ctx count. A parse error is not a shutdown.
//   - The migration exclusion: in internal/db, everything reachable
//     from RunMigrations is excluded by call-graph fixpoint, not by a
//     file list. That alone takes internal/db from 53 sites to 8.
//
// The residual tail was frozen in shutdown_classification_baseline.txt
// and burned down to ZERO in the same release: the baseline is now
// empty and every ctx-bound failure log in both packages classifies.
// The file stays as the mechanism — SHRINK-ONLY, so a new site is a
// build failure and a fixed site must leave the baseline in the same
// change. Regenerate after a burn-down wave:
// AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run ShutdownClassification
//
// The guard below counts sites EXAMINED, never violations found — an
// empty baseline is the GOAL state, and guarding on the violation
// count would make the completed burn-down fail forever (v0.27.122
// hit exactly that in the srctest ratchet).
//
// Deliberately NOT go/types: the two rules above buy the precision that
// mattered without it. Type information does not need golang.org/x/tools
// (this comment once said it did): the standard library's go/types with
// importer.ForCompiler(fset, "source", nil) type-checks the test packages —
// see test_close_ordering_test.go's sharedTypeChecker. If the residual ever
// needs interface-method resolution, that is the way in.
func TestShutdownClassificationRatchet(t *testing.T) {
	violations := scanShutdownClassification(t)

	const baselineFile = "shutdown_classification_baseline.txt"
	if os.Getenv("AVELOXIS_UPDATE_BASELINE") == "1" {
		var b strings.Builder
		b.WriteString("# Shutdown-classification baseline — ctx-bound failure logs that do\n")
		b.WriteString("# not yet classify context.Canceled, frozen at ratchet introduction.\n")
		b.WriteString("# SHRINK-ONLY: fixing a site means deleting its line here in the same\n")
		b.WriteString("# change; a new site is a build failure.\n")
		b.WriteString("# Regenerate: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run ShutdownClassification\n")
		for _, v := range violations {
			b.WriteString(v)
			b.WriteByte('\n')
		}
		if err := os.WriteFile(baselineFile, []byte(b.String()), 0o644); err != nil {
			t.Fatalf("write baseline: %v", err)
		}
		t.Logf("baseline regenerated with %d entries", len(violations))
		return
	}

	raw, err := os.ReadFile(baselineFile)
	if err != nil {
		t.Fatalf("baseline missing (%v) — seed it: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run ShutdownClassification", err)
	}
	baseline := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		baseline[line] = true
	}

	current := map[string]bool{}
	for _, v := range violations {
		current[v] = true
	}

	for _, v := range violations {
		if !baseline[v] {
			t.Errorf("NEW unclassified ctx-bound failure log:\n  %s\n"+
				"A cancelled context is a `stop serve`, not a failure. Classify it "+
				"(`if errors.Is(err, context.Canceled) { return … }`) between the call and the log. "+
				"If this site genuinely cannot be cancelled, say why at the site — do not add it to the baseline.", v)
		}
	}
	fixed := 0
	for entry := range baseline {
		if !current[entry] {
			fixed++
			t.Errorf("baseline entry no longer violates — delete it (the ratchet only shrinks):\n  %s", entry)
		}
	}
	if fixed > 0 {
		t.Logf("%d baselined site(s) were fixed; run AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run ShutdownClassification", fixed)
	}
}

type shutdownFunc struct {
	name, file, body string
}

// scanShutdownClassification returns the sorted violation keys.
func scanShutdownClassification(t *testing.T) []string {
	t.Helper()
	var out []string

	// internal/db: everything reachable from RunMigrations is migration
	// code, excluded by fixpoint rather than by a file list.
	dbFns := parseFuncs(t, "../internal/db")
	migration := reachableFrom(dbFns, "RunMigrations")
	if len(migration) < 30 {
		t.Fatalf("the RunMigrations reachability walk resolved only %d functions — it is not "+
			"resolving calls any more, so every migration site would be reported as a violation. "+
			"Re-anchor the walk before trusting this ratchet.", len(migration))
	}
	dbViolations, dbExamined := auditFuncs(dbFns, migration)
	out = append(out, dbViolations...)

	// internal/collector is entirely collection path: no exclusions.
	colViolations, colExamined := auditFuncs(parseFuncs(t, "../internal/collector"), nil)
	out = append(out, colViolations...)

	// Guard on sites EXAMINED, never on violations found: an empty
	// baseline is the GOAL state, and guarding on the violation count
	// would make a completed burn-down fail forever (the v0.27.122
	// lesson, repeated here on the first day the burn-down finished).
	if dbExamined+colExamined < 50 {
		t.Fatalf("the scan examined only %d ctx-bound failure logs (db %d, collector %d) — the "+
			"corpus or the regexes broke; a silently-empty analyzer passes forever",
			dbExamined+colExamined, dbExamined, colExamined)
	}
	sort.Strings(out)
	return out
}

func parseFuncs(t *testing.T, dir string) []shutdownFunc {
	t.Helper()
	var fns []shutdownFunc
	fset := token.NewFileSet()
	err := filepath.Walk(dir, func(p string, i os.FileInfo, e error) error {
		if e != nil || i.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", p, perr)
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			lo := fset.Position(fd.Body.Pos()).Offset
			hi := fset.Position(fd.Body.End()).Offset
			if lo < 0 || hi > len(src) || lo >= hi {
				continue
			}
			// Comment-stripped (worklist §4): the audit read raw bodies, so a
			// `// return` comment in place of the real one passed.
			fns = append(fns, shutdownFunc{name: fd.Name.Name, file: filepath.Base(p), body: srctest.StripGoComments(string(src[lo:hi]))})
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	if len(fns) < 50 {
		t.Fatalf("%s yielded only %d functions — the corpus broke", dir, len(fns))
	}
	return fns
}

var shutdownCallRe = regexp.MustCompile(`\b(\w+)\(`)

// reachableFrom is a call-name fixpoint from seed. Bare-name resolution
// is deliberately OVER-inclusive: for an EXCLUSION set, reaching too
// much only costs coverage, never a false accusation.
func reachableFrom(fns []shutdownFunc, seed string) map[string]bool {
	byName := map[string]shutdownFunc{}
	for _, f := range fns {
		byName[f.name] = f
	}
	reach := map[string]bool{}
	queue := []string{seed}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if reach[cur] {
			continue
		}
		reach[cur] = true
		f, ok := byName[cur]
		if !ok {
			continue
		}
		for _, m := range shutdownCallRe.FindAllStringSubmatch(f.body, -1) {
			if _, known := byName[m[1]]; known && !reach[m[1]] {
				queue = append(queue, m[1])
			}
		}
	}
	return reach
}

// shutdownLogHeadRe finds the start of a WARN/ERROR call with a literal
// message; findShutdownLogs reads its arguments from there.
var shutdownLogHeadRe = regexp.MustCompile(`\.(?:Warn|Error)\(\s*"([^"]*)"`)

// shutdownErrorAttrRe is the `"error", <var>` pair at the start of the text
// after a top-level string literal's opening quote.
var shutdownErrorAttrRe = regexp.MustCompile(`^"error",\s*(\w+)`)

// findShutdownLogs finds every WARN/ERROR carrying a top-level "error"
// attribute, as [start, end, msgStart, msgEnd, varStart, varEnd] offsets
// (the regexp submatch layout auditFuncs reads). The arguments are read by
// a parenthesis-depth scan that skips string, raw-string and rune literals,
// so any nesting before the attribute is crossed: the old `[^)]*` stopped
// at the first `)` (worklist §4; three real shutdown-as-failure sites hid
// that way), and its one-level successor still never examined a log with
// `len(x.Items())` or `truncateForLog([]byte(s), n)` ahead of the error
// (PR #218 review D7). An "error" key inside a nested call (slog.Any) is
// not the log's attribute, as before.
func findShutdownLogs(body string) [][]int {
	var out [][]int
	for _, h := range shutdownLogHeadRe.FindAllStringSubmatchIndex(body, -1) {
		depth := 0
	scan:
		for i := h[1]; i < len(body); i++ {
			switch c := body[i]; c {
			case '(', '[', '{':
				depth++
			case ')', ']', '}':
				if depth == 0 {
					break scan // the call's closing paren: no error attribute
				}
				depth--
			case '"', '`', '\'':
				if c == '"' && depth == 0 {
					if m := shutdownErrorAttrRe.FindStringSubmatchIndex(body[i:]); m != nil {
						out = append(out, []int{h[0], i + m[1], h[2], h[3], i + m[2], i + m[3]})
						break scan
					}
				}
				i = skipLiteral(body, i)
			}
		}
	}
	return out
}

// skipLiteral returns the offset of the closing quote of the string, raw
// string or rune literal opening at i (the end of body when unterminated).
func skipLiteral(body string, i int) int {
	q := body[i]
	for j := i + 1; j < len(body); j++ {
		switch body[j] {
		case '\\':
			if q != '`' {
				j++
			}
		case q:
			return j
		}
	}
	return len(body)
}

func auditFuncs(fns []shutdownFunc, exclude map[string]bool) (violations []string, examined int) {
	var out []string
	for _, f := range fns {
		if exclude[f.name] {
			continue
		}
		for _, loc := range findShutdownLogs(f.body) {
			msg := f.body[loc[2]:loc[3]]
			errVar := f.body[loc[4]:loc[5]]
			prodAt, ok := producerOffset(f.body, loc[0], errVar)
			if !ok {
				continue // not ctx-bound: a parse error is not a shutdown
			}
			examined++
			// The classification must sit between the producing call and
			// the log (a guard elsewhere in the function does not count —
			// L14: a loop-top guard cannot see the in-flight call that
			// observed the cancellation) AND it must TERMINATE, or the
			// log is still reached. Checking only that the token exists
			// is the decorative-gate class: keeping
			// `if errors.Is(err, context.Canceled) {` and replacing its
			// `return` with a bare `continue` (or a Debug log) escapes a
			// presence check while restoring the exact defect.
			if logUnreachableOnCancel(f.body, prodAt, loc[0], errVar) {
				continue
			}
			out = append(out, fmt.Sprintf("%s::%s::%s", f.file, f.name, msg))
		}
	}
	return out, examined
}

// logUnreachableOnCancel reports whether a cancellation classification
// between the producing call and the log makes the log UNREACHABLE when
// the error is context.Canceled. That is the property; "the token
// appears somewhere in between" is not.
//
// Checking only for the token is the decorative-gate class: keeping
// `if errors.Is(err, context.Canceled) {` and replacing its `return`
// with a bare `continue` escapes a presence check while restoring the
// exact defect — in a child-write loop the item is skipped, the
// enclosing row completes, and it is stamped processed with that child
// missing.
//
// Four shapes satisfy the property, all of them in the tree:
//
//	if errors.Is(err, ctx.Canceled) { … return … }; log   // terminates
//	if errors.Is(err, ctx.Canceled) { abort = true; continue }; log
//	if errors.Is(err, ctx.Canceled) { … } else { log }    // guarded else
//	if !errors.Is(err, ctx.Canceled) { log }              // negated guard
//
// The second is "record the abort, then drain" (breadth.go sets
// aborted/abortErr and cancels the remaining fetches), so a
// continue/break counts only when the block also ASSIGNS.
func logUnreachableOnCancel(body string, prodAt, logAt int, errVar string) bool {
	if guardedByAnotherErrorClass(body, prodAt, logAt, errVar) {
		return true
	}
	for _, at := range classifyRe.FindAllStringIndex(body[prodAt:logAt], -1) {
		abs := prodAt + at[0]
		negated := abs > 0 && strings.TrimSpace(body[max(0, abs-1):abs]) == "!"
		blockStart, blockEnd, ok := enclosingBlock(body, prodAt+at[1])
		if !ok {
			continue
		}
		if negated {
			// The log sits inside the not-canceled block.
			if logAt > blockStart && logAt < blockEnd {
				return true
			}
			continue
		}
		block := body[blockStart : blockEnd+1]
		if strings.Contains(block, "return") {
			return true
		}
		if (strings.Contains(block, "continue") || strings.Contains(block, "break")) &&
			assignRe.MatchString(block) {
			return true
		}
		// `} else {` immediately after: the log is in the else arm.
		rest := body[blockEnd+1:]
		if trimmed := strings.TrimLeft(rest, " \t"); strings.HasPrefix(trimmed, "else") {
			elseStart, elseEnd, ok := enclosingBlock(body, blockEnd+1)
			if ok && logAt > elseStart && logAt < elseEnd {
				return true
			}
		}
	}
	return false
}

// guardedByAnotherErrorClass reports a log that a cancellation cannot reach
// because it sits inside an arm keyed on ANOTHER error class (worklist §4,
// v0.29.68: the wider shutdownLogRe made three such logs visible): the
// enclosing `if errors.Is(<err>, <sentinel>) {` with a sentinel that is not
// context.Canceled — a cancellation is never that sentinel — or a preceding
// `if <err> == nil || !<isPredicate>(<err>) { return … }`, after which the
// log runs only when the predicate held (a predicate on the error names a
// failure class, which a cancellation is not). Either way the arm itself
// is the classification. Both tests bind to the LOG's error variable, and
// a NEGATED sentinel arm (`!errors.Is(err, X)`) is no guard — a cancelled
// error is not X, so the negation holds and the log is reached (batch 7b
// review round 1: the first cut accepted a negated arm, a sentinel tested
// on another variable and a predicate on another variable; fixtures in
// TestShutdownExemptionShapes).
//
// Round 2 of the same review: the match must sit in an `if` HEADER (a
// `case` clause is not one), and the header's boolean structure decides —
// a sentinel arm widened by a top-level `||` (`errors.Is(err, X) || err !=
// nil`) is reached by a cancellation, and an early return narrowed by a
// top-level `&&` (`!isX(err) && retries > 3`) may not return at all.
//
// PR #218 review D6: the early return counts only as a SIBLING statement
// preceding the log in the log's own block (nested in another if or a loop
// body it runs on some paths only), and only when the predicate names
// another class — a name containing cancel, shutdown or context (any case)
// lets exactly the cancellation through to the log.
//
// PR #218 fix review r1 F3: two more some-paths-only shapes are refused. An
// `} else if …` header sits in the log's block but runs only when the prior
// condition was false; and a guard inside a switch's case clause shares the
// switch's brace with every other clause, so it counts only when the log is
// in the SAME clause. DECIDED (the safe direction): an early return that
// dominates a log nested one block deeper (`if err == nil || !isX(err) {
// return }` then `if verbose { log }`) does guard it, but is refused — the
// sibling test cannot tell that shape from the nested-guard escapes above,
// and a later round must not widen it.
func guardedByAnotherErrorClass(body string, prodAt, logAt int, errVar string) bool {
	span := body[prodAt:logAt]
	for _, m := range sentinelArmRe.FindAllStringSubmatchIndex(span, -1) {
		negated, v, sentinel := span[m[2]:m[3]] != "", span[m[4]:m[5]], span[m[6]:m[7]]
		if negated || v != errVar || sentinel == "context.Canceled" {
			continue
		}
		header, brace, ok := ifHeader(body, prodAt+m[0])
		if !ok || !isConjunct(header, "errors.Is("+v+", "+sentinel+")") {
			continue
		}
		start, end, ok := enclosingBlock(body, brace)
		if ok && logAt > start && logAt < end {
			return true
		}
	}
	for _, m := range negatedPredicateRe.FindAllStringSubmatchIndex(span, -1) {
		if span[m[4]:m[5]] != errVar || namesCancellation(span[m[2]:m[3]]) {
			continue
		}
		header, brace, ok := ifHeader(body, prodAt+m[0])
		if !ok || hasTopLevelOp(header, "&&") {
			continue
		}
		// The early return guards the log only as a SIBLING statement in the
		// log's own block (PR #218 review D6): nested in another if or a
		// loop body, it runs only on some paths and the log is reached on
		// the others.
		if strings.HasPrefix(strings.TrimSpace(header), "} else if") {
			continue // runs only when the prior condition was false (F3)
		}
		ifAt := brace - len(header) + strings.Index(header, "if")
		open := innermostOpenBrace(body, ifAt)
		if open != innermostOpenBrace(body, logAt) {
			continue
		}
		if caseClauseAt(body, open, ifAt) != caseClauseAt(body, open, logAt) {
			continue // another case clause of the same switch (F3)
		}
		start, end, ok := enclosingBlock(body, brace)
		if ok && end < logAt && strings.Contains(body[start:end+1], "return") {
			return true
		}
	}
	return false
}

// caseClauseAt returns the offset of the last `case …:` or `default:`
// label at the top level of the block opened at open that precedes at, or
// -1 when there is none (the block is not a switch or select body, or at
// precedes its first clause). Two offsets in one block share a clause
// exactly when the results are equal (PR #218 fix review r1 F3).
func caseClauseAt(body string, open, at int) int {
	label := -1
	depth := 0
	lineStart := open + 1
	for i := open + 1; i < at; i++ {
		switch body[i] {
		case '{':
			depth++
		case '}':
			depth--
		case '\n':
			lineStart = i + 1
			continue
		}
		if i == lineStart || strings.TrimSpace(body[lineStart:i]) == "" {
			if depth == 0 {
				rest := body[i:]
				if strings.HasPrefix(rest, "case ") || strings.HasPrefix(rest, "default:") {
					label = i
				}
			}
		}
	}
	return label
}

// namesCancellation reports whether a predicate's name (package qualifier
// included) is about cancellation — cancel, shutdown or context, in any
// case. `if err == nil || !isCanceled(err) { return }` lets exactly the
// cancellation through to the log, so such a predicate is no other error
// class (PR #218 review D6).
func namesCancellation(name string) bool {
	lower := strings.ToLower(name)
	for _, w := range []string{"cancel", "shutdown", "context"} {
		if strings.Contains(lower, w) {
			return true
		}
	}
	return false
}

// innermostOpenBrace returns the offset of the `{` of the innermost block
// enclosing offset at, or -1 at the top level of body. Like enclosingBlock
// it counts braces in comment-stripped source and does not skip string
// literals; a brace inside a string can only misplace a guard, and a
// misplaced guard is refused (the safe direction).
func innermostOpenBrace(body string, at int) int {
	depth := 0
	for i := at - 1; i >= 0; i-- {
		switch body[i] {
		case '}':
			depth++
		case '{':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

// ifHeader returns the `if` header that contains offset at — from the `if`
// (or `} else if`) that opens its line to the `{` that closes it — and the
// offset of that brace. A match on a line that does not start an if
// statement (a `case` clause, a plain expression) is not a header.
func ifHeader(body string, at int) (header string, brace int, ok bool) {
	lineStart := strings.LastIndex(body[:at], "\n") + 1
	// at may sit on the whitespace before the condition (the sentinel regex
	// admits leading spaces), so the lead is compared trimmed.
	lead := strings.TrimSpace(body[lineStart:at])
	if lead != "if" && !strings.HasPrefix(lead, "if ") && lead != "} else if" && !strings.HasPrefix(lead, "} else if ") {
		return "", 0, false
	}
	depth := 0
	for i := at; i < len(body); i++ {
		switch body[i] {
		case '(':
			depth++
		case ')':
			depth--
		case '{':
			if depth == 0 {
				return body[lineStart:i], i, true
			}
		case '\n':
			return "", 0, false
		}
	}
	return "", 0, false
}

// isConjunct reports whether want is, byte for byte, one of the top-level
// `&&`-joined conjuncts of the if header's condition (the text after the
// last top-level `;`, before the brace). Exactness is the guard (batch 7b
// review round 3): `errors.Is(err, X) == false`, `!= true`, `x := errors.Is(
// err, X); !x` and `!(errors.Is(err, X))` are all reached by a cancellation,
// and none is the bare call as a conjunct; a top-level `||` makes the whole
// condition one disjunction, in which the call is not a conjunct either.
// DECIDED (round 4): the shapes this refuses although they DO guard —
// `if (errors.Is(err, X)) {`, a no-space `errors.Is(err,X)` (gofmt forbids
// it), a multi-line header, a `;` inside a string outside parentheses — are
// reported as violations, the safe direction; none is in the corpus, and a
// later round must not widen the exemption to admit them.
func isConjunct(header, want string) bool {
	cond := header
	if i := strings.Index(cond, "if "); i >= 0 {
		cond = cond[i+3:]
	}
	if i := lastTopLevel(cond, ";"); i >= 0 {
		cond = cond[i+1:]
	}
	if hasTopLevelOp(cond, "||") {
		return false
	}
	for _, part := range splitTopLevel(cond, "&&") {
		if strings.TrimSpace(part) == want {
			return true
		}
	}
	return false
}

// lastTopLevel returns the offset of the last occurrence of op outside every
// parenthesis, or -1.
func lastTopLevel(s, op string) int {
	depth, last := 0, -1
	for i := 0; i+len(op) <= len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(op)] == op {
			last = i
		}
	}
	return last
}

// splitTopLevel splits s on op outside every parenthesis.
func splitTopLevel(s, op string) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i+len(op) <= len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && s[i:i+len(op)] == op {
			parts = append(parts, s[start:i])
			start = i + len(op)
			i += len(op) - 1
		}
	}
	return append(parts, s[start:])
}

// hasTopLevelOp reports whether the boolean operator op appears in header
// outside every parenthesis.
func hasTopLevelOp(header, op string) bool {
	depth := 0
	for i := 0; i+len(op) <= len(header); i++ {
		switch header[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && header[i:i+len(op)] == op {
			return true
		}
	}
	return false
}

var (
	// `[!]errors.Is(<err>, <sentinel>)`: the negation, the error variable
	// and the sentinel are the submatches.
	sentinelArmRe = regexp.MustCompile(`(!?)\s*errors\.Is\((\w+),\s*([\w.]+)\)`)
	// `!isSomething(<err>)` or `!IsSomething(<err>)`: a negated predicate on the
	// error, whose enclosing block returns (the early-return shape). The
	// submatches are the predicate's name (qualifier included) and the error.
	negatedPredicateRe = regexp.MustCompile(`!\s*((?:\w+\.)?[iI]s\w*)\((\w+)\)`)

	// Two spellings make a log unreachable on cancel, and BOTH are in
	// the tree. `errors.Is(err, context.Canceled)` classifies the error;
	// `ctx.Err() != nil` asks the context directly — which a worker loop
	// prefers, because it catches the cancel however the error surfaced
	// (an exec'd child reports `signal: killed`, never context.Canceled
	// — the pass-35 lesson). Recognising only the first would have
	// reported ~60 already-correct sites as violations.
	classifyRe = regexp.MustCompile(`errors\.Is\([^)]*context\.Canceled\)|ctx\.Err\(\)\s*!=\s*nil`)
	assignRe   = regexp.MustCompile(`\w\s*(?::=|=|\+\+)[^=]`)
)

// enclosingBlock brace-matches the first `{` at or after from.
func enclosingBlock(body string, from int) (start, end int, ok bool) {
	open := strings.Index(body[from:], "{")
	if open < 0 {
		return 0, 0, false
	}
	start = from + open
	depth := 0
	for i := start; i < len(body); i++ {
		switch body[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return start, i, true
			}
		}
	}
	return 0, 0, false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// producerOffset finds the nearest assignment to v before the log and
// reports whether that statement mentions ctx.
func producerOffset(body string, upto int, v string) (int, bool) {
	assign := regexp.MustCompile(`\b` + regexp.QuoteMeta(v) + `\s*(?::=|=)[^=]`)
	locs := assign.FindAllStringIndex(body[:upto], -1)
	if len(locs) == 0 {
		return 0, false
	}
	last := locs[len(locs)-1]
	end := strings.Index(body[last[0]:], "\n")
	if end < 0 {
		end = len(body) - last[0]
	}
	if !strings.Contains(body[last[0]:last[0]+end], "ctx") {
		return 0, false
	}
	return last[0], true
}

// TestShutdownExemptionShapes drives guardedByAnotherErrorClass through the
// shapes batch 7b review round 1 found it accepting, and the two it is for
// (and, since PR #218 review D7, findShutdownLogs through a doubly nested
// argument).
// Each body has one failure log; the verdict is whether a cancellation can
// reach it.
func TestShutdownExemptionShapes(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		unreachable bool
	}{
		{"sentinel arm on the log's error", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		logger.Warn("no row", "error", err)
	}
`, true},
		{"negated sentinel arm: a cancelled error is not ErrNoRows, so the log is reached", `
	err := s.store.Ping(ctx)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel tested on another variable", `
	err := s.store.Ping(ctx)
	if errors.Is(other, pgx.ErrNoRows) {
		if err != nil {
			logger.Warn("ping failed", "error", err)
		}
	}
`, false},
		{"early return on a negated predicate of the log's error", `
	err := sendBatch(ctx, b)
	if err == nil || !isStalePreparedStatement(err) {
		return err
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, true},
		{"negated predicate on another variable", `
	err := sendBatch(ctx, b)
	if !isSPDXLicense(id) {
		return err
	}
	if err != nil {
		logger.Warn("send failed", "error", err)
	}
`, false},
		// Round 2: the header's structure, not the token.
		{"sentinel arm widened by || with the canceled sentinel", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, context.Canceled) {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel arm widened by || err != nil", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel arm widened by || another predicate", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) || isTransient(err) {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"a case clause is not an if header", `
	err := s.store.Ping(ctx)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	default:
		if err != nil {
			logger.Warn("ping failed", "error", err)
		}
	}
`, false},
		{"early return narrowed by &&", `
	err := sendBatch(ctx, b)
	if !isStalePreparedStatement(err) && retries > 3 {
		return err
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, false},
		{"a switch on the sentinel test is not an if header", `
	err := s.store.Ping(ctx)
	switch errors.Is(err, pgx.ErrNoRows) {
	case false:
		logger.Warn("ping failed", "error", err)
	}
`, false},
		// Round 3: negations of the call itself, and an init-clause alias.
		{"sentinel compared == false", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) == false {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel compared != true", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) != true {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel through an init-clause alias, negated", `
	err := s.store.Ping(ctx)
	if x := errors.Is(err, pgx.ErrNoRows); !x {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel negated in parentheses", `
	err := s.store.Ping(ctx)
	if !(errors.Is(err, pgx.ErrNoRows)) {
		logger.Warn("ping failed", "error", err)
	}
`, false},
		{"sentinel through an init-clause alias, plain", `
	err := s.store.Ping(ctx)
	if x := errors.Is(err, pgx.ErrNoRows); x {
		logger.Warn("no row", "error", err)
	}
`, false},
		{"parenthesised whole sentinel test (decided: refused, the safe direction)", `
	err := s.store.Ping(ctx)
	if (errors.Is(err, pgx.ErrNoRows)) {
		logger.Warn("no row", "error", err)
	}
`, false},
		{"sentinel arm narrowed by && is still a guard", `
	err := s.store.Ping(ctx)
	if errors.Is(err, pgx.ErrNoRows) && strict {
		logger.Warn("no row", "error", err)
	}
`, true},
		{"early return widened by || is still a return", `
	err := sendBatch(ctx, b)
	if err == nil || !isStalePreparedStatement(err) || tooMany(err) {
		return err
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, true},
		// PR #218 review D6: the early return must be a sibling of the log
		// in the same block, and the predicate must name another class.
		{"early return nested in another if does not guard the log", `
	err := sendBatch(ctx, b)
	if retrying {
		if err == nil || !isStalePreparedStatement(err) {
			return err
		}
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, false},
		{"early return inside a loop body does not guard a log after the loop", `
	err := sendBatch(ctx, b)
	for _, x := range xs {
		if err == nil || !isStalePreparedStatement(err) {
			return err
		}
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, false},
		{"sibling early return inside the same nested block still guards", `
	if retrying {
		err := sendBatch(ctx, b)
		if err == nil || !isStalePreparedStatement(err) {
			return err
		}
		logger.Warn("stale prepared statement — retrying", "error", err)
	}
`, true},
		{"a predicate naming cancellation is not another class", `
	err := sendBatch(ctx, b)
	if err == nil || !isCanceled(err) {
		return err
	}
	logger.Warn("cancelled", "error", err)
`, false},
		{"a predicate naming shutdown is not another class", `
	err := sendBatch(ctx, b)
	if err == nil || !IsShutdownErr(err) {
		return err
	}
	logger.Warn("shutting down", "error", err)
`, false},
		{"a predicate naming the context is not another class", `
	err := sendBatch(ctx, b)
	if err == nil || !ctxutil.IsContextErr(err) {
		return err
	}
	logger.Warn("context ended", "error", err)
`, false},
		// PR #218 fix review r1 F3: an else-if runs only when the prior
		// condition was false, and a case body shares the switch's brace
		// with every other case.
		{"early return in an else-if arm does not guard the log", `
	err := sendBatch(ctx, b)
	if retrying {
		wait()
	} else if err == nil || !isStalePreparedStatement(err) {
		return err
	}
	logger.Warn("stale prepared statement — retrying", "error", err)
`, false},
		{"early return in another case clause does not guard the log", `
	err := sendBatch(ctx, b)
	switch mode {
	case 1:
		if err == nil || !isStalePreparedStatement(err) {
			return err
		}
	case 2:
		logger.Warn("stale prepared statement — retrying", "error", err)
	}
`, false},
		{"early return in the log's own case clause still guards", `
	err := sendBatch(ctx, b)
	switch mode {
	case 1:
		wait()
	case 2:
		if err == nil || !isStalePreparedStatement(err) {
			return err
		}
		logger.Warn("stale prepared statement — retrying", "error", err)
	}
`, true},
		{"an outer early return dominating a log one block deeper (decided: refused, the safe direction)", `
	err := sendBatch(ctx, b)
	if err == nil || !isStalePreparedStatement(err) {
		return err
	}
	if verbose {
		logger.Warn("stale prepared statement — retrying", "error", err)
	}
`, false},
		// PR #218 review D7: a log whose arguments nest parentheses two deep
		// before the error attribute is still examined.
		{"doubly nested parentheses before the error attribute", `
	err := s.store.Ping(ctx)
	logger.Warn("ping failed", "items", len(x.Items()), "body", truncateForLog([]byte(s), n), "error", err)
`, false},
	}
	for _, tc := range cases {
		body := tc.body
		locs := findShutdownLogs(body)
		if len(locs) != 1 {
			t.Fatalf("%s: fixture has %d failure logs; want one", tc.name, len(locs))
		}
		loc := locs[0]
		errVar := body[loc[4]:loc[5]]
		prodAt, ok := producerOffset(body, loc[0], errVar)
		if !ok {
			t.Fatalf("%s: fixture's producer not found", tc.name)
		}
		if got := logUnreachableOnCancel(body, prodAt, loc[0], errVar); got != tc.unreachable {
			t.Errorf("%s: unreachable on cancel = %v; want %v", tc.name, got, tc.unreachable)
		}
	}
}
