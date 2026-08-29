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
// Deliberately NOT go/types: full cross-package resolution would pull
// golang.org/x/tools in for a test-only analyzer. The two rules above
// buy the precision that mattered without it; if the residual ever
// needs interface-method resolution, that is the trigger to revisit.
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
			b.WriteString(v + "\n")
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
			fns = append(fns, shutdownFunc{name: fd.Name.Name, file: filepath.Base(p), body: string(src[lo:hi])})
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

var shutdownLogRe = regexp.MustCompile(`\.(?:Warn|Error)\(\s*"([^"]*)"[^)]*"error",\s*(\w+)`)

func auditFuncs(fns []shutdownFunc, exclude map[string]bool) (violations []string, examined int) {
	var out []string
	for _, f := range fns {
		if exclude[f.name] {
			continue
		}
		for _, loc := range shutdownLogRe.FindAllStringSubmatchIndex(f.body, -1) {
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
			if logUnreachableOnCancel(f.body, prodAt, loc[0]) {
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
func logUnreachableOnCancel(body string, prodAt, logAt int) bool {
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

var (
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
