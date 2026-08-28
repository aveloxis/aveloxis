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
// What remains is a genuine legacy tail, frozen in
// shutdown_classification_baseline.txt and SHRINK-ONLY: a new site is a
// build failure, and a baselined site that gets fixed must be deleted
// from the baseline in the same change. Regenerate after a burn-down
// wave: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run ShutdownClassification
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
	out = append(out, auditFuncs(dbFns, migration)...)

	// internal/collector is entirely collection path: no exclusions.
	out = append(out, auditFuncs(parseFuncs(t, "../internal/collector"), nil)...)

	if len(out) == 0 {
		t.Fatal("the scan found zero sites in either package — the corpus or the regexes broke; " +
			"a silently-empty analyzer passes forever")
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

func auditFuncs(fns []shutdownFunc, exclude map[string]bool) []string {
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
			// The classification must sit between the producing call and
			// the log (a guard elsewhere in the function does not count —
			// L14: a loop-top guard cannot see the in-flight call that
			// observed the cancellation) AND it must TERMINATE, or the
			// log is still reached. Checking only that the token exists
			// is the decorative-gate class: keeping
			// `if errors.Is(err, context.Canceled) {` and replacing its
			// `return` with a bare `continue` (or a Debug log) escapes a
			// presence check while restoring the exact defect.
			if classificationTerminates(f.body[prodAt:loc[0]]) {
				continue
			}
			out = append(out, fmt.Sprintf("%s::%s::%s", f.file, f.name, msg))
		}
	}
	return out
}

// classificationTerminates reports whether the window contains a
// cancellation classification whose BLOCK ends the work before the log.
//
// `return` is the terminator this rule wants. A bare `continue` is not:
// in a child-write loop it skips the item and lets the enclosing row
// complete and be stamped processed with that child missing — the
// pre-fix defect minus the WARN. The one legitimate non-return shape is
// "record the abort, then drain" (breadth.go sets aborted/abortErr and
// cancels the remaining fetches), so a `continue`/`break` counts only
// when the block also ASSIGNS — i.e. it recorded something.
func classificationTerminates(window string) bool {
	for _, at := range regexp.MustCompile(`errors\.Is\([^)]*context\.Canceled\)`).FindAllStringIndex(window, -1) {
		open := strings.Index(window[at[1]:], "{")
		if open < 0 {
			continue
		}
		start := at[1] + open
		depth, end := 0, -1
		for i := start; i < len(window); i++ {
			switch window[i] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					end = i
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			continue
		}
		block := window[start : end+1]
		if strings.Contains(block, "return") {
			return true
		}
		if (strings.Contains(block, "continue") || strings.Contains(block, "break")) &&
			regexp.MustCompile(`\w\s*(?::=|=|\+\+)[^=]`).MatchString(block) {
			return true // recorded the abort, then drains
		}
	}
	return false
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
