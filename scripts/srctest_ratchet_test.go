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

// The srctest migration RATCHET (v0.27.118) — the operator's
// "fix when touched, and aggressively validate" mechanism, in the
// house tripwire style aimed at the test suite itself.
//
// The 2026-08-20 inventory found ~40 near-identical read helpers, ~23
// function-body extractors (five incompatible variants), duplicated
// comment strippers and repo-root finders scattered across test files.
// internal/srctest is now the single home for those operations, with
// STRANGLER adoption (no bulk migration — refactoring working pins is
// itself a regression risk; see docs/contributing/testing.md).
//
// srctest_migration_baseline.txt freezes the legacy sites that existed
// at ratchet introduction. This test enforces both directions:
//   - a detected legacy-helper definition NOT in the baseline FAILS:
//     new duplication is banned — use internal/srctest;
//   - a baseline entry whose site no longer exists FAILS: delete the
//     line (the stale-allowlist reverse check). The baseline can only
//     SHRINK — burn-down progress is `wc -l` on one file, and touching
//     a legacy test file pairs naturally with deleting its lines in
//     the same diff.
//
// Regenerate after a migration wave: AVELOXIS_UPDATE_BASELINE=1
// go test ./scripts/ -run TestSrctestMigrationRatchet
// (the golden-file pattern from the manifest corpus). Review the diff:
// it must ONLY DELETE lines.

// v0.27.122 (Copilot round 14, suppressed — real): detection is
// AST-based (go/parser), replacing regexes whose bounded-lookahead
// read-helper rule could cross a function boundary (false attribution)
// and whose name-prefix extractor rule missed non-"extract" names like
// `region`. Each top-level FuncDecl in a test file is classified from
// its OWN parsed span. Inline os.ReadFile calls are fine (plain reads
// of package-local files stay idiomatic); what ratchets down is
// defining NEW named helpers that duplicate srctest.
var (
	stripperNameRe = regexp.MustCompile(`(?i)^strip\w*comment`)
	extractNameRe  = regexp.MustCompile(`(?i)(extract|region|funcbody)`)
)

// classifyLegacyHelpers returns the names of legacy-duplicate helper
// definitions in one parsed test file.
func classifyLegacyHelpers(src string) ([]string, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "t.go", src, 0)
	if err != nil {
		return nil, err
	}
	spanOf := func(n ast.Node) string {
		return src[fset.Position(n.Pos()).Offset:fset.Position(n.End()).Offset]
	}
	var out []string
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := fn.Name.Name
		// The ratchet tracks HELPER DEFINITIONS, not test functions —
		// a Test body inlining a legacy idiom (e.g. the cut-at-next-
		// func extraction) is fix-when-touched material, but flagging
		// whole tests would explode the baseline far past the
		// documented "named helpers duplicating srctest" contract.
		if strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Benchmark") || strings.HasPrefix(name, "Fuzz") {
			continue
		}
		body := spanOf(fn.Body)
		paramsHaveTesting := false
		for _, p := range fn.Type.Params.List {
			if strings.Contains(spanOf(p.Type), "testing.") {
				paramsHaveTesting = true
			}
		}
		resultStringy := false
		if fn.Type.Results != nil && len(fn.Type.Results.List) > 0 {
			rt := spanOf(fn.Type.Results.List[0].Type)
			resultStringy = rt == "string" || rt == "[]byte"
		}
		switch {
		// Read helpers: takes testing.T/TB, returns string/[]byte,
		// body reads a file. The BODY-scoped check cannot attribute a
		// neighboring function's os.ReadFile (the regex flaw).
		case paramsHaveTesting && resultStringy && strings.Contains(body, "os.ReadFile"):
			out = append(out, name)
		// Extractors: any name carrying extract/region/funcbody that
		// returns a string, OR any body using the cut-at-next-func
		// idiom regardless of name.
		case resultStringy && extractNameRe.MatchString(name),
			strings.Contains(body, "\\nfunc "):
			out = append(out, name)
		// Comment strippers.
		case stripperNameRe.MatchString(name):
			out = append(out, name)
		// Repo-root finders: the go.mod walk idiom.
		case strings.Contains(body, `"go.mod"`):
			out = append(out, name)
		}
	}
	return out, nil
}

func detectLegacySites(t *testing.T, root string) map[string]bool {
	t.Helper()
	sites := map[string]bool{}
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if strings.HasSuffix(filepath.ToSlash(path), "internal/srctest") {
					return filepath.SkipDir // the replacement itself
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			if filepath.ToSlash(rel) == "scripts/srctest_ratchet_test.go" {
				return nil // the detector itself
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			names, cerr := classifyLegacyHelpers(string(b))
			if cerr != nil {
				return fmt.Errorf("%s: %w", rel, cerr)
			}
			for _, n := range names {
				sites[filepath.ToSlash(rel)+" "+n] = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "test files scanned by the ratchet", scanned, 300)
	return sites
}

func TestSrctestMigrationRatchet(t *testing.T) {
	root := srctest.Root(t)
	baselinePath := filepath.Join(root, "scripts", "srctest_migration_baseline.txt")
	detected := detectLegacySites(t, root)

	if os.Getenv("AVELOXIS_UPDATE_BASELINE") == "1" {
		var lines []string
		for s := range detected {
			lines = append(lines, s)
		}
		sort.Strings(lines)
		content := "# srctest migration baseline — legacy helper sites frozen at ratchet\n" +
			"# introduction (v0.27.118). SHRINK-ONLY: entries are deleted as files\n" +
			"# migrate to internal/srctest; new entries are a build failure.\n" +
			"# Regenerate: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run Ratchet\n" +
			strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(baselinePath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("baseline regenerated: %d sites", len(lines))
		return
	}

	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("baseline missing (%v) — seed it: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run Ratchet", err)
	}
	baseline := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		baseline[line] = true
	}
	// v0.27.122 (round 14, active): an EMPTY baseline is the ratchet's
	// GOAL state, not a broken scan — when every legacy site has
	// migrated, the detector keeps rejecting new duplication against a
	// zero-entry baseline forever. Only a missing/unreadable file (the
	// os.ReadFile fatal above) is an error.

	var newDup, stale []string
	for s := range detected {
		if !baseline[s] {
			newDup = append(newDup, s)
		}
	}
	for s := range baseline {
		if !detected[s] {
			stale = append(stale, s)
		}
	}
	sort.Strings(newDup)
	sort.Strings(stale)
	for _, s := range newDup {
		t.Errorf("NEW legacy helper %q — duplicating srctest is banned; use internal/srctest (Read/FuncBody/Strip*/Root) instead", s)
	}
	for _, s := range stale {
		t.Errorf("stale baseline entry %q — the site is gone (migrated?); delete its line from %s (the baseline only shrinks)", s, "scripts/srctest_migration_baseline.txt")
	}
	if len(newDup) == 0 && len(stale) == 0 {
		t.Logf("ratchet: %d legacy sites remain", len(baseline))
	}
}

// TestRatchetAcceptsEmptyBaseline — v0.27.122 (round 14, active): the
// burn-down's COMPLETED state is an empty baseline; a minimum-entries
// guard would make success fail permanently. This pin bans such a
// guard from returning.
func TestRatchetAcceptsEmptyBaseline(t *testing.T) {
	b, err := os.ReadFile("srctest_ratchet_test.go")
	if err != nil {
		t.Fatal(err)
	}
	needle := `MinCount(t, ` + `"baseline entries"` // split so this pin can't self-match
	if strings.Contains(string(b), needle) {
		t.Error("the ratchet must accept an empty baseline — it is the goal state, not a broken scan")
	}
}
