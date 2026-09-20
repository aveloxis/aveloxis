// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoRawGitHubBaseURLReadOutsideConfig — v0.29.57, Copilot review
// 5261384568. `config.PlatformConfig.BaseURL` for GitHub is empty when the
// operator leaves it unset; `GitHubAPIBase()` is the ONE accessor that turns
// that into public GitHub (SR-17). Seven commands still read the raw field
// and built clients that issued hostless requests, after six reviews had
// each converted the sites they named. The string pins those rounds left
// could see only the call sites they spelled out; this one parses every
// non-test Go file outside internal/config and refuses ANY read of
// <x>.GitHub.BaseURL — an assignment to it (tests and config loading write
// it) is not a read. Worklist item 44 (forge instances) replaces the
// accessor with instance routing; until then this is the ratchet that keeps
// the raw field from growing readers.
func TestNoRawGitHubBaseURLReadOutsideConfig(t *testing.T) {
	root := srctest.Root(t)
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if filepath.Base(path) == "config" && filepath.Dir(path) == filepath.Join(root, "internal") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			src, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			rel, _ := filepath.Rel(root, path)
			for _, line := range rawGitHubBaseReads(t, rel, string(src)) {
				t.Errorf("%s:%d reads the raw cfg.GitHub.BaseURL — use GitHubAPIBase(): the field is empty when unset and a client built from it issues hostless requests", rel, line)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files scanned", scanned, 300)
}

// rawGitHubBaseReads returns the line of every read of <x>.GitHub.BaseURL in
// src. Assignment targets are writes and are not reported; a composite
// literal `PlatformConfig{BaseURL: …}` never forms the selector chain.
func rawGitHubBaseReads(t testing.TB, name, src string) []int {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	writes := map[ast.Expr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		// Only a plain assignment or definition is a pure write: a compound
		// one (+=) READS the field first (fix-review round 1).
		if as, ok := n.(*ast.AssignStmt); ok && (as.Tok == token.ASSIGN || as.Tok == token.DEFINE) {
			for _, l := range as.Lhs {
				writes[l] = true
			}
		}
		return true
	})
	var lines []int
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "BaseURL" || writes[sel] {
			return true
		}
		block, ok := sel.X.(*ast.SelectorExpr)
		if !ok || block.Sel.Name != "GitHub" {
			return true
		}
		lines = append(lines, fset.Position(sel.Pos()).Line)
		return true
	})
	return lines
}

// Each rule of the scanner has a fixture that fails if the rule is dropped.
func TestRawGitHubBaseReadsFixtures(t *testing.T) {
	const pre = "package p\n\nfunc f(cfg *C, keys any) {\n\t"
	for _, tc := range []struct {
		name, body string
		want       int
	}{
		{"constructor argument", `github.New(cfg.GitHub.BaseURL, keys, nil)`, 1},
		{"local copy", `base := cfg.GitHub.BaseURL; _ = base`, 1},
		{"comparison", `if cfg.GitHub.BaseURL == "" { return }`, 1},
		{"deeper receiver", `_ = s.cfg.GitHub.BaseURL`, 1},
		{"two reads", `_ = cfg.GitHub.BaseURL; _ = cfg.GitHub.BaseURL`, 2},
		{"assignment is a write", `cfg.GitHub.BaseURL = "https://x"`, 0},
		{"tuple assignment is a write", `_, cfg.GitHub.BaseURL = g()`, 0},
		{"compound assignment reads", `cfg.GitHub.BaseURL += "/x"`, 1},
		{"the accessor", `_ = cfg.GitHub.GitHubAPIBase()`, 0},
		{"GitLab's raw field is another rule's", `_ = cfg.GitLab.BaseURL`, 0},
		{"composite literal", `_ = C{GitHub: P{BaseURL: "x"}}`, 0},
		{"unrelated BaseURL", `_ = cfg.Web.BaseURL`, 0},
	} {
		got := rawGitHubBaseReads(t, tc.name+".go", pre+tc.body+"\n}\n")
		if len(got) != tc.want {
			t.Errorf("%s: %d reads flagged, want %d (lines %v)", tc.name, len(got), tc.want, got)
		}
	}
}
