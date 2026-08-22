// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package srctest is the shared engine for the repo's source-contract
// tests (v0.27.118 — the regression-infrastructure plan). Before it,
// ~40 near-identical read helpers, 23 function-body extractors (five
// incompatible variants with DIFFERENT scan windows), and duplicated
// comment strippers were scattered across ~385 test files; two review
// rounds found real coverage holes caused by exactly that duplication.
//
// Rules:
//   - stdlib-only imports (no import cycles possible, zero go.mod cost);
//   - imported ONLY from *_test.go files — a self-guard tripwire
//     (selfguard_test.go) fails the build if production code imports it,
//     and `go list -deps ./cmd/aveloxis` must never contain it;
//   - every helper carries its historical bug class as a negative
//     fixture in srctest_test.go. Fix helpers, never fixtures.
//
// Adoption is STRANGLER-ONLY: new source-contract tests use this
// package; legacy tests migrate when touched (tracked by the ratchet
// baseline in scripts/); there is deliberately NO bulk migration — see
// docs/contributing/testing.md.
package srctest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var rootOnce = sync.OnceValues(func() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
})

// Root returns the absolute repository root (the directory containing
// go.mod), found by walking up from the working directory. Memoized —
// each test binary runs with its package directory as CWD, so the walk
// resolves identically for every test in the binary.
func Root(t testing.TB) string {
	t.Helper()
	root, err := rootOnce()
	if err != nil {
		t.Fatalf("srctest.Root: cannot locate go.mod above the working directory: %v", err)
	}
	return root
}

// Read returns the contents of the file at a REPO-ROOT-RELATIVE path,
// fataling on error. It exists to kill the "../../docs/..." fragility
// for cross-package reads; package-local files may keep using plain
// os.ReadFile.
func Read(t testing.TB, repoRelPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), filepath.FromSlash(repoRelPath)))
	if err != nil {
		t.Fatalf("srctest.Read(%s): %v", repoRelPath, err)
	}
	return string(b)
}

// PackageFiles returns repo-root-relative path → source for every
// non-test .go file directly in repoRelDir (non-recursive — the
// flagship column-writer tripwire's corpus rules). The self-check
// guard is a REQUIRED parameter: fewer than minFiles read is a fatal
// "my corpus walk broke", never a silent vacuous pass.
func PackageFiles(t testing.TB, repoRelDir string, minFiles int) map[string]string {
	t.Helper()
	dir := filepath.Join(Root(t), filepath.FromSlash(repoRelDir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("srctest.PackageFiles(%s): %v", repoRelDir, err)
	}
	out := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("srctest.PackageFiles: read %s: %v", name, err)
		}
		out[repoRelDir+"/"+name] = string(b)
	}
	MinCount(t, "non-test .go files in "+repoRelDir, len(out), minFiles)
	return out
}

// FuncBody returns the source of the function or method whose
// declaration STARTS WITH sig (e.g. "func (s *PostgresStore)
// UpsertRepo("), from the `func` keyword through the body's closing
// brace — located via the go/ast declaration, sliced from the ORIGINAL
// source so comments inside the body are preserved (pins target them;
// compose with StripGoComments when a pin must not match comment
// text). This is THE one extractor (it replaced five incompatible
// variants whose scan windows disagreed): the AST route can neither
// anchor inside a comment that mentions the signature nor stop at a
// signature type literal's braces, and text after the closing brace
// (trailing comments, the next function's doc comment) is excluded.
func FuncBody(t testing.TB, src, sig string) string {
	t.Helper()
	// v0.27.122 (Copilot round 14, active): the extraction is AST-based
	// (stdlib go/parser — no dependency cost). The two lexical
	// generations both had real false-window classes: a bare substring
	// anchor could start inside a doc comment mentioning the signature
	// (round 13), and brace counting returned early on signatures
	// containing type literals (`func f(v struct{ X int })` — the
	// parameter type's braces balance before the body opens). Parsing
	// kills both structurally: declarations come from the AST, and the
	// returned span is the ORIGINAL source text from the `func` keyword
	// through the body's closing brace (token offsets into src), so
	// comments INSIDE the body are preserved for pins that target them.
	// The sig contract is unchanged: the declaration's gofmt'd source
	// must start with sig (e.g. "func (s *PostgresStore) UpsertRepo(").
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("srctest.FuncBody: source does not parse (%v) — this helper takes whole valid Go files", err)
	}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		start := fset.Position(fn.Pos()).Offset // fn.Pos() = the `func` keyword (doc comment excluded)
		end := fset.Position(fn.End()).Offset
		decl := src[start:end]
		if strings.HasPrefix(decl, sig) {
			return decl
		}
	}
	t.Fatalf("srctest.FuncBody: no declaration starts with %q", sig)
	return ""
}

// TypeBody returns the source of the named top-level type declaration
// (struct/interface/alias), from its `type` keyword through its end —
// the FuncBody sibling for struct-shape pins (v0.27.122). Same AST
// guarantees: cannot anchor in comments, cannot stop at nested type
// literals.
func TypeBody(t testing.TB, src, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "src.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("srctest.TypeBody: source does not parse (%v)", err)
	}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != name {
				continue
			}
			// v0.27.154 (round 33): a grouped `type (...)` declaration's
			// GenDecl spans EVERY sibling type — returning it let a
			// shape pin pass because the required field lived on a
			// DIFFERENT type in the same group. Grouped declarations
			// slice the matched TypeSpec alone ("exactly the named
			// type" is the contract).
			if gd.Lparen.IsValid() {
				return src[fset.Position(ts.Pos()).Offset:fset.Position(ts.End()).Offset]
			}
			return src[fset.Position(gd.Pos()).Offset:fset.Position(gd.End()).Offset]
		}
	}
	t.Fatalf("srctest.TypeBody: type %q not found", name)
	return ""
}

// MinCount fatals when got < min, naming what was being counted — the
// standard "my own scan broke" guard so corpus-walking tests fail
// loudly instead of passing vacuously.
func MinCount(t testing.TB, what string, got, min int) {
	t.Helper()
	if got < min {
		t.Fatalf("srctest self-check: only %d %s found (need >= %d) — the scan itself broke; fix the test, do not trust this run", got, what, min)
	}
}
