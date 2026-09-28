// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import (
	"go/build"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// CompiledTestFile reports whether `go test ./...` run from root — the
// module root the walk started from — compiles the _test.go at path on
// every platform CI runs. go/build leaves a file out SILENTLY — the package
// still reports ok, and `-run` says "no tests to run" — when its name
// starts with `_` or `.`, when it carries a GOOS or GOARCH suffix for
// another platform, or when a `//go:build` line excludes it; and the
// `./...` pattern never matches a `testdata` directory, a `_`- or
// `.`-prefixed directory, a directory with its own go.mod (a nested
// module), or a package below a `vendor` directory (the vendor directory
// itself is matched). A symlinked directory is also never matched; the walks
// that call this (WalkDir, ReadDir with IsDir) never follow one, so the
// predicate does not check for it. A registry that proves "the named test exists" by walking
// _test.go files must walk only the files that compile, or a test moved
// into `_zz_test.go` — or declared under scripts/testdata — still "exists"
// while never running (url-log redaction review rounds 9–10; the
// standing-rules, convergence and testdb registries walked the same way).
//
// The name rules are go/build's own (MatchFile), evaluated with no
// operating system or architecture so a suffix for THIS machine is refused
// too: a test that exists only on the developer's platform is not a CI
// test. The header rule is stricter than go/build's: any `//go:build` or
// `// +build` line before the package clause refuses the file, satisfied
// here or not — a constraint that holds on this machine may not hold on
// CI, and the registries' contract is "runs everywhere". A constraint
// AFTER the package clause is not one (go/build ignores it and go test
// then fails the package loudly — misplaced directive — so nothing is
// silent there).
func CompiledTestFile(t testing.TB, root, path string) bool {
	t.Helper()
	dir, name := filepath.Split(path)
	if !strings.HasSuffix(name, "_test.go") {
		t.Fatalf("srctest.CompiledTestFile: %s is not a _test.go file", path)
	}
	rel, err := filepath.Rel(root, filepath.Clean(dir))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		t.Fatalf("srctest.CompiledTestFile: %s is not under the module root %s", path, root)
	}
	if rel != "." {
		probe := root
		els := strings.Split(rel, string(filepath.Separator))
		for i, el := range els {
			if el == "testdata" || strings.HasPrefix(el, "_") || strings.HasPrefix(el, ".") {
				return false // the go tool never looks inside
			}
			if el == "vendor" && i < len(els)-1 {
				return false // `./...` lists a vendor directory but no package below it (round 13)
			}
			probe = filepath.Join(probe, el)
			if _, err := os.Stat(filepath.Join(probe, "go.mod")); err == nil {
				return false // a nested module: `go test ./...` from root stops at its boundary
			}
		}
	}
	ctx := build.Default
	ctx.GOOS, ctx.GOARCH = "none", "none"
	ok, err := ctx.MatchFile(dir, name)
	if err != nil {
		t.Fatalf("srctest.CompiledTestFile: %s: %v", path, err)
	}
	if !ok {
		return false
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("srctest.CompiledTestFile: parse %s: %v", path, err)
	}
	for _, cg := range f.Comments {
		if cg.Pos() > f.Package {
			break
		}
		for _, c := range cg.List {
			if constraint.IsGoBuild(c.Text) || constraint.IsPlusBuild(c.Text) {
				return false
			}
		}
	}
	return true
}
