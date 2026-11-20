// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Dev/build dependency expansion (v0.27.45, summary/19 P2 — the
// numpy fix). Python's dev/test/build tooling lives in manifest
// families the walk never visited: requirements-variant files
// (requirements-dev.txt, test_requirements.txt, or another .txt directly
// inside a requirements/ directory — never requirements.txt itself),
// pyproject [build-system].requires / [project.optional-dependencies]
// / PEP 735 [dependency-groups] / poetry groups, Pipfile
// [dev-packages], setup.py tests_require/extras_require, setup.cfg
// [options.extras_require]. Everything here is gated on
// collection.dev_build_deps (default FALSE — the findings-volume
// driver; canary on the small chaoss.tv DB before any default flip,
// per the v0.27.19 first-wave lesson). Knob off = the walk's
// pre-v0.27.45 row set, byte-identical.
//
// The Go C1 classifier (classifyGoModTestOnlyDeps) is NOT gated: it
// only relabels already-collected go.mod deps (test-only modules →
// test scope), adding no rows and no findings volume — the same
// posture as the P1 relabels.

package collector

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// classifyGoModTestOnlyDeps is the Go C1 classifier (operator
// decision #7: "since we are Go", the go.mod honesty gap gets a real
// fix, not a doc note). go.mod cannot distinguish test-only deps
// structurally, but the repo's own source can: a module whose
// packages are imported ONLY from _test.go files is test-scope.
// Static import scan via go/parser ImportsOnly — no toolchain, no
// subprocess, no module downloads. Modules never imported at all
// (indirect deps, tool deps) honestly stay runtime: absence of
// evidence is not test evidence, and unknown scopes present as
// runtime everywhere (IsRuntimeScope).
//
// NOT gated on dev_build_deps: this relabels existing rows without
// adding any (the P1 posture).
func classifyGoModTestOnlyDeps(workDir string, deps []model.LibyearDep) []model.LibyearDep {
	hasGoDeps := false
	for _, d := range deps {
		if d.Manager == "go" {
			hasGoDeps = true
			break
		}
	}
	if !hasGoDeps {
		return deps
	}

	type usage struct{ nonTest, test bool }
	imports := map[string]*usage{}
	fset := token.NewFileSet()
	const maxGoFiles = 50000 // pathology bound; beyond it, skip silently (deps keep runtime)
	seen := 0
	_ = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == "node_modules" || base == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		seen++
		if seen > maxGoFiles {
			return filepath.SkipAll
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil || f == nil {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			u := imports[p]
			if u == nil {
				u = &usage{}
				imports[p] = u
			}
			if isTest {
				u.test = true
			} else {
				u.nonTest = true
			}
		}
		return nil
	})

	for i := range deps {
		if deps[i].Manager != "go" || !model.IsRuntimeScope(deps[i].Type) {
			continue
		}
		mod := deps[i].Name
		anyTest, anyNonTest := false, false
		for p, u := range imports {
			if p == mod || strings.HasPrefix(p, mod+"/") {
				anyTest = anyTest || u.test
				anyNonTest = anyNonTest || u.nonTest
			}
		}
		if anyTest && !anyNonTest {
			deps[i].Type = model.ScopeTest
		}
	}
	return deps
}
