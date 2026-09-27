// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
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
	"gopkg.in/yaml.v3"
)

// TestCostTestsRunOutsideTheRaceDetectorInCI — v0.29.67 review round 23: the
// cost tests (inputs four times apart, the growth of the work pinned) skip
// under the race detector, and CI's test job runs only -race, so without a
// separate step they would never run in CI. Every test that skips on
// raceBuild must be selected, in its package, by a go test step in test.yml
// that does not use -race.
//
// The convention that makes that exact (round 26): raceBuild is referenced
// only directly inside a Test function. Rounds 24-26 found skips the pin
// missed each time it followed indirection (a skip on a test's fourth line,
// a helper, a method, a func-valued var assigned in init, a struct field), so
// the pin follows none: any other reference fails it, and each Test that
// names raceBuild is a cost test. "Directly" is exact (round 27): only in an
// if condition of the Test, never inside a function literal, an assignment
// or an argument, so a Test cannot hand the value to another test. Every Go
// file of the module is read (round 27: a package outside internal/ and a
// non-test file both escaped), and only the ValueSpec that declares the
// constant is exempt (round 27: a second constant in its block escaped).
func TestCostTestsRunOutsideTheRaceDetectorInCI(t *testing.T) {
	root := srctest.Root(t)
	type costTest struct{ pkg, name string }
	var tests []costTest
	bad := func(path, what string) {
		rel, _ := filepath.Rel(root, path)
		t.Errorf("%s: raceBuild %s; reference it only in an if condition directly inside a Test function, so this pin can schedule it", filepath.ToSlash(rel), what)
	}
	isRace := func(n ast.Node) bool { id, ok := n.(*ast.Ident); return ok && id.Name == "raceBuild" }
	mentions := func(n ast.Node) bool {
		found := false
		ast.Inspect(n, func(c ast.Node) bool {
			found = found || isRace(c)
			return !found
		})
		return found
	}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "testdata", "vendor", ".git", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, filepath.Dir(path))
		pkg := "./" + filepath.ToSlash(rel) + "/"
		for _, decl := range f.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok {
				for _, sp := range gd.Specs {
					if vs, ok := sp.(*ast.ValueSpec); ok && len(vs.Names) == 1 && vs.Names[0].Name == "raceBuild" && gd.Tok == token.CONST {
						continue // the declaration itself (its value is a literal)
					}
					if mentions(sp) {
						bad(path, "is referenced in a package-level declaration")
					}
				}
				continue
			}
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || !mentions(fd) {
				continue
			}
			if fd.Recv != nil || !strings.HasPrefix(fd.Name.Name, "Test") || fd.Body == nil {
				bad(path, "is referenced outside a Test function (in "+fd.Name.Name+")")
				continue
			}
			// Inside the Test: every reference must sit in an if condition
			// and not inside a function literal.
			allowed := map[ast.Node]bool{}
			var walk func(n ast.Node, inCond, inLit bool)
			walk = func(n ast.Node, inCond, inLit bool) {
				ast.Inspect(n, func(c ast.Node) bool {
					switch c := c.(type) {
					case *ast.FuncLit:
						walk(c.Body, false, true)
						return false
					case *ast.IfStmt:
						if c.Init != nil {
							walk(c.Init, false, inLit)
						}
						walk(c.Cond, true, inLit)
						walk(c.Body, false, inLit)
						if c.Else != nil {
							walk(c.Else, false, inLit)
						}
						return false
					case *ast.Ident:
						if c.Name == "raceBuild" && inCond && !inLit {
							allowed[c] = true
						} else if c.Name == "raceBuild" {
							bad(path, "is used in "+fd.Name.Name+" other than in an if condition")
						}
					}
					return true
				})
			}
			walk(fd.Body, false, false)
			if len(allowed) > 0 {
				tests = append(tests, costTest{pkg: pkg, name: fd.Name.Name})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(tests, func(i, j int) bool { return tests[i].pkg+tests[i].name < tests[j].pkg+tests[j].name })
	// The denominator: the cost tests of v0.29.67 at least.
	srctest.MinCount(t, "tests that skip on raceBuild", len(tests), 5)

	var wf struct {
		Jobs map[string]struct {
			Steps []struct {
				Run string `yaml:"run"`
			} `yaml:"steps"`
		} `yaml:"jobs"`
	}
	if err := yaml.Unmarshal([]byte(srctest.Read(t, ".github/workflows/test.yml")), &wf); err != nil {
		t.Fatal(err)
	}
	runFlag := regexp.MustCompile(`-run '([^']+)'`)
	selected := func(ct costTest) bool {
		for _, job := range wf.Jobs {
			for _, st := range job.Steps {
				if !strings.Contains(st.Run, "go test") || strings.Contains(st.Run, "-race") {
					continue
				}
				m := runFlag.FindStringSubmatch(st.Run)
				if m == nil || !strings.Contains(st.Run+" ", " "+ct.pkg+" ") {
					continue
				}
				if regexp.MustCompile(m[1]).MatchString(ct.name) {
					return true
				}
			}
		}
		return false
	}
	for _, ct := range tests {
		if !selected(ct) {
			t.Errorf("%s in %s skips under the race detector, but no non-race go test step in test.yml runs it", ct.name, ct.pkg)
		}
	}
}
