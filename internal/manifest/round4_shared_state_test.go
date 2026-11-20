// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// R4-M5. Manifests are read on several workers at once, each with its own
// Walk, so nothing a parse does may write a package-level variable. Round 3
// added two counters "for tests" that every pom and every composer.json
// wrote (a data race the package's own serial suite could not see).

// packageVarWritesAllowed are the package-level variables that code outside
// an init function may write, each with why that is not a write during a
// parse. A prefix ending in "." covers the methods of a type.
var packageVarWritesAllowed = map[string]string{}

// TestNoParseWritesAPackageVariable reads the package's own source: every
// assignment, increment or decrement whose target is a package-level
// variable (the variable itself, an element of it, or a field of it), in
// any function that is not an init, is a failure unless it is named above.
// An atomic's methods are not assignments, which is the point: a counter
// that must be shared is an atomic.
func TestNoParseWritesAPackageVariable(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var parsed []*ast.File
	vars := map[string]bool{}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, f)
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.VAR {
				for _, s := range g.Specs {
					for _, n := range s.(*ast.ValueSpec).Names {
						vars[n.Name] = true
					}
				}
			}
		}
	}
	if len(parsed) < 10 || len(vars) < 20 {
		t.Fatalf("%d files and %d package-level variables read: the test is not reading the package", len(parsed), len(vars))
	}
	// root is the identifier an assignment's target starts from.
	var root func(e ast.Expr) *ast.Ident
	root = func(e ast.Expr) *ast.Ident {
		switch x := e.(type) {
		case *ast.Ident:
			return x
		case *ast.IndexExpr:
			return root(x.X)
		case *ast.SelectorExpr:
			return root(x.X)
		case *ast.StarExpr:
			return root(x.X)
		case *ast.ParenExpr:
			return root(x.X)
		}
		return nil
	}
	// packageLevel: the identifier names a package-level variable. The
	// parser resolves an identifier to what its own file declares, local
	// scopes included; one it does not resolve is declared in another file
	// of the package (or is not a variable at all).
	packageLevel := func(id *ast.Ident) bool {
		if id == nil || !vars[id.Name] {
			return false
		}
		if id.Obj == nil {
			return true
		}
		if id.Obj.Kind != ast.Var {
			return false
		}
		spec, ok := id.Obj.Decl.(*ast.ValueSpec)
		if !ok {
			return false
		}
		for _, f := range parsed {
			for _, d := range f.Decls {
				if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.VAR {
					for _, s := range g.Specs {
						if s == spec {
							return true
						}
					}
				}
			}
		}
		return false
	}
	var found []string
	examined := 0
	for _, f := range parsed {
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil || (fn.Recv == nil && fn.Name.Name == "init") {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				var targets []ast.Expr
				switch x := n.(type) {
				case *ast.AssignStmt:
					if x.Tok != token.DEFINE {
						targets = x.Lhs
					}
				case *ast.IncDecStmt:
					targets = []ast.Expr{x.X}
				}
				for _, e := range targets {
					examined++
					if id := root(e); packageLevel(id) {
						if _, allowed := packageVarWritesAllowed[id.Name]; !allowed {
							found = append(found, fmt.Sprintf("%s: %s writes the package-level variable %s", fset.Position(e.Pos()), fn.Name.Name, id.Name))
						}
					}
				}
				return true
			})
		}
	}
	if examined < 500 {
		t.Fatalf("only %d assignments examined: the test is not reading the package", examined)
	}
	sort.Strings(found)
	for _, f := range found {
		t.Error(f + ": manifests are read on several workers at once; keep it on the Walk, pass it in, or make it an atomic")
	}
	for name, why := range packageVarWritesAllowed {
		if !vars[name] || why == "" {
			t.Errorf("packageVarWritesAllowed names %q: not a package-level variable, or no reason given", name)
		}
	}
}

// And driven: poms, Gradle builds and composer.json files read on several
// goroutines, each with its own Walk. Run under -race, a write to shared
// state fails here.
func TestManifestsAreReadOnSeveralWorkersAtOnce(t *testing.T) {
	var plugins, deps strings.Builder
	for i := 0; i < 50; i++ {
		fmt.Fprintf(&plugins, "<plugin><groupId>p</groupId><artifactId>plugin-%d</artifactId><dependencies><dependency><groupId>p</groupId><artifactId>d%d</artifactId><version>1</version></dependency></dependencies></plugin>", i, i)
		fmt.Fprintf(&deps, "<dependency><groupId>g</groupId><artifactId>a%d</artifactId><version>${v}</version></dependency>", i)
	}
	files := map[string]string{
		"pom.xml": "<project><groupId>g</groupId><artifactId>a</artifactId><version>1</version><properties><v>2.0</v></properties><build><plugins>" + plugins.String() +
			"</plugins></build><dependencies>" + deps.String() + "</dependencies></project>",
		"build.gradle":             "ext { v = '1.2' }\ndependencies {\n  implementation \"com.example:lib:${v}\"\n  implementation platform('org.example:bom:1')\n}\n",
		"settings.gradle":          "rootProject.name = 'x'\n",
		"gradle.properties":        "kotlinVersion=1.9.0\n",
		"composer.json":            `{"require": {"vendor/one": "^1.0", "in/a": "*"}, "repositories": [{"type": "path", "url": "packages/*"}]}`,
		"packages/a/composer.json": `{"name": "in/a"}`,
		"packages/b/composer.json": `{"name": "in/b"}`,
		"Gemfile":                  "source 'https://rubygems.org'\ngem 'rails', '~> 7.0'\n",
	}
	root := writeTree(t, files)
	var wg sync.WaitGroup
	results := make([]string, 6)
	for g := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var sb strings.Builder
			for round := 0; round < 20; round++ {
				w := NewWalk(root, nil)
				for _, rel := range []string{"pom.xml", "build.gradle", "composer.json", "Gemfile"} {
					got := w.Read(filepath.Join(root, rel), Options{})
					names := make([]string, 0, len(got.Deps))
					for _, d := range got.Deps {
						names = append(names, fmt.Sprintf("%s@%s/%v", d.Name, d.Version, d.NonRegistry))
					}
					sort.Strings(names)
					fmt.Fprintf(&sb, "%s:%v:%v\n", rel, names, got.Failure)
				}
			}
			results[g] = sb.String()
		}()
	}
	wg.Wait()
	for g := 1; g < len(results); g++ {
		if results[g] != results[0] {
			t.Fatalf("worker %d read the same files differently from worker 0", g)
		}
	}
	if !strings.Contains(results[0], "g:a0@2.0") || !strings.Contains(results[0], "in/a@*/true") || !strings.Contains(results[0], "com.example:lib@1.2") {
		t.Errorf("the files were not read as expected:\n%s", strings.SplitN(results[0], "pom.xml", 3)[1])
	}
}
