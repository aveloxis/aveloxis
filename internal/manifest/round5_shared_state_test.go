// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

// Review round 5 (M5-8). TestNoParseWritesAPackageVariable reads
// assignments, and a package-level variable can be written without one. The
// rule is decided here as a class, by what can REACH a package-level
// variable to change it, and each way is refused:
//
//   - an assignment, increment or decrement (TestNoParseWritesAPackageVariable,
//     for function declarations; here also inside a function literal that is
//     itself a package-level variable's value);
//   - delete or clear, called on one;
//   - its address taken (a write through the pointer, or through a helper
//     that is handed the pointer, is then an ordinary assignment to a local);
//   - a package-level map or slice handed to a function (which can then
//     assign to its elements): only the builtins that read may be given one.
//
// What this cannot see is a method with a pointer receiver that writes its
// receiver; the package's only variables with methods are the standard
// library's (a compiled pattern, a replacer, an atomic), which are safe for
// use from several goroutines. That claim, and every other path, is
// TestManifestsAreReadOnSeveralWorkersAtOnce's under -race, for the kinds it
// reads.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// packageVarReachAllowed are the places where a package-level variable is
// reached in one of those ways and is not written, each with why. The key
// is "function: variable".
var packageVarReachAllowed = map[string]string{
	"the value of byName: roster": "the index of the roster is built once, when the package is initialised, and holds pointers to its entries; nothing assigns through them (TestNoParseWritesAPackageVariable reads every assignment)",
}

// packageVarReaches is the check, over Go source given by file name: the
// package's own, or a test's.
func packageVarReaches(t *testing.T, sources map[string]string) (found []string, examined int) {
	t.Helper()
	fset := token.NewFileSet()
	var parsed []*ast.File
	vars := map[string]bool{}      // every package-level variable
	container := map[string]bool{} // those declared as a map or a slice
	isContainer := func(spec *ast.ValueSpec, i int) bool {
		kind := func(e ast.Expr) bool {
			switch x := e.(type) {
			case *ast.MapType:
				return true
			case *ast.ArrayType:
				return x.Len == nil
			}
			return false
		}
		if spec.Type != nil && kind(spec.Type) {
			return true
		}
		if i < len(spec.Values) {
			switch v := spec.Values[i].(type) {
			case *ast.CompositeLit:
				return kind(v.Type)
			case *ast.CallExpr:
				if id, ok := v.Fun.(*ast.Ident); ok && id.Name == "make" && len(v.Args) > 0 {
					return kind(v.Args[0])
				}
			}
		}
		return false
	}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var src any
		if sources[name] != "" {
			src = sources[name]
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, f)
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.VAR {
				for _, s := range g.Specs {
					spec := s.(*ast.ValueSpec)
					for i, n := range spec.Names {
						vars[n.Name] = true
						if isContainer(spec, i) {
							container[n.Name] = true
						}
					}
				}
			}
		}
	}
	specs := map[*ast.ValueSpec]bool{}
	for _, f := range parsed {
		for _, d := range f.Decls {
			if g, ok := d.(*ast.GenDecl); ok && g.Tok == token.VAR {
				for _, s := range g.Specs {
					specs[s.(*ast.ValueSpec)] = true
				}
			}
		}
	}
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
		case *ast.SliceExpr:
			return root(x.X)
		}
		return nil
	}
	packageLevel := func(id *ast.Ident) bool {
		if id == nil || !vars[id.Name] {
			return false
		}
		if id.Obj == nil {
			return true // declared in another file of the package
		}
		spec, ok := id.Obj.Decl.(*ast.ValueSpec)
		return ok && id.Obj.Kind == ast.Var && specs[spec]
	}
	readers := map[string]bool{"len": true, "cap": true}
	inspect := func(where string, body ast.Node, assignments bool) {
		report := func(pos token.Pos, id *ast.Ident, how string) {
			if _, ok := packageVarReachAllowed[where+": "+id.Name]; !ok {
				found = append(found, fmt.Sprintf("%s: %s %s the package-level variable %s", fset.Position(pos), where, how, id.Name))
			}
		}
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				if assignments && x.Tok != token.DEFINE {
					for _, e := range x.Lhs {
						examined++
						if id := root(e); packageLevel(id) {
							report(e.Pos(), id, "writes")
						}
					}
				}
			case *ast.IncDecStmt:
				if assignments {
					examined++
					if id := root(x.X); packageLevel(id) {
						report(x.Pos(), id, "writes")
					}
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					examined++
					if id := root(x.X); packageLevel(id) {
						report(x.Pos(), id, "takes the address of")
					}
				}
			case *ast.CallExpr:
				fn, _ := x.Fun.(*ast.Ident)
				builtin := fn != nil && fn.Obj == nil && !vars[fn.Name]
				for i, arg := range x.Args {
					examined++
					id := root(arg)
					if !packageLevel(id) {
						continue
					}
					switch {
					case builtin && (fn.Name == "delete" || fn.Name == "clear") && i == 0:
						report(arg.Pos(), id, fn.Name+"s from")
					case builtin && (readers[fn.Name] || (fn.Name == "append" && i > 0)):
						// Read: measured, or copied into another slice.
					case container[id.Name]:
						if _, plain := arg.(*ast.Ident); plain {
							report(arg.Pos(), id, "hands to a function")
						}
					}
				}
			}
			return true
		})
	}
	for _, f := range parsed {
		for _, d := range f.Decls {
			switch x := d.(type) {
			case *ast.FuncDecl:
				if x.Body != nil && !(x.Recv == nil && x.Name.Name == "init") {
					// Its assignments are TestNoParseWritesAPackageVariable's.
					inspect(x.Name.Name, x.Body, false)
				}
			case *ast.GenDecl:
				if x.Tok != token.VAR {
					continue
				}
				for _, s := range x.Specs {
					spec := s.(*ast.ValueSpec)
					for i, v := range spec.Values {
						name := "a package-level value"
						if i < len(spec.Names) {
							name = "the value of " + spec.Names[i].Name
						}
						ast.Inspect(v, func(n ast.Node) bool {
							if lit, ok := n.(*ast.FuncLit); ok {
								inspect(name, lit.Body, true)
								return false
							}
							return true
						})
					}
				}
			}
		}
	}
	sort.Strings(found)
	return found, examined
}

func TestNothingReachesAPackageVariableToWriteIt(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{}
	for _, name := range files {
		if !strings.HasSuffix(name, "_test.go") {
			sources[name] = ""
		}
	}
	found, examined := packageVarReaches(t, sources)
	if examined < 3000 {
		t.Fatalf("only %d call arguments, address-ofs and function literals' statements examined: the test is not reading the package", examined)
	}
	for _, f := range found {
		t.Error(f + ": manifests are read on several workers at once; keep it on the Walk, pass it in, or make it an atomic")
	}
	for key, why := range packageVarReachAllowed {
		if why == "" {
			t.Errorf("packageVarReachAllowed names %q with no reason", key)
		}
	}
	// An exception that excuses nothing is stale: removed, each one must be
	// a finding again.
	allowed := packageVarReachAllowed
	packageVarReachAllowed = map[string]string{}
	all, _ := packageVarReaches(t, sources)
	packageVarReachAllowed = allowed
	if len(all) != len(allowed) {
		t.Errorf("%d exceptions are listed and %d places need one: %v", len(allowed), len(all), all)
	}
}

// Each way the review found past the first test, as source: the check must
// find every one, and nothing in the same code written without them.
func TestTheReachCheckFindsEachWay(t *testing.T) {
	const head = "package p\n\nvar table = map[string]bool{\"a\": true}\nvar list []string\nvar count int\n\n"
	for name, c := range map[string]struct {
		body string
		want string
	}{
		"delete on a package map":                {"func parse(s string) { delete(table, s) }\n", "deletes from the package-level variable table"},
		"clear on a package map":                 {"func parse(s string) { clear(table) }\n", "clears from the package-level variable table"},
		"a write inside a package-level literal": {"var seam = func() { count++ }\n\nfunc parse(s string) { seam() }\n", "writes the package-level variable count"},
		"an assignment inside one":               {"var seam = func(s string) { list = append(list, s) }\n", "writes the package-level variable list"},
		"a write through a pointer":              {"func parse(s string) {\n\tp := &count\n\t*p = *p + 1\n}\n", "takes the address of the package-level variable count"},
		"an append through a helper":             {"func add(l *[]string, s string) { *l = append(*l, s) }\n\nfunc parse(s string) { add(&list, s) }\n", "takes the address of the package-level variable list"},
		"a map handed to a function that writes": {"func mark(m map[string]bool, s string) { m[s] = true }\n\nfunc parse(s string) { mark(table, s) }\n", "hands to a function the package-level variable table"},
		"the address of an element":              {"var grid [][]int\n\nfunc parse(i int) *int { return &grid[i][0] }\n", "takes the address of the package-level variable grid"},
	} {
		found, _ := packageVarReaches(t, map[string]string{"p.go": head + c.body})
		if !strings.Contains(strings.Join(found, "\n"), c.want) {
			t.Errorf("%s: found %v, want a finding that it %s", name, found, c.want)
		}
	}
	clean := "func parse(s string) (bool, int) {\n\tlocal := map[string]bool{}\n\tdelete(local, s)\n\tp := &local\n\t_ = p\n\tn := len(table) + len(list)\n\tfor k := range table {\n\t\t_ = k\n\t}\n\treturn table[s], n + count\n}\n"
	if found, examined := packageVarReaches(t, map[string]string{"p.go": head + clean}); len(found) != 0 || examined == 0 {
		t.Errorf("code that only reads the package's variables: found %v (%d examined)", found, examined)
	}
}
