// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDBTierPackagesUseTheirOwnDatabase (v0.29.57): every package whose tests
// read AVELOXIS_TEST_DB must run them through internal/testdb's Main with both
// hooks — prepare (the migrated, bootstrapped deployment the tests rely on) and
// verify (the residue check CI used to run over the shared database). A package
// that reads the variable without it would test against the shared base
// database, where rows an earlier killed run left behind make tests fail, or
// pass, for reasons that have nothing to do with the code. That is how
// TestResolveMirrorLinkByNodeIDRoutesByPrefix came to fail on every run after
// one 40P01.
func TestDBTierPackagesUseTheirOwnDatabase(t *testing.T) {
	root := srctest.Root(t)
	byDir := map[string][]*ast.File{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), p, b, 0)
		if err != nil {
			return err
		}
		dir, _ := filepath.Rel(root, filepath.Dir(p))
		byDir[dir] = append(byDir[dir], f)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	readers := 0
	for dir, files := range byDir {
		reads, callsMain := dbTierStatus(files)
		if !reads {
			continue
		}
		readers++
		if !callsMain {
			t.Errorf("%s: its tests read AVELOXIS_TEST_DB but it has no TestMain calling testdb.Main(m, prepare, verify) with both hooks — add testmain_test.go (see internal/api/testmain_test.go)", dir)
		}
	}
	// Guard the denominator: a walk that stopped finding the DB tier would
	// pass vacuously.
	if reads, _ := dbTierStatus(byDir[filepath.Join("internal", "db")]); !reads {
		t.Fatalf("the scan found no AVELOXIS_TEST_DB reader in internal/db — the walk is broken (found %d reading packages)", readers)
	}
}

// dbTierStatus reports whether a package's test files read AVELOXIS_TEST_DB
// (os.Getenv / os.LookupEnv of the literal, of testdb.EnvVar, or of a const or
// var holding either — the "const testDBEnv = …" idiom) and whether they call
// testdb.Main with three arguments, both hooks non-nil. A string that merely
// mentions the variable — a comment, a skip message — is not a read.
func dbTierStatus(files []*ast.File) (reads, callsMain bool) {
	names := map[string]bool{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, v := range vs.Values {
				if i < len(vs.Names) && namesTheVariable(v, nil) {
					names[vs.Names[i].Name] = true
				}
			}
			return true
		})
	}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}
			if pkg.Name == "os" && (sel.Sel.Name == "Getenv" || sel.Sel.Name == "LookupEnv") && len(call.Args) == 1 && namesTheVariable(call.Args[0], names) {
				reads = true
			}
			if pkg.Name == "testdb" && sel.Sel.Name == "Main" && len(call.Args) == 3 && !isNilIdent(call.Args[1]) && !isNilIdent(call.Args[2]) {
				callsMain = true
			}
			return true
		})
	}
	return reads, callsMain
}

func namesTheVariable(e ast.Expr, names map[string]bool) bool {
	switch x := e.(type) {
	case *ast.BasicLit:
		return x.Value == `"AVELOXIS_TEST_DB"`
	case *ast.SelectorExpr:
		id, ok := x.X.(*ast.Ident)
		return ok && id.Name == "testdb" && x.Sel.Name == "EnvVar"
	case *ast.Ident:
		return names[x.Name]
	}
	return false
}

func isNilIdent(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == "nil"
}

// The analysis on the shapes that matter, including the const idiom the L10
// pass found escaping the first AST version.
func TestDBTierStatusFixtures(t *testing.T) {
	for _, c := range []struct {
		name            string
		src             string
		reads, callMain bool
	}{
		{"literal Getenv", `func TestX(t *testing.T) { _ = os.Getenv("AVELOXIS_TEST_DB") }`, true, false},
		{"LookupEnv", `func TestX(t *testing.T) { _, _ = os.LookupEnv("AVELOXIS_TEST_DB") }`, true, false},
		{"testdb.EnvVar", `func TestX(t *testing.T) { _ = os.Getenv(testdb.EnvVar) }`, true, false},
		{"package const", "const testDBEnv = \"AVELOXIS_TEST_DB\"\nfunc TestX(t *testing.T) { _ = os.Getenv(testDBEnv) }", true, false},
		{"local const", `func TestX(t *testing.T) { const env = "AVELOXIS_TEST_DB"; _ = os.Getenv(env) }`, true, false},
		{"mention only", `func TestX(t *testing.T) { t.Skip("set AVELOXIS_TEST_DB to run") }`, false, false},
		{"both hooks", `func TestMain(m *testing.M) { os.Exit(testdb.Main(m, prepare.Deployment, prepare.Verify)) }`, false, true},
		{"nil verify", `func TestMain(m *testing.M) { os.Exit(testdb.Main(m, prepare.Deployment, nil)) }`, false, false},
		{"nil prepare", `func TestMain(m *testing.M) { os.Exit(testdb.Main(m, nil, prepare.Verify)) }`, false, false},
	} {
		f, err := parser.ParseFile(token.NewFileSet(), "x_test.go", "package p\n"+c.src+"\n", 0)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		reads, callsMain := dbTierStatus([]*ast.File{f})
		if reads != c.reads || callsMain != c.callMain {
			t.Errorf("%s: reads=%v callsMain=%v, want %v %v", c.name, reads, callsMain, c.reads, c.callMain)
		}
	}
}
