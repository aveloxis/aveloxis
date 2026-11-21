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
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The store's one seam on what it sends (internal/db/stmttrace.Wrap) is nil
// in every binary, and the write-policy judge that sets it, with the
// PostgreSQL parser it links, is in none (0.29.213). Over every non-test Go
// file of the module:
//
//   - only the one file that opens pools and connections (internal/db: every
//     opener, 0.29.214) reads the seam, and only the judge gives it a value;
//   - only the DB-tier test harness (internal/testdb) imports the judge, and
//     installs it;
//   - nothing outside internal/testdb imports the harness.
//
// A _test.go file may do any of these: test code is in no binary.
func TestTheStatementSeamIsSetOnlyByTheTestHarness(t *testing.T) {
	const (
		module   = "github.com/aveloxis/aveloxis/"
		seamPkg  = module + "internal/db/stmttrace"
		judgePkg = module + "internal/testdb/writejudge"
		harness  = module + "internal/testdb"
		// (Named in two parts: this test walks the tree, it does not read
		// that file by name.)
		storeFile = "internal/db/" + "open" + ".go"
		judgeDir  = "internal/testdb/writejudge/"
		harnessIn = "internal/testdb/"
	)
	root := srctest.Root(t)
	files, seamReads, seamWrites, judgeImports, installs := 0, 0, 0, 0, 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if name := d.Name(); p != root && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" || name == "node_modules" || rel == "docs") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, rel, b, 0)
		if err != nil {
			return err
		}
		files++
		// The names this file gives the three packages.
		local := map[string]string{}
		for _, imp := range af.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			name := path[strings.LastIndex(path, "/")+1:]
			if imp.Name != nil {
				name = imp.Name.Name
			}
			switch {
			case path == seamPkg:
				local[name] = "seam"
				if rel != storeFile && !strings.HasPrefix(rel, judgeDir) {
					t.Errorf("%s imports the statement seam: only the store's pool construction (%s) reads it and only the judge sets it", rel, storeFile)
				}
			case path == judgePkg:
				local[name] = "judge"
				judgeImports++
				if !strings.HasPrefix(rel, harnessIn) {
					t.Errorf("%s imports the write-policy judge, which links the PostgreSQL parser: only the DB-tier test harness and tests may", rel)
				}
			case path == harness || strings.HasPrefix(path, harness+"/"):
				if !strings.HasPrefix(rel, harnessIn) {
					t.Errorf("%s imports %s: the DB-tier test harness is for tests", rel, path)
				}
			}
			if imp.Name != nil && imp.Name.Name == "." && (path == seamPkg || path == judgePkg) {
				t.Errorf("%s dot-imports %s: its names could not be followed", rel, path)
			}
		}
		isSeam := func(e ast.Expr) bool {
			for {
				if p, ok := e.(*ast.ParenExpr); ok {
					e = p.X
					continue
				}
				break
			}
			sel, ok := e.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Wrap" {
				return false
			}
			id, ok := sel.X.(*ast.Ident)
			return ok && local[id.Name] == "seam"
		}
		written := map[ast.Expr]bool{}
		write := func(e ast.Expr, how string) {
			if !isSeam(e) {
				return
			}
			written[e] = true
			seamWrites++
			if !strings.HasPrefix(rel, judgeDir) {
				t.Errorf("%s: the statement seam is %s: only the write-policy judge gives it a value, and no binary holds the judge", fset.Position(e.Pos()), how)
			}
		}
		ast.Inspect(af, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.AssignStmt:
				for _, l := range x.Lhs {
					write(l, "assigned")
				}
			case *ast.UnaryExpr:
				if x.Op == token.AND {
					write(x.X, "addressed")
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "Install" || sel.Sel.Name == "InstallChild") {
					if id, ok := sel.X.(*ast.Ident); ok && local[id.Name] == "judge" {
						installs++
						if !strings.HasPrefix(rel, harnessIn) {
							t.Errorf("%s: installs the write-policy judge outside the DB-tier test harness", fset.Position(x.Pos()))
						}
					}
				}
			}
			return true
		})
		ast.Inspect(af, func(n ast.Node) bool {
			if e, ok := n.(ast.Expr); ok && isSeam(e) && !written[e] {
				seamReads++
				if rel != storeFile {
					t.Errorf("%s: reads the statement seam: only %s does", fset.Position(e.Pos()), storeFile)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The denominators: the store tests the seam and calls it, and tests it
	// once more before it wraps a caller's BeforeConnect hook (0.29.215);
	// the judge assigns it once; the harness imports the judge and installs
	// it twice (a package's own run; a re-executed test binary's, 0.29.215).
	if files < 300 || seamReads != 3 || seamWrites != 1 || judgeImports != 1 || installs != 2 {
		t.Errorf("%d production files read; the seam is read %d times (want 3: the store's two tests of it and its call), given a value %d times (want 1: the judge's Install), the judge imported by %d production files and installed %d times (want 2 each: internal/testdb)",
			files, seamReads, seamWrites, judgeImports, installs)
	}
}
