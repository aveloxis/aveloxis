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

// TestEveryRowsLoopChecksErr — old problem O1 (2026-09-29, the
// CODING-STANDARDS conformance scan, rule DB-2): a `for rows.Next()` loop
// ends on the LAST row or on an ERROR, and only rows.Err() tells them apart,
// so a connection dropped mid-result returned a truncated list as if it were
// complete (affiliations.go additionally marked its half-loaded data
// "loaded" for the process lifetime). Every function in non-test Go that
// iterates `x.Next()` must call `x.Err()` on the same variable.
func TestEveryRowsLoopChecksErr(t *testing.T) {
	root := srctest.Root(t)
	loops := 0
	for _, dir := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || d.Name() == "_build") {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			for _, miss := range rowsLoopsWithoutErr(t, path, &loops) {
				t.Errorf("%s: %s", rel, miss)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "rows.Next() loops examined", loops, 100)
}

// rowsLoopsWithoutErr lists the `for x.Next()` loops in a file whose
// enclosing function (or function literal) never calls x.Err().
func rowsLoopsWithoutErr(t *testing.T, path string, loops *int) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var out []string
	check := func(body *ast.BlockStmt) {
		if body == nil {
			return
		}
		errCalls := map[string]bool{}
		type loop struct {
			name string
			pos  token.Pos
		}
		var found []loop
		ast.Inspect(body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.FuncLit:
				return false // checked on its own
			case *ast.ForStmt:
				if c, ok := x.Cond.(*ast.CallExpr); ok && len(c.Args) == 0 {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Next" {
						if id, ok := sel.X.(*ast.Ident); ok {
							found = append(found, loop{id.Name, x.Pos()})
						}
					}
				}
			case *ast.CallExpr:
				if sel, ok := x.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Err" && len(x.Args) == 0 {
					if id, ok := sel.X.(*ast.Ident); ok {
						errCalls[id.Name] = true
					}
				}
			}
			return true
		})
		for _, l := range found {
			*loops++
			if !errCalls[l.name] {
				out = append(out, fset.Position(l.pos).String()+": `for "+l.name+".Next()` with no "+l.name+".Err() check: a result cut short by an error reads as complete")
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			check(x.Body)
		case *ast.FuncLit:
			check(x.Body)
		}
		return true
	})
	return out
}
