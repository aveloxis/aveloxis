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

// TestWritableFileClosesAreChecked — old problem O3 (2026-09-29): on a file
// opened for writing, Close is where a delayed write error (a full disk, a
// network filesystem) surfaces, so a discarded Close can report a write as
// done that never reached the disk. The staged scorecard binary was
// chmodded and renamed into place after an unchecked Close, and the
// long-jobs watchdog and the shell-profile append deferred theirs. A
// writable *os.File's Close must be checked, except on a path that is
// already returning a failure (the write error is the one reported) and
// in the reviewed exceptions below.
func TestWritableFileClosesAreChecked(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
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
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", rel, perr)
			}
			for _, v := range uncheckedWritableCloses(fset, f, &examined) {
				if reason, ok := writableCloseExceptions[filepath.ToSlash(rel)+" "+v.fn]; ok && reason != "" {
					continue
				}
				t.Errorf("%s:%d: %s.Close() on a file opened for writing is discarded in %s — check it (a delayed write error surfaces there)", rel, v.line, v.name, v.fn)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// The denominator: files opened for writing that the check examined.
	srctest.MinCount(t, "writable files examined", examined, 5)
}

// writableCloseExceptions are reviewed discards, keyed "path function".
var writableCloseExceptions = map[string]string{
	"cmd/aveloxis/main.go startComponent": "the log file is the CHILD's stdout/stderr; the parent never writes to it, so its copy's Close has no write to report",
}

type writableClose struct {
	name, fn string
	line     int
}

// uncheckedWritableCloses returns the discarded Close calls on variables
// assigned from os.Create, os.CreateTemp or os.OpenFile with a write flag.
// A deferred Close is always reported; a bare `x.Close()` statement is
// allowed only when its block ends in a return whose last result is not
// nil (a failure path).
func uncheckedWritableCloses(fset *token.FileSet, f *ast.File, examined *int) []writableClose {
	var out []writableClose
	check := func(fn string, body *ast.BlockStmt) {
		writable := map[string]bool{}
		ast.Inspect(body, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok && n != ast.Node(body) {
				return true // closures share the enclosing function's files
			}
			as, ok := n.(*ast.AssignStmt)
			if !ok || len(as.Rhs) != 1 || len(as.Lhs) == 0 {
				return true
			}
			if call, ok := as.Rhs[0].(*ast.CallExpr); ok && opensForWriting(call) {
				if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name != "_" {
					writable[id.Name] = true
					*examined++
				}
			}
			return true
		})
		if len(writable) == 0 {
			return
		}
		ast.Inspect(body, func(n ast.Node) bool {
			blk, ok := n.(*ast.BlockStmt)
			if !ok {
				return true
			}
			for _, st := range blk.List {
				var call *ast.CallExpr
				deferred := false
				switch s := st.(type) {
				case *ast.DeferStmt:
					call, deferred = s.Call, true
				case *ast.ExprStmt:
					call, _ = s.X.(*ast.CallExpr)
				}
				name := closedVar(call)
				if name == "" || !writable[name] {
					continue
				}
				if !deferred && endsInFailureReturn(blk) {
					continue
				}
				out = append(out, writableClose{name, fn, fset.Position(call.Pos()).Line})
			}
			return true
		})
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
			check(fd.Name.Name, fd.Body)
		}
	}
	return out
}

func opensForWriting(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "os" {
		return false
	}
	switch sel.Sel.Name {
	case "Create", "CreateTemp":
		return true
	case "OpenFile":
		if len(call.Args) < 2 {
			return false
		}
		writes := false
		ast.Inspect(call.Args[1], func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok {
				switch s.Sel.Name {
				case "O_WRONLY", "O_RDWR", "O_APPEND", "O_CREATE", "O_TRUNC":
					writes = true
				}
			}
			return true
		})
		return writes
	}
	return false
}

func closedVar(call *ast.CallExpr) string {
	if call == nil || len(call.Args) != 0 {
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Close" {
		return ""
	}
	if id, ok := sel.X.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func endsInFailureReturn(blk *ast.BlockStmt) bool {
	if len(blk.List) == 0 {
		return false
	}
	ret, ok := blk.List[len(blk.List)-1].(*ast.ReturnStmt)
	if !ok || len(ret.Results) == 0 {
		return false
	}
	last, ok := ret.Results[len(ret.Results)-1].(*ast.Ident)
	return !ok || last.Name != "nil"
}

// TestWritableCloseCheckerCorpus proves the checker both ways.
func TestWritableCloseCheckerCorpus(t *testing.T) {
	src := `package p
import "os"
func deferred() { f, _ := os.OpenFile("x", os.O_APPEND|os.O_WRONLY, 0); defer f.Close(); f.Write(nil) }
func success() error { f, _ := os.Create("x"); f.Write(nil); f.Close(); return nil }
func failure() error { f, err := os.CreateTemp("", "x"); if err != nil { f.Close(); return err }; return f.Close() }
func readOnly() { f, _ := os.OpenFile("x", os.O_RDONLY, 0); defer f.Close() }
func opened() { f, _ := os.Open("x"); defer f.Close() }
func checked() error { f, _ := os.Create("x"); if err := f.Close(); err != nil { return err }; return nil }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "corpus.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	got := map[string]bool{}
	for _, v := range uncheckedWritableCloses(fset, f, &n) {
		got[v.fn] = true
	}
	for _, want := range []string{"deferred", "success"} {
		if !got[want] {
			t.Errorf("%s: a discarded writable Close was not flagged", want)
		}
	}
	for _, clean := range []string{"failure", "readOnly", "opened", "checked"} {
		if got[clean] {
			t.Errorf("%s: flagged, want clean", clean)
		}
	}
	if n != 4 {
		t.Errorf("examined %d writable files, want 4 (deferred, success, failure, checked)", n)
	}
}
