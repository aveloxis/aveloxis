// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package testdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// testdb and testdb/prepare are test support: they create and drop databases,
// and prepare signs up a bootstrap admin. Linked into production, the latter
// would make `_testdb_bootstrap_admin` — a login nobody can sign in as — the
// first signup and so the admin of a fresh deployment (L10 round 11). No
// non-test file outside the subtree may import them (parsed import paths, as
// TestSrctestIsTestOnly does), and db.PrepareTestDeployment, exported because
// prepare needs it, has exactly the two reviewed callers.
func TestTestdbIsTestOnly(t *testing.T) {
	root := srctest.Root(t)
	const banned = "github.com/aveloxis/aveloxis/internal/testdb"
	allowedCallers := map[string]bool{
		filepath.Join("internal", "testdb", "prepare", "prepare.go"): true,
		filepath.Join("internal", "db", "testmain_test.go"):          true,
	}
	scanned := 0
	seenIn := map[string]int{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "_build") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		f, perr := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if perr != nil {
			t.Errorf("parse %s: %v", rel, perr)
			return nil
		}
		scanned++
		inSubtree := strings.HasPrefix(rel, filepath.Join("internal", "testdb")+string(filepath.Separator))
		if !strings.HasSuffix(path, "_test.go") && !inSubtree {
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				if p == banned || strings.HasPrefix(p, banned+"/") {
					t.Errorf("%s: NON-TEST file imports %s — test support only", rel, p)
				}
			}
		}
		// Every reference, called or not: a method value
		// (`prep := s.PrepareTestDeployment`) escaped a calls-only count
		// (L10 round 12). The declaration itself is a FuncDecl name, not a
		// selector, so it is not counted.
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "PrepareTestDeployment" {
				return true
			}
			seenIn[rel]++
			if !allowedCallers[rel] {
				t.Errorf("%s references PrepareTestDeployment — only testdb/prepare and internal/db's TestMain may (it signs up a bootstrap admin)", rel)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Guard the denominator: each reviewed caller is seen exactly once.
	if scanned < 100 {
		t.Fatalf("scanned only %d files — the walk is broken", scanned)
	}
	for file := range allowedCallers {
		if seenIn[file] != 1 {
			t.Errorf("%s references PrepareTestDeployment %d times, want exactly 1 — the reviewed callers changed", file, seenIn[file])
		}
	}
}
