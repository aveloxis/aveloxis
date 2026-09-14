// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// reconcileCallers is every production function allowed to call
// KeyPool.Reconcile (v0.30.0 Phase C, live key reload). Reconcile replaces a
// pool's tokens wholesale, so the pairing of tokens with a pool IS the
// cross-instance key-leak boundary — and TestKeyedClientBaseURLAllowlist only
// sees constructor calls. Pairing lives in exactly two places: the GitLab
// router, entry by entry with that entry's own partition slice, and the
// forgekeys GitHub reconcile, the one pool that has no instances. Which
// GitHub pool reaches it (serve's and web's own) is wiring, pinned by
// TestServeWiresKeyMaintainer. A new caller is a new pairing to review.
var reconcileCallers = map[string]bool{
	"internal/platform/gitlab/instances.go:ReconcileKeys": true,
	"internal/forgekeys/maintainer.go:reconcileGitHub":    true,
}

func TestKeyPoolReconcileHasOnlyReviewedCallers(t *testing.T) {
	root := srctest.Root(t)
	found := map[string]bool{}
	examined := 0
	for _, top := range []string{"internal", "cmd", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			examined++
			// Package-level declarations are walked too: a Reconcile call in
			// a var initializer has no enclosing function and is always a
			// finding.
			for _, decl := range f.Decls {
				owner := "(package level)"
				if fd, ok := decl.(*ast.FuncDecl); ok {
					owner = fd.Name.Name
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.SelectorExpr:
						if x.Sel.Name != "Reconcile" {
							return true
						}
						site := rel + ":" + owner
						if !reconcileCallers[site] {
							t.Errorf("%s: %s references Reconcile — only %v may pair tokens with a key pool", fset.Position(x.Pos()), owner, sortedKeys(reconcileCallers))
						}
						found[site] = true
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files under internal/, cmd/ and scripts/", examined, 300)
	for site := range reconcileCallers {
		if !found[site] {
			t.Errorf("allowlisted Reconcile caller %s no longer calls it — shrink the allowlist", site)
		}
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
