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

// TestNoTokenIsSlicedForALog — CodeQL alert 201 (PR #220): the key pool
// named keys by token[:8] and add-key by token[:4]+"..."+token[len-4:],
// both carrying secret characters into logs. A key is named ONLY by
// platform.TokenHash (SR-17), so no non-test source slices a value whose
// name says it holds a token (Token, token, githubToken, …). The ban is on
// the operation; the denominator is every slice expression examined.
// Decided boundary: the check is by name, without types, so it also flags
// a slice of a LIST named like tokens (tokens[1:]), which exposes no
// token's characters; the safer error direction was kept — name such a
// list for what it holds (fields, parts, words).
func TestNoTokenIsSlicedForALog(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"cmd", "internal"} {
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
			f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				t.Fatalf("parse %s: %v", rel, perr)
			}
			for _, h := range tokenSlices(fset, f, &examined) {
				t.Errorf("%s:%d slices %s — name a key with platform.TokenHash, never by its characters", rel, h.line, h.name)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "slice expressions examined", examined, 200)
}

type tokenSlice struct {
	name string
	line int
}

// tokenSlices reports every slice expression whose operand is named like a
// token: an identifier, or a selector chain ending in one.
func tokenSlices(fset *token.FileSet, f *ast.File, examined *int) []tokenSlice {
	var out []tokenSlice
	ast.Inspect(f, func(n ast.Node) bool {
		s, ok := n.(*ast.SliceExpr)
		if !ok {
			return true
		}
		*examined++
		var name string
		switch x := s.X.(type) {
		case *ast.Ident:
			name = x.Name
		case *ast.SelectorExpr:
			name = x.Sel.Name
		case *ast.ParenExpr:
			if id, ok := x.X.(*ast.Ident); ok {
				name = id.Name
			}
		}
		if strings.Contains(strings.ToLower(name), "token") {
			out = append(out, tokenSlice{name, fset.Position(s.Pos()).Line})
		}
		return true
	})
	return out
}

// TestTokenSliceCorpus proves the check both ways.
func TestTokenSliceCorpus(t *testing.T) {
	src := `package p
func a(token string) string { return token[:4] + "..." + token[len(token)-4:] }
func b(k struct{ Token string }) string { return k.Token[:8] }
func c(githubToken string) string { return (githubToken)[:8] }
func d(tokens []string) []string { return tokens[1:] }
func clean(key string) string { return key[:1] }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "corpus.go", src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	got := map[string]int{}
	for _, h := range tokenSlices(fset, f, &n) {
		got[h.name]++
	}
	for name, want := range map[string]int{"token": 2, "Token": 1, "githubToken": 1, "tokens": 1} {
		if got[name] != want {
			t.Errorf("%s: flagged %d times, want %d", name, got[name], want)
		}
	}
	if got["key"] != 0 {
		t.Error("a slice of a non-token value was flagged")
	}
	if n != 6 {
		t.Errorf("examined %d slice expressions, want 6", n)
	}
}
