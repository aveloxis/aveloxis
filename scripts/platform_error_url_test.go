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

// TestPlatformErrorsRedactTheirURL — v0.29.71 (personal data in INFO logs):
// the forge client's errors embed the request URL, and they reach log lines
// as the error attribute, where TestEveryURLLogAttributeIsRedacted cannot
// see them. A search URL carries an author's email in its query and a
// stored URL can carry credentials, so every fmt.Errorf argument in
// internal/platform named as a URL (isURLNamedValue) goes through
// RedactURLUserinfo. The denominator is every fmt.Errorf examined.
func TestPlatformErrorsRedactTheirURL(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	err := filepath.WalkDir(filepath.Join(root, "internal", "platform"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		for _, line := range unredactedURLErrorArgs(t, rel, string(src), &examined) {
			t.Errorf("%s:%d: an fmt.Errorf argument named as a URL is not wrapped in RedactURLUserinfo — the error reaches a log line", rel, line)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	srctest.MinCount(t, "fmt.Errorf calls examined in internal/platform", examined, 100)
}

// unredactedURLErrorArgs returns the line of every fmt.Errorf argument
// named as a URL whose value is not redacted (redactedValue).
func unredactedURLErrorArgs(t testing.TB, name, src string, examined *int) []int {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	var out []int
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Errorf" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "fmt" {
			return true
		}
		*examined++
		for _, a := range call.Args[1:] {
			if isURLNamedValue(a) && !isErrorValue(a) && !redactedValue(a) {
				out = append(out, fset.Position(a.Pos()).Line)
			}
		}
		return true
	})
	return out
}

// isErrorValue says an argument is an error value by its name (a sentinel
// such as ErrInvalidRepoURL, or err), which a "…URL" suffix does not make a
// URL.
func isErrorValue(e ast.Expr) bool {
	var name string
	switch x := e.(type) {
	case *ast.Ident:
		name = x.Name
	case *ast.SelectorExpr:
		name = x.Sel.Name
	default:
		return false
	}
	return strings.HasPrefix(name, "Err") || strings.HasPrefix(name, "err")
}

// TestPlatformErrorURLCorpus proves the check both ways.
func TestPlatformErrorURLCorpus(t *testing.T) {
	src := `package p
import "fmt"
func a(url, newURL, body string, err error) {
	_ = fmt.Errorf("%w: %s", err, url)
	_ = fmt.Errorf("%w: %s (to %s)", err, RedactURLUserinfo(url), newURL)
	_ = fmt.Errorf("%w: %s", err, RedactURLUserinfo(url))
	_ = fmt.Errorf("%s: %s", body, "literal")
	_ = fmt.Errorf("%w: missing host", ErrInvalidRepoURL)
}
`
	n := 0
	got := unredactedURLErrorArgs(t, "corpus.go", src, &n)
	if len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Errorf("flagged lines %v, want [4 5] (bare url; bare newURL next to a redacted url)", got)
	}
	if n != 5 {
		t.Errorf("examined %d fmt.Errorf calls, want 5", n)
	}
}
