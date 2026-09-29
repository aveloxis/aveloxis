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

// TestHTTPServersComeFromHTTPServer — NET-6 (2026-09-29): three http.Server
// literals set no timeouts. Every server is built by internal/httpserver
// (the bound from http_timeout_seconds). Review r1 F3: a substring match on
// "http.Server{" let http.ListenAndServe, http.Serve and new(http.Server)
// through, each a server with no timeouts; the AST is checked instead.
func TestHTTPServersComeFromHTTPServer(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"cmd", "internal"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if filepath.ToSlash(filepath.Dir(rel)) == "internal/httpserver" {
				return nil
			}
			examined++
			for _, what := range timeoutlessServers(t, path) {
				t.Errorf("%s: %s starts or builds an http.Server without the timeouts; use internal/httpserver", rel, what)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined for http.Server construction", examined, 200)
}

// timeoutlessServers lists every construction of an http.Server in a file
// that does not go through internal/httpserver.
func timeoutlessServers(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	httpName := ""
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) == "net/http" {
			httpName = "http"
			if imp.Name != nil {
				httpName = imp.Name.Name
			}
		}
	}
	if httpName == "" {
		return nil
	}
	isHTTP := func(e ast.Expr, names ...string) bool {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Name != httpName {
			return false
		}
		for _, n := range names {
			if sel.Sel.Name == n {
				return true
			}
		}
		return false
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CompositeLit:
			if isHTTP(x.Type, "Server") {
				out = append(out, fset.Position(x.Pos()).String()+" http.Server literal")
			}
		case *ast.CallExpr:
			if isHTTP(x.Fun, "ListenAndServe", "ListenAndServeTLS", "Serve", "ServeTLS") {
				out = append(out, fset.Position(x.Pos()).String()+" "+httpName+"."+x.Fun.(*ast.SelectorExpr).Sel.Name)
			}
			if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 && isHTTP(x.Args[0], "Server") {
				out = append(out, fset.Position(x.Pos()).String()+" new(http.Server)")
			}
		case *ast.ValueSpec:
			if x.Type != nil && isHTTP(x.Type, "Server") {
				out = append(out, fset.Position(x.Pos()).String()+" var of type http.Server")
			}
		}
		return true
	})
	return out
}

// TestTimeoutlessServersCorpus — the detector's own cases (the r1 F3
// escapes included).
func TestTimeoutlessServersCorpus(t *testing.T) {
	for src, want := range map[string]int{
		"package p\nimport \"net/http\"\nfunc f(h http.Handler) { _ = http.ListenAndServe(\":0\", h) }": 1,
		"package p\nimport \"net/http\"\nfunc f() { _ = new(http.Server) }":                             1,
		"package p\nimport \"net/http\"\nvar s http.Server":                                             1,
		"package p\nimport \"net/http\"\nfunc f() { _ = &http.Server{} }":                               1,
		"package p\nimport nh \"net/http\"\nfunc f(h nh.Handler) { _ = nh.Serve(nil, h) }":              1,
		"package p\nimport \"net/http\"\nfunc f(s *http.Server) { _ = s.ListenAndServe() }":             0,
		"package p\nimport \"net/http\"\nfunc f() *http.Client { return &http.Client{} }":               0,
	} {
		p := filepath.Join(t.TempDir(), "x.go")
		if err := os.WriteFile(p, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
		if got := len(timeoutlessServers(t, p)); got != want {
			t.Errorf("%q: %d findings, want %d", src, got, want)
		}
	}
}
