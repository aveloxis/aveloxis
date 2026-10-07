// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The identity decides whether an answer is per-caller, and a per-caller
// answer is marked `private, no-store` by the function that reads it —
// never by each handler on its own (PR #226 review 5408306640 on /me; L10
// rounds 1–3 of 0.29.75 found the class one route at a time: groups, admin
// users, search, compare, entity search, the compare snapshot). So a read
// of the identity — any `.Value(authCtxKey{})` call, whatever wraps it
// (round 3: the two-step read and the nil probe escaped a pin on the type
// assertion alone) — is allowed only inside the named functions, each of
// which marks or is reached only through one that marks; a handler reads
// it through them. The scan walks every function of the package, func
// literals attributed to their enclosing declaration and package-level
// literals refused outright; the denominator is every read in the package.
func TestIdentityReadsGoThroughTheMarkingHelpers(t *testing.T) {
	allowed := map[string]string{
		"requireUser":        "marks on success",
		"callerIdentity":     "marks when present",
		"authorizeRepo":      "marks when it adds the caller's one-time Shared-with-Me notice; its refusal carries no caller data; the cached routes re-mark their own answers",
		"recordComparison":   "writes a side record and nothing to the response; its two callers (compare, snapshot) mark through callerIdentity",
		"resolveEntityRepos": "marks a signed-in caller's answer itself (the scope decision lives here)",
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var all []byte
	reads := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, src...)
		if !strings.Contains(string(src), "authCtxKey{}") {
			continue
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		var stack []string // enclosing FuncDecl names; empty = package level
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case nil:
				return false
			case *ast.FuncDecl:
				stack = append(stack, x.Name.Name)
				if x.Body != nil {
					ast.Inspect(x.Body, func(m ast.Node) bool { return inspectIdentityRead(t, f, m, stack, allowed, &reads) })
				}
				stack = stack[:len(stack)-1]
				return false
			}
			return inspectIdentityRead(t, f, n, stack, allowed, &reads)
		})
	}
	if reads < 5 {
		t.Fatalf("expected the package's identity reads to be scanned, found %d", reads)
	}
	for name := range allowed {
		if !strings.Contains(string(all), "func "+name+"(") && !strings.Contains(string(all), ") "+name+"(") {
			t.Errorf("allowlist names %s, which no longer exists", name)
		}
	}
}

// inspectIdentityRead reports a `.Value(authCtxKey{})` call at n against
// the enclosing declaration.
func inspectIdentityRead(t *testing.T, file string, n ast.Node, stack []string, allowed map[string]string, reads *int) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return true
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Value" {
		return true
	}
	if cl, ok := call.Args[0].(*ast.CompositeLit); !ok || !isIdent(cl.Type, "authCtxKey") {
		return true
	}
	*reads++
	where := "a package-level function literal"
	if len(stack) > 0 {
		where = stack[len(stack)-1]
	}
	if _, ok := allowed[where]; !ok {
		t.Errorf("%s: %s reads the caller's identity directly — read it through requireUser (required) or callerIdentity (optional), which mark a per-caller answer no-store; or allowlist the function here with the reason", file, where)
	}
	return true
}

func isIdent(e ast.Expr, name string) bool {
	id, ok := e.(*ast.Ident)
	return ok && id.Name == name
}
