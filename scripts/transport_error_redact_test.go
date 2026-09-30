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

// TestEveryPlatformDoRedactsItsError — v0.29.71 review round 2 F1: a
// transport failure's *url.Error quotes the request URL (a searched email,
// a repository_url query, credentials), so every `resp, err := x.Do(req)`
// in internal/platform is followed at once by `err =
// RedactTransportError(err)`. The HTTP client's is proven at runtime
// (TestTransportErrorsCarryNoSearchedAddress); this pins the other clients.
// The denominator is every such Do examined.
func TestEveryPlatformDoRedactsItsError(t *testing.T) {
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
		for _, line := range unredactedDoSites(t, rel, string(src), &examined) {
			t.Errorf("%s:%d: a request's Do is not followed by err = RedactTransportError(err)", rel, line)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	srctest.MinCount(t, "Do calls examined in internal/platform", examined, 6)
}

// unredactedDoSites returns the line of every one-argument `.Do(x)` call
// (a function-literal argument — sync.Once — is not a request) that is not
// `a, err := x.Do(y)` (or `=`) directly in a block with the next statement
// `err = RedactTransportError(err)` on that same error variable. Every
// other form — an if-init, a return, a nested expression — is flagged
// (review round 3: those were never examined).
func unredactedDoSites(t testing.TB, name, src string, examined *int) []int {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	redacted := map[*ast.CallExpr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		blk, ok := n.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i, st := range blk.List {
			as, ok := st.(*ast.AssignStmt)
			if !ok || len(as.Lhs) != 2 || len(as.Rhs) != 1 {
				continue
			}
			call, ok := as.Rhs[0].(*ast.CallExpr)
			if !ok || !isRequestDo(call) {
				continue
			}
			errID, ok := as.Lhs[1].(*ast.Ident)
			if ok && i+1 < len(blk.List) && isRedactTransportAssign(blk.List[i+1], errID.Name) {
				redacted[call] = true
			}
		}
		return true
	})
	var out []int
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isRequestDo(call) {
			return true
		}
		*examined++
		if !redacted[call] {
			out = append(out, fset.Position(call.Pos()).Line)
		}
		return true
	})
	return out
}

// isRequestDo is a one-argument .Do call whose argument is not a function
// literal (sync.Once.Do takes one).
func isRequestDo(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Do" || len(call.Args) != 1 {
		return false
	}
	_, isLit := call.Args[0].(*ast.FuncLit)
	return !isLit
}

// isRedactTransportAssign is `errName = RedactTransportError(errName)`,
// bare or package-qualified, on the same variable both sides.
func isRedactTransportAssign(st ast.Stmt, errName string) bool {
	as, ok := st.(*ast.AssignStmt)
	if !ok || as.Tok != token.ASSIGN || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
		return false
	}
	if lhs, ok := as.Lhs[0].(*ast.Ident); !ok || lhs.Name != errName {
		return false
	}
	call, ok := as.Rhs[0].(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	if arg, ok := call.Args[0].(*ast.Ident); !ok || arg.Name != errName {
		return false
	}
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name == "RedactTransportError"
	case *ast.SelectorExpr:
		return fn.Sel.Name == "RedactTransportError"
	}
	return false
}

// TestPlatformDoCorpus proves the check both ways.
func TestPlatformDoCorpus(t *testing.T) {
	src := `package p
func a(c *C, req *R) {
	resp, err := c.http.Do(req)
	err = platform.RedactTransportError(err)
	_ = resp
}
func b(c *C, req *R) {
	resp, err := c.inner.Do(req)
	_, _ = resp, err
}
func d(once *O) { once.Do(func() {}) }
func e(c *C, req *R) error {
	if resp, err := c.http.Do(req); err != nil {
		return err
	}
	return nil
}
func g(c *C, req *R) (*Resp, error) { return c.http.Do(req) }
func h(c *C, req *R) {
	resp, err := c.http.Do(req)
	err = platform.RedactTransportError(nil)
	_, _ = resp, err
}
`
	n := 0
	got := unredactedDoSites(t, "corpus.go", src, &n)
	if n != 5 || len(got) != 4 || got[0] != 8 || got[1] != 13 || got[2] != 18 || got[3] != 20 {
		t.Errorf("examined %d, flagged %v; want 5 examined and lines 8 (no redaction), 13 (if-init), 18 (return), 20 (wrong argument)", n, got)
	}
}
