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

// v0.29.91 whole-PR follow-up: boosting a repository reorders everyone's
// collection, so it is an administrator's (or the operator's) operation.
// The web GUI's boost was reachable by any signed-in account. The store's
// PrioritizeRepo has no caller identity (the CLI and serve's own monitor
// have none either), so the rule lives at each deciding caller, and this
// pin scans for the OPERATION — every call of .PrioritizeRepo( in the
// module, found with go/ast as every selector naming it, called or a method
// value — not a route list. Each call site is
// reviewed, and the two account-facing ones are checked for their gate.
func TestEveryPrioritizeCallerIsAnAdminOrOperatorPath(t *testing.T) {
	// Keyed dir|func (srctest.SiteKey, v0.29.98): a split of the file
	// holding a caller does not churn the registry.
	reviewed := map[string]string{
		srctest.SiteKey("internal/api", "handleAdminPrioritizeRepo"): "the API's admin route: s.requireAdmin first (checked below)",
		srctest.SiteKey("internal/web", "handleMonitorPrioritize"):   "the web GUI's boost: registered only behind requireAdmin (checked below)",
		srctest.SiteKey("internal/monitor", "handlePrioritize"):      "serve's own :5555 monitor: no accounts; the operator binds it (--monitor, 127.0.0.1 by default in the docs)",
		srctest.SiteKey("cmd/aveloxis", "runPrioritize"):             "the operator's `aveloxis prioritize` command",
	}
	root := srctest.Root(t)
	found := map[string]int{}
	examined := 0
	for _, dir := range []string{"internal", "cmd", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			examined++
			rel, _ := filepath.Rel(root, path)
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				where := srctest.PackageLevel
				if ok {
					where = fn.Name.Name
				}
				// Every selector naming the method, called or not (a method
				// value such as `boost := s.store.PrioritizeRepo` escaped a
				// call-only match). The declaration in internal/db/queue.go
				// and the interface method in internal/monitor/monitor.go are
				// identifiers, not selectors, so neither is counted.
				ast.Inspect(decl, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "PrioritizeRepo" {
						found[srctest.SiteKey(srctest.SiteDir(rel), where)]++
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
	srctest.MinCount(t, "non-test Go files parsed", examined, 300)
	for site, n := range found {
		if _, ok := reviewed[site]; !ok || n != 1 {
			t.Errorf("%s calls PrioritizeRepo %d time(s) and is not a reviewed site: a boost reorders everyone's collection — gate it on an administrator, then review it here", site, n)
		}
	}
	for site := range reviewed {
		if found[site] != 1 {
			t.Errorf("%s: reviewed, but %d call(s) found — update the list", site, found[site])
		}
	}
	// The two account-facing sites carry their gate.
	api := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/api/portal.go"), "func (s *Server) handleAdminPrioritizeRepo("))
	if gate, call := strings.Index(api, "s.requireAdmin(w, r)"), strings.Index(api, ".PrioritizeRepo("); gate < 0 || gate > call {
		t.Error("handleAdminPrioritizeRepo must check requireAdmin before PrioritizeRepo")
	}
	web := srctest.StripGoComments(srctest.PackageSource(t, "internal/web", 4))
	if strings.Count(web, "s.handleMonitorPrioritize") != 1 || !strings.Contains(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("), "s.requireAdmin(s.handleMonitorPrioritize)") {
		t.Error("handleMonitorPrioritize must be referenced exactly once, registered behind requireAdmin")
	}
}
