// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRequestHandlersLogThroughLogFailure — NET-6 review r4 F1: handlers
// that log a store error themselves logged ERROR/WARN "context deadline
// exceeded" when http_timeout_seconds (or a departed client) ended the
// request; three rounds fixed the helpers one site at a time, and a
// token ratchet could not see a site that never names the context errors.
// In the api, web and monitor packages, a function taking an
// *http.Request that logs an "error" at WARN or ERROR does it through
// logFailure (which drops the request's own end to Debug), or appears here
// with the reason its error cannot be the request's end. The api is also
// pinned per route by TestNoEndpointBlamesTheRequestsEnd.
func TestRequestHandlersLogThroughLogFailure(t *testing.T) {
	reviewed := map[string]string{
		"internal/web/server.go|failed to clear the pending email after a failed confirmation send":       "a detached clean-up (WithoutCancel): its failure leaves the dashboard's pending banner, never a nobody-is-listening case (NET-6 review r7 F1)",
		"internal/web/server.go|failed to send confirmation email":                                        "the mailer takes no context: an SMTP failure is never the request's end (NET-6 review r6 F2)",
		"internal/web/server.go|oauth callback: the forge did not answer within the callback bound":       "logOAuthFailure: its first arm returns (Debug) when reqCtx is done; this is the 30 s callback bound's own expiry on a live request",
		"internal/web/server.go|oauth callback: forge request failed":                                     "logOAuthFailure: its first arm returns (Debug) when reqCtx is done",
		"internal/api/server.go|request failed":                                                           "serverError: guarded by the httpserver.RequestEnded(r.Context(), err) return above it",
		"internal/web/server.go|request failed":                                                           "serverError: guarded by the httpserver.RequestEnded(r.Context(), err) return above it",
		"internal/monitor/monitor.go|request failed":                                                      "serverError: guarded by the httpserver.RequestEnded(r.Context(), err) return above it",
		"internal/api/auth.go|session token could not be resolved — store failure, request refused (503)": "refuseStoreError: guarded by the httpserver.RequestEnded(r.Context(), err) return above it",
		"internal/api/portal.go|group add: some repositories could not be added":                          "db.ErrAddItemsFailed carries counts; a context end takes the default arm (serverError)",
		"internal/api/portal.go|admin add-request approval refused — the org cannot be registered":        "a refusal on the request's content, not a store error",
		"internal/api/forge_id_changes.go|admin forge-ID adopt: request body unreadable":                  "a body read error (EOF, limit), not a context end",
		"internal/web/admin.go|add-request approval refused — the org cannot be registered":               "a refusal on the request's content, not a store error",
		"internal/web/server.go|building the GitHub user request failed":                                  "http.NewRequestWithContext: a malformed request, never a context end",
		"internal/web/server.go|building the GitLab user request failed":                                  "http.NewRequestWithContext: a malformed request, never a context end",
		"internal/web/server.go|github /user response unmarshal failed":                                   "JSON decoding of a body already read",
		"internal/web/server.go|gitlab /user response unmarshal failed":                                   "JSON decoding of a body already read",
		"internal/web/server.go|failed to upsert OAuth user":                                              "guarded: the httpserver.RequestEnded return just above it (pinned by TestLoginLogsAFailedAdminLookup)",
		"internal/web/server.go|admin flag lookup failed at login — session created as non-admin":         "guarded: the httpserver.RequestEnded return just above it (pinned by TestLoginLogsAFailedAdminLookup)",
		"internal/web/server.go|failed to send welcome email":                                             "the mailer's own error; the send runs off the request context",
		"internal/web/server.go|org not added — invalid URL":                                              "URL validation, not a store error",
		"internal/web/server.go|repo page: stats unavailable":                                             "guarded: `err != nil && !httpserver.RequestEnded(err)`",
		"internal/web/server.go|monitor page: repository details unavailable":                             "guarded: `err != nil && !httpserver.RequestEnded(err)`",
		"internal/web/server.go|monitor page: repository stats unavailable":                               "guarded: `err != nil && !httpserver.RequestEnded(err)`",
	}
	root := srctest.Root(t)
	seen := map[string]bool{}
	examined := 0
	for _, dir := range []string{"internal/api", "internal/web", "internal/monitor"} {
		files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			rel, _ := filepath.Rel(root, f)
			rel = filepath.ToSlash(rel)
			fset := token.NewFileSet()
			af, err := parser.ParseFile(fset, f, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range af.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil || !takesHTTPRequest(fd) {
					continue
				}
				examined++
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if _, ok := n.(*ast.FuncLit); ok {
						return false // a goroutine or callback: its own context
					}
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := c.Fun.(*ast.SelectorExpr)
					if !ok || (sel.Sel.Name != "Error" && sel.Sel.Name != "Warn") {
						return true
					}
					// A logger field (s.logger) or a logger variable/parameter
					// (NET-6 review r5 F2: dashboardEmailGate and
					// submitAccountEmail log through a `logger` parameter).
					isLogger := false
					switch x := sel.X.(type) {
					case *ast.SelectorExpr:
						isLogger = x.Sel.Name == "logger"
					case *ast.Ident:
						isLogger = x.Name == "logger"
					}
					if !isLogger || len(c.Args) == 0 {
						return true
					}
					hasErr := false
					for _, a := range c.Args[1:] {
						if bl, ok := a.(*ast.BasicLit); ok && bl.Value == `"error"` {
							hasErr = true
						}
					}
					if !hasErr {
						return true
					}
					msg := "(non-literal message)"
					if bl, ok := c.Args[0].(*ast.BasicLit); ok {
						msg, _ = strconv.Unquote(bl.Value)
					}
					key := rel + "|" + msg
					seen[key] = true
					if _, ok := reviewed[key]; !ok {
						t.Errorf("%s: %s logs an error at %s directly from a request handler (%s); use httpserver.LogFailure so the request's own end (http_timeout_seconds, a departed client) is not blamed, or review it here with the reason", fset.Position(c.Pos()), fd.Name.Name, strings.ToUpper(sel.Sel.Name), msg)
					}
					return true
				})
			}
		}
	}
	for key := range reviewed {
		if !seen[key] {
			t.Errorf("reviewed entry %q no longer matches a site — remove it", key)
		}
	}
	srctest.MinCount(t, "request-handler functions examined", examined, 100)
}

// takesHTTPRequest reports whether a function runs on a request path: it
// takes an *http.Request, or a context.Context (a helper handed
// r.Context(), such as fetchGitHubPrimaryEmail — NET-6 review r5 F2).
func takesHTTPRequest(fd *ast.FuncDecl) bool {
	for _, p := range fd.Type.Params.List {
		if st, ok := p.Type.(*ast.StarExpr); ok {
			if sel, ok := st.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "Request" {
				return true
			}
		}
		if sel, ok := p.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Context" {
			if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" {
				return true
			}
		}
	}
	return false
}

// TestLogFailureClassifiesByTheRequestsContext — NET-6 review r6 F1: a
// LogFailure handed the OAuth callback's own 30 s context downgraded that
// bound's expiry on a live request to Debug. LogFailure's context must be
// the request's: `r.Context()` of the function's *http.Request, or a
// context.Context parameter the function never reassigns (its callers
// pass r.Context(); a caller cannot be checked here, so the parameter's
// name is part of the contract). Anything else is reviewed.
func TestLogFailureClassifiesByTheRequestsContext(t *testing.T) {
	reviewed := map[string]string{
		"internal/web/server.go|render": "no request in scope; a render refused after the bound is http.ErrHandlerTimeout, which RequestEnded recognises on any context",
	}
	root := srctest.Root(t)
	seen := map[string]bool{}
	calls := 0
	for _, dir := range []string{"internal/api", "internal/web", "internal/monitor"} {
		files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			rel, _ := filepath.Rel(root, f)
			rel = filepath.ToSlash(rel)
			fset := token.NewFileSet()
			af, err := parser.ParseFile(fset, f, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range af.Decls {
				fd, ok := d.(*ast.FuncDecl)
				if !ok || fd.Body == nil {
					continue
				}
				reqParams, ctxParams := map[string]bool{}, map[string]bool{}
				for _, p := range fd.Type.Params.List {
					if st, ok := p.Type.(*ast.StarExpr); ok {
						if sel, ok := st.X.(*ast.SelectorExpr); ok && sel.Sel.Name == "Request" {
							for _, n := range p.Names {
								reqParams[n.Name] = true
							}
						}
					}
					if sel, ok := p.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Context" {
						for _, n := range p.Names {
							ctxParams[n.Name] = true
						}
					}
				}
				reassigned := map[string]bool{}
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					if as, ok := n.(*ast.AssignStmt); ok {
						for _, l := range as.Lhs {
							if id, ok := l.(*ast.Ident); ok {
								reassigned[id.Name] = true
							}
						}
					}
					return true
				})
				ast.Inspect(fd.Body, func(n ast.Node) bool {
					c, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := c.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "LogFailure" || len(c.Args) == 0 {
						return true
					}
					if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "httpserver" {
						return true
					}
					calls++
					okCtx := false
					switch a := c.Args[0].(type) {
					case *ast.CallExpr: // r.Context()
						if s2, ok := a.Fun.(*ast.SelectorExpr); ok && s2.Sel.Name == "Context" {
							if id, ok := s2.X.(*ast.Ident); ok && reqParams[id.Name] && !reassigned[id.Name] {
								okCtx = true
							}
						}
					case *ast.Ident:
						okCtx = ctxParams[a.Name] && !reassigned[a.Name]
					}
					key := rel + "|" + fd.Name.Name
					if !okCtx {
						seen[key] = true
						if _, ok := reviewed[key]; !ok {
							t.Errorf("%s: %s passes LogFailure a context that is not the request's (r.Context() or an unreassigned context parameter): a derived or background context misclassifies a live request's failure", fset.Position(c.Pos()), fd.Name.Name)
						}
					}
					return true
				})
			}
		}
	}
	for key := range reviewed {
		if !seen[key] {
			t.Errorf("reviewed entry %q no longer matches a site — remove it", key)
		}
	}
	srctest.MinCount(t, "LogFailure calls examined", calls, 40)
}
