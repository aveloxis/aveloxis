// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The route matrix (v0.29.98, stage 0.5 P-2 guard 1; permanent): every
// route Handler registers, with the gate it registers it behind, is READ
// FROM Handler's own source (go/ast: pattern → requireAuth / requireAdmin
// / public), so the expected table and the registration are one list
// (L13), and then exercised at runtime through the real Handler:
//
//   - anonymous: every gated route answers 302 /login before its handler;
//   - a signed-in non-administrator: every requireAdmin route answers
//     403 before its handler runs; a requireAuth route reaches its
//     handler; a public route never answers the admin gate's 403.
//
// A public route is not checked anonymously: its handler decides (the
// home page and logout redirect to /login themselves). Both gates answer
// before the handler, so the store is nil here; a handler that IS reached
// and touches the store panics, which the matrix reads as "reached"
// (recovered). Each request carries its own fresh session: logout, a
// public route, deletes the session it is sent. Stage 2's
// method-and-path routing and CSRF edit this table through Handler, and
// the split of server.go must leave it green.

type routeGate struct{ pattern, gate string }

func registeredRoutes(t *testing.T) []routeGate {
	t.Helper()
	_, file := srctest.PackageFuncAt(t, "internal/web", "func (s *Server) Handler(")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(srctest.Root(t), filepath.FromSlash(file)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var routes []routeGate
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "Handler" || fd.Recv == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (sel.Sel.Name != "HandleFunc" && sel.Sel.Name != "Handle") {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "mux" {
				return true
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok {
				t.Fatalf("Handler registers a route with a non-literal pattern at %s", fset.Position(call.Pos()))
			}
			pattern, _ := strconv.Unquote(lit.Value)
			gate := "public"
			if wrap, ok := call.Args[1].(*ast.CallExpr); ok {
				if ws, ok := wrap.Fun.(*ast.SelectorExpr); ok && (ws.Sel.Name == "requireAuth" || ws.Sel.Name == "requireAdmin") {
					gate = ws.Sel.Name
				}
			}
			routes = append(routes, routeGate{pattern, gate})
			return true
		})
	}
	srctest.MinCount(t, "routes registered by Handler", len(routes), 20)
	return routes
}

var routeParam = regexp.MustCompile(`\{[^}]*\}`)

// requestFor turns a ServeMux pattern into one request that matches it.
func requestFor(pattern string) *http.Request {
	method, path := http.MethodGet, pattern
	if i := strings.Index(pattern, " "); i > 0 {
		method, path = pattern[:i], pattern[i+1:]
	}
	path = routeParam.ReplaceAllStringFunc(path, func(p string) string {
		if p == "{$}" {
			return ""
		}
		return "1"
	})
	if strings.HasSuffix(path, "/") && path != "/" {
		path += "1"
	}
	return httptest.NewRequest(method, path, nil)
}

// serve runs the request through h; a handler that panics on the nil
// store was reached, which is what the gates must not let happen.
func serve(h http.Handler, r *http.Request) (code int, location string, reached bool) {
	rec := httptest.NewRecorder()
	func() {
		defer func() {
			if recover() != nil {
				reached = true
			}
		}()
		h.ServeHTTP(rec, r)
	}()
	return rec.Code, rec.Header().Get("Location"), reached
}

func TestRouteMatrixGatesEveryRegisteredRoute(t *testing.T) {
	s := New(nil, config.WebConfig{Addr: ":0", APIInternalURL: "http://127.0.0.1:1", DevMode: true},
		nil, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := s.Handler()

	gated := 0
	for i, rt := range registeredRoutes(t) {
		// Anonymous: the gate, not the handler, answers.
		if rt.gate != "public" {
			gated++
			code, loc, reached := serve(h, requestFor(rt.pattern))
			if reached || code != http.StatusFound || loc != "/login" {
				t.Errorf("%s (%s): anonymous got %d %q (reached=%v); want 302 /login before the handler", rt.pattern, rt.gate, code, loc, reached)
			}
		}
		// A signed-in non-administrator, with a session of its own.
		token := "route-matrix-" + strconv.Itoa(i)
		s.sessionMu.Lock()
		s.sessions[token] = &Session{UserID: 1, LoginName: "matrix", IsAdmin: false, ExpiresAt: time.Now().Add(time.Hour)}
		s.sessionMu.Unlock()
		r := requestFor(rt.pattern)
		r.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: token})
		code, loc, reached := serve(h, r)
		if code == http.StatusNotFound {
			t.Errorf("%s: the generated request did not match its route (404) — the matrix exercised nothing for it (review B6)", rt.pattern)
		}
		switch rt.gate {
		case "requireAdmin":
			if reached || code != http.StatusForbidden {
				t.Errorf("%s (requireAdmin): a non-administrator got %d (reached=%v); want 403 before the handler", rt.pattern, code, reached)
			}
		case "requireAuth":
			if code == http.StatusFound && loc == "/login" {
				t.Errorf("%s (requireAuth): a signed-in account was redirected to /login", rt.pattern)
			}
			if code == http.StatusForbidden {
				t.Errorf("%s (requireAuth): a signed-in account got 403 — the route is not an administrator's", rt.pattern)
			}
		case "public":
			if code == http.StatusForbidden {
				t.Errorf("%s (public): a signed-in account got 403 — a public route is behind the admin gate", rt.pattern)
			}
		}
	}
	srctest.MinCount(t, "gated routes", gated, 10)
}
