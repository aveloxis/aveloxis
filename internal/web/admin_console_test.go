// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Stage 3 of summary/59 (0.29.109): the admin console is part of the
// template set. The pins below are derived from Handler's own route
// registrations (L13: the dispatch site is the list), so a page added
// later is covered the day it is registered.

// adminGETHandlers reads Handler's source and returns, for every GET route
// registered behind requireAdmin, the handler method it names.
func adminGETHandlers(t *testing.T) map[string]string {
	t.Helper()
	_, file := srctest.PackageFuncAt(t, "internal/web", "func (s *Server) Handler(")
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(srctest.Root(t), filepath.FromSlash(file)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	routes := map[string]string{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "HandleFunc" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		pattern, _ := strconv.Unquote(lit.Value)
		if !strings.HasPrefix(pattern, "GET ") {
			return true
		}
		wrap, ok := call.Args[1].(*ast.CallExpr)
		if !ok || len(wrap.Args) != 1 {
			return true
		}
		if ws, ok := wrap.Fun.(*ast.SelectorExpr); !ok || ws.Sel.Name != "requireAdmin" {
			return true
		}
		if h, ok := wrap.Args[0].(*ast.SelectorExpr); ok {
			routes[pattern] = h.Sel.Name
		}
		return true
	})
	srctest.MinCount(t, "administrator GET routes", len(routes), 3)
	return routes
}

// Every administrator page renders through the template set (D-20): its
// handler calls s.render, and the package writes no HTML by hand — the
// one Fprintf of markup left is errors.go's static sign-up-limit page,
// which has no session, no nav and no template data, and is listed here
// as the reason.
func TestAdminPagesRenderThroughTheTemplateSet(t *testing.T) {
	// confirmPage is the one shared render of the confirm template (D-19).
	confirm := srctest.StripGoComments(srctest.PackageFunc(t, "internal/web", "func (s *Server) confirmPage("))
	if !strings.Contains(confirm, `s.render(w, r, "admin_confirm"`) {
		t.Error("confirmPage must render the admin_confirm template through s.render")
	}
	for pattern, handler := range adminGETHandlers(t) {
		body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/web", "func (s *Server) "+handler+"("))
		if !strings.Contains(body, "s.render(") && !strings.Contains(body, "s.confirmPage(") {
			t.Errorf("%s (%s) must render a template through s.render (or the confirm page), not write HTML by hand", pattern, handler)
		}
	}
	src := srctest.StripGoComments(srctest.PackageSource(t, "internal/web", 10))
	handWritten := strings.Count(src, "Fprint(w, `<") + strings.Count(src, "Fprintf(w, `<") + strings.Count(src, "Fprint(w, \"<") + strings.Count(src, "Fprintf(w, \"<")
	if handWritten != 1 {
		t.Errorf("%d hand-written HTML writes in internal/web, want exactly one (errors.go's static sign-up-limit page): pages are templates", handWritten)
	}
}

// The top nav is one define (L18, stage 3a): every page template invokes
// it, and the markup lives in nav.html alone — the seven identical copies
// it replaced had already begun to be edited one at a time.
func TestTopNavIsOneDefine(t *testing.T) {
	partials := map[string]bool{"head.html": true, "nav.html": true, "adminLinks.html": true, "compareSearchWidget.html": true, "paginationNav.html": true, "login.html": true}
	// The e-mail gate page has no breadcrumb: the dashboard sends an account
	// there until an address is confirmed, so it offers no way onward.
	noBreadcrumb := map[string]bool{"account_email.html": true}
	pages := 0
	for _, file := range templateOrder {
		src, err := templateFS.ReadFile("templates/" + file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(src)
		if file == "nav.html" {
			if !strings.Contains(text, `<div class="nav">`) {
				t.Error("nav.html must hold the top nav markup")
			}
			continue
		}
		if strings.Contains(text, `<div class="nav">`) {
			t.Errorf("%s spells the top nav itself; invoke {{template \"nav\" .}}", file)
		}
		if partials[file] {
			continue
		}
		pages++
		if !strings.Contains(text, `{{template "nav" .}}`) {
			t.Errorf("%s does not invoke the shared nav", file)
		}
		if !noBreadcrumb[file] && !strings.Contains(text, `{{template "adminLinks" .}}`) {
			t.Errorf("%s does not invoke the shared adminLinks (the breadcrumb's administrator links)", file)
		}
	}
	srctest.MinCount(t, "page templates", pages, 7)
}

// The console's links are shown to administrators only, on every page
// (D-21; a non-admin's link would only lead to a 403): the Monitor link
// test of 0.29.91 generalised to the adminLinks define.
func TestAdminLinksAreShownToAdministratorsOnly(t *testing.T) {
	s := New(nil, config.WebConfig{}, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	// The breadcrumb's anchors, text included: the monitor page's own
	// "Clear" link is also href="/monitor", and the collection page links its
	// parent, so a bare href would read a page's body as the nav.
	links := []string{`<a href="/monitor">Monitor</a>`, `<a href="/admin/groups/pending">Approvals</a>`, `<a href="/admin/users">Users</a>`, `<a href="/admin/capacity">Capacity</a>`, `<a href="/admin/api-tokens">API tokens</a>`, `<a href="/admin/collections">Collections</a>`}
	for _, page := range []string{"dashboard", "group", "repo_detail", "compare", "monitor", "admin_approvals", "admin_users", "admin_capacity", "admin_tokens", "admin_collections", "admin_collection"} {
		for _, admin := range []bool{false, true} {
			if !admin && strings.HasPrefix(page, "admin_") {
				continue // a console page is never rendered for a non-admin (the gate answers 403 first); its breadcrumb may link its parent page
			}
			// Each page renders with its golden fixture (the handler's data
			// shape) and the session under test; a render error is a failure,
			// never a vacuous pass (round-1 review of 0.29.109, web F7).
			data := map[string]any{}
			if fixture, ok := renderFixtures[page].(map[string]any); ok {
				for k, v := range fixture {
					data[k] = v
				}
			}
			data["Session"] = &Session{LoginName: "u", IsAdmin: admin}
			var b strings.Builder
			if err := s.tmpl.ExecuteTemplate(&b, page, data); err != nil {
				t.Fatalf("render %s: %v", page, err)
			}
			for _, link := range links {
				if got := strings.Contains(b.String(), link); got != admin {
					t.Errorf("%s page, admin=%v: %s shown = %v", page, admin, link, got)
				}
			}
		}
	}
}

// Every route under /admin/ is registered behind requireAdmin (0.29.188).
// The service refuses a non-administrator by itself, so a route registered
// behind requireAuth still answers 403; this pins the outer gate too, which
// keeps a signed-in non-administrator from reaching a handler's parsing and
// from learning which administrator routes exist by their different errors.
// Derived from Handler's registrations: a route added later is covered.
func TestEveryAdminRouteIsBehindRequireAdmin(t *testing.T) {
	_, file := srctest.PackageFuncAt(t, "internal/web", "func (s *Server) Handler(")
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(srctest.Root(t), filepath.FromSlash(file)), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	examined := 0
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); !ok || sel.Sel.Name != "HandleFunc" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		pattern, _ := strconv.Unquote(lit.Value)
		_, path, _ := strings.Cut(pattern, " ")
		if !strings.HasPrefix(path, "/admin/") {
			return true
		}
		examined++
		wrap, ok := call.Args[1].(*ast.CallExpr)
		if ok {
			if ws, isSel := wrap.Fun.(*ast.SelectorExpr); isSel && ws.Sel.Name == "requireAdmin" {
				return true
			}
		}
		t.Errorf("%s is not registered behind s.requireAdmin", pattern)
		return true
	})
	srctest.MinCount(t, "/admin/ routes examined", examined, 20)
}
