// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestMonitorRouteRegistered verifies the web server registers a /monitor route.
func TestMonitorRouteRegistered(t *testing.T) {
	if !strings.Contains(srctest.PackageFunc(t, "internal/web", "func (s *Server) Handler("), `"/monitor"`) {
		t.Error("Handler must register a /monitor route for the integrated monitor dashboard")
	}
}

// TestMonitorTemplateExists verifies the monitor template is defined.
func TestMonitorTemplateExists(t *testing.T) {
	code := allTemplates // the embedded set, the bytes templates.go held (v0.29.98)

	if !strings.Contains(code, `"monitor"`) {
		t.Error("templates.go must define a 'monitor' template for the integrated monitor dashboard")
	}
}

// TestMonitorNavLinkExists verifies the nav bar has a "Monitor" link
// next to "Home" across all page templates.
func TestMonitorNavLinkExists(t *testing.T) {
	code := allTemplates // the embedded set, the bytes templates.go held (v0.29.98)

	// The dashboard breadcrumb should have Monitor link.
	if !strings.Contains(code, "/monitor") {
		t.Error("templates.go must include a /monitor navigation link in the nav/breadcrumb area")
	}
}

// TestMonitorHandlerExists verifies the web server has a handleMonitor method.
func TestMonitorHandlerExists(t *testing.T) {
	srctest.PackageFunc(t, "internal/web", "func (s *Server) handleMonitor(") // fatal when it is gone
}

// TestMonitorPagination verifies the monitor supports pagination.
func TestMonitorPagination(t *testing.T) {
	fnBody := srctest.PackageFunc(t, "internal/web", "func (s *Server) handleMonitor(")

	if !strings.Contains(fnBody, "page") {
		t.Error("handleMonitor must support pagination (read 'page' query parameter)")
	}
}

// TestListQueuePageExists verifies the DB has a paginated queue list method.
func TestListQueuePageExists(t *testing.T) {
	src, err := os.ReadFile("../db/queue.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "ListQueuePage") {
		t.Error("db/queue.go must have ListQueuePage(ctx, limit, offset) for paginated monitor")
	}
}

// TestMonitorBehindAuth verifies both monitor routes are behind
// requireAdmin (v0.29.91 whole-PR follow-up; requireAuth before, which let
// any signed-in account read the queue and boost any repository). The
// behavior: TestMonitorRoutesAreAdministratorOnly.
func TestMonitorBehindAuth(t *testing.T) {
	// The whole package (PackageSource, v0.29.98): a second registration
	// in any file is one reference too many.
	code := srctest.StripGoComments(srctest.PackageSource(t, "internal/web", 4))
	// Each handler is referenced exactly once, and that reference is its
	// requireAdmin registration (a second, bare registration escapes a
	// presence check).
	for handler, want := range map[string]string{
		"s.handleMonitor)":           `requireAdmin(s.handleMonitor))`,
		"s.handleMonitorPrioritize)": `requireAdmin(s.handleMonitorPrioritize))`,
	} {
		if strings.Count(code, handler) != 1 || strings.Count(code, want) != 1 {
			t.Errorf("%s must be referenced once, registered behind requireAdmin (%d references)", strings.TrimSuffix(handler, ")"), strings.Count(code, handler))
		}
	}
	for _, h := range []string{"s.handleMonitor", "s.handleMonitorPrioritize"} {
		if n := len(regexp.MustCompile(regexp.QuoteMeta(h)+`\b`).FindAllString(code, -1)); n != 1 {
			t.Errorf("%s is referenced %d times; want 1 (its requireAdmin registration)", h, n)
		}
	}
	if strings.Contains(code, "requireAuth(s.handleMonitor") {
		t.Error("a monitor route is registered behind requireAuth (any signed-in account)")
	}
}
