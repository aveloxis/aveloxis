package web

import (
	"os"
	"strings"
	"testing"
)

// TestMonitorRouteRegistered verifies the web server registers a /monitor route.
func TestMonitorRouteRegistered(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "/monitor") {
		t.Error("web server.go must register a /monitor route for the integrated monitor dashboard")
	}
}

// TestMonitorTemplateExists verifies the monitor template is defined.
func TestMonitorTemplateExists(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, `"monitor"`) {
		t.Error("templates.go must define a 'monitor' template for the integrated monitor dashboard")
	}
}

// TestMonitorNavLinkExists verifies the nav bar has a "Monitor" link
// next to "Home" across all page templates.
func TestMonitorNavLinkExists(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The dashboard breadcrumb should have Monitor link.
	if !strings.Contains(code, "/monitor") {
		t.Error("templates.go must include a /monitor navigation link in the nav/breadcrumb area")
	}
}

// TestMonitorHandlerExists verifies the web server has a handleMonitor method.
func TestMonitorHandlerExists(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "handleMonitor") {
		t.Error("web server.go must have a handleMonitor method for the monitor dashboard")
	}
}

// TestMonitorPagination verifies the monitor supports pagination.
func TestMonitorPagination(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the function definition, not just a reference.
	idx := strings.Index(code, "func (s *Server) handleMonitor(")
	if idx < 0 {
		t.Skip("handleMonitor function not found")
	}
	fnBody := code[idx:]
	if len(fnBody) > 3000 {
		fnBody = fnBody[:3000]
	}

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

// TestMonitorBehindAuth verifies the monitor route is behind requireAuth.
func TestMonitorBehindAuth(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the route registration section.
	if strings.Contains(code, `"/monitor"`) && !strings.Contains(code, `requireAuth(s.handleMonitor)`) {
		t.Error("monitor route must be behind requireAuth — the old standalone monitor " +
			"was unauthenticated, but the integrated version should require login")
	}
}
