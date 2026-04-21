package monitor

// Source-contract tests for handleDashboard. The full handler requires a
// live Postgres connection to run end-to-end; these checks pin the
// important wiring decisions so an accidental regression back to the
// N+1 pattern fails loudly in unit-test runs.

import (
	"os"
	"strings"
	"testing"
)

// TestHandleDashboardUsesListQueuePage verifies the dashboard paginates
// at the SQL layer instead of pulling the entire queue every refresh.
// Before this change, ListQueue (no LIMIT) returned every row in the
// fleet and the browser was asked to render thousands of <tr>s.
func TestHandleDashboardUsesListQueuePage(t *testing.T) {
	data, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Server) handleDashboard")
	if idx < 0 {
		t.Fatal("handleDashboard not found")
	}
	body := src[idx:]
	end := strings.Index(body, "\nfunc ")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "ListQueuePage") {
		t.Error("handleDashboard must call ListQueuePage (paginated); ListQueue returns the whole fleet")
	}
	if strings.Contains(body, "s.store.ListQueue(") {
		t.Error("handleDashboard must not call ListQueue — use ListQueuePage")
	}
}

// TestHandleDashboardUsesReposBatch enforces a single batch repo lookup
// instead of one GetRepoByID per queue row. This is the core of the
// N+1 fix: the original version fired ~400K queries per refresh on a
// large fleet.
func TestHandleDashboardUsesReposBatch(t *testing.T) {
	data, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Server) handleDashboard")
	if idx < 0 {
		t.Fatal("handleDashboard not found")
	}
	body := src[idx:]
	end := strings.Index(body, "\nfunc ")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "GetReposBatch") {
		t.Error("handleDashboard must call GetReposBatch for a single round-trip")
	}
	if strings.Contains(body, "GetRepoByID") {
		t.Error("handleDashboard must not call GetRepoByID in a loop (N+1 regression)")
	}
}

// TestHandleDashboardParsesPageParams makes sure the handler respects
// the query params rather than hard-coding page=1. Otherwise the
// pagination controls render but don't work.
func TestHandleDashboardParsesPageParams(t *testing.T) {
	data, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *Server) handleDashboard")
	if idx < 0 {
		t.Fatal("handleDashboard not found")
	}
	body := src[idx:]
	end := strings.Index(body, "\nfunc ")
	if end > 0 {
		body = body[:end]
	}

	if !strings.Contains(body, "parsePageParams") {
		t.Error("handleDashboard must call parsePageParams to honor ?page=&page_size=&q=")
	}
}
