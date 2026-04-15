package web

import (
	"os"
	"strings"
	"testing"
)

// TestMonitorSearchBarExists verifies the monitor template has a search bar.
func TestMonitorSearchBarExists(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the monitor template.
	idx := strings.Index(code, `{{define "monitor"}}`)
	if idx < 0 {
		t.Fatal("cannot find monitor template")
	}
	tmpl := code[idx:]
	end := strings.Index(tmpl[1:], "{{define")
	if end > 0 {
		tmpl = tmpl[:end+1]
	}

	if !strings.Contains(tmpl, `name="q"`) {
		t.Error("monitor template must have a search input with name=\"q\"")
	}
}

// TestMonitorFirstLastPageNav verifies the monitor has First and Last page links.
func TestMonitorFirstLastPageNav(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, `{{define "monitor"}}`)
	if idx < 0 {
		t.Fatal("cannot find monitor template")
	}
	tmpl := code[idx:]
	end := strings.Index(tmpl[1:], "{{define")
	if end > 0 {
		tmpl = tmpl[:end+1]
	}

	if !strings.Contains(tmpl, "First") {
		t.Error("monitor template must have a 'First' page navigation link")
	}
	if !strings.Contains(tmpl, "Last") {
		t.Error("monitor template must have a 'Last' page navigation link")
	}
}

// TestMonitorPaginationPreservesSearch verifies page links include the search query.
func TestMonitorPaginationPreservesSearch(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, `{{define "monitor"}}`)
	if idx < 0 {
		t.Fatal("cannot find monitor template")
	}
	tmpl := code[idx:]
	end := strings.Index(tmpl[1:], "{{define")
	if end > 0 {
		tmpl = tmpl[:end+1]
	}

	// Pagination links must include .Query to preserve search across pages.
	if !strings.Contains(tmpl, ".Query") {
		t.Error("monitor pagination links must include .Query to preserve " +
			"search term across page navigation")
	}
}

// TestMonitorHandlerReadsSearchParam verifies handleMonitor reads the q parameter.
func TestMonitorHandlerReadsSearchParam(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Server) handleMonitor(")
	if idx < 0 {
		t.Fatal("cannot find handleMonitor")
	}
	fnBody := code[idx:]
	if len(fnBody) > 4000 {
		fnBody = fnBody[:4000]
	}

	if !strings.Contains(fnBody, `Get("q")`) {
		t.Error("handleMonitor must read the 'q' query parameter for search")
	}
}

// TestListQueuePageAcceptsSearch verifies the DB method supports a search filter.
func TestListQueuePageAcceptsSearch(t *testing.T) {
	src, err := os.ReadFile("../db/queue.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *PostgresStore) ListQueuePage(")
	if idx < 0 {
		t.Fatal("cannot find ListQueuePage")
	}
	sig := code[idx : idx+200]

	if !strings.Contains(sig, "search") {
		t.Error("ListQueuePage must accept a search parameter to filter by repo owner/name")
	}
}
