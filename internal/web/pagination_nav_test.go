package web

// Tests for the shared {{define "paginationNav"}} template and its
// adoption by /monitor and /groups. The monitor pagination broke in a
// way that didn't show up in template-rendered HTML (correct hrefs),
// so the fix is twofold: (1) unify on the known-working Groups pattern
// so both pages have identical pagination behavior, and (2) fix the
// N+1 GetRepoByID loop in handleMonitor which made page renders slow
// enough to race the 10s auto-refresh — a click to Next/Prev that
// takes 12s to return gets canceled by location.reload() firing on
// the still-rendered old page.

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestPaginationNavTemplateDefined asserts the shared block exists.
func TestPaginationNavTemplateDefined(t *testing.T) {
	if !strings.Contains(allTemplates, `{{define "paginationNav"}}`) {
		t.Fatal(`templates.go must define a shared "paginationNav" block ` +
			`so /monitor and /groups render identical pagination controls`)
	}
}

// TestPaginationNavRendersFirstPrevNextLast verifies the shared block
// renders all four controls plus a sliding page window on a middle
// page. Inputs: BasePath, Page, TotalPages, Query, PageWindow.
func TestPaginationNavRendersFirstPrevNextLast(t *testing.T) {
	tmpl := buildMonitorTmpl(t)
	var buf bytes.Buffer
	err := tmpl.ExecuteTemplate(&buf, "paginationNav", map[string]interface{}{
		"BasePath":   "/monitor",
		"Page":       3,
		"TotalPages": 5,
		"Query":      "",
		"PageWindow": []int{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	cases := []string{
		`href="/monitor?page=1"`,
		`href="/monitor?page=2"`,
		`href="/monitor?page=4"`,
		`href="/monitor?page=5"`,
		"First",
		"Previous",
		"Next",
		"Last",
	}
	for _, want := range cases {
		if !strings.Contains(html, want) {
			t.Errorf("paginationNav missing %q\nRendered:\n%s", want, html)
		}
	}
}

// TestPaginationNavPreservesQuery ensures the search term carries across
// every link — First/Prev/page-number/Next/Last. A single missing &q=
// would make that control silently drop the user's search.
func TestPaginationNavPreservesQuery(t *testing.T) {
	tmpl := buildMonitorTmpl(t)
	var buf bytes.Buffer
	err := tmpl.ExecuteTemplate(&buf, "paginationNav", map[string]interface{}{
		"BasePath":   "/monitor",
		"Page":       3,
		"TotalPages": 5,
		"Query":      "foo",
		"PageWindow": []int{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	// Every expected page target (1,2,4,5) must carry q=foo.
	// html/template URL-encodes & as &amp; inside href; accept either.
	for _, page := range []string{"1", "2", "4", "5"} {
		wantA := `href="/monitor?page=` + page + `&q=foo"`
		wantB := `href="/monitor?page=` + page + `&amp;q=foo"`
		if !strings.Contains(html, wantA) && !strings.Contains(html, wantB) {
			t.Errorf("page %s link missing q=foo\nRendered:\n%s", page, html)
		}
	}
}

// TestPaginationNavDisablesFirstPrevOnPage1 — on page 1 the First and
// Previous controls must render as disabled spans, not live anchors.
func TestPaginationNavDisablesFirstPrevOnPage1(t *testing.T) {
	tmpl := buildMonitorTmpl(t)
	var buf bytes.Buffer
	err := tmpl.ExecuteTemplate(&buf, "paginationNav", map[string]interface{}{
		"BasePath":   "/monitor",
		"Page":       1,
		"TotalPages": 5,
		"Query":      "",
		"PageWindow": []int{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	if strings.Contains(html, `href="/monitor?page=0"`) {
		t.Error("page 1: Previous must not link to page=0 (invalid)")
	}
	if !strings.Contains(html, `href="/monitor?page=2"`) {
		t.Error("page 1: Next must still link to page=2")
	}
}

// TestPaginationNavDisablesNextLastOnLastPage — conversely.
func TestPaginationNavDisablesNextLastOnLastPage(t *testing.T) {
	tmpl := buildMonitorTmpl(t)
	var buf bytes.Buffer
	err := tmpl.ExecuteTemplate(&buf, "paginationNav", map[string]interface{}{
		"BasePath":   "/monitor",
		"Page":       5,
		"TotalPages": 5,
		"Query":      "",
		"PageWindow": []int{1, 2, 3, 4, 5},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	if strings.Contains(html, `href="/monitor?page=6"`) {
		t.Error("last page: Next must not link past TotalPages")
	}
	if !strings.Contains(html, `href="/monitor?page=4"`) {
		t.Error("last page: Previous must link to prior page")
	}
}

// TestMonitorTemplateUsesPaginationNav — the /monitor view must call
// the shared template, so any future fix there benefits both pages.
func TestMonitorTemplateUsesPaginationNav(t *testing.T) {
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

	if !strings.Contains(tmpl, `{{template "paginationNav"`) {
		t.Error(`/monitor template must invoke {{template "paginationNav" ...}} ` +
			`instead of inlining its own pagination markup`)
	}
}

// TestGroupTemplateUsesPaginationNav — ditto for /groups.
func TestGroupTemplateUsesPaginationNav(t *testing.T) {
	src, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, `{{define "group"}}`)
	if idx < 0 {
		t.Fatal("cannot find group template")
	}
	tmpl := code[idx:]
	end := strings.Index(tmpl[1:], "{{define")
	if end > 0 {
		tmpl = tmpl[:end+1]
	}

	if !strings.Contains(tmpl, `{{template "paginationNav"`) {
		t.Error(`/groups template must also invoke the shared paginationNav ` +
			`so both pages share one source of truth`)
	}
}

// TestHandleMonitorUsesGetReposBatch — the N+1 GetRepoByID loop in
// handleMonitor is the working theory for why Prev/Next failed: page
// renders taking >10s race against the setTimeout(location.reload,
// 10000) on the still-rendered old page. Batching the repo lookup
// makes page renders constant-time regardless of page size.
func TestHandleMonitorUsesGetReposBatch(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *Server) handleMonitor(")
	if idx < 0 {
		t.Fatal("cannot find handleMonitor")
	}
	body := code[idx:]
	end := strings.Index(body[1:], "\nfunc ")
	if end > 0 {
		body = body[:end+1]
	}

	if !strings.Contains(body, "GetReposBatch") {
		t.Error("handleMonitor must use GetReposBatch for a single round-trip " +
			"repo lookup; the per-row GetRepoByID loop makes page renders so " +
			"slow that the 10s auto-refresh races against click-navigation " +
			"and cancels it")
	}
	if strings.Contains(body, "s.store.GetRepoByID(r.Context()") {
		t.Error("handleMonitor must NOT call GetRepoByID in the per-row loop " +
			"(N+1 regression)")
	}
}
