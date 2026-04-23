package web

// Directly render the "monitor" template with concrete inputs and assert
// on the exact href attributes the browser will see. Speculating about
// template behavior is how pagination bugs hide — rendered HTML is the
// authoritative test.

import (
	"bytes"
	"html/template"
	"strings"
	"testing"
)

// buildMonitorTmpl parses templates.go the same way Server does, so the
// template funcmap and definitions match production. Keeps the test
// independent of Server/DB/OAuth wiring.
func buildMonitorTmpl(t *testing.T) *template.Template {
	t.Helper()
	return template.Must(template.New("").Funcs(template.FuncMap{
		"truncate": func(s string, n int) string {
			if len(s) <= n {
				return s
			}
			return s[:n] + "..."
		},
		"dict": func(values ...interface{}) map[string]interface{} {
			m := make(map[string]interface{})
			for i := 0; i < len(values)-1; i += 2 {
				m[values[i].(string)] = values[i+1]
			}
			return m
		},
		"add":      func(a, b int) int { return a + b },
		"subtract": func(a, b int) int { return a - b },
	}).Parse(allTemplates))
}

// sessionStub carries just the fields the monitor template reads.
type sessionStub struct {
	AvatarURL string
	LoginName string
}

func baseMonitorData(page, totalPages, total int, query string) map[string]interface{} {
	return map[string]interface{}{
		"Session": sessionStub{LoginName: "tester"},
		"Stats": map[string]int{
			"total":      total,
			"queued":     total - 1,
			"collecting": 1,
		},
		"Jobs":       []any{}, // empty — we're testing pagination, not rows
		"Page":       page,
		"TotalPages": totalPages,
		"Total":      total,
		"Query":      query,
	}
}

// TestMonitorTemplatePrevNextHrefs renders the template on page 3 of 5
// and asserts the Prev/Next anchors target pages 2 and 4 respectively.
// If subtract/add are misbound or template funcs fail, this fails.
func TestMonitorTemplatePrevNextHrefs(t *testing.T) {
	tmpl := buildMonitorTmpl(t)

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "monitor", baseMonitorData(3, 5, 500, "")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	cases := []struct {
		name, want string
	}{
		{"First link targets page=1", `href="/monitor?page=1"`},
		{"Prev link targets page=2", `href="/monitor?page=2"`},
		{"Next link targets page=4", `href="/monitor?page=4"`},
		{"Last link targets page=5", `href="/monitor?page=5"`},
	}
	for _, tc := range cases {
		if !strings.Contains(html, tc.want) {
			t.Errorf("%s: HTML missing %q", tc.name, tc.want)
		}
	}
}

// TestMonitorTemplatePrevNextHrefsWithQuery renders page 3 of 5 with an
// active search and asserts that EVERY pagination link preserves q=foo.
// The existing TestMonitorPaginationPreservesSearch checks the SOURCE
// text contains .Query but doesn't verify the rendered href — which is
// where real breakage hides (e.g., {{.Query}} vs $.Query scoping, URL
// escaping of ampersands, etc.).
func TestMonitorTemplatePrevNextHrefsWithQuery(t *testing.T) {
	tmpl := buildMonitorTmpl(t)

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "monitor", baseMonitorData(3, 5, 500, "foo")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	// Note: html/template URL-context-escapes the ampersand between
	// page and q into &amp;. Browsers parse &amp; in an href back to
	// a literal &, so both the raw and escaped form are valid targets
	// — we accept either but they must all carry the q= parameter.
	cases := []struct{ name, wantA, wantB string }{
		{"First carries q", `href="/monitor?page=1&q=foo"`, `href="/monitor?page=1&amp;q=foo"`},
		{"Prev carries q", `href="/monitor?page=2&q=foo"`, `href="/monitor?page=2&amp;q=foo"`},
		{"Next carries q", `href="/monitor?page=4&q=foo"`, `href="/monitor?page=4&amp;q=foo"`},
		{"Last carries q", `href="/monitor?page=5&q=foo"`, `href="/monitor?page=5&amp;q=foo"`},
	}
	for _, tc := range cases {
		if !strings.Contains(html, tc.wantA) && !strings.Contains(html, tc.wantB) {
			t.Errorf("%s: HTML missing BOTH %q and %q", tc.name, tc.wantA, tc.wantB)
		}
	}
}

// TestMonitorTemplatePage1DisablesPrev — on page 1 Prev and First must
// render as disabled <span>s, not live links, so the browser gives
// visual feedback that there's nowhere to go back to.
func TestMonitorTemplatePage1DisablesPrev(t *testing.T) {
	tmpl := buildMonitorTmpl(t)

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "monitor", baseMonitorData(1, 5, 500, "")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	// First/Prev must NOT appear as live <a href="..."> on page 1.
	if strings.Contains(html, `href="/monitor?page=0"`) {
		t.Error("page 1: Prev must not link to page=0 (invalid)")
	}
	// Next must still link to page 2.
	if !strings.Contains(html, `href="/monitor?page=2"`) {
		t.Error("page 1: Next must link to page=2")
	}
}

// TestMonitorTemplateLastPageDisablesNext — on the final page, Next and
// Last must be disabled. A live href="/monitor?page=N+1" would let the
// user click past the end into an empty result set, confusing the UX.
func TestMonitorTemplateLastPageDisablesNext(t *testing.T) {
	tmpl := buildMonitorTmpl(t)

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "monitor", baseMonitorData(5, 5, 500, "")); err != nil {
		t.Fatalf("execute: %v", err)
	}
	html := buf.String()

	if strings.Contains(html, `href="/monitor?page=6"`) {
		t.Error("last page: Next must not link past totalPages")
	}
	if !strings.Contains(html, `href="/monitor?page=4"`) {
		t.Error("last page: Prev must link to previous page")
	}
}
