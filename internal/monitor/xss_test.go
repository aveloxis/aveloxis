package monitor

import (
	"html/template"
	"os"
	"strings"
	"testing"
)

// TestMonitorEscapesOwnerRepo verifies the monitor dashboard HTML-escapes
// repo owner and name fields to prevent stored XSS. An attacker can submit
// a URL like https://gitlab.evil.example/<script>alert(1)</script>/poc via
// the web GUI. Without escaping, the script tag renders in the operator's
// browser on the auto-refreshing monitor dashboard.
func TestMonitorEscapesOwnerRepo(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The monitor must import html/template for escaping.
	if !strings.Contains(code, `"html/template"`) {
		t.Error("monitor.go must import html/template for HTML escaping of user-controlled values")
	}

	// Find the dashboard handler section where queue rows are rendered.
	// The fmt.Fprintf that writes Owner/Repo must use escaped values.
	if strings.Contains(code, `j.Owner, j.Repo, j.Plat`) {
		t.Error("monitor.go must NOT pass raw j.Owner and j.Repo to fmt.Fprintf — " +
			"these values come from user-submitted URLs and must be HTML-escaped " +
			"via template.HTMLEscapeString to prevent stored XSS")
	}
}

// TestMonitorEscapesLastError verifies LastError is HTML-escaped.
// The title="%s" attribute interpolation with an unescaped error string
// allows attribute breakout.
func TestMonitorEscapesLastError(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must not have raw *j.LastError in fmt.Sprintf for the title attribute.
	if strings.Contains(code, `*j.LastError`) && !strings.Contains(code, "HTMLEscapeString") {
		t.Error("monitor.go must HTML-escape LastError before embedding in title attribute")
	}
}

// TestMonitorEscapesLockedBy verifies LockedBy (worker hostname) is escaped.
func TestMonitorEscapesLockedBy(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if strings.Contains(code, `*j.LockedBy`) && !strings.Contains(code, "HTMLEscapeString") {
		t.Error("monitor.go must HTML-escape LockedBy before embedding in HTML")
	}
}

// TestMonitorEscapesPaginationQuery verifies that the pagination href
// built from the user-controlled `q` search parameter passes through
// BOTH url.Values encoding AND template.HTMLEscapeString before being
// interpolated into the pagination link href attributes. CodeQL flagged
// the pre-fix code (raw "&" concatenation + url.QueryEscape only) as a
// reflected XSS sink even though url.QueryEscape alone is in fact safe
// against HTML-attribute breakout — the double-escape plus `&amp;`
// separator is spec-correct HTML and unambiguous to static analyzers.
func TestMonitorEscapesPaginationQuery(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The old pattern — raw "&" concatenation with url.QueryEscape —
	// must not come back.
	if strings.Contains(code, `qsNoPage += "&q=" + url.QueryEscape(`) {
		t.Error("monitor.go must not build pagination query strings via raw '&' concatenation " +
			"with url.QueryEscape — use url.Values.Encode() + template.HTMLEscapeString for " +
			"static-analyzer-friendly, spec-correct HTML escaping")
	}

	// The new pattern must be present: url.Values{} + template.HTMLEscapeString.
	if !strings.Contains(code, `qs := url.Values{}`) {
		t.Error("monitor.go pagination must build the query with url.Values{} (structured) " +
			"so each value gets URL-escaped before HTML-escaping")
	}
	if !strings.Contains(code, `template.HTMLEscapeString(qs.Encode())`) {
		t.Error("monitor.go pagination must HTML-escape qs.Encode() before interpolating " +
			"into the href attribute")
	}

	// The href template must use &amp; (HTML entity) not raw & between params.
	if strings.Contains(code, `href="/?page=%d&%s"`) {
		t.Error("monitor.go pagination href must use &amp; (HTML entity) not raw & as the " +
			"parameter separator inside an href attribute")
	}
	if !strings.Contains(code, `href="/?page=%d&amp;%s"`) {
		t.Error("monitor.go pagination href must use &amp; as the parameter separator")
	}
}

// TestHTMLEscapeStringBehavior verifies the escaping function works as expected
// for the XSS payloads identified in the security audit.
func TestHTMLEscapeStringBehavior(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`<script>alert(1)</script>`, `&lt;script&gt;alert(1)&lt;/script&gt;`},
		{`<img/src=x/onerror=alert(1)>`, `&lt;img/src=x/onerror=alert(1)&gt;`},
		{`" onmouseover="alert(1)`, `&#34; onmouseover=&#34;alert(1)`},
		{`normal-owner`, `normal-owner`},
	}
	for _, tt := range tests {
		got := template.HTMLEscapeString(tt.input)
		if got != tt.want {
			t.Errorf("HTMLEscapeString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
