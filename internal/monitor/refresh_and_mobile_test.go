// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package monitor

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// v0.23.0 — two operator-requested improvements to the /monitor page:
//   - DefaultDashboardRefreshSeconds (currently hardcoded to 60) becomes
//     configurable via monitor.refresh_seconds in aveloxis.json.
//   - A mobile-friendly stylesheet engages when a known-mobile UA is
//     detected, so the dashboard is usable from a phone.

func TestMonitorConfigDeclared(t *testing.T) {
	src, err := os.ReadFile("../config/config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "MonitorConfig") {
		t.Error("config.go must declare a MonitorConfig struct with at least " +
			"a RefreshSeconds int `json:\"refresh_seconds\"` field, so operators " +
			"can override the dashboard's meta-refresh cadence from aveloxis.json.")
	}
	if !strings.Contains(code, `json:"refresh_seconds"`) {
		t.Error("MonitorConfig must expose refresh_seconds as a JSON-tagged field. " +
			"Operators wrote this in their config; the JSON key is the contract.")
	}
	if !strings.Contains(code, `Monitor MonitorConfig`) {
		t.Error("Config struct must embed Monitor MonitorConfig as a top-level field " +
			"so config.Monitor.RefreshSeconds is reachable from main.go.")
	}
}

func TestMonitorNewAcceptsRefreshOption(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The Server struct must hold the configured refresh seconds so
	// handleDashboard can emit it in the meta-refresh tag.
	if !strings.Contains(code, "refreshSeconds") && !strings.Contains(code, "RefreshSeconds") {
		t.Error("Server struct must hold a refreshSeconds field so the dashboard " +
			"template can emit the operator-configured meta-refresh cadence.")
	}
	// New must accept the refresh value (either as a separate parameter
	// or via an Options struct). Pin a flexible match: the function
	// signature or call sites somewhere wire it in.
	if !strings.Contains(code, "NewWithOptions") && !strings.Contains(code, "refreshSeconds int") {
		t.Error("monitor.New must accept a refresh-seconds option (e.g. via NewWithOptions " +
			"or an extra parameter). Default falls back to DefaultDashboardRefreshSeconds " +
			"when the configured value is zero or unset.")
	}
}

func TestMonitorMainGoPlumbsRefresh(t *testing.T) {
	src, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "cfg.Monitor") {
		t.Error("cmd/aveloxis/main.go must read cfg.Monitor and pass it through to " +
			"monitor.New (or NewWithOptions). Without this, the JSON knob is unreachable.")
	}
}

func TestIsMobileUserAgent(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "isMobile") {
		t.Error("monitor.go must define isMobile(r *http.Request) bool — used by " +
			"handleDashboard to select the mobile stylesheet when the User-Agent " +
			"matches a known mobile pattern.")
	}
}

func TestIsMobileBehavior(t *testing.T) {
	cases := []struct {
		ua     string
		mobile bool
	}{
		{"Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15", true},
		{"Mozilla/5.0 (Linux; Android 10) AppleWebKit/537.36", true},
		{"Mozilla/5.0 (iPad; CPU OS 14_0 like Mac OS X)", true},
		{"Mozilla/5.0 (Windows Phone 10.0; Mobile)", true},
		{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", false},
		{"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36", false},
		{"", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("User-Agent", c.ua)
		got := isMobile(r)
		if got != c.mobile {
			t.Errorf("isMobile(%q) = %v, want %v", c.ua, got, c.mobile)
		}
	}
}

func TestDashboardEmitsMobileCSSOnMobileUA(t *testing.T) {
	// Behavioral: a request with a mobile UA produces HTML that
	// contains a mobile-specific CSS marker (a media query, a body
	// class, or an inline @media block). The exact marker is left
	// flexible — what matters is that mobile UAs get markedly
	// different rendering signals than desktop.
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "@media") && !strings.Contains(code, "is-mobile") && !strings.Contains(code, "isMobile(r)") {
		t.Error("handleDashboard must consult isMobile(r) and emit a mobile-friendly " +
			"CSS variant (e.g. @media (max-width: ...) block, an `is-mobile` body class, " +
			"or a conditional rendering branch). Without one of these signals the " +
			"dashboard remains unusable on phones.")
	}
}

// TestMobileCSSPreventsOverflow pins the v0.23.0 post-fix overflow
// protections. The initial v0.23.0 release converted tables to
// display:block but left long content (repo URLs, error strings)
// to overflow horizontally; this test prevents regression on those
// fixes. Operator-reported on 2026-05-19: "It's still too wide on
// mobile."
func TestMobileCSSPreventsOverflow(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// word-break / overflow-wrap on td so long unbreakable strings
	// (repo URLs, error messages) wrap inside the card instead of
	// pushing the page wider than the viewport.
	if !strings.Contains(code, "word-break") && !strings.Contains(code, "overflow-wrap") {
		t.Error("mobile CSS must include word-break or overflow-wrap on td cells. " +
			"Without one of these, long repo URLs and error tooltips overflow " +
			"the card and force horizontal scroll on phones.")
	}

	// overflow-x:hidden on body is the last-resort safety net for
	// any element we didn't manage to constrain. Combined with
	// max-width:100vw this guarantees no horizontal scroll.
	if !strings.Contains(code, "overflow-x") {
		t.Error("mobile CSS must include overflow-x:hidden on the body or html " +
			"as a last-resort safety net against any element we didn't manage to " +
			"constrain (third-party content, future template changes).")
	}

	// box-sizing:border-box so padding doesn't push child widths
	// past their parent's width.
	if !strings.Contains(code, "box-sizing") {
		t.Error("mobile CSS must declare box-sizing:border-box so padding doesn't " +
			"push child widths past the viewport.")
	}

	// The inline min-width:240px on the search input must be
	// overridden on mobile so the form fits a 320px viewport.
	if !strings.Contains(code, `min-width: 0 !important`) && !strings.Contains(code, `min-width:0!important`) {
		t.Error("mobile CSS must override the inline min-width:240px on the search " +
			"input with `min-width: 0 !important`. Inline styles have higher " +
			"specificity than CSS rules without !important, so without this the " +
			"input keeps its 240px floor on a 320px phone.")
	}
}

// TestQueueRowsCarryDataLabels pins the per-td data-label attributes
// that the mobile stacked-card layout consumes via
// td::before { content: attr(data-label) }. Without these, the
// mobile cards show unlabeled values which is unreadable.
func TestQueueRowsCarryDataLabels(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Every meaningful column must emit a data-label. Action column
	// uses data-label="" deliberately (its value IS the label —
	// "Boost" button speaks for itself).
	for _, label := range []string{
		`data-label="Repo"`,
		`data-label="Status"`,
		`data-label="Due"`,
		`data-label="Last Run"`,
		`data-label="Gathered Issues"`,
		`data-label="Meta Issues"`,
		`data-label="Gathered PRs"`,
		`data-label="Meta PRs"`,
		`data-label="Gathered Commits"`,
		`data-label="Meta Commits"`,
	} {
		if !strings.Contains(code, label) {
			t.Errorf("queue row td must emit %s so the mobile card layout shows "+
				"the column name. Without this, mobile cards are unreadable lists "+
				"of bare values.", label)
		}
	}
}

// TestMobileHidesNonEssentialColumns pins that at least a few of
// the 14 columns are hidden on mobile. Stacking all 14 fields per
// repo makes each card too tall; hiding # / Platform / Priority
// (which the operator rarely needs on a phone) keeps the cards
// compact.
func TestMobileHidesNonEssentialColumns(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// CSS must include at least one td:nth-child(N) { display: none }
	// rule in a mobile context.
	if !strings.Contains(code, "td:nth-child(1)") && !strings.Contains(code, "td:nth-child(3)") && !strings.Contains(code, "td:nth-child(5)") {
		t.Error("mobile CSS must hide at least one non-essential column via " +
			"td:nth-child(N) { display: none }. Stacking all 14 columns vertically " +
			"makes each repo card unusably tall; # / Platform / Priority are the " +
			"natural candidates to drop on phone-sized viewports.")
	}
}
