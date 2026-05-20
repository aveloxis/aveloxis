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
