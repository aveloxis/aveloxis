// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The --addr flag must default to EMPTY, so runAPI can tell "operator
// said nothing" from "operator asked for this address". With a
// non-empty flag default the config value would be unreachable: every
// invocation would look explicit, and `aveloxis start api` — which
// passes only --config — would keep binding the hardcoded port
// (v0.28.19).
func TestAPIAddrFlagDefaultsEmptySoConfigCanWin(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	if !strings.Contains(src, `StringVar(&addr, "addr", "",`) {
		t.Error(`the --addr flag must declare an empty default (StringVar(&addr, "addr", "", ...)). A non-empty default makes cfg.API.Addr dead config: runAPI cannot distinguish it from an explicit --addr, so a backgrounded ` + "`aveloxis start api`" + ` would ignore the config file entirely.`)
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func runAPI("))
	if !strings.Contains(body, "cfg.API.AddrOrDefault()") {
		t.Error("runAPI must fall back to cfg.API.AddrOrDefault() when --addr is empty, or the config parameter does nothing")
	}
	if !strings.Contains(body, `if addr == "" {`) {
		t.Error("runAPI must treat an empty --addr as unset — that is the whole precedence rule (flag beats config beats default)")
	}
}

// The address actually bound is logged, not the requested one — they
// differ whenever the config supplies it (SR-10's logging half).
func TestAPIListenLogReportsTheEffectiveAddr(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func runAPI("))
	listen := strings.Index(body, "API server listening")
	fallback := strings.Index(body, "cfg.API.AddrOrDefault()")
	if listen < 0 || fallback < 0 {
		t.Fatal("re-anchor this pin: runAPI no longer logs the listen address or no longer resolves the config fallback")
	}
	if fallback > listen {
		t.Error("runAPI logs the listen address BEFORE resolving the config fallback, so the log would report an empty addr while the server binds the configured one — log the effective value, at the point of use")
	}
}

// The mismatch that api.addr makes easy: move the API and the web
// GUI keeps proxying to the old port, so every chart 502s while both
// processes look healthy.
func TestWarnAPIPortMismatch(t *testing.T) {
	cases := []struct {
		name, apiAddr, internalURL string
		wantWarn                   bool
	}{
		{"aligned", "127.0.0.1:8383", "http://127.0.0.1:8383", false},
		{"moved api, stale web url", "127.0.0.1:9383", "http://127.0.0.1:8383", true},
		{"moved api, web url updated", "127.0.0.1:9383", "http://127.0.0.1:9383", false},
		{"api on all interfaces, web loopback same port", "0.0.0.0:8383", "http://127.0.0.1:8383", false},
		// A different HOST is a split deployment or an nginx hop; this
		// machine's api.addr says nothing about it.
		{"remote api host", "127.0.0.1:9383", "http://10.0.0.7:8383", false},
		{"no port in url", "127.0.0.1:9383", "http://127.0.0.1", false},
		{"unparseable url", "127.0.0.1:9383", "://nope", false},
		{"ipv6 loopback mismatch", "127.0.0.1:9383", "http://[::1]:8383", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
			cfg := &config.Config{}
			cfg.API.Addr = tc.apiAddr
			cfg.Web.APIInternalURL = tc.internalURL

			warnAPIPortMismatch(cfg, logger)

			warned := strings.Contains(buf.String(), "api_internal_url points at a loopback port")
			if warned != tc.wantWarn {
				t.Errorf("warned=%v want=%v for api.addr=%q api_internal_url=%q.\nA missed warning means the operator debugs empty charts with both processes reporting healthy; a spurious one trains them to ignore it.\nlog: %s",
					warned, tc.wantWarn, tc.apiAddr, tc.internalURL, buf.String())
			}
			if warned && !strings.Contains(buf.String(), "fix=") {
				t.Error("the warning must carry the concrete fix — the operator should not have to work out which of the two values to change")
			}
		})
	}
}
