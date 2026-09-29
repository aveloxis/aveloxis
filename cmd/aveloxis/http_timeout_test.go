// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/httpserver"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestHTTPTimeoutReachesEveryServer — NET-6 (2026-09-29), end to end (SR-10):
// aveloxis.json's http_timeout_seconds becomes the monitor's, the api's and
// the web GUI's server timeouts (the web's /api proxy, a hard-coded 15 s
// wait, now runs under the web's bound — internal/web
// TestAPIProxyUnderTheBound).
func TestHTTPTimeoutReachesEveryServer(t *testing.T) {
	p := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(p, []byte(`{"http_timeout_seconds": 77}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatal(err)
	}
	const want = 77 * time.Second
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for name, srv := range map[string]*http.Server{
		"monitor": newMonitorServer(cfg, ":0", http.NotFoundHandler(), logger),
		"api":     newAPIServer(cfg, ":0", http.NotFoundHandler(), logger),
		"web":     newWebHTTPServer(cfg, ":0", http.NotFoundHandler(), logger),
	} {
		if srv.ReadHeaderTimeout != want || srv.ReadTimeout != want || srv.IdleTimeout != want || srv.WriteTimeout != want+httpserver.WriteMargin {
			t.Errorf("%s: timeouts %v/%v/%v/%v; want %v and write %v", name, srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout, srv.WriteTimeout, want, want+httpserver.WriteMargin)
		}
	}
	// Wiring: each listener is built through its builder.
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	for _, call := range []string{"newMonitorServer(cfg, monitorAddr,", "newAPIServer(cfg, addr,", "newWebHTTPServer(cfg, cfg.Web.Addr,"} {
		if !strings.Contains(src, call) {
			t.Errorf("main.go must build its server with %s…)", call)
		}
	}
}
