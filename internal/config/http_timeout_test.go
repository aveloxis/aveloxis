// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/httpserver"
)

// TestHTTPTimeoutSeconds — NET-6 (2026-09-29): http_timeout_seconds bounds
// the monitor, api and web servers and the web's /api proxy. One default
// layer (DefaultConfig, 180); a value that cannot be a bound is refused at
// load, naming the key, never coerced (SR-10).
func TestHTTPTimeoutSeconds(t *testing.T) {
	load := func(body string) (*Config, error) {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return Load(p)
	}
	cfg, err := load(`{}`)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.HTTPTimeout(); got != 180*time.Second {
		t.Errorf("default HTTPTimeout = %v; want 180s", got)
	}
	if cfg, err = load(`{"http_timeout_seconds": 600}`); err != nil || cfg.HTTPTimeout() != 10*time.Minute {
		t.Errorf("http_timeout_seconds 600 → %v, %v; want 10m", cfg, err)
	}
	// NET-6 review r1 F2: the largest accepted value must still leave room
	// for the write margin (9223372036 overflowed the web's write deadline into a
	// negative — i.e. no — write deadline).
	maxOK := strconv.FormatInt(MaxHTTPTimeoutSeconds, 10)
	if cfg, err := load(`{"http_timeout_seconds": ` + maxOK + `}`); err != nil || cfg.HTTPTimeout()+httpserver.WriteMargin <= 0 {
		t.Errorf("the maximum %s must load and leave a positive write deadline: %v", maxOK, err)
	}
	tooBig := strconv.FormatInt(MaxHTTPTimeoutSeconds+1, 10)
	if _, err := load(`{"http_timeout_seconds": ` + tooBig + `}`); err == nil || !strings.Contains(err.Error(), maxOK) {
		t.Errorf("%s must be refused naming the maximum %s: %v", tooBig, maxOK, err)
	}
	for _, bad := range []string{"0", "-5", "9999999999999"} {
		if _, err := load(`{"http_timeout_seconds": ` + bad + `}`); err == nil || !strings.Contains(err.Error(), "http_timeout_seconds") {
			t.Errorf("http_timeout_seconds %s must be refused naming the key, got %v", bad, err)
		}
	}
}
