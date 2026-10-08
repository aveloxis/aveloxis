// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/api"
)

// TestAPICacheMaxAgeFollowsTheEnrichmentInterval — SR-10, end to end from
// the JSON: the API's per-repository cache (O11 option 4, v0.29.71) may
// reuse an answer within a collection generation for at most one contributor
// enrichment interval, the cadence at which what it shows can change outside
// a collection. The default layer is the config accessor's (30 min).
func TestAPICacheMaxAgeFollowsTheEnrichmentInterval(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for json, want := range map[string]time.Duration{
		`{"collection": {"enrich_interval_minutes": 45}}`: 45 * time.Minute,
		`{}`: 30 * time.Minute,
	} {
		cfg := loadConfigJSON(t, json)
		if got := apiOptions(cfg, logger).ResponseCacheMaxAge; got != want {
			t.Errorf("%s: ResponseCacheMaxAge = %v, want %v", json, got, want)
		}
	}
}

// TestAPIRepositoryPageCacheFollowsTheConfig — SR-10, end to end from the
// JSON to the server the API process builds: the budget and the re-warm
// cadence it logs at startup are the EFFECTIVE values (after the single
// default layer in the config accessors), including the explicit zeros.
func TestAPIRepositoryPageCacheFollowsTheConfig(t *testing.T) {
	for json, want := range map[string][]string{
		`{}`:                                   {"max_bytes=0", "kept_without_budget=1000", "rewarm_interval=0s", "enriched_ttl=30m0s"},
		`{"api": {"response_cache_mb": 2048}}`: {"max_bytes=2147483648", "kept_without_budget=0", "rewarm_interval=1m0s"},
		`{"api": {"response_cache_mb": 64, "cache_rewarm_seconds": 15}}`:                                  {"max_bytes=67108864", "rewarm_interval=15s"},
		`{"api": {"response_cache_mb": 0, "cache_rewarm_seconds": 0}}`:                                    {"max_bytes=0", "kept_without_budget=1000", "rewarm_interval=0s", "front_end_secret_set=false"},
		`{"api": {"trusted_proxy": "127.0.0.1", "front_end_secret": "0123456789abcdef0123456789abcdef"}}`: {"front_end_secret_set=true"},
	} {
		cfg := loadConfigJSON(t, json)
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		opts := apiOptions(cfg, logger)
		if opts.RequestTimeout != cfg.HTTPTimeout() {
			t.Errorf("%s: re-warm requests must run under http_timeout_seconds, got %v", json, opts.RequestTimeout)
		}
		srv, err := api.NewWithOptions(nil, logger, opts)
		if err != nil {
			t.Fatal(err)
		}
		line := ""
		for _, l := range strings.Split(logs.String(), "\n") {
			if strings.Contains(l, "API repository page cache") {
				line = l
			}
		}
		for _, w := range want {
			if !strings.Contains(line, w) {
				t.Errorf("%s: startup log %q must report %s", json, line, w)
			}
		}
		if cfg.API.FrontEndSecret != "" && (opts.FrontEndSecret != cfg.API.FrontEndSecret || strings.Contains(logs.String(), cfg.API.FrontEndSecret)) {
			t.Errorf("%s: the secret must reach the server and never the log", json)
		}
		if strings.Contains(json, `"cache_rewarm_seconds": 0`) {
			logs.Reset()
			srv.RunRewarm(context.Background()) // returns at once when off
			// The off line must report the zero interval: a server without a
			// database also says "off" (no state reader), so the bare phrase
			// would pass for any interval (whole-branch review).
			if !strings.Contains(logs.String(), "re-warm off") || !strings.Contains(logs.String(), "rewarm_interval=0s") {
				t.Errorf("%s: an explicit 0 must turn the re-warm off; log %q", json, logs.String())
			}
		}
	}
}
