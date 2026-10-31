// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/api"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestAPICachesIgnoreTheEnrichmentInterval — D6 (0.29.120, summary/63),
// end to end from the JSON: collection.enrich_interval_minutes paces the
// enrichment sweep only; the api's caches are durable until recollection, so
// no value of it reaches the api process or its start-up lines.
func TestAPICachesIgnoreTheEnrichmentInterval(t *testing.T) {
	for _, json := range []string{`{"collection": {"enrich_interval_minutes": 45}}`, `{}`} {
		cfg := loadConfigJSON(t, json)
		var logs bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logs, nil))
		if _, err := api.NewWithOptions(nil, logger, apiOptions(cfg, logger)); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(logs.String(), "ttl=") || strings.Contains(logs.String(), "enrich_interval") {
			t.Errorf("%s: the api's start-up lines still report an age limit:\n%s", json, logs.String())
		}
	}
}

// TestAPIRepositoryPageCacheFollowsTheConfig — SR-10, end to end from the
// JSON to the server the API process builds: the budget and the re-warm
// cadence it logs at startup are the EFFECTIVE values (after the single
// default layer in the config accessors), including the explicit zeros.
func TestAPIRepositoryPageCacheFollowsTheConfig(t *testing.T) {
	for json, want := range map[string][]string{
		`{}`:                                   {"max_bytes=0", "kept_without_budget=1000", "kept_pools=2", "rewarm_interval=0s"},
		`{"api": {"response_cache_mb": 2048}}`: {"max_bytes=2147483648", "kept_without_budget=0", "kept_pools=0", "rewarm_interval=1m0s"},
		`{"api": {"response_cache_mb": 64, "cache_rewarm_seconds": 15}}`:                                  {"max_bytes=67108864", "rewarm_interval=15s"},
		`{"api": {"response_cache_mb": 0, "cache_rewarm_seconds": 0}}`:                                    {"max_bytes=0", "kept_without_budget=1000", "kept_pools=2", "rewarm_interval=0s", "front_end_secret_set=false"},
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

// Whole-branch review of 0.29.119–0.29.123, A7: the re-warm saves the
// replay list when it stops and then releases the list's lock, and runAPI
// waits for that — bounded by the shutdown timeout — before the deferred
// store.Close takes the pool away. The re-warm runs on its own cancel, so a
// server that stopped on an error (ctx still live) stops it too.
func TestRunAPIWaitsForTheRewarmsLastSave(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runAPI("))
	steps := []string{
		"rewarmCtx, cancelRewarm := context.WithCancel(ctx)",
		"defer close(rewarmDone)",
		"apiServer.RunRewarm(rewarmCtx)",
		"err = serveUntilDone(",
		"cancelRewarm()",
		"<-rewarmDone",
	}
	at := -1
	for _, s := range steps {
		i := strings.Index(body[at+1:], s)
		if i < 0 {
			t.Fatalf("runAPI must %q after the step before it (stop the re-warm, then wait for its save within the bound)", s)
		}
		at += 1 + i
	}
}

// ASVS 5.0 review of 0.29.125 (V16.4.2, D-N3): the component log files hold
// sign-ins and administrator actions; they are created for the owner alone.
func TestComponentLogsAreCreatedForTheOwnerAlone(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func startComponent("))
	if !strings.Contains(body, "os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)") {
		t.Error("startComponent must open the component's log file with mode 0600")
	}
}

// ASVS 5.0 review of 0.29.125 (V13.3.2, D-N12): aveloxis.json holds the
// database password, the OAuth client secrets and the forge keys; a file
// other users can read is reported at every start.
func TestLoadConfigWarnsWhenOthersCanReadTheFile(t *testing.T) {
	for _, c := range []struct {
		mode os.FileMode
		warn bool
	}{{0o600, false}, {0o640, true}, {0o644, true}} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{}`), c.mode); err != nil {
			t.Fatal(err)
		}
		var logs bytes.Buffer
		loadConfig(path, slog.New(slog.NewTextHandler(&logs, nil)))
		if got := strings.Contains(logs.String(), "readable by other users"); got != c.warn {
			t.Errorf("mode %o: warned=%t, want %t\n%s", c.mode, got, c.warn, logs.String())
		}
	}
}
