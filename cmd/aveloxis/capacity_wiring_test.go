// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.89 (SR-10, config value → behavior): an aveloxis.json "capacity"
// word reaches every store the process opens, through loadConfig — the
// one door every command takes. A missing file means WEB for every quota.
func TestCapacityWordsReachEveryStore(t *testing.T) {
	t.Cleanup(func() { db.SetProcessCapacitySources(nil) })
	p := filepath.Join(t.TempDir(), "aveloxis.json")
	if err := os.WriteFile(p, []byte(`{"capacity": {"repos_per_account": "OFF", "requests_per_hour": "DEFAULT"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	loadConfig(p, slog.New(slog.NewTextHandler(io.Discard, nil)))
	store := new(db.PostgresStore) // no database: the word is decided in process
	if got := store.CapacitySource(capacity.QuotaReposPerAccount); got != capacity.SourceOff {
		t.Errorf("repos_per_account = %q; want OFF from aveloxis.json", got)
	}
	if got := store.CapacitySource(capacity.QuotaRequestsPerHour); got != capacity.SourceDefault {
		t.Errorf("requests_per_hour = %q; want DEFAULT", got)
	}
	if got := store.CapacitySource(capacity.QuotaRequestsPerDay); got != capacity.SourceWeb {
		t.Errorf("requests_per_day (no line) = %q; want WEB", got)
	}
	loadConfig(filepath.Join(t.TempDir(), "missing.json"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if got := store.CapacitySource(capacity.QuotaReposPerAccount); got != capacity.SourceWeb {
		t.Errorf("after a missing config file repos_per_account = %q; want WEB", got)
	}
}

// The three long-running processes log each quota as applied at startup.
func TestLongRunningProcessesLogTheCapacityInForce(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	for _, component := range []string{`"serve"`, `"web"`, `"api"`} {
		if !strings.Contains(src, "store.LogCapacityInForce(ctx, "+component+")") {
			t.Errorf("%s does not log the capacity quotas in force at startup", component)
		}
	}
}
