// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

// `refresh-views --set` accepts the two view sets or both; anything else is
// refused before a connection is opened (v0.29.61, worklist 48).
func TestRefreshViewsSetValues(t *testing.T) {
	for _, ok := range []string{"all", "8knot", "supply-chain"} {
		if !refreshViewsSets[ok] {
			t.Errorf("--set %s must be accepted", ok)
		}
	}
	for _, bad := range []string{"", "views", "8Knot", "supply_chain"} {
		if refreshViewsSets[bad] {
			t.Errorf("--set %q must be refused", bad)
		}
	}
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	fn := src[strings.Index(string(src), "func refreshViewsCmd("):]
	if strings.Index(string(fn), "refreshViewsSets[set]") > strings.Index(string(fn), "db.NewPostgresStore(") {
		t.Error("--set must be validated BEFORE the store is opened")
	}
}

// loadConfig (v0.29.61): a MISSING file runs on defaults with a WARN, as
// before; a file that exists but is invalid must never — it exits. The
// distinction is config.ErrNotFound, the one sentinel Load wraps for it.
func TestLoadConfigTellsMissingFromInvalid(t *testing.T) {
	dir := t.TempDir()
	if _, err := config.Load(filepath.Join(dir, "absent.json")); !errors.Is(err, config.ErrNotFound) {
		t.Errorf("a missing file must be config.ErrNotFound, got %v", err)
	}
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"collection": {"supply_chain_refresh_hours": -1}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(bad); err == nil || errors.Is(err, config.ErrNotFound) {
		t.Errorf("an invalid file must be an error that is NOT not-found, got %v", err)
	}
	// Runtime: the seam records the exit instead of taking the process.
	var exited []int
	prev := exitProcess
	exitProcess = func(code int) { exited = append(exited, code) }
	t.Cleanup(func() { exitProcess = prev })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if cfg := loadConfig(filepath.Join(dir, "absent.json"), logger); cfg == nil || len(exited) != 0 {
		t.Errorf("a missing file must yield the defaults without exiting (exits=%v)", exited)
	}
	loadConfig(bad, logger)
	if len(exited) != 1 || exited[0] != 1 {
		t.Errorf("an invalid file must exit 1, got exits=%v", exited)
	}
}
