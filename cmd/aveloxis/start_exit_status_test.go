// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/pidfile"
)

// PR #197 round 16 (Copilot round 6, finding 1): `aveloxis start`
// printed "Failed to start <component>" and then returned nil, so a
// refused start — the round-11 finding-2 unknown-liveness refusal
// included — exited 0 having started NOTHING, and `start all` reported
// success over a silently partial fleet (the v0.27.106 nonzero-exit
// convention: a CLI that did not do what it was asked must say so in
// its exit status, because cron and deploy scripts read nothing else).
//
// Driven through the REAL RunE: web is never gated (the deploy gate
// runs only for serve), so no database is needed, and a corrupt
// pidfile makes componentAlreadyRunning return its error arm before
// anything is spawned. The absence of web.log afterwards is the proof
// that the refusal happened before the exec.
func TestStartExitsNonzeroWhenAComponentRefusesToStart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	pidPath := pidfile.Path("web")
	if err := os.WriteFile(pidPath, []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := filepath.Join(home, "aveloxis.json") // never read: web is not gated
	cmd := startCmd(&cfg)
	err := cmd.RunE(cmd, []string{"web"})
	if err == nil {
		t.Fatal("start web with an unreadable pidfile returned nil — the refusal must reach the exit status, not only stdout")
	}
	if !strings.Contains(err.Error(), "web") {
		t.Errorf("the error must name the component that failed, got: %v", err)
	}

	if _, statErr := os.Stat(pidfile.LogPath("web")); statErr == nil {
		t.Error("web.log exists — the component was started despite the refusal")
	}
	got, _ := os.ReadFile(pidPath)
	if string(got) != "not-a-pid\n" {
		t.Errorf("the unreadable pidfile must be left for the operator, got %q", got)
	}
}
