// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestLibmagicProbeReadsACancelAsCancel (batch 4a review round 16, the
// round-15 fix's red-first test): `brew list libmagic` failing because the
// walk was cancelled is not "libmagic is missing" — Ctrl-C printed
// "Installing libmagic via Homebrew…" and a failed-install warning. A brew
// shim on PATH answers `list` with exit 1 (missing) and records `install`;
// with a live ctx the install is attempted, with a cancelled one nothing is
// printed. The function only acts on darwin, so elsewhere this test skips.
func TestLibmagicProbeReadsACancelAsCancel(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("installLibmagicIfNeeded acts only on darwin")
	}
	bin := t.TempDir()
	logPath := filepath.Join(bin, "argv.log")
	shim := "#!/bin/sh\necho \"$@\" >> '" + logPath + "'\n[ \"$1\" = list ] && exit 1\nexit 0\n"
	if err := os.WriteFile(filepath.Join(bin, "brew"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if out := captureCollectorStdout(t, func() { installLibmagicIfNeeded(cancelled) }); out != "" {
		t.Errorf("a cancelled walk printed %q; want nothing — the probe failed on the cancel, not on a missing libmagic", strings.TrimSpace(out))
	}
	out := captureCollectorStdout(t, func() { installLibmagicIfNeeded(context.Background()) })
	if !strings.Contains(out, "Installing libmagic via Homebrew") {
		t.Errorf("a live walk with libmagic missing printed %q; want the install attempted", strings.TrimSpace(out))
	}
	logged, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logged), "install libmagic") {
		t.Errorf("the shim saw %q; want `brew install libmagic` on the live walk", strings.TrimSpace(string(logged)))
	}
}

func captureCollectorStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() { os.Stdout = saved }()
	fn()
	_ = w.Close()
	return <-done
}
