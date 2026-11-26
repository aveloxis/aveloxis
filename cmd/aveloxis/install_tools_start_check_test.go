// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// installedToolStubs puts a fake scc, scorecard and scancode on a PATH of
// their own, so install-tools finds all three installed and installs
// nothing. scancode's --version exits with scancodeExit.
func installedToolStubs(t *testing.T, scancodeExit string) {
	t.Helper()
	dir := t.TempDir()
	for name, script := range map[string]string{
		"scc":       "#!/bin/sh\nexit 0\n",
		"scorecard": "#!/bin/sh\nexit 0\n",
		"scancode":  "#!/bin/sh\necho \"ValueError: did not find any 'pre_scan' hooks\" >&2\nexit " + scancodeExit + "\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+":/bin:/usr/bin")
}

// 0.29.234 (operator, 2026-11-25): `aveloxis install-tools` on a host whose
// scancode is installed but cannot start exits non-zero, every time it is
// run, so `aveloxis install-tools && aveloxis start all` stops. Before, an
// installed scancode was skipped unasked: after a failed, timed-out or
// interrupted pluggy hold the second run exited 0 (review rounds 18 to 20).
func TestInstallToolsFailsOverAnInstalledScancodeThatDoesNotStart(t *testing.T) {
	installedToolStubs(t, "1")
	for run := 1; run <= 2; run++ {
		err := runInstallTools(context.Background())
		if err == nil {
			t.Fatalf("run %d: install-tools must exit non-zero while the installed scancode does not start", run)
		}
		if !strings.Contains(err.Error(), "1 of 3 tools failed") {
			t.Errorf("run %d: want the one failed tool reported, got %v", run, err)
		}
	}
}

func TestInstallToolsPassesOverAnInstalledScancodeThatStarts(t *testing.T) {
	installedToolStubs(t, "0")
	if err := runInstallTools(context.Background()); err != nil {
		t.Fatalf("all three installed and scancode starts: want nil, got %v", err)
	}
}

// The two tests above drive the already-installed site only: runInstallTools
// reads the tool registry itself, so a test cannot make it install without
// the network. This is the wiring pin for the other site (review round 21):
// both the tool found installed and the tool just installed are asked
// whether they start, and each asks before its success line is printed.
func TestInstallToolsAsksBothAnInstalledAndAJustInstalledToolToStart(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func runInstallTools("))
	if n := strings.Count(body, "!toolStarts(ctx, tool, path)"); n != 2 {
		t.Fatalf("runInstallTools asks toolStarts %d time(s); want 2: the already-installed tool and the one just installed", n)
	}
	for _, success := range []string{`already installed: %s`, `%s installed: %s`} {
		at := strings.Index(body, success)
		if at < 0 {
			t.Fatalf("runInstallTools no longer prints %q: this test reads the wrong text", success)
		}
		before := body[:at]
		check := strings.LastIndex(before, "!toolStarts(ctx, tool, path)")
		look := strings.LastIndex(before, "exec.LookPath(tool.CheckBinary)")
		if check < look || look < 0 {
			t.Errorf("the success line %q must come after a toolStarts check on the path its own LookPath found", success)
		}
	}
}

// A check that runs out of its bound is said to be that (round 21): the
// install timeout's words ("a slow network or registry ... install by hand")
// are wrong for `scancode --version`, which uses neither.
func TestStartCheckTimeoutHasItsOwnWords(t *testing.T) {
	got := startCheckFailureText("scancode", context.DeadlineExceeded)
	if strings.Contains(got, "registry") || strings.Contains(got, "install by hand") {
		t.Errorf("a timed-out start check must not use the install timeout's advice: %q", got)
	}
	for _, want := range []string{"scancode --version", "did not answer"} {
		if !strings.Contains(got, want) {
			t.Errorf("want %q in %q", want, got)
		}
	}
	if got := startCheckFailureText("scancode", os.ErrNotExist); !strings.Contains(got, os.ErrNotExist.Error()) {
		t.Errorf("another failure is printed as it is, got %q", got)
	}
}
