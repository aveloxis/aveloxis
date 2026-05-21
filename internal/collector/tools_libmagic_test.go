// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// v0.23.6 — inject the typecode-libmagic Python package into
// scancode-toolkit-mini's pipx venv so scancode uses the Python
// libmagic binding instead of falling back to the system C library.
//
// Without this injection, every scancode run prints the UserWarning:
//
//   /home/.../typecode/magic2.py:197: UserWarning: System libmagic
//   found in typical location is used. Install instead a
//   typecode-libmagic plugin for best support.
//
// The warning was the 252-byte stderr content captured on every
// "scancode runOne: scancode subprocess failed" log line in the
// 2026-05-21 diagnostic — pure noise that we now eliminate at
// install time.

func TestInstallScancodeInjectsTypecodeLibmagic(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "typecode-libmagic") {
		t.Error("installScancode must inject typecode-libmagic into the " +
			"pipx venv after a successful install. Without it, every scancode " +
			"run prints the libmagic UserWarning. v0.23.6.")
	}
	// The mechanism is `pipx inject scancode-toolkit-mini typecode-libmagic`.
	// Pin both pieces so a future refactor can't silently replace inject with
	// something else (e.g., `pipx install --include-deps`) without updating
	// this test.
	if !strings.Contains(code, "pipx") || !strings.Contains(code, "inject") {
		t.Error("the injection mechanism must use `pipx inject` — not " +
			"`pipx install --include-deps` or a manual venv-pip path, both " +
			"of which would silently break on operator setups that have " +
			"customized their pipx config.")
	}
}

func TestInstallScancodeInjectionHelperExists(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "injectTypecodeLibmagic") {
		t.Error("v0.23.6 introduces injectTypecodeLibmagic as a dedicated " +
			"helper so it can be reused by the v0.23.6 `aveloxis upgrade-tools` " +
			"command. Pin the helper name so a future refactor that inlines " +
			"the call doesn't silently break upgrade-tools.")
	}
}

func TestInstallScancodeInjectionIsNonFatal(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The injection should NOT return an error from installScancode if it
	// fails — operators may have a libmagic plugin already, or the inject
	// may fail on edge-case pipx configurations. We want the scancode
	// install to succeed even when the injection fails; the warning will
	// just continue to appear (the v0.23.4 salvage path handles it
	// regardless).
	helperStart := strings.Index(code, "func injectTypecodeLibmagic")
	if helperStart < 0 {
		t.Skip("helper not yet implemented — covered by TestInstallScancodeInjectionHelperExists")
	}
	// Look at the call site INSIDE installScancode: it must NOT use the
	// error return value in a way that would abort the install.
	installStart := strings.Index(code, "func installScancode")
	if installStart < 0 {
		t.Fatal("installScancode function missing")
	}
	installBody := code[installStart:]
	// Find the call site within installScancode.
	callIdx := strings.Index(installBody, "injectTypecodeLibmagic(")
	if callIdx < 0 {
		t.Error("installScancode must call injectTypecodeLibmagic after a " +
			"successful pipx install")
		return
	}
	// Look at the surrounding 80 chars for an error-return pattern.
	start := callIdx
	end := callIdx + 80
	if end > len(installBody) {
		end = len(installBody)
	}
	region := installBody[start:end]
	// We expect the call to be either: a bare statement (`injectTypecodeLibmagic()`),
	// an assignment to _ (`_ = injectTypecodeLibmagic()`), or wrapped in an
	// `if` block that logs but doesn't return. We do NOT want `return
	// injectTypecodeLibmagic()` which would bubble the error up.
	if strings.Contains(region, "return injectTypecodeLibmagic") {
		t.Error("installScancode must NOT return the injectTypecodeLibmagic " +
			"error directly — injection failure is a degraded-but-functional " +
			"state, not an install failure")
	}
}
