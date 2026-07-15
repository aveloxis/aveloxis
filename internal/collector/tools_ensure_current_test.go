// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// v0.27.6 — ensureScancodeCurrent unification.
//
// The regression this closes: the monthly CheckAndUpdateTools →
// installScancode path ran a bare `pipx install scancode-toolkit-mini`
// against an ALREADY-INSTALLED package, which fails ("already seems
// to be installed" — pipx install is not upgrade), so the
// typecode-libmagic injection in the success branch never re-ran; the
// code then fell through to `pip install --user`, creating a SECOND,
// UNINJECTED scancode that could shadow the pipx venv's binary. The
// standalone upgrade-tools CLI did it correctly; the monthly path had
// diverged. All three paths now share ensureScancodeCurrent.

// pipxRecorderStub writes a fake pipx that appends its args to
// logFile. failOn (optional) makes the named subcommand exit 1 with
// pipx's real already-installed message.
func pipxRecorderStub(t *testing.T, logFile, failOn string) string {
	t.Helper()
	binDir := t.TempDir()
	script := `#!/bin/sh
echo "$@" >> "` + logFile + `"
if [ "$1" = "` + failOn + `" ]; then
  echo "'scancode-toolkit-mini' already seems to be installed." >&2
  exit 1
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "pipx"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binDir
}

// pipRecorderStub adds fake pip3/pip binaries that record invocations
// — used to prove the pip fallback is NEVER reached on the installed
// branch.
func pipRecorderStub(t *testing.T, binDir, logFile string) {
	t.Helper()
	script := `#!/bin/sh
echo "PIP $@" >> "` + logFile + `"
exit 0
`
	for _, name := range []string{"pip3", "pip"} {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnsureScancodeCurrentInstalledUpgradesAndReinjects(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "calls.log")
	binDir := pipxRecorderStub(t, logFile, "") // everything succeeds
	pipRecorderStub(t, binDir, logFile)
	t.Setenv("PATH", binDir+":/bin:/usr/bin")

	if err := EnsureScancodeCurrent(true); err != nil {
		t.Fatalf("installed-branch upgrade must succeed with a healthy pipx, got %v", err)
	}
	got, _ := os.ReadFile(logFile)
	calls := string(got)
	if !strings.Contains(calls, "upgrade "+scancodePipxPackage) {
		t.Errorf("installed branch must run `pipx upgrade %s`, got:\n%s", scancodePipxPackage, calls)
	}
	if !strings.Contains(calls, "inject "+scancodePipxPackage+" typecode-libmagic") {
		t.Errorf("installed branch must ALWAYS re-inject typecode-libmagic after upgrade (pipx upgrade can rebuild the venv), got:\n%s", calls)
	}
	// The two negative halves of the regression:
	if strings.Contains(calls, "install "+scancodePipxPackage) {
		t.Errorf("NEGATIVE TRIPWIRE: the installed branch must never run a bare `pipx install` — that's the exact call that failed monthly and cascaded into the shadow pip install. Got:\n%s", calls)
	}
	if strings.Contains(calls, "PIP ") {
		t.Errorf("NEGATIVE TRIPWIRE: the installed branch must never fall back to pip — a second, uninjected scancode can shadow the venv binary. Got:\n%s", calls)
	}
}

func TestEnsureScancodeCurrentInstalledUpgradeFailureIsSurfacedNotPipFallback(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "calls.log")
	binDir := pipxRecorderStub(t, logFile, "upgrade") // upgrade exits 1
	pipRecorderStub(t, binDir, logFile)
	t.Setenv("PATH", binDir+":/bin:/usr/bin")

	err := EnsureScancodeCurrent(true)
	if err == nil {
		t.Fatal("a failed pipx upgrade on the installed branch must surface as an error — silently 'fixing' it with pip was the shadow-install bug")
	}
	got, _ := os.ReadFile(logFile)
	if strings.Contains(string(got), "PIP ") {
		t.Errorf("the installed branch must NOT reach the pip fallback even when pipx upgrade fails, got:\n%s", string(got))
	}
}

func TestEnsureScancodeCurrentFreshInstallsAndInjects(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "calls.log")
	binDir := pipxRecorderStub(t, logFile, "")
	t.Setenv("PATH", binDir+":/bin:/usr/bin")

	if err := EnsureScancodeCurrent(false); err != nil {
		t.Fatalf("fresh install must succeed, got %v", err)
	}
	got, _ := os.ReadFile(logFile)
	calls := string(got)
	if !strings.Contains(calls, "install "+scancodePipxPackage) {
		t.Errorf("fresh branch must `pipx install`, got:\n%s", calls)
	}
	if !strings.Contains(calls, "inject "+scancodePipxPackage+" typecode-libmagic") {
		t.Errorf("fresh branch must inject typecode-libmagic after install, got:\n%s", calls)
	}
}

// TestPipxUpgradeScancodeBodyHasNoInstall is the source-level half of
// the negative tripwire: the upgrade helper may never regrow an
// install call.
func TestPipxUpgradeScancodeBodyHasNoInstall(t *testing.T) {
	src, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatal(err)
	}
	body := scancodeMethodBody(t, string(src), "func pipxUpgradeScancode(")
	// Strip // comments before matching (the v0.21.5 / v0.27.4
	// lesson: doc comments legitimately NAME the forbidden pattern).
	var code []string
	for _, line := range strings.Split(body, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	stripped := strings.Join(code, "\n")
	if strings.Contains(stripped, `"install"`) {
		t.Error("pipxUpgradeScancode must never pass \"install\" to pipx — upgrade is the only verb allowed on the installed branch")
	}
	if !strings.Contains(stripped, `"upgrade"`) {
		t.Error("pipxUpgradeScancode must run pipx upgrade")
	}
}

// TestAllThreePathsRouteThroughEnsureScancodeCurrent pins the
// unification wiring: the tool-registry InstallFunc (install-tools +
// the monthly CheckAndUpdateTools both call it via runToolInstall)
// delegates, and the upgrade-tools CLI delegates.
func TestAllThreePathsRouteThroughEnsureScancodeCurrent(t *testing.T) {
	toolsSrc, err := os.ReadFile("tools.go")
	if err != nil {
		t.Fatal(err)
	}
	installBody := scancodeMethodBody(t, string(toolsSrc), "func installScancode(")
	if !strings.Contains(installBody, "ensureScancodeCurrent(") {
		t.Error("installScancode (the InstallFunc behind install-tools AND the monthly CheckAndUpdateTools) must delegate to ensureScancodeCurrent with the observed install state")
	}
	if !strings.Contains(installBody, `exec.LookPath("scancode")`) {
		t.Error("installScancode must derive alreadyInstalled from exec.LookPath(\"scancode\") — the same gate CheckAndUpdateTools uses to decide a tool is 'installed'")
	}

	cmdSrc, err := os.ReadFile("../../cmd/aveloxis/upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cmdSrc), "EnsureScancodeCurrent(true)") {
		t.Error("the upgrade-tools CLI must delegate to collector.EnsureScancodeCurrent(true) — a private re-implementation is how the three paths diverged in the first place")
	}
}
