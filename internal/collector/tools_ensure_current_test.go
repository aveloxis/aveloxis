// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
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

// pipxPinFailingStub is pipxRecorderStub with one failure: the pluggy
// hold (`pipx inject --force ...`) exits 1; install, upgrade and the
// typecode injection succeed. 0.29.234, review round 18.
func pipxPinFailingStub(t *testing.T, logFile string) string {
	t.Helper()
	binDir := t.TempDir()
	script := `#!/bin/sh
echo "$@" >> "` + logFile + `"
if [ "$1 $2" = "inject --force" ]; then
  echo "pip failed to build package" >&2
  exit 1
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "pipx"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return binDir
}

// 0.29.234 (review round 18, findings 1 and 3): a failed pluggy hold was a
// printed warning and a nil error, so `install-tools` printed "scancode
// installed" and exited 0 over a scancode that cannot start, and the monthly
// update counted it as updated. It is an error now on both pipx branches,
// one that names the retry command. On the fresh branch it must NOT fall
// through to pip (a second scancode beside the pipx one), and the typecode
// injection still runs so one retry command finishes the install.
func TestEnsureScancodeCurrentReportsAFailedPluggyHold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		installed bool
		first     string
	}{
		{"fresh install", false, "install " + scancodePipxPackage},
		{"upgrade", true, "upgrade " + scancodePipxPackage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logFile := filepath.Join(t.TempDir(), "calls.log")
			binDir := pipxPinFailingStub(t, logFile)
			pipRecorderStub(t, binDir, logFile)
			t.Setenv("PATH", binDir+":/bin:/usr/bin")

			err := EnsureScancodeCurrent(context.Background(), tc.installed)
			if err == nil {
				t.Fatal("a failed pluggy hold must be an error: scancode does not start with pluggy 1.7.0, and install-tools would print success over it")
			}
			if !errors.Is(err, errScancodePinFailed) {
				t.Errorf("the error must be errScancodePinFailed, got %v", err)
			}
			if want := "pipx inject --force " + scancodePipxPackage + " '" + scancodePluggyPin + "'"; !strings.Contains(err.Error(), want) {
				t.Errorf("the error must give the retry command %q, got %v", want, err)
			}
			// Round 19, finding 1: a rerun of install-tools finds scancode
			// on PATH and retries nothing; the message must say which
			// command does.
			if !strings.Contains(err.Error(), "aveloxis upgrade-tools") || !strings.Contains(err.Error(), "install-tools` does not retry") {
				t.Errorf("the error must name `aveloxis upgrade-tools` as the retry and say a rerun of install-tools is not one, got %v", err)
			}
			got, _ := os.ReadFile(logFile)
			calls := string(got)
			if !strings.Contains(calls, tc.first) {
				t.Fatalf("the stub was not reached with %q:\n%s", tc.first, calls)
			}
			if strings.Contains(calls, "PIP ") {
				t.Errorf("a failed hold must never fall through to pip: that leaves a second scancode beside the pipx one. Got:\n%s", calls)
			}
			if !strings.Contains(calls, "inject "+scancodePipxPackage+" typecode-libmagic") {
				t.Errorf("the typecode injection must still run after a failed hold, got:\n%s", calls)
			}
		})
	}
}

// 0.29.234 (review round 19, finding 1a): a hold that fails because the
// per-tool bound ran out is reported as the hold's failure, with its retry
// command, on the fresh branch as on the upgrade branch. The fresh branch
// joined the context's error onto it, which made install-tools print its
// generic "timed out ... rerun" line with no command, and a rerun finds
// scancode installed and does nothing.
func TestAHoldThatRunsOutOfTimeStillGivesTheRetryCommand(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "calls.log")
	binDir := t.TempDir()
	// install answers at once; the hold outlives the deadline and is killed.
	script := `#!/bin/sh
echo "$@" >> "` + logFile + `"
if [ "$1 $2" = "inject --force" ]; then
  exec sleep 60
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "pipx"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	pipRecorderStub(t, binDir, logFile)
	t.Setenv("PATH", binDir+":/bin:/usr/bin")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := EnsureScancodeCurrent(ctx, false)
	got, _ := os.ReadFile(logFile)
	if !strings.Contains(string(got), "inject --force") {
		t.Fatalf("the hold was not reached before the deadline (a slow machine?):\n%s", got)
	}
	if !errors.Is(err, errScancodePinFailed) {
		t.Fatalf("want errScancodePinFailed, got %v", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the hold's failure must not carry the deadline as its own error: install-tools would print only \"timed out\" and drop the retry command. Got %v", err)
	}
	if !strings.Contains(err.Error(), scancodePluggyHoldCmd) {
		t.Errorf("the error must give the retry command, got %v", err)
	}
	if strings.Contains(string(got), "PIP ") {
		t.Errorf("no pip after a failed hold, got:\n%s", got)
	}
}

// 0.29.234 (review round 18, finding 4): every by-hand scancode recipe the
// binary prints carries the hold. Three of them (the tool registry's manual
// install, the no-pipx upgrade error, the no-pip error) still gave a bare
// install, which builds a scancode that does not start.
func TestScancodeByHandRecipesCarryThePluggyPin(t *testing.T) {
	hold := "pipx inject --force " + scancodePipxPackage + " '" + scancodePluggyPin + "'"
	registered := 0
	for _, tool := range ExternalTools() {
		if tool.Name != "scancode" {
			continue
		}
		registered++
		if !strings.Contains(tool.InstallCmd, hold) {
			t.Errorf("scancode's InstallCmd (printed as the manual install) must carry %q, got %q", hold, tool.InstallCmd)
		}
	}
	srctest.MinCount(t, "registry entries named scancode", registered, 1)
	// No pipx on PATH, scancode installed: a host where pip installed it.
	// Its recipe is pip's (round 19, finding 3: the message gave three
	// pipx commands to the one host that cannot run them).
	t.Setenv("PATH", t.TempDir())
	err := EnsureScancodeCurrent(context.Background(), true)
	pipUpgrade := "pip3 install --user --upgrade " + scancodePipxPackage + " '" + scancodePluggyPin + "'"
	if err == nil || !strings.Contains(err.Error(), pipUpgrade) {
		t.Errorf("the no-pipx upgrade error must carry %q, got %v", pipUpgrade, err)
	}
	if err != nil && strings.Contains(err.Error(), "pipx upgrade") {
		t.Errorf("the no-pipx upgrade error must not tell a host without pipx to run pipx upgrade, got %v", err)
	}
	// Neither pipx nor pip: the fresh-install error's recipe.
	err = EnsureScancodeCurrent(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), hold) {
		t.Errorf("the neither-pipx-nor-pip error must carry %q, got %v", hold, err)
	}
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

	if err := EnsureScancodeCurrent(context.Background(), true); err != nil {
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
	if !strings.Contains(calls, "inject --force "+scancodePipxPackage+" "+scancodePluggyPin) {
		t.Errorf("installed branch must hold pluggy at %s after the upgrade (0.29.230: scancode does not start with pluggy 1.7.0), got:\n%s", scancodePluggyPin, calls)
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

	err := EnsureScancodeCurrent(context.Background(), true)
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

	if err := EnsureScancodeCurrent(context.Background(), false); err != nil {
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
	if !strings.Contains(calls, "inject --force "+scancodePipxPackage+" "+scancodePluggyPin) {
		t.Errorf("fresh branch must hold pluggy at %s after the install: a new venv gets pluggy 1.7.0 and scancode does not start, got:\n%s", scancodePluggyPin, calls)
	}
}

// 0.29.230: the pip fallback (a host without pipx) installs scancode and the
// pluggy bound in ONE pip command, so pip resolves them together.
func TestEnsureScancodeCurrentPipFallbackCarriesThePluggyPin(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "calls.log")
	binDir := t.TempDir() // no pipx here at all
	pipRecorderStub(t, binDir, logFile)
	t.Setenv("PATH", binDir+":/bin:/usr/bin")
	t.Setenv("HOME", t.TempDir())

	if err := EnsureScancodeCurrent(context.Background(), false); err != nil {
		t.Fatalf("fresh install through pip must succeed, got %v", err)
	}
	got, _ := os.ReadFile(logFile)
	if want := "PIP install --user " + scancodePipxPackage + " " + scancodePluggyPin; !strings.Contains(string(got), want) {
		t.Errorf("the pip fallback must run %q, got:\n%s", want, got)
	}
}

// The README and the commands guide give the by-hand command the binary
// runs. Until 0.29.231 the weekly tools job was held here too, because it
// carried its own copy of the install (unpinned and red from 2026-10-19 to
// 0.29.230 while the binary's install had the same fault unseen).
func TestScancodePluggyPinIsTheSameEverywhere(t *testing.T) {
	want := "pipx inject --force " + scancodePipxPackage + " '" + scancodePluggyPin + "'"
	// The weekly tools job is not in this list since 0.29.231: it installs
	// through `aveloxis install-tools`, so it runs the constant itself
	// (TestCanaryToolsJobInstallsThroughTheBinary forbids a line of its own).
	for _, rel := range []string{"README.md", "docs/guide/commands.md", "docs/getting-started/upgrading.md"} {
		if !strings.Contains(srctest.Read(t, rel), want) {
			t.Errorf("%s must carry the command the binary runs: %s", rel, want)
		}
	}
	// 0.29.234 (round 18, finding 4): no page gives a by-hand scancode
	// install without the hold; two did (one named another package). A
	// page either sends the reader to `aveloxis install-tools` or carries
	// the hold on the same line.
	root := srctest.Root(t)
	pages := []string{"README.md"}
	err := filepath.WalkDir(filepath.Join(root, "docs"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == "_build" {
			return filepath.SkipDir
		}
		if !d.IsDir() && strings.HasSuffix(p, ".md") {
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return rerr
			}
			pages = append(pages, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, rel := range pages {
		for i, line := range strings.Split(srctest.Read(t, rel), "\n") {
			if !strings.Contains(line, "pipx install scancode-toolkit") {
				continue
			}
			checked++
			if !strings.Contains(line, want) {
				t.Errorf("%s:%d gives a by-hand scancode install without %q", rel, i+1, want)
			}
		}
	}
	// The pip repair is spelled one way, the binary's (round 20).
	if up := srctest.Read(t, "docs/getting-started/upgrading.md"); !strings.Contains(up, scancodePipByHandUpgrade) {
		t.Errorf("docs/getting-started/upgrading.md must give the pip repair as the binary prints it: %s", scancodePipByHandUpgrade)
	}
	// The README's tools table is one such line: zero means the scan read nothing.
	srctest.MinCount(t, "by-hand scancode install lines examined", checked, 1)
	if scancodePluggyPin != "pluggy<1.7" {
		t.Errorf("scancodePluggyPin = %q: moving it needs the live canary (TestLiveScancodeOutputShape) run against the new bound first", scancodePluggyPin)
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
	if !strings.Contains(string(cmdSrc), "EnsureScancodeCurrent(ctx, true)") {
		t.Error("the upgrade-tools CLI must delegate to collector.EnsureScancodeCurrent(ctx, true) — a private re-implementation is how the three paths diverged in the first place")
	}
}

// scancodeStub writes a fake scancode that prints body's lines and exits
// with code, and returns its path.
func scancodeStub(t *testing.T, dir, stderrLine string, code int) string {
	t.Helper()
	p := filepath.Join(dir, "scancode")
	script := "#!/bin/sh\n[ \"$1\" = \"--version\" ] || exit 64\necho \"" + stderrLine + "\" >&2\nexit " + strconv.Itoa(code) + "\n"
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// 0.29.234 (operator, 2026-11-25, after review rounds 18 to 20 found one
// gap from three sides): install-tools asks an installed scancode whether
// it starts. A scancode left without the pluggy hold (a failed, timed-out
// or interrupted hold) was "already installed" to every later run.
func TestCheckScancodeStarts(t *testing.T) {
	var check func(context.Context, string) error
	for _, tool := range ExternalTools() {
		if tool.Name == "scancode" {
			check = tool.CheckStarts
		}
	}
	if check == nil {
		t.Fatal("the registry's scancode entry must carry CheckStarts: install-tools reads it from there")
	}
	t.Run("a scancode that starts", func(t *testing.T) {
		if err := check(context.Background(), scancodeStub(t, t.TempDir(), "ScanCode version 32.5.0", 0)); err != nil {
			t.Errorf("want nil, got %v", err)
		}
	})
	t.Run("a scancode that does not", func(t *testing.T) {
		path := scancodeStub(t, t.TempDir(), "ValueError: did not find any 'pre_scan' hooks in <module>", 1)
		err := check(context.Background(), path)
		if err == nil {
			t.Fatal("a scancode whose --version exits 1 does not start")
		}
		for _, want := range []string{path, "pre_scan", scancodePluggyHoldCmd, scancodePipByHandUpgrade} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the error must carry %q (what failed, its own last line, both repairs), got %v", want, err)
			}
		}
	})
	t.Run("a stop is not a verdict", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := check(ctx, scancodeStub(t, t.TempDir(), "ScanCode version 32.5.0", 0))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("an ended context is the context's error, got %v", err)
		}
		if strings.Contains(err.Error(), "does not start") {
			t.Errorf("an interrupted check must not say scancode does not start, got %v", err)
		}
	})
}
