// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.27.6 — behavioral tests for the self-healing ladder and the
// typecode-libmagic env-pair discovery (scancode_remediate.go).
//
// July 2026 root cause (verified on chaoss.tv): the system libmagic
// was healthy and the wheel was injected, yet typecode's plugin
// resolution failed inside the mini venv and fell back to the system
// libmagic — a 5.39 wheel .so reading the 5.45-compiled system
// magic.mgc, producing thousands of "offset invalid" warnings. The
// TYPECODE_LIBMAGIC_* env pair (checked FIRST by typecode/magic2.py)
// is the deterministic fix; the ladder applies it, injects when the
// wheel is absent, and demotes the OS package reinstall to
// advice-only.

func remediateTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeRemediation builds deps whose probe walks through the given
// status sequence (repeating the last entry) and whose runCmd records
// every invocation.
type fakeRemediation struct {
	probeSeq  []string // db.StatusOK / db.StatusBroken per call
	probeIdx  int
	commands  []string
	pairOK    bool // discovery result
	pairAfter bool // discovery result AFTER an inject command ran
	applied   []typecodeEnvPair
	injected  bool
}

func (f *fakeRemediation) deps() remediationDeps {
	return remediationDeps{
		logger: remediateTestLogger(),
		goos:   "linux",
		probe: func(ctx context.Context) (string, string, bool) {
			idx := f.probeIdx
			if idx > len(f.probeSeq)-1 {
				idx = len(f.probeSeq) - 1
			}
			s := f.probeSeq[idx]
			f.probeIdx++
			if s == db.StatusOK {
				return db.StatusOK, "", false
			}
			return db.StatusBroken, "libmagic spam", true
		},
		runCmd: func(ctx context.Context, name string, args ...string) error {
			f.commands = append(f.commands, name+" "+strings.Join(args, " "))
			if name == "pipx" && len(args) > 0 && args[0] == "inject" {
				f.injected = true
			}
			return nil
		},
		discover: func() (typecodeEnvPair, bool) {
			if f.pairOK || (f.injected && f.pairAfter) {
				return typecodeEnvPair{LibPath: "/venv/libmagic.so", DBPath: "/venv/magic.mgc"}, true
			}
			return typecodeEnvPair{}, false
		},
		applyEnvPair: func(p typecodeEnvPair) { f.applied = append(f.applied, p) },
	}
}

func TestRemediationEnvPairingIsStepOneAndSufficient(t *testing.T) {
	// Discovery succeeds and the re-probe passes: the ladder must
	// stop after step 1 — no injection, no OS commands.
	f := &fakeRemediation{probeSeq: []string{db.StatusOK}, pairOK: true}
	status, _ := remediateCorruptLibmagic(context.Background(), f.deps(), db.StatusBroken, "broken")
	if status != db.StatusOK {
		t.Fatalf("ladder must return OK when the env pairing fixes the probe, got %q", status)
	}
	if len(f.applied) != 1 {
		t.Errorf("the discovered pair must be applied exactly once, got %d", len(f.applied))
	}
	if len(f.commands) != 0 {
		t.Errorf("no external commands may run when the env pairing alone fixes the host (injection alone was NEVER a durable fix on chaoss.tv — pairing is primary), got %v", f.commands)
	}
}

func TestRemediationInjectsWhenWheelAbsentThenApplies(t *testing.T) {
	// Wheel absent → step 1 skips, step 2 injects, re-discovery
	// succeeds, pair applied, probe passes.
	f := &fakeRemediation{probeSeq: []string{db.StatusOK}, pairOK: false, pairAfter: true}
	status, _ := remediateCorruptLibmagic(context.Background(), f.deps(), db.StatusBroken, "broken")
	if status != db.StatusOK {
		t.Fatalf("ladder must return OK after inject + pairing, got %q", status)
	}
	if !f.injected {
		t.Error("with the wheel absent, step 2 must run `pipx inject scancode-toolkit-mini typecode-libmagic`")
	}
	if len(f.applied) != 1 {
		t.Errorf("the pair must be applied after the post-inject re-discovery, got %d applications", len(f.applied))
	}
}

func TestRemediationExhaustedIsAdviceOnly(t *testing.T) {
	// Everything fails: the ladder must return the broken status and
	// must NOT execute any OS package manager — the reinstall is
	// LAST-resort ADVICE only (July 2026 RCA: the system DB was never
	// the problem; auto-reinstalling packages on a host where the
	// venv pairing is the real issue mutates the OS for nothing).
	f := &fakeRemediation{probeSeq: []string{db.StatusBroken}, pairOK: false, pairAfter: false}
	status, detail := remediateCorruptLibmagic(context.Background(), f.deps(), db.StatusBroken, "orig detail")
	if status != db.StatusBroken || detail != "orig detail" {
		t.Errorf("exhausted ladder must surface the broken classification, got %q / %q", status, detail)
	}
	for _, c := range f.commands {
		if strings.Contains(c, "apt-get") || strings.Contains(c, "brew") || strings.Contains(c, "sudo") {
			t.Errorf("the OS package reinstall must be advice-only — never executed. Ran: %q", c)
		}
	}
}

func TestRemediationStopsOnNonLibmagicReclassification(t *testing.T) {
	// After step 1 the probe reports a DIFFERENT broken class (e.g.
	// no-JSON without the fingerprint): the ladder can't help and
	// must stop, surfacing the new classification.
	f := &fakeRemediation{pairOK: true}
	f.probeSeq = []string{"reclassified"}
	deps := f.deps()
	deps.probe = func(ctx context.Context) (string, string, bool) {
		return db.StatusBroken, "no valid JSON", false // NOT libmagic
	}
	status, detail := remediateCorruptLibmagic(context.Background(), deps, db.StatusBroken, "orig")
	if status != db.StatusBroken || detail != "no valid JSON" {
		t.Errorf("a non-libmagic reclassification must stop the ladder and surface the NEW detail, got %q / %q", status, detail)
	}
	if f.injected {
		t.Error("the ladder must not keep injecting once the failure is no longer libmagic-shaped")
	}
}

// --- wheel discovery ---

func fakeVenv(t *testing.T, withLib, withDB bool) string {
	t.Helper()
	root := t.TempDir()
	sp := filepath.Join(root, "lib", "python3.11", "site-packages", "typecode_libmagic")
	if withLib {
		libDir := filepath.Join(sp, "lib")
		if err := os.MkdirAll(libDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(libDir, "libmagic.so"), []byte("elf"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if withDB {
		dataDir := filepath.Join(sp, "data")
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dataDir, "magic.mgc"), []byte("mgc"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestTypecodePairInVenvFindsMatchedPair(t *testing.T) {
	root := fakeVenv(t, true, true)
	pair, ok := typecodePairInVenv(root)
	if !ok {
		t.Fatal("a complete wheel tree must be discovered")
	}
	if !strings.HasSuffix(pair.LibPath, "libmagic.so") || !strings.HasSuffix(pair.DBPath, "magic.mgc") {
		t.Errorf("discovered pair wrong: %+v", pair)
	}
}

func TestTypecodePairInVenvRequiresBothHalves(t *testing.T) {
	// Pinning only one half would recreate the exact version-mismatch
	// class the pairing exists to prevent.
	if _, ok := typecodePairInVenv(fakeVenv(t, true, false)); ok {
		t.Error("a wheel with the .so but no magic.mgc must NOT be discovered")
	}
	if _, ok := typecodePairInVenv(fakeVenv(t, false, true)); ok {
		t.Error("a wheel with magic.mgc but no shared library must NOT be discovered")
	}
	if _, ok := typecodePairInVenv(t.TempDir()); ok {
		t.Error("an empty venv must not be discovered")
	}
}

func TestDiscoverTypecodeLibmagicFollowsScancodeBinary(t *testing.T) {
	// The primary search root is derived from the scancode binary on
	// PATH: <venv>/bin/scancode → two Dir() hops → venv root.
	root := fakeVenv(t, true, true)
	binDir := filepath.Join(root, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(binDir, "scancode")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("HOME", t.TempDir()) // keep the pipx-default fallback away from the real HOME

	pair, ok := discoverTypecodeLibmagic()
	if !ok {
		t.Fatal("discovery must resolve the venv from the scancode binary on PATH")
	}
	if !strings.Contains(pair.LibPath, root) || !strings.Contains(pair.DBPath, root) {
		t.Errorf("discovered pair must come from the binary's venv, got %+v", pair)
	}
}

func TestDiscoverTypecodeLibmagicFallsBackToPipxDefault(t *testing.T) {
	// No scancode on PATH: the pipx default venv location under HOME
	// must still be searched (handles wrapper scripts the binary
	// derivation can't follow).
	home := t.TempDir()
	venv := filepath.Join(home, ".local", "share", "pipx", "venvs", scancodePipxPackage)
	sp := filepath.Join(venv, "lib", "python3.12", "site-packages", "typecode_libmagic")
	for _, d := range []string{filepath.Join(sp, "lib"), filepath.Join(sp, "data")} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sp, "lib", "libmagic.so"), []byte("elf"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sp, "data", "magic.mgc"), []byte("mgc"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir()) // no scancode anywhere
	t.Setenv("HOME", home)

	pair, ok := discoverTypecodeLibmagic()
	if !ok {
		t.Fatal("discovery must fall back to the pipx default venv path")
	}
	if !strings.Contains(pair.DBPath, venv) {
		t.Errorf("pair must come from the pipx default venv, got %+v", pair)
	}
}

func TestPickSharedObject(t *testing.T) {
	dir := t.TempDir()
	so := filepath.Join(dir, "libmagic.so.1")
	if err := os.WriteFile(so, []byte("elf"), 0o644); err != nil {
		t.Fatal(err)
	}
	notLib := filepath.Join(dir, "libmagic.txt")
	_ = os.WriteFile(notLib, []byte("x"), 0o644)
	if got := pickSharedObject([]string{notLib, so}); got != so {
		t.Errorf("pickSharedObject must select the .so/.dylib file, got %q", got)
	}
	if got := pickSharedObject([]string{notLib}); got != "" {
		t.Errorf("no shared object present must return empty, got %q", got)
	}
}
