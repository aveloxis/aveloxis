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
	"time"
)

// v0.27.6 — dispatcher gating on toolchain health + the pinned
// TYPECODE_LIBMAGIC_* env pair on every scancode subprocess.
//
// The gate deliberately REVISES the v0.25.x awareness-only operator
// decision: on 2026-06-11 the preflight logged SYSTEM-LEVEL FAILURE
// at startup and the dispatcher then ran 2,473 scans on the broken
// toolchain anyway (stderr artifacts to 9.5 GB, every worker wedged).

func gateTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// healthyScancodeStub writes a fake `scancode` script onto a private
// PATH. The stub records its environment (one VAR=value per line) to
// envDump and writes a minimally-valid scancode JSON to whatever path
// follows its --json flag, then exits 0 — a "healthy toolchain".
func healthyScancodeStub(t *testing.T, envDump string) {
	t.Helper()
	binDir := t.TempDir()
	script := `#!/bin/sh
env > "` + envDump + `"
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "--json" ]; then out="$a"; fi
  prev="$a"
done
if [ -n "$out" ]; then
  printf '%s' '{"headers":[{"errors":[],"extra_data":{"files_count":1}}]}' > "$out"
fi
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "scancode"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// PATH = stub dir + /bin + /usr/bin so /bin/sh, env, printf resolve.
	t.Setenv("PATH", binDir+":/bin:/usr/bin")
}

// TestProbeCarriesTypecodeEnvPair is the behavioral half of the
// env-pairing tripwire: a pinned pair must ARRIVE in the scancode
// subprocess's environment.
func TestProbeCarriesTypecodeEnvPair(t *testing.T) {
	envDump := filepath.Join(t.TempDir(), "env.txt")
	healthyScancodeStub(t, envDump)

	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{CloneDir: t.TempDir()})
	w.setTypecodeEnvPair(typecodeEnvPair{LibPath: "/venv/libmagic.so", DBPath: "/venv/magic.mgc"})

	status, _, corrupt := w.probeScancodeHealth(context.Background())
	if status != "ok" {
		t.Fatalf("healthy stub must probe ok, got %q (corrupt=%v)", status, corrupt)
	}
	dump, err := os.ReadFile(envDump)
	if err != nil {
		t.Fatalf("stub never ran or never dumped env: %v", err)
	}
	env := string(dump)
	if !strings.Contains(env, "TYPECODE_LIBMAGIC_PATH=/venv/libmagic.so") ||
		!strings.Contains(env, "TYPECODE_LIBMAGIC_DB_PATH=/venv/magic.mgc") {
		t.Errorf("the pinned typecode env pair must arrive in the scancode subprocess environment — without it typecode's broken plugin resolution silently falls back to the system libmagic (the July 2026 chaoss.tv root cause). Got env:\n%s", env)
	}
}

// TestEveryScancodeExecSourcesWorkerEnv is the source-contract half:
// each place the worker execs the scancode binary must set cmd.Env
// from w.scancodeEnv() (the seam that carries the pair).
func TestEveryScancodeExecSourcesWorkerEnv(t *testing.T) {
	for file, fn := range map[string]string{
		"scancode_worker.go":    "func (w *ScancodeWorker) executeScan(",
		"scancode_preflight.go": "func (w *ScancodeWorker) probeScancodeHealth(",
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		body := scancodeMethodBody(t, string(data), fn)
		if !strings.Contains(body, "cmd.Env = w.scancodeEnv()") {
			t.Errorf("%s in %s must set `cmd.Env = w.scancodeEnv()` — a scancode exec that skips the seam can silently run against the wrong libmagic pairing", fn, file)
		}
	}
}

// TestDispatcherGatesOnToolchainHealth is the negative tripwire
// against the "unconditional dispatcher" regression: the health gate
// must sit in the dispatcher loop BEFORE the claim, so a BROKEN
// toolchain claims NOTHING (June 11: 2,473 scans ran after the
// preflight had already said BROKEN).
func TestDispatcherGatesOnToolchainHealth(t *testing.T) {
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) dispatcher(")
	gate := strings.Index(body, "w.healthy.Load()")
	claim := strings.Index(body, "ClaimNextScancodeRepo(")
	if gate < 0 {
		t.Fatal("dispatcher must consult w.healthy.Load() — while the toolchain is BROKEN it must claim NOTHING (this deliberately revises the v0.25.x awareness-only decision; see the June 11 evidence in the dispatcher comment)")
	}
	if claim < 0 {
		t.Fatal("dispatcher must still claim via ClaimNextScancodeRepo")
	}
	if gate > claim {
		t.Error("the health gate must be checked BEFORE ClaimNextScancodeRepo — gating after the claim locks rows on a broken toolchain")
	}
	if !strings.Contains(body, "awaitHealthyToolchain") {
		t.Error("the paused dispatcher must park in awaitHealthyToolchain (re-probe every 15 min, auto-resume on a passing probe)")
	}
}

// TestAwaitHealthyToolchainResumesOnPassingProbe drives the pause
// loop behaviorally: a broken gate + a healthy scancode stub must
// auto-resume within a few (shortened) recheck ticks.
func TestAwaitHealthyToolchainResumesOnPassingProbe(t *testing.T) {
	envDump := filepath.Join(t.TempDir(), "env.txt")
	healthyScancodeStub(t, envDump)

	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{CloneDir: t.TempDir()})
	w.healthy.Store(false)
	w.healthRecheck = 20 * time.Millisecond

	done := make(chan struct{})
	go func() {
		w.awaitHealthyToolchain(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("awaitHealthyToolchain must return once a probe passes — the dispatcher would otherwise stay paused forever on a healed host")
	}
	if !w.healthy.Load() {
		t.Error("a passing probe must flip the health gate back to true")
	}
}

// TestAwaitHealthyToolchainHonorsCtxCancel: a still-broken host plus
// shutdown must not leak the pause goroutine.
func TestAwaitHealthyToolchainHonorsCtxCancel(t *testing.T) {
	// PATH with no scancode → the probe classifies not_installed
	// (never OK), so only ctx-cancel can end the loop.
	t.Setenv("PATH", t.TempDir())
	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{CloneDir: t.TempDir()})
	w.healthy.Store(false)
	w.healthRecheck = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.awaitHealthyToolchain(ctx)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("awaitHealthyToolchain must exit promptly on ctx cancel")
	}
	if w.healthy.Load() {
		t.Error("a never-passing probe must leave the gate broken")
	}
}

// TestPreflightRunsLadderAndSetsGate pins the preflight orchestration:
// env-pair verification first, then the probe, the remediation ladder
// on the libmagic fingerprint, and finally the health gate.
func TestPreflightRunsLadderAndSetsGate(t *testing.T) {
	data, err := os.ReadFile("scancode_preflight.go")
	if err != nil {
		t.Fatal(err)
	}
	body := scancodeMethodBody(t, string(data), "func (w *ScancodeWorker) preflight(")
	ensure := strings.Index(body, "w.ensureTypecodeEnvPair(ctx)")
	probe := strings.Index(body, "w.probeScancodeHealth(ctx)")
	ladder := strings.Index(body, "remediateCorruptLibmagic(")
	gate := strings.Index(body, "w.setToolchainHealth(")
	if ensure < 0 || probe < 0 || ladder < 0 || gate < 0 {
		t.Fatalf("preflight must ensure the env pair, probe, run the ladder on the libmagic fingerprint, and set the health gate (found ensure=%d probe=%d ladder=%d gate=%d)", ensure, probe, ladder, gate)
	}
	if !(ensure < probe && probe < ladder && ladder < gate) {
		t.Error("preflight ordering must be: ensureTypecodeEnvPair → probe → remediation ladder → setToolchainHealth — probing before the pairing wastes the deterministic fix")
	}
}

// TestEnsureTypecodeEnvPairInjectsWhenWheelAbsent drives the startup
// injection-verification behaviorally with a recording pipx stub: a
// missing wheel must trigger `pipx inject scancode-toolkit-mini
// typecode-libmagic`.
func TestEnsureTypecodeEnvPairInjectsWhenWheelAbsent(t *testing.T) {
	binDir := t.TempDir()
	logFile := filepath.Join(t.TempDir(), "pipx.log")
	stub := `#!/bin/sh
echo "$@" >> "` + logFile + `"
exit 0
`
	if err := os.WriteFile(filepath.Join(binDir, "pipx"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+":/bin:/usr/bin") // no scancode → discovery fails
	t.Setenv("HOME", t.TempDir())             // no pipx-default venv either

	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{CloneDir: t.TempDir()})
	w.ensureTypecodeEnvPair(context.Background())

	got, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatal("pipx stub never invoked — with the wheel undiscoverable, the preflight must run the injection")
	}
	if !strings.Contains(string(got), "inject "+scancodePipxPackage+" typecode-libmagic") {
		t.Errorf("expected `pipx inject %s typecode-libmagic`, got %q", scancodePipxPackage, string(got))
	}
}

// TestStaleLockWindowDerivedFromCap is the negative tripwire for the
// June 2026 duplicate-claim bug: the worker must DERIVE the claim
// query's stale-lock window from the adaptive-timeout cap instead of
// leaning on the 12h constant that undercut 24h at-cap scans.
func TestStaleLockWindowDerivedFromCap(t *testing.T) {
	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{RunTimeoutCap: 24 * time.Hour})
	if got := w.staleLockWindow(); got != 26*time.Hour {
		t.Errorf("staleLockWindow must be runTimeoutCap + 2h (26h for the default 24h cap), got %v — a window at or below the cap lets a second worker claim a repo whose scan is legitimately still running", got)
	}
	// The dispatcher must pass the derived window into the claim.
	src := readScancodeWorkerSource(t)
	body := scancodeMethodBody(t, src, "func (w *ScancodeWorker) dispatcher(")
	if !strings.Contains(body, "w.staleLockWindow()") {
		t.Error("dispatcher must pass w.staleLockWindow() into ClaimNextScancodeRepo — hardcoding a window (or omitting it) re-creates the duplicate-claim interleaving confirmed in the June 2026 logs")
	}
}
