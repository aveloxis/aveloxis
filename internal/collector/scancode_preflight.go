// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// scancode_preflight.go is the scancode startup health check. A
// system-level failure of the scancode toolchain — most importantly the
// libmagic "offset invalid" warning-spam condition (the 2026-06-09 incident:
// millions of warnings per load, 9.5+ GB of stderr per repo, every worker
// wedged) — silently degrades the whole subsystem. The preflight runs ONE
// scancode invocation against a tiny synthetic input at startup, classifies
// the outcome, logs a systemic failure prominently, and records it in
// aveloxis_ops.aveloxis_status.
//
// v0.27.6 REVISES the original awareness-only contract (see
// scancode_remediate.go for the June 11 evidence): the preflight now also
//
//  1. discovers the venv's matched typecode-libmagic pair and pins it via
//     the TYPECODE_LIBMAGIC_* env vars on every scancode subprocess (the
//     deterministic fix for the July 2026 root cause — typecode's plugin
//     resolution failing inside the mini venv and cross-loading a wheel
//     .so against the system's differently-versioned compiled magic DB),
//     injecting the wheel first when it's absent;
//  2. runs the remediation ladder when the probe hits the libmagic
//     fingerprint; and
//  3. feeds the worker's toolchain health gate — while BROKEN, the
//     dispatcher claims NOTHING and awaitHealthyToolchain re-probes every
//     15 minutes, auto-resuming on a passing probe.

const (
	scancodePreflightTimeout   = 90 * time.Second // the health check must never hang the worker
	scancodePreflightStderrCap = 1 << 20          // 1 MB of stderr head is plenty to detect the signature
	scancodePreflightRepeatN   = 50               // a single line repeated >= this many times = systemic spam
	scancodeStatusName         = "scancode"
	scancodeStatusSource       = "scancode preflight"
)

// capWriter captures up to cap bytes (the head) of a stream and discards the
// rest, never blocking the writer. Used so a spamming subprocess can't make the
// preflight buffer gigabytes (the exact bug the preflight is meant to detect).
type capWriter struct {
	buf []byte
	cap int
}

func (w *capWriter) Write(p []byte) (int, error) {
	if room := w.cap - len(w.buf); room > 0 {
		if room > len(p) {
			room = len(p)
		}
		w.buf = append(w.buf, p[:room]...)
	}
	return len(p), nil
}

// preflight runs the startup health sequence: env-pair pinning, the health
// probe, the remediation ladder on the libmagic fingerprint, status
// recording, and the dispatcher health gate. Called from Run before the
// dispatcher starts claiming work.
func (w *ScancodeWorker) preflight(ctx context.Context) {
	// v0.27.6 primary self-healing: pin the venv's matched
	// typecode-libmagic pair on every scancode subprocess. Runs before the
	// probe so a host that only needed the pairing probes healthy on the
	// first try.
	w.ensureTypecodeEnvPair(ctx)

	status, detail, libmagicCorrupt := w.probeScancodeHealth(ctx)
	if status == "" {
		// The probe itself couldn't run (temp-dir failure). Don't claim
		// health either way; the gate stays at its fail-open default.
		return
	}

	if status == db.StatusBroken && libmagicCorrupt {
		status, detail = remediateCorruptLibmagic(ctx, w.remediationDeps(), status, detail)
	}

	w.recordScancodeStatus(ctx, status, detail)
	w.setToolchainHealth(status)
}

// remediationDeps wires the worker's real seams into the testable ladder.
func (w *ScancodeWorker) remediationDeps() remediationDeps {
	return remediationDeps{
		logger:       w.logger,
		goos:         runtime.GOOS,
		probe:        w.probeScancodeHealth,
		runCmd:       runLoggedCommand(w.logger),
		discover:     discoverTypecodeLibmagic,
		applyEnvPair: w.setTypecodeEnvPair,
	}
}

// ensureTypecodeEnvPair discovers the wheel's matched (.so, magic.mgc) pair
// and pins it for every scancode subprocess. When the wheel is absent it runs
// the injection once and re-discovers. Best-effort: a host where discovery
// fails falls back to typecode's own resolution order, and the health probe +
// dispatcher gate remain the safety net.
func (w *ScancodeWorker) ensureTypecodeEnvPair(ctx context.Context) {
	if pair, ok := discoverTypecodeLibmagic(); ok {
		w.setTypecodeEnvPair(pair)
		w.logger.Info("scancode preflight: pinned typecode-libmagic pair on every scancode subprocess",
			typecodeLibmagicPathEnv, pair.LibPath,
			typecodeLibmagicDBPathEnv, pair.DBPath)
		return
	}
	w.logger.Warn("scancode preflight: typecode-libmagic wheel not found in the scancode venv — injecting",
		"command", "pipx inject "+scancodePipxPackage+" typecode-libmagic")
	if err := runLoggedCommand(w.logger)(ctx, "pipx", "inject", scancodePipxPackage, "typecode-libmagic"); err != nil {
		w.logger.Error("scancode preflight: typecode-libmagic injection failed",
			"error", err,
			"retry_hint", "pipx inject "+scancodePipxPackage+" typecode-libmagic")
	}
	if pair, ok := discoverTypecodeLibmagic(); ok {
		w.setTypecodeEnvPair(pair)
		w.logger.Info("scancode preflight: pinned typecode-libmagic pair after injection",
			typecodeLibmagicPathEnv, pair.LibPath,
			typecodeLibmagicDBPathEnv, pair.DBPath)
		return
	}
	w.logger.Warn("scancode preflight: env pinning unavailable (wheel not discoverable) — typecode's own plugin resolution / system fallback will decide which libmagic loads")
}

// probeScancodeHealth runs one scancode invocation against a tiny synthetic
// input and classifies the outcome. Returns ("", "", false) when the probe
// itself could not run (the caller treats that as "unknown"). Pure with
// respect to worker state — it records nothing; preflight and
// awaitHealthyToolchain own the bookkeeping.
func (w *ScancodeWorker) probeScancodeHealth(ctx context.Context) (status, detail string, libmagicCorrupt bool) {
	scancodePath, err := exec.LookPath("scancode")
	if err != nil {
		st, d := classifyScancodeHealth(false, runtime.GOOS, "", false)
		return st, d, false
	}

	dir, err := os.MkdirTemp(w.cloneDir, "scancode-preflight-")
	if err != nil {
		// Can't run the check — don't claim health either way; log and move on.
		w.logger.Warn("scancode preflight: could not create temp dir; skipping health check", "error", err)
		return "", "", false
	}
	defer os.RemoveAll(dir)
	// One trivial file with a recognizable license/copyright line so scancode
	// has something to do — but the libmagic load (where the corruption surfaces)
	// happens regardless of content.
	_ = os.WriteFile(filepath.Join(dir, "preflight.txt"),
		[]byte("// Copyright 2026 Aveloxis\n// SPDX-License-Identifier: MIT\nhello\n"), 0o644)
	outputPath := filepath.Join(dir, "out.json")

	pctx, cancel := context.WithTimeout(ctx, scancodePreflightTimeout)
	defer cancel()
	cmd := exec.CommandContext(pctx, scancodePath,
		"-clpi", "--only-findings", "--json", outputPath, "--quiet",
		"--timeout", "60", "--processes", "1",
		"--max-in-memory", strconv.Itoa(w.maxInMemory),
		dir,
	)
	// v0.27.6: the probe runs with the same pinned typecode env pair as the
	// real scans — probing a DIFFERENT libmagic than the scans would use
	// makes the health verdict meaningless.
	cmd.Env = w.scancodeEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 10 * time.Second
	stderr := &capWriter{cap: scancodePreflightStderrCap}
	cmd.Stderr = stderr
	cmd.Stdout = &capWriter{cap: 4096}
	_ = cmd.Run() // non-zero exit is expected on a broken toolchain; we classify from stderr + output

	_, _, jsonOK := salvageScancodeOutput(outputPath)
	libmagicCorrupt = countLibmagicWarnings(string(stderr.buf)) >= scancodePreflightRepeatN
	status, detail = classifyScancodeHealth(true, runtime.GOOS, string(stderr.buf), jsonOK)
	return status, detail, libmagicCorrupt
}

// setToolchainHealth flips the dispatcher gate from a recorded status.
func (w *ScancodeWorker) setToolchainHealth(status string) {
	w.healthy.Store(status == db.StatusOK)
}

// awaitHealthyToolchain parks the dispatcher while the toolchain is BROKEN:
// one WARN on entering the pause, a re-probe every healthRecheck (15 min in
// production), one INFO on exit — the v0.25.0 distribution-dispatcher pause
// pattern. An operator fixing the host out-of-band (or the remediation env
// pairing taking effect after a venv change) auto-resumes claims without a
// restart.
func (w *ScancodeWorker) awaitHealthyToolchain(ctx context.Context) {
	w.logger.Warn("scancode dispatcher paused — toolchain BROKEN; claiming NOTHING until a health probe passes",
		"recheck_interval", w.healthRecheck.String())
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.healthRecheck):
		}
		status, detail, _ := w.probeScancodeHealth(ctx)
		if status == "" {
			// Probe couldn't run; stay paused and try again next tick.
			continue
		}
		w.recordScancodeStatus(ctx, status, detail)
		w.setToolchainHealth(status)
		if status == db.StatusOK {
			w.logger.Info("scancode dispatcher resuming — health probe passed")
			return
		}
	}
}

// recordScancodeStatus logs the outcome (prominently when broken) and upserts
// the status row.
func (w *ScancodeWorker) recordScancodeStatus(ctx context.Context, status, detail string) {
	switch status {
	case db.StatusOK:
		w.logger.Info("scancode preflight: healthy", "status", status)
	default:
		// Prominent: this means scancode will not produce useful results until
		// the operator acts (and, as of v0.27.6, that the dispatcher is gated).
		w.logger.Error("scancode preflight: SYSTEM-LEVEL FAILURE — scancode will not work until fixed",
			"status", status, "detail", detail)
	}
	// Defensive nil guard: the behavioral tests drive the health gate
	// without a database; production always has a store.
	if w.store == nil {
		return
	}
	if err := w.store.SetAveloxisStatus(ctx, scancodeStatusName, status, detail, scancodeStatusSource); err != nil {
		w.logger.Warn("scancode preflight: failed to record status", "error", err)
	}
}

// classifyScancodeHealth maps a preflight run's outcome to a status + an
// operator-facing detail/remediation string. Pure (no IO) so it's unit-tested
// directly against captured stderr.
func classifyScancodeHealth(installed bool, goos, stderr string, jsonValid bool) (status, detail string) {
	if !installed {
		return db.StatusNotInstalled,
			"scancode binary not found on PATH — run 'aveloxis install-tools'"
	}
	// libmagic magic-database mismatch/corruption — the same failure shape on
	// Linux and macOS. scancode (typecode → python-magic → libmagic) spams
	// 'magic.mgc, NNNN: Warning: offset ... invalid' at enormous volume,
	// bogging scans down until the wall-clock timeout SIGKILLs them (14+ GB
	// stderr per repo, 2026-06-09). July 2026 RCA: the usual cause is a
	// version-mismatched wheel .so reading a foreign compiled magic.mgc —
	// fixed deterministically by the TYPECODE_LIBMAGIC_* env pairing the
	// v0.27.6 preflight pins (see scancode_remediate.go).
	//
	// VOLUME, not presence, is the signal. A healthy/repaired libmagic — e.g.
	// after `aveloxis upgrade-tools` injects typecode-libmagic, or on a host
	// whose `file` package merely has a few tolerable bad magic entries — can
	// still emit a HANDFUL of these warnings while scans complete normally and
	// produce valid output. Flagging on mere presence false-positives that
	// working install (observed 2026-06-10: valid scans + no failure stderr
	// files, yet the preflight tripped). The wedging bug is different in kind:
	// the corrupt DB emits one warning per bad entry at load time, repeating
	// the fingerprint thousands of times (the preflight's 1 MB stderr cap fills
	// completely). So require the fingerprint to repeat at the same "systemic
	// spam" threshold the generic check uses.
	//
	// The 'magic.mgc' compiled-DB name is shared across OSes; libmagic's C
	// parser also emits the identical 'Warning: offset ... invalid' shape
	// regardless of platform or DB path. The warning lines vary by line number
	// and offset, so the generic single-line mostRepeatedLine check below
	// under-counts them — count the fingerprint across all lines here instead.
	if countLibmagicWarnings(stderr) >= scancodePreflightRepeatN {
		return db.StatusBroken,
			"system libmagic magic database appears corrupt: scancode emits 'offset invalid' warning spam that wedges every scan. " +
				"Fix: run 'aveloxis upgrade-tools' to inject typecode-libmagic into the scancode venv (works on any OS), " + osLibmagicHint(goos)
	}
	// Generic systemic signal: the same error/warning line repeated many times
	// means the toolchain is misconfigured and scans will not complete.
	if line, n := mostRepeatedLine(stderr); n >= scancodePreflightRepeatN {
		d := line
		if len(d) > 160 {
			d = d[:160] + "…"
		}
		return db.StatusBroken,
			"scancode emitted a repeated error " + strconv.Itoa(n) + "+ times — the toolchain is likely misconfigured and scans will not complete. Repeated line: " + d
	}
	if !jsonValid {
		return db.StatusBroken,
			"scancode health check produced no valid JSON output — check the scancode install ('aveloxis upgrade-tools')"
	}
	return db.StatusOK, ""
}

// osLibmagicHint returns an OS-appropriate fallback remediation for a corrupt
// libmagic database. The primary fix ('aveloxis upgrade-tools' injects
// typecode-libmagic into the scancode venv; the v0.27.6 preflight pins the
// wheel pair via TYPECODE_LIBMAGIC_* env vars automatically) is cross-OS;
// this is the reinstall-the-system-library escape hatch, which differs per
// platform. v0.27.6: never executed automatically — advice only.
func osLibmagicHint(goos string) string {
	switch goos {
	case "darwin":
		return "or reinstall the OS library (brew reinstall libmagic)."
	case "linux":
		// /usr/share/misc/magic.mgc ships from the libmagic-mgc package on
		// Debian/Ubuntu (NOT libmagic1 or file) — reinstalling only the latter
		// two leaves a corrupt magic.mgc in place. Include all three to be safe.
		return "or reinstall the OS package (apt-get install --reinstall libmagic-mgc libmagic1 file)."
	default:
		return "or reinstall your OS's libmagic/file package."
	}
}

// countLibmagicWarnings counts lines carrying the OS-independent libmagic
// magic-DB corruption fingerprint. Matches either the compiled-DB name
// (magic.mgc) on an 'invalid' line, or the generic 'magic'+'Warning'+'offset'+
// 'invalid' shape libmagic's C parser emits on Linux and macOS. The caller
// uses the COUNT (not mere presence) so a handful of benign warnings from a
// working install don't read as the wedging bug. Input is bounded (preflight
// caps stderr at 1 MB).
func countLibmagicWarnings(s string) int {
	if s == "" {
		return 0
	}
	n := 0
	for _, line := range strings.Split(s, "\n") {
		hasMagicMgc := strings.Contains(line, "magic.mgc") && strings.Contains(line, "invalid")
		hasGeneric := strings.Contains(line, "magic") && strings.Contains(line, "Warning") &&
			strings.Contains(line, "offset") && strings.Contains(line, "invalid")
		if hasMagicMgc || hasGeneric {
			n++
		}
	}
	return n
}

// mostRepeatedLine returns the most frequently repeated non-blank line in s and
// its count. Input is bounded (preflight caps stderr at 1 MB).
func mostRepeatedLine(s string) (string, int) {
	if s == "" {
		return "", 0
	}
	counts := make(map[string]int)
	best, bestN := "", 0
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		counts[line]++
		if counts[line] > bestN {
			best, bestN = line, counts[line]
		}
	}
	return best, bestN
}
