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

// scancode_preflight.go is the v0.25.x scancode startup health check. A
// system-level failure of the scancode toolchain — most importantly a corrupt
// system libmagic database (the 2026-06-09 incident: /usr/share/misc/magic.mgc
// emitting millions of "offset invalid" warnings, producing 14+ GB of stderr
// per repo and wedging every worker) — silently degrades the whole subsystem.
// The preflight runs ONE scancode invocation against a tiny synthetic input at
// startup, classifies the outcome, logs a systemic failure prominently, and
// records it in aveloxis_ops.aveloxis_status so the operator can see it.
//
// Awareness only (per operator direction): it does NOT disable scancode; that
// is a deliberate follow-up. The point here is to stop the failure from being
// invisible.

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

// preflight runs the scancode health check once and records the result. Called
// from Run before the dispatcher starts claiming work.
func (w *ScancodeWorker) preflight(ctx context.Context) {
	scancodePath, err := exec.LookPath("scancode")
	if err != nil {
		status, detail := classifyScancodeHealth(false, runtime.GOOS, "", false)
		w.recordScancodeStatus(ctx, status, detail)
		return
	}

	dir, err := os.MkdirTemp(w.cloneDir, "scancode-preflight-")
	if err != nil {
		// Can't run the check — don't claim health either way; log and move on.
		w.logger.Warn("scancode preflight: could not create temp dir; skipping health check", "error", err)
		return
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
	status, detail := classifyScancodeHealth(true, runtime.GOOS, string(stderr.buf), jsonOK)
	w.recordScancodeStatus(ctx, status, detail)
}

// recordScancodeStatus logs the outcome (prominently when broken) and upserts
// the status row.
func (w *ScancodeWorker) recordScancodeStatus(ctx context.Context, status, detail string) {
	switch status {
	case db.StatusOK:
		w.logger.Info("scancode preflight: healthy", "status", status)
	default:
		// Prominent: this means scancode will not produce useful results until
		// the operator acts.
		w.logger.Error("scancode preflight: SYSTEM-LEVEL FAILURE — scancode will not work until fixed",
			"status", status, "detail", detail)
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
	// Corrupt libmagic database — the same failure on Linux and macOS. scancode
	// (typecode → python-magic → libmagic) spams 'magic.mgc, NNNN: Warning:
	// offset ... invalid' at enormous volume, bogging scans down until the
	// wall-clock timeout SIGKILLs them (14+ GB stderr per repo, 2026-06-09).
	// The 'magic.mgc' compiled-DB name is shared across OSes; libmagic's C
	// parser also emits the identical 'Warning: offset ... invalid' shape
	// regardless of platform or DB path, so match EITHER signal — this stays
	// robust whether the warning cites /usr/share/misc/magic.mgc (Ubuntu), a
	// Homebrew/macOS magic file, or an uncompiled magic source dir.
	corruptMagic := strings.Contains(stderr, "magic.mgc") ||
		(strings.Contains(stderr, "magic") && strings.Contains(stderr, "Warning") &&
			strings.Contains(stderr, "offset") && strings.Contains(stderr, "invalid"))
	if corruptMagic {
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
// typecode-libmagic into the scancode venv) is cross-OS; this is the
// reinstall-the-system-library escape hatch, which differs per platform.
func osLibmagicHint(goos string) string {
	switch goos {
	case "darwin":
		return "or reinstall the OS library (brew reinstall libmagic)."
	case "linux":
		return "or reinstall the OS package (apt-get install --reinstall libmagic1 file)."
	default:
		return "or reinstall your OS's libmagic/file package."
	}
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
