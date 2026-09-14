// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// scancode_remediate.go (v0.27.6) — self-healing for the scancode
// toolchain.
//
// # Why the awareness-only decision was revised
//
// The v0.25.x preflight reliably DETECTED the libmagic "offset
// invalid" warning-spam fingerprint but was awareness-only by
// explicit operator decision at the time. The June 2026 production
// logs settled the question: on June 11 the preflight logged
// SYSTEM-LEVEL FAILURE at startup and the dispatcher then ran 2,473
// scans on the broken toolchain anyway — producing stderr artifacts
// up to 9.5 GB and wedging every worker. Detection without
// remediation-or-gating just timestamps the damage.
//
// # The July 2026 root cause (verified on the production host)
//
// The system libmagic was HEALTHY the whole time (debsums clean,
// `file` clean). The typecode-libmagic wheel WAS injected and its
// entry point registered — yet typecode's plugin resolution fails at
// runtime inside scancode-toolkit-mini's venv and silently falls back
// to "typical system locations": a 5.39-vintage wheel .so loading the
// system's 5.45-compiled /usr/share/misc/magic.mgc → format mismatch
// → the parser treats the compiled DB as TEXT magic source →
// thousands of "offset invalid" warnings per load. That is why
// injection alone was never a durable fix, and why reinstalling OS
// packages could never help. libmagic's own MAGIC env var is ignored
// (typecode passes explicit paths to magic_load).
//
// The mechanism that wins unconditionally (typecode/magic2.py checks
// them FIRST, before plugin resolution and the system fallback) is
// the env-var pair:
//
//	TYPECODE_LIBMAGIC_PATH    = the wheel's libmagic shared library
//	TYPECODE_LIBMAGIC_DB_PATH = the wheel's magic.mgc
//
// So the v0.27.6 primary self-healing mechanism is: DISCOVER the
// venv's matched (.so, magic.mgc) pair at startup and pin BOTH env
// vars on the environment of EVERY scancode subprocess — the wheel's
// library can then never be mismatched against a foreign compiled DB.
//
// # The ladder
//
//	(i)   discover + apply the env pairing (the deterministic fix);
//	(ii)  inject typecode-libmagic if the wheel is absent, then
//	      re-discover and apply;
//	(iii) still broken → LAST-resort advice only: log the OS package
//	      reinstall command for hosts where the system DB really is
//	      corrupt — never executed automatically.
//
// A health re-probe runs after each step. If the ladder is exhausted
// the dispatcher gates on the BROKEN status (scancode_worker.go),
// re-probing every 15 minutes and auto-resuming on a passing probe.
// Every attempt is logged loudly so the operator always knows what
// the worker did (the only mutation is inside the scancode venv).

package collector

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
)

// scancodePipxPackage is the pipx-managed distribution of scancode.
const scancodePipxPackage = "scancode-toolkit-mini"

// Env var names typecode/magic2.py checks FIRST — before plugin
// resolution and before the "typical system locations" fallback
// (verified in the installed source, ~lines 180 and 244).
const (
	typecodeLibmagicPathEnv   = "TYPECODE_LIBMAGIC_PATH"
	typecodeLibmagicDBPathEnv = "TYPECODE_LIBMAGIC_DB_PATH"
)

// linuxLibmagicRepairHint is the exact reinstall command for a corrupt
// Debian/Ubuntu magic database — LAST-resort advice only (v0.27.6:
// never executed automatically; the July 2026 RCA showed the system DB
// is usually healthy and the venv pairing is the real fix).
// /usr/share/misc/magic.mgc ships from libmagic-mgc (NOT libmagic1 or
// file), so all three are named.
const linuxLibmagicRepairHint = "sudo apt-get install --reinstall -y libmagic-mgc libmagic1 file"

// typecodeEnvPair is the discovered wheel-matched libmagic pair.
type typecodeEnvPair struct {
	LibPath string // TYPECODE_LIBMAGIC_PATH — the wheel's shared library
	DBPath  string // TYPECODE_LIBMAGIC_DB_PATH — the wheel's magic.mgc
}

// asEnv renders the pair as environment entries for exec.Cmd.Env.
func (p typecodeEnvPair) asEnv() []string {
	return []string{
		typecodeLibmagicPathEnv + "=" + p.LibPath,
		typecodeLibmagicDBPathEnv + "=" + p.DBPath,
	}
}

// discoverTypecodeLibmagic locates the typecode-libmagic wheel's
// matched (shared library, magic.mgc) pair inside the scancode venv.
//
// Search order:
//  1. the venv derived from the scancode binary on PATH — the binary
//     lives at <venv>/bin/scancode, so two Dir() hops up from the
//     symlink-resolved path give the venv root;
//  2. pipx's default venv location
//     (~/.local/share/pipx/venvs/scancode-toolkit-mini) for setups
//     where the PATH entry is a wrapper the derivation can't follow.
//
// Within a venv root the wheel files live at
// lib/python*/site-packages/typecode_libmagic/{lib/libmagic*, data/magic.mgc}.
func discoverTypecodeLibmagic() (typecodeEnvPair, bool) {
	var roots []string
	if scPath, err := exec.LookPath("scancode"); err == nil {
		resolved := scPath
		if r, err := filepath.EvalSymlinks(scPath); err == nil {
			resolved = r
		}
		roots = append(roots, filepath.Dir(filepath.Dir(resolved)))
	}
	if home, err := os.UserHomeDir(); err == nil {
		roots = append(roots, filepath.Join(home, ".local", "share", "pipx", "venvs", scancodePipxPackage))
	}
	for _, root := range roots {
		if pair, ok := typecodePairInVenv(root); ok {
			return pair, true
		}
	}
	return typecodeEnvPair{}, false
}

// typecodePairInVenv checks one candidate venv root for the wheel's
// matched pair. Both halves must be present — pinning only one would
// recreate the exact mismatch class the pairing exists to prevent.
func typecodePairInVenv(venvRoot string) (typecodeEnvPair, bool) {
	sitePackages, _ := filepath.Glob(filepath.Join(venvRoot, "lib", "python*", "site-packages"))
	for _, sp := range sitePackages {
		dbPath := filepath.Join(sp, "typecode_libmagic", "data", "magic.mgc")
		if !fileExistsAndNonEmpty(dbPath) {
			continue
		}
		libMatches, _ := filepath.Glob(filepath.Join(sp, "typecode_libmagic", "lib", "libmagic*"))
		lib := pickSharedObject(libMatches)
		if lib == "" {
			continue
		}
		return typecodeEnvPair{LibPath: lib, DBPath: dbPath}, true
	}
	return typecodeEnvPair{}, false
}

// pickSharedObject selects the shared library from glob matches:
// a regular file whose name carries .so / .dylib (linux wheels ship
// libmagic.so; macOS wheels libmagic.dylib).
func pickSharedObject(candidates []string) string {
	for _, c := range candidates {
		base := filepath.Base(c)
		if !strings.Contains(base, ".so") && !strings.Contains(base, ".dylib") {
			continue
		}
		if info, err := os.Stat(c); err == nil && !info.IsDir() && info.Size() > 0 {
			return c
		}
	}
	return ""
}

// remediationDeps injects the effectful seams (health probe, wheel
// discovery, env application, subprocess execution) so the ladder's
// ordering and gating logic is behaviorally testable without a real
// scancode/pipx on the host.
type remediationDeps struct {
	logger *slog.Logger
	goos   string
	// probe re-runs the scancode health check. Returns
	// (status, detail, libmagicCorrupt); status == "" means the
	// probe itself could not run (treat as "unknown, assume no
	// change").
	probe func(ctx context.Context) (status, detail string, libmagicCorrupt bool)
	// runCmd executes one external command and returns its error.
	// The production implementation logs the invocation and its
	// combined output.
	runCmd func(ctx context.Context, name string, args ...string) error
	// discover locates the typecode-libmagic wheel pair.
	discover func() (typecodeEnvPair, bool)
	// applyEnvPair installs the discovered pair so every subsequent
	// scancode exec (probe + scans) carries the env vars.
	applyEnvPair func(typecodeEnvPair)
}

// remediateCorruptLibmagic runs the v0.27.6 remediation ladder after
// the preflight classified the toolchain BROKEN with the libmagic
// warning-spam fingerprint. Returns the final (status, detail) —
// StatusOK when a step fixed the host, or the (possibly refreshed)
// broken classification when the ladder is exhausted. The caller
// records the result and gates the dispatcher on it.
func remediateCorruptLibmagic(ctx context.Context, deps remediationDeps,
	brokenStatus, brokenDetail string) (string, string) {

	log := deps.logger

	// Step (i): discover + apply the TYPECODE_LIBMAGIC_* env pairing —
	// the deterministic fix. typecode checks these env vars before
	// plugin resolution and before the system fallback, so a matched
	// wheel (.so, magic.mgc) pair can never be cross-loaded against a
	// foreign compiled DB.
	log.Warn("scancode remediation step 1/3: discovering the venv's typecode-libmagic pair and pinning it via env")
	if pair, ok := deps.discover(); ok {
		deps.applyEnvPair(pair)
		log.Warn("scancode remediation step 1/3: env pairing applied",
			typecodeLibmagicPathEnv, pair.LibPath,
			typecodeLibmagicDBPathEnv, pair.DBPath)
		if status, detail, done := reprobeAfterStep(ctx, deps, "typecode env pairing"); done {
			return status, detail
		}
	} else {
		log.Warn("scancode remediation step 1/3: typecode-libmagic wheel not found in the scancode venv — proceeding to injection")
	}

	// Step (ii): inject the wheel, re-discover, apply. The Python
	// binding + its own compiled DB bypass the system libmagic
	// entirely once the env pairing points at them.
	log.Warn("scancode remediation step 2/3: injecting typecode-libmagic into the scancode venv",
		"command", "pipx inject "+scancodePipxPackage+" typecode-libmagic")
	if err := deps.runCmd(ctx, "pipx", "inject", scancodePipxPackage, "typecode-libmagic"); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			log.Error("scancode remediation step 2/3 FAILED: typecode-libmagic injection", "error", err)
		}
	}
	if pair, ok := deps.discover(); ok {
		deps.applyEnvPair(pair)
		log.Warn("scancode remediation step 2/3: env pairing applied after injection",
			typecodeLibmagicPathEnv, pair.LibPath,
			typecodeLibmagicDBPathEnv, pair.DBPath)
	} else {
		log.Error("scancode remediation step 2/3: wheel still not discoverable after injection")
	}
	if status, detail, done := reprobeAfterStep(ctx, deps, "typecode-libmagic injection"); done {
		return status, detail
	}

	// Step (iii): exhausted. LAST-resort advice only — the July 2026
	// RCA showed the system magic DB is usually healthy (the spam
	// comes from a version-mismatched wheel .so reading it), so
	// reinstalling OS packages is deliberately NOT executed
	// automatically; the hint stays for the hosts where the system DB
	// really is corrupt. The dispatcher stays gated and re-probes
	// every 15 minutes, so an out-of-band operator fix auto-resumes
	// claims.
	switch deps.goos {
	case "linux":
		log.Error("scancode remediation exhausted — toolchain still BROKEN; dispatcher will claim nothing until a health probe passes",
			"last_resort_hint", "if the SYSTEM magic DB is genuinely corrupt, run: "+linuxLibmagicRepairHint,
			"status", brokenStatus, "detail", brokenDetail)
	case "darwin":
		log.Error("scancode remediation exhausted — toolchain still BROKEN; dispatcher will claim nothing until a health probe passes",
			"last_resort_hint", "if the SYSTEM libmagic is genuinely corrupt, run: brew reinstall libmagic",
			"status", brokenStatus, "detail", brokenDetail)
	default:
		log.Error("scancode remediation exhausted — toolchain still BROKEN; dispatcher will claim nothing until a health probe passes",
			"last_resort_hint", "if the SYSTEM libmagic is genuinely corrupt, reinstall your OS's libmagic/file package",
			"status", brokenStatus, "detail", brokenDetail)
	}
	return brokenStatus, brokenDetail
}

// reprobeAfterStep re-runs the health probe after a remediation step.
// done=true means the ladder should stop: either the probe passed
// (status carries StatusOK) or it re-classified to a NON-libmagic
// failure (a different problem the ladder can't help with). done=false
// means "still the libmagic fingerprint (or probe couldn't run) —
// continue to the next step".
func reprobeAfterStep(ctx context.Context, deps remediationDeps, step string) (string, string, bool) {
	status, detail, libmagicCorrupt := deps.probe(ctx)
	switch {
	case status == "":
		deps.logger.Warn("scancode remediation: re-probe could not run after step — continuing ladder", "step", step)
		return "", "", false
	case status == db.StatusOK:
		deps.logger.Info("scancode remediation SUCCEEDED — health probe passed", "fixed_by", step)
		return status, detail, true
	case !libmagicCorrupt:
		// Broken, but no longer the libmagic fingerprint: the
		// remaining ladder steps target libmagic specifically and
		// can't help. Surface the new classification.
		deps.logger.Error("scancode remediation: still broken after step, with a NON-libmagic classification — stopping ladder",
			"step", step, "detail", detail)
		return status, detail, true
	default:
		deps.logger.Warn("scancode remediation: still broken after step — continuing ladder",
			"step", step, "detail", detail)
		return status, detail, false
	}
}

// runLoggedCommand is the production runCmd: LookPath + run with
// combined output captured into the log so the operator sees exactly
// what each remediation attempt did.
func runLoggedCommand(logger *slog.Logger) func(ctx context.Context, name string, args ...string) error {
	return func(ctx context.Context, name string, args ...string) error {
		path, err := exec.LookPath(name)
		if err != nil {
			logger.Warn("scancode remediation: command not on PATH", "command", name, "error", err)
			return err
		}
		cmd := exec.CommandContext(ctx, path, args...)
		out, err := cmd.CombinedOutput()
		logger.Info("scancode remediation: ran command",
			"command", name+" "+strings.Join(args, " "),
			"error", err,
			"output_tail", tailOfString(string(out), 2048))
		return err
	}
}

// tailOfString returns the last max bytes of s.
func tailOfString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[len(s)-max:]
}
