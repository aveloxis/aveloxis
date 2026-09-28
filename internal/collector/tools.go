// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — tools.go defines all external tools that Aveloxis
// can optionally use during collection. These tools are installed via
// `aveloxis install-tools` and checked at runtime — if a tool is missing,
// the corresponding analysis phase is silently skipped.
package collector

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// ToolUpdateInterval is how often we check for updated tool versions.
// Default: 30 days. On startup, if the last check was longer ago than this,
// we re-run `go install ...@latest` for each installed tool to pull updates.
const ToolUpdateInterval = 30 * 24 * time.Hour

// ExternalTool describes an optional third-party tool used by Aveloxis.
type ExternalTool struct {
	Name        string                                    // display name
	CheckBinary string                                    // binary name to look up on PATH (exec.LookPath)
	InstallCmd  string                                    // go install or other install command (also used for manual install display)
	InstallFunc func(ctx context.Context) error           // custom install function; takes priority over InstallCmd when set; every subprocess and request it runs is bound to ctx
	BinDir      func(ctx context.Context) (string, error) // where the install puts the binary, when Go's rule decides it (nil when the installer — pipx — chooses and says so itself)
	Description string                                    // what the tool does
	Purpose     string                                    // which collection phase uses it
}

// ExternalTools returns the list of all optional tools that Aveloxis can use.
// Each tool is independently optional — if not installed, its phase is skipped.
func ExternalTools() []ExternalTool {
	return []ExternalTool{
		{
			Name:        "scc",
			CheckBinary: "scc",
			InstallCmd:  "go install github.com/boyter/scc/v3@latest",
			BinDir:      GoBinDir,
			Description: "Sloc Cloc and Code — counts lines of code, comments, blanks, and complexity per file per language",
			Purpose:     "Phase 4 (Analysis): populates the repo_labor table with per-file code metrics",
		},
		{
			Name:        "scorecard",
			CheckBinary: "scorecard",
			InstallCmd:  "see https://github.com/ossf/scorecard/releases",
			InstallFunc: installScorecardBinary,
			BinDir:      GoBinDir,
			Description: "OpenSSF Scorecard — evaluates open source project security practices across 18+ checks",
			Purpose:     "Phase 4b (Analysis): populates repo_deps_scorecard with security check results (Code-Review, Maintained, Vulnerabilities, etc.)",
		},
		{
			Name:        "scancode",
			CheckBinary: "scancode",
			InstallCmd:  "pipx install scancode-toolkit-mini",
			InstallFunc: installScancode,
			Description: "ScanCode Toolkit — detects licenses, copyrights, and packages per file with precise line-level attribution",
			Purpose:     "Phase 4c (Analysis): populates aveloxis_scan.scancode_file_results with per-file license and copyright detections (runs every 30 days per repo)",
		},
	}
}

// scorecardDownloadURL builds the GitHub release download URL for a pre-built
// scorecard binary. Scorecard v5 does not expose a go-installable cmd package,
// so we download the pre-built binary from GitHub releases instead.
func scorecardDownloadURL(version, goos, goarch string) string {
	// Release assets use version without "v" prefix: scorecard_5.4.0_darwin_arm64.tar.gz
	bare := strings.TrimPrefix(version, "v")
	filename := fmt.Sprintf("scorecard_%s_%s_%s.tar.gz", bare, goos, goarch)
	return fmt.Sprintf("https://github.com/ossf/scorecard/releases/download/%s/%s", version, filename)
}

// toolFetchClient bounds each of the tool-update check's two GitHub requests
// (the release lookup and the binary download — the bound is PER request,
// so the serial pair is at most twice it). Through v0.29.67 both went
// through http.DefaultClient, which has no timeout, so a stalled connection
// at serve startup hung the check forever (batch 4a review round 11, an
// aside). The bound is the download's: scorecard_5.4.0_linux_amd64.tar.gz
// was 25,318,282 bytes on 2026-09-26 (Content-Length after the release
// redirect), and 5 minutes covers that on a 1 MB/s link with about 12×
// headroom; the lookup shares it rather than carrying a second constant.
var toolFetchClient = &http.Client{Timeout: 5 * time.Minute}

// ToolInstallBound bounds one tool's whole install or upgrade — its
// subprocesses (`go install`, pipx, pip, brew) and its requests together.
// Derived from toolFetchClient at call time (a test that swaps the client
// moves it too): the scorecard path is two serial bounded requests, so
// twice that bound; a module-proxy or PyPI stall inside a subprocess
// (batch 4a review round 12: those legs were unbounded and ctx-less, and a
// `stop serve` during the check orphaned the child) is held to the same
// budget. Shared with the install-tools and upgrade-tools commands.
func ToolInstallBound() time.Duration { return 2 * toolFetchClient.Timeout }

// runToolCommand runs one install subprocess under the caller's context
// with the whole process group killed on cancel (groupKilled) and the
// cancellation reported as the context's error, not "signal: killed"
// (execErr) — the scheduler-workers rule for every subprocess (batch 4a
// review round 13).
func runToolCommand(ctx context.Context, cmd *exec.Cmd) error {
	groupKilled(cmd)
	// In its own process group the child cannot use the controlling
	// terminal: a prompt (pip's getpass for an authenticated index, git's
	// credential prompt under `go install`) is stopped by SIGTTIN instead
	// of answered, and the operator watches ToolInstallBound() run out. The
	// two ecosystems' no-prompt variables turn a prompt into an immediate,
	// named failure (review round 14). A Homebrew formula install never
	// reads the terminal (round 15: NONINTERACTIVE is the installer
	// script's knob, unread by `brew install`, so it is not set). A
	// caller's own environment is kept; the process environment is the
	// default os/exec would have used.
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	cmd.Env = append(cmd.Env, nonInteractiveEnv...)
	return execErr(ctx, cmd.Run())
}

// nonInteractiveEnv is what every tool command carries so no installer
// prompts: pip (PIP_NO_INPUT — pip still consults the keyring) and git
// (GIT_TERMINAL_PROMPT=0). Credentials must come from configuration or a
// keyring; both CLIs' help says so.
var nonInteractiveEnv = []string{"PIP_NO_INPUT=1", "GIT_TERMINAL_PROMPT=0"}

// scorecardLatestReleaseURL is the release lookup's URL; a variable so the
// bounded-fetch test can point it at a fixture.
var scorecardLatestReleaseURL = "https://api.github.com/repos/ossf/scorecard/releases/latest"

// scorecardLatestVersion fetches the latest release tag from the GitHub API.
func scorecardLatestVersion(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scorecardLatestReleaseURL, nil)
	if err != nil {
		return "", fmt.Errorf("building the scorecard release request: %w", err)
	}
	req.Header.Set("X-GitHub-Api-Version", platform.GitHubAPIVersion) // every GitHub REST request pins the version (worklist item 15)
	resp, err := toolFetchClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetching latest scorecard release: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub API returned %d", resp.StatusCode)
	}
	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", fmt.Errorf("decoding release JSON: %w", err)
	}
	if release.TagName == "" {
		return "", fmt.Errorf("empty tag_name in release response")
	}
	return release.TagName, nil
}

// installScorecardBinary downloads the pre-built scorecard tarball from GitHub
// releases, extracts the binary, and places it in GoBinDir().
func installScorecardBinary(ctx context.Context) error {
	version, err := scorecardLatestVersion(ctx)
	if err != nil {
		return err
	}

	url := scorecardDownloadURL(version, runtime.GOOS, runtime.GOARCH)
	dl, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building the scorecard download request: %w", err)
	}
	resp, err := toolFetchClient.Do(dl)
	if err != nil {
		return fmt.Errorf("downloading scorecard: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned %d for %s", resp.StatusCode, url)
	}

	// Destination: where `go install` would put it (GoBinDir), the
	// directory install-tools names when the tool lands off PATH.
	destDir, err := GoBinDir(ctx)
	if err != nil {
		return err
	}
	dest := filepath.Join(destDir, "scorecard")

	// Extract the scorecard binary from the .tar.gz archive.
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("decompressing tarball: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("scorecard binary not found in tarball")
		}
		if err != nil {
			return fmt.Errorf("reading tarball: %w", err)
		}
		// The binary is typically named "scorecard" or "scorecard-<os>-<arch>".
		base := filepath.Base(hdr.Name)
		if base == "scorecard" || strings.HasPrefix(base, "scorecard-") {
			tmp, err := os.CreateTemp("", "scorecard-*")
			if err != nil {
				return fmt.Errorf("creating temp file: %w", err)
			}
			defer func() { _ = os.Remove(tmp.Name()) }()

			if _, err := io.Copy(tmp, tr); err != nil {
				tmp.Close()
				return fmt.Errorf("writing scorecard binary: %w", err)
			}
			tmp.Close()

			if err := os.Chmod(tmp.Name(), 0o755); err != nil {
				return fmt.Errorf("chmod: %w", err)
			}
			if err := os.Rename(tmp.Name(), dest); err != nil {
				return fmt.Errorf("moving scorecard to %s: %w", dest, err)
			}

			fmt.Printf("scorecard %s installed to %s\n", version, dest)
			if w := shadowWarning("scorecard", dest); w != "" {
				fmt.Println(w)
			}
			return nil
		}
	}
}

// IsToolUpdateCheckDue returns true if enough time has passed since the last
// tool update check to warrant re-checking. Default interval: 30 days.
func IsToolUpdateCheckDue(lastCheck time.Time) bool {
	if lastCheck.IsZero() {
		return true
	}
	return time.Since(lastCheck) > ToolUpdateInterval
}

// CheckAndUpdateTools re-installs all installed tools to pull the latest version.
// Only runs tools that are already on PATH — does not install missing tools.
// Called on scheduler startup when the last check was > 30 days ago.
//
// The timestamp file is stored at ~/.aveloxis-tool-check to track when we last ran.
func CheckAndUpdateTools(ctx context.Context, logger *slog.Logger) {
	lastCheck := readToolCheckTimestamp()
	if !IsToolUpdateCheckDue(lastCheck) {
		return
	}

	logger.Info("checking for tool updates (monthly check)")
	updated := 0

	for _, tool := range ExternalTools() {
		if ctx.Err() != nil {
			logger.Info("tool update check interrupted — the next start re-runs it", "updated", updated)
			return // the timestamp is not written: the check is still due
		}
		// Only update tools that are already installed.
		if _, err := exec.LookPath(tool.CheckBinary); err != nil {
			continue
		}

		logger.Info("updating tool", "name", tool.Name)
		// Each tool's install is bounded (ToolInstallBound) and cancelled
		// with the caller's ctx, so a stop during the check ends the
		// subprocess instead of orphaning it (batch 4a review round 12).
		bound := ToolInstallBound()
		tctx, cancel := context.WithTimeout(ctx, bound)
		err := runToolInstall(tctx, tool)
		timedOut := errors.Is(tctx.Err(), context.DeadlineExceeded) // still readable after cancel
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("tool update check interrupted — the next start re-runs it", "name", tool.Name, "updated", updated)
				return
			}
			if timedOut {
				// Named for the operator (round 13: the subprocess reports
				// "signal: killed", never the deadline); the installed tool
				// keeps working at its old version and the next monthly
				// check retries.
				logger.Warn("tool update timed out — retried at the next monthly check", "name", tool.Name, "bound", bound)
				continue
			}
			logger.Warn("failed to update tool", "name", tool.Name, "error", err)
			continue
		}
		updated++
	}

	if updated > 0 {
		logger.Info("tool update check complete", "updated", updated)
	}
	writeToolCheckTimestamp(logger)
}

// RunToolInstall executes the install for a tool, preferring InstallFunc when set.
func RunToolInstall(ctx context.Context, tool ExternalTool) error {
	return runToolInstall(ctx, tool)
}

func runToolInstall(ctx context.Context, tool ExternalTool) error {
	if tool.InstallFunc != nil {
		return tool.InstallFunc(ctx)
	}
	parts := strings.Fields(tool.InstallCmd)
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return runToolCommand(ctx, cmd)
}

// toolCheckTimestampFile returns the path to the timestamp file.
func toolCheckTimestampFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/.aveloxis-tool-check"
	}
	return fmt.Sprintf("%s/.aveloxis-tool-check", home)
}

func readToolCheckTimestamp() time.Time {
	data, err := os.ReadFile(toolCheckTimestampFile())
	if err != nil {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, strings.TrimSpace(string(data)))
	if err != nil {
		return time.Time{}
	}
	return t
}

func writeToolCheckTimestamp(logger *slog.Logger) {
	if err := os.WriteFile(toolCheckTimestampFile(), []byte(time.Now().Format(time.RFC3339)), 0o644); err != nil {
		logger.Warn("failed to write tool check timestamp", "error", err)
	}
}

// installScancode is the tool-registry entry point for scancode. It
// delegates to ensureScancodeCurrent with the observed install state,
// so ALL THREE historical call paths (install-tools, the monthly
// CheckAndUpdateTools, and the upgrade-tools CLI) share one
// install/upgrade/inject implementation. v0.27.6 — see
// ensureScancodeCurrent for the divergence this unification fixes.
func installScancode(ctx context.Context) error {
	// Scancode depends on libmagic (native C library for file type detection).
	// Install it if missing.
	installLibmagicIfNeeded(ctx)

	_, lookErr := exec.LookPath("scancode")
	return ensureScancodeCurrent(ctx, lookErr == nil)
}

// EnsureScancodeCurrent is the ONE scancode install/upgrade/inject
// path (v0.27.6 unification). Semantics:
//
//   - alreadyInstalled == false → `pipx install scancode-toolkit-mini`
//     then inject typecode-libmagic (pip --user remains the fresh-
//     install-only fallback for hosts without pipx);
//   - alreadyInstalled == true  → `pipx upgrade scancode-toolkit-mini`
//     then ALWAYS re-inject (pipx upgrade may rebuild the venv,
//     dropping a prior injection). There is NO pip fallback and NO
//     bare `pipx install` on this branch.
//
// Why: pre-v0.27.6, the monthly CheckAndUpdateTools path ran
// installScancode's bare `pipx install` against an already-installed
// package, which FAILS ("already seems to be installed" — pipx
// install is not upgrade), so the injection inside the success branch
// never re-ran; the code then fell through to a bare
// `pip install --user`, creating a SECOND, UNINJECTED scancode that
// could shadow the pipx venv's binary on PATH. The standalone
// `aveloxis upgrade-tools` CLI did it correctly (pipx upgrade +
// re-inject); the monthly path had silently diverged. Both now call
// this helper, and a negative tripwire pins that the installed branch
// never regrows a bare install.
func EnsureScancodeCurrent(ctx context.Context, alreadyInstalled bool) error {
	return ensureScancodeCurrent(ctx, alreadyInstalled)
}

func ensureScancodeCurrent(ctx context.Context, alreadyInstalled bool) error {
	pipxPath, pipxErr := exec.LookPath("pipx")

	if alreadyInstalled {
		if pipxErr != nil {
			return fmt.Errorf("scancode is installed but pipx is not on PATH — cannot upgrade in place. "+
				"Install pipx, or upgrade manually: pipx upgrade %s && pipx inject %s typecode-libmagic",
				scancodePipxPackage, scancodePipxPackage)
		}
		return pipxUpgradeScancode(ctx, pipxPath)
	}

	// Each failure is carried to the caller (final whole-tree review F2,
	// 2026-09-28): a timed-out pipx must not fall through to pip under the
	// expired context and end in "neither pipx nor pip found", and a pip
	// that ran and failed is a failure, not an absence.
	var pipxFail error
	if pipxErr == nil {
		err := pipxFreshInstallScancode(ctx, pipxPath)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("scancode install via pipx: %w", errors.Join(err, ctx.Err()))
		}
		// pipx failed on a FRESH install — fall through to pip.
		fmt.Println("pipx install failed, trying pip...")
		pipxFail = err
	}
	err := pipInstallScancodeFresh(ctx)
	switch {
	case err == nil:
		return nil
	case pipxFail != nil && errors.Is(err, errNoPip):
		return fmt.Errorf("scancode install via pipx failed and no pip is on PATH to fall back to: %w", pipxFail)
	case pipxFail != nil:
		return fmt.Errorf("scancode install failed: pipx: %w; %w", pipxFail, err)
	case errors.Is(err, errNoPip):
		return fmt.Errorf("scancode install failed: neither pipx nor pip found. Install Python 3.10+ and run: pipx install %s", scancodePipxPackage)
	}
	return err
}

// errNoPip is pipInstallScancodeFresh's answer when neither pip3 nor pip is
// on PATH — an absence, told apart from a pip that ran and failed.
var errNoPip = errors.New("neither pip3 nor pip is on PATH")

// pipxUpgradeScancode is the installed-branch implementation:
// `pipx upgrade` (never `pipx install`, which fails on an installed
// package) followed by an UNCONDITIONAL typecode-libmagic re-inject
// — pipx upgrade may have rebuilt the venv, losing a prior injection.
// The re-inject failure is non-fatal (degraded-but-functional).
func pipxUpgradeScancode(ctx context.Context, pipxPath string) error {
	fmt.Printf("Upgrading %s via pipx...\n", scancodePipxPackage)
	cmd := exec.CommandContext(ctx, pipxPath, "upgrade", scancodePipxPackage)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := runToolCommand(ctx, cmd); err != nil {
		return fmt.Errorf("pipx upgrade %s: %w (if scancode was installed via `pip install --user` rather than pipx, "+
			"uninstall it and run `aveloxis install-tools` to move it into a pipx venv)",
			scancodePipxPackage, err)
	}
	if err := injectTypecodeLibmagic(ctx, pipxPath, scancodePipxPackage); err != nil {
		fmt.Printf("warning: typecode-libmagic re-injection failed: %v\n", err)
		fmt.Println("  scancode upgrade succeeded; the libmagic UserWarning may continue to print.")
		fmt.Println("  to retry: pipx inject scancode-toolkit-mini typecode-libmagic")
	}
	return nil
}

// pipxFreshInstallScancode installs scancode into a new pipx venv and
// injects typecode-libmagic.
//
// We install scancode-toolkit-mini instead of the full scancode-toolkit to
// avoid native C dependency issues (pyicu, intbitset) that require pkg-config,
// ICU development libraries, and a compatible C compiler. The mini package
// has full license/copyright/package detection — it only omits advanced archive
// extraction and Unicode normalization features we don't need (we scan
// already-extracted code checkouts).
func pipxFreshInstallScancode(ctx context.Context, pipxPath string) error {
	fmt.Printf("Installing %s via pipx...\n", scancodePipxPackage)
	cmd := exec.CommandContext(ctx, pipxPath, "install", scancodePipxPackage)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := runToolCommand(ctx, cmd); err != nil {
		return err
	}
	// v0.23.6: inject typecode-libmagic into the freshly-built
	// venv. Without this, every scancode run emits the
	// libmagic UserWarning that dominated the 2026-05-21
	// stderr noise. Non-fatal: if injection fails (custom pipx
	// configuration, network blocked, etc.) the install still
	// succeeds — the warning continues to print but scancode
	// still works.
	if err := injectTypecodeLibmagic(ctx, pipxPath, scancodePipxPackage); err != nil {
		fmt.Printf("warning: typecode-libmagic injection failed: %v\n", err)
		fmt.Println("  scancode still works; the libmagic UserWarning will continue to print.")
		fmt.Println("  to retry: pipx inject scancode-toolkit-mini typecode-libmagic")
	}
	return nil
}

// pipInstallScancodeFresh is the FRESH-INSTALL-ONLY pip fallback for
// hosts without pipx. Deliberately unreachable from the installed
// branch (v0.27.6): running it against a host that already has
// scancode creates a second, uninjected copy that can shadow the pipx
// venv's binary — the exact regression vector the monthly updater had.
func pipInstallScancodeFresh(ctx context.Context) error {
	var lastErr error
	for _, pip := range []string{"pip3", "pip"} {
		pipPath, err := exec.LookPath(pip)
		if err != nil {
			continue
		}
		if ctx.Err() != nil {
			break // the bound expired on the previous pip: its error says so
		}
		fmt.Printf("Installing %s via %s --user...\n", scancodePipxPackage, pip)
		cmd := exec.CommandContext(ctx, pipPath, "install", "--user", scancodePipxPackage)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err = runToolCommand(ctx, cmd)
		if err == nil {
			// pip --user installs to a platform-specific bin dir that
			// may not be on PATH. Detect it and add to shell profile.
			if _, lookErr := exec.LookPath("scancode"); lookErr != nil {
				ensurePythonUserBinOnPath(ctx)
			}
			return nil
		}
		lastErr = fmt.Errorf("%s install --user %s: %w", pip, scancodePipxPackage, err)
	}
	if lastErr != nil {
		return lastErr
	}
	return errNoPip
}

// injectTypecodeLibmagic (v0.23.6) runs `pipx inject scancode-toolkit-mini
// typecode-libmagic` to add the Python libmagic binding to scancode's venv.
//
// Without this injection, scancode's typecode subsystem falls back to the
// system libmagic shared library, which emits this UserWarning on every
// scan invocation:
//
//	/home/.../typecode/magic2.py:197: UserWarning: System libmagic found
//	in typical location is used. Install instead a typecode-libmagic
//	plugin for best support.
//
// The warning is harmless but it dominated the v0.23.3 stderr-capture
// output (the entire 252-byte buffer was just this one line), making it
// impossible to see real scancode errors. v0.23.4 introduced the
// salvage-on-exit-1 path; v0.23.6 eliminates the warning at install time.
//
// Idempotent: `pipx inject` is a no-op when the package is already
// injected into the target venv. Safe to call multiple times.
//
// Returns nil on success, an error otherwise. Callers should treat
// injection failure as a warning, NOT a fatal install error — operators
// with custom pipx configurations or air-gapped networks may legitimately
// fail this step, and the scancode install itself remains functional.
//
// Exported as InjectTypecodeLibmagic for the v0.23.6 `aveloxis
// upgrade-tools` command in cmd/aveloxis/upgrade_tools_cmd.go.
func injectTypecodeLibmagic(ctx context.Context, pipxPath, scancodePkg string) error {
	fmt.Printf("Injecting typecode-libmagic into %s venv...\n", scancodePkg)
	cmd := exec.CommandContext(ctx, pipxPath, "inject", scancodePkg, "typecode-libmagic")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return runToolCommand(ctx, cmd)
}

// InjectTypecodeLibmagic is the public alias for injectTypecodeLibmagic.
// Used by cmd/aveloxis/upgrade_tools_cmd.go's RunE handler.
func InjectTypecodeLibmagic(ctx context.Context, pipxPath, scancodePkg string) error {
	return injectTypecodeLibmagic(ctx, pipxPath, scancodePkg)
}

// installLibmagicIfNeeded installs the libmagic native library if it's not
// already available. Scancode uses it for file type detection.
//   - macOS: brew install libmagic
//   - Debian/Ubuntu: apt-get install libmagic1
//   - RHEL/CentOS: yum install file-libs
func installLibmagicIfNeeded(ctx context.Context) {
	// Quick check: if libmagic is loadable, we're good.
	// The file command uses libmagic, so checking for it is a reasonable proxy.
	// On macOS, Homebrew installs to /opt/homebrew/lib or /usr/local/lib.
	if runtime.GOOS == "darwin" {
		if _, err := exec.LookPath("brew"); err == nil {
			// Check if already installed via brew.
			check := exec.CommandContext(ctx, "brew", "list", "libmagic")
			groupKilled(check)
			if check.Run() != nil {
				if ctx.Err() != nil {
					return // the probe failed because the walk was cancelled, not because libmagic is missing (round 15)
				}
				fmt.Println("Installing libmagic via Homebrew (required by scancode)...")
				cmd := exec.CommandContext(ctx, "brew", "install", "libmagic")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := runToolCommand(ctx, cmd); err != nil {
					fmt.Printf("  Warning: brew install libmagic failed: %v\n", err)
				}
			}
		}
	} else {
		// On Linux, check for the shared library.
		if _, err := os.Stat("/usr/lib/x86_64-linux-gnu/libmagic.so.1"); err != nil {
			if _, err := os.Stat("/usr/lib64/libmagic.so.1"); err != nil {
				fmt.Println("  Note: libmagic may need to be installed (apt-get install libmagic1 or yum install file-libs)")
			}
		}
	}
}

// ensurePythonUserBinOnPath detects the Python user bin directory and appends
// a PATH export to the user's shell profile if it's not already there.
func ensurePythonUserBinOnPath(ctx context.Context) {
	// Determine the Python user bin directory.
	var binDir string
	for _, py := range []string{"python3", "python"} {
		pyPath, err := exec.LookPath(py)
		if err != nil {
			continue
		}
		probe := exec.CommandContext(ctx, pyPath, "-m", "site", "--user-base")
		groupKilled(probe)
		out, err := probe.Output()
		if err == nil {
			binDir = filepath.Join(strings.TrimSpace(string(out)), "bin")
			break
		}
	}
	if binDir == "" {
		fmt.Println("  Could not determine Python user bin directory.")
		return
	}

	// Find the shell profile to update.
	home, err := os.UserHomeDir()
	if err != nil {
		fmt.Printf("  Add to your shell profile: export PATH=\"%s:$PATH\"\n", binDir)
		return
	}

	shell := os.Getenv("SHELL")
	var profile string
	switch {
	case strings.HasSuffix(shell, "zsh"):
		profile = filepath.Join(home, ".zshrc")
	case strings.HasSuffix(shell, "bash"):
		// Prefer .bash_profile on macOS, .bashrc on Linux.
		profile = filepath.Join(home, ".bash_profile")
		if _, err := os.Stat(profile); err != nil {
			profile = filepath.Join(home, ".bashrc")
		}
	default:
		profile = filepath.Join(home, ".profile")
	}

	// Check if the line is already present.
	exportLine := fmt.Sprintf("export PATH=\"%s:$PATH\"", binDir)
	existing, err := os.ReadFile(profile)
	if err == nil && strings.Contains(string(existing), binDir) {
		// Already in profile — just not active in this session.
		fmt.Printf("  %s is already in %s but not in this session's PATH.\n", binDir, profile)
		fmt.Printf("  Run: source %s\n", profile)
		return
	}

	// Append to profile.
	f, err := os.OpenFile(profile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Printf("  Could not update %s: %v\n", profile, err)
		fmt.Printf("  Add manually: %s\n", exportLine)
		return
	}
	defer f.Close()

	_, err = fmt.Fprintf(f, "\n# Added by aveloxis install-tools for scancode\n%s\n", exportLine)
	if err != nil {
		fmt.Printf("  Could not write to %s: %v\n", profile, err)
		return
	}

	fmt.Printf("  Added Python user bin to %s\n", profile)
	fmt.Printf("  Run: source %s\n", profile)
}

// GoBinDir is where `go install` puts binaries (batch 4a review rounds
// 16–17). cmd/go reads GOBIN and GOPATH from its env file (`go env -w`) as
// well as the process environment, so when go is on PATH the answer is `go
// env GOBIN GOPATH`'s; without go (scorecard's installer needs none) the
// environment rule stands in: GOBIN, else the first GOPATH entry's bin,
// else ~/go/bin. A directory that is not absolute is an error — `go
// install` refuses it, and a relative PATH entry cannot be exported
// usefully (exec.LookPath rejects binaries found through one).
func GoBinDir(ctx context.Context) (string, error) {
	gobin, gopath := os.Getenv("GOBIN"), os.Getenv("GOPATH")
	if goPath, err := exec.LookPath("go"); err == nil {
		probe := exec.CommandContext(ctx, goPath, "env", "GOBIN", "GOPATH")
		groupKilled(probe)
		// The installed toolchain answers, whatever module the process sits
		// in (round 18: under GOTOOLCHAIN=auto a newer go line in the cwd's
		// go.mod made this probe download a toolchain, or fail offline).
		probe.Env = append(os.Environ(), "GOTOOLCHAIN=local")
		out, err := probe.Output()
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
				return "", fmt.Errorf("go env GOBIN GOPATH: %w: %s", execErr(ctx, err), strings.TrimSpace(string(exitErr.Stderr)))
			}
			return "", fmt.Errorf("go env GOBIN GOPATH: %w", execErr(ctx, err))
		}
		// Exactly two lines, either possibly empty (round 18: trimming every
		// trailing newline made "GOBIN\n\n" — an empty GOPATH — one line).
		lines := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
		if len(lines) != 2 {
			return "", fmt.Errorf("go env GOBIN GOPATH answered %q; want two lines", out)
		}
		gobin, gopath = lines[0], lines[1]
	}
	dir := gobin
	if dir == "" && gopath != "" {
		dir = filepath.Join(filepath.SplitList(gopath)[0], "bin")
	}
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("cannot determine home directory: %w", err)
		}
		dir = filepath.Join(home, "go", "bin")
	}
	if !filepath.IsAbs(dir) {
		return "", fmt.Errorf("the Go binary directory %q is not absolute (GOBIN or the first GOPATH entry) — go install refuses it", dir)
	}
	return dir, nil
}

// shadowWarning says when PATH resolves name to a copy other than the one
// just written (round 17: before v0.29.68 scorecard went to GOPATH/bin
// whatever GOBIN said, so an upgrade into GoBinDir can leave the old copy
// first on PATH — serve would keep running it while the upgrade reported
// success). Empty when the written copy is the one PATH finds, or none is.
func shadowWarning(name, written string) string {
	found, err := exec.LookPath(name)
	if err != nil {
		return ""
	}
	if a, errA := filepath.EvalSymlinks(found); errA == nil {
		found = a
	}
	w := written
	if b, errB := filepath.EvalSymlinks(written); errB == nil {
		w = b
	}
	if found == w {
		return ""
	}
	return fmt.Sprintf("warning: %s was written to %s, but %s is first on PATH — remove the older copy or reorder PATH, or serve keeps running it", name, written, found)
}
