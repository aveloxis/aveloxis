// Package collector — tools.go defines all external tools that Aveloxis
// can optionally use during collection. These tools are installed via
// `aveloxis install-tools` and checked at runtime — if a tool is missing,
// the corresponding analysis phase is silently skipped.
package collector

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
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
)

// ToolUpdateInterval is how often we check for updated tool versions.
// Default: 30 days. On startup, if the last check was longer ago than this,
// we re-run `go install ...@latest` for each installed tool to pull updates.
const ToolUpdateInterval = 30 * 24 * time.Hour

// ExternalTool describes an optional third-party tool used by Aveloxis.
type ExternalTool struct {
	Name        string       // display name
	CheckBinary string       // binary name to look up on PATH (exec.LookPath)
	InstallCmd  string       // go install or other install command (also used for manual install display)
	InstallFunc func() error // custom install function; takes priority over InstallCmd when set
	Description string       // what the tool does
	Purpose     string       // which collection phase uses it
}

// ExternalTools returns the list of all optional tools that Aveloxis can use.
// Each tool is independently optional — if not installed, its phase is skipped.
func ExternalTools() []ExternalTool {
	return []ExternalTool{
		{
			Name:        "scc",
			CheckBinary: "scc",
			InstallCmd:  "go install github.com/boyter/scc/v3@latest",
			Description: "Sloc Cloc and Code — counts lines of code, comments, blanks, and complexity per file per language",
			Purpose:     "Phase 4 (Analysis): populates the repo_labor table with per-file code metrics",
		},
		{
			Name:        "scorecard",
			CheckBinary: "scorecard",
			InstallCmd:  "see https://github.com/ossf/scorecard/releases",
			InstallFunc: installScorecardBinary,
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

// scorecardLatestVersion fetches the latest release tag from the GitHub API.
func scorecardLatestVersion() (string, error) {
	resp, err := http.Get("https://api.github.com/repos/ossf/scorecard/releases/latest")
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
// releases, extracts the binary, and places it in $GOPATH/bin (or ~/go/bin).
func installScorecardBinary() error {
	version, err := scorecardLatestVersion()
	if err != nil {
		return err
	}

	url := scorecardDownloadURL(version, runtime.GOOS, runtime.GOARCH)
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("downloading scorecard: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned %d for %s", resp.StatusCode, url)
	}

	// Determine destination: $GOPATH/bin or ~/go/bin.
	destDir := os.Getenv("GOPATH")
	if destDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("cannot determine home directory: %w", err)
		}
		destDir = home + "/go"
	}
	destDir = destDir + "/bin"
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
			defer os.Remove(tmp.Name())

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
func CheckAndUpdateTools(logger *slog.Logger) {
	lastCheck := readToolCheckTimestamp()
	if !IsToolUpdateCheckDue(lastCheck) {
		return
	}

	logger.Info("checking for tool updates (monthly check)")
	updated := 0

	for _, tool := range ExternalTools() {
		// Only update tools that are already installed.
		if _, err := exec.LookPath(tool.CheckBinary); err != nil {
			continue
		}

		logger.Info("updating tool", "name", tool.Name)
		if err := runToolInstall(tool); err != nil {
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
func RunToolInstall(tool ExternalTool) error {
	return runToolInstall(tool)
}

func runToolInstall(tool ExternalTool) error {
	if tool.InstallFunc != nil {
		return tool.InstallFunc()
	}
	parts := strings.Fields(tool.InstallCmd)
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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

// installScancode installs ScanCode Toolkit via pipx (preferred) or pip.
// ScanCode is a Python tool, so Python 3.10+ must be available.
// pipx is preferred because it creates an isolated virtual environment.
//
// We install scancode-toolkit-mini instead of the full scancode-toolkit to
// avoid native C dependency issues (pyicu, intbitset) that require pkg-config,
// ICU development libraries, and a compatible C compiler. The mini package
// has full license/copyright/package detection — it only omits advanced archive
// extraction and Unicode normalization features we don't need (we scan
// already-extracted code checkouts).
func installScancode() error {
	const pkg = "scancode-toolkit-mini"

	// Scancode depends on libmagic (native C library for file type detection).
	// Install it if missing.
	installLibmagicIfNeeded()

	// Try pipx first — creates an isolated venv automatically.
	if pipxPath, err := exec.LookPath("pipx"); err == nil {
		fmt.Printf("Installing %s via pipx...\n", pkg)
		cmd := exec.Command(pipxPath, "install", pkg)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err == nil {
			return nil
		}
		// pipx failed — fall through to pip.
		fmt.Println("pipx install failed, trying pip...")
	}

	// Fall back to pip install --user.
	for _, pip := range []string{"pip3", "pip"} {
		pipPath, err := exec.LookPath(pip)
		if err != nil {
			continue
		}
		fmt.Printf("Installing %s via %s --user...\n", pkg, pip)
		cmd := exec.Command(pipPath, "install", "--user", pkg)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err == nil {
			// pip --user installs to a platform-specific bin dir that
			// may not be on PATH. Detect it and add to shell profile.
			if _, lookErr := exec.LookPath("scancode"); lookErr != nil {
				ensurePythonUserBinOnPath()
			}
			return nil
		}
	}

	return fmt.Errorf("scancode install failed: neither pipx nor pip found. Install Python 3.10+ and run: pipx install %s", pkg)
}

// installLibmagicIfNeeded installs the libmagic native library if it's not
// already available. Scancode uses it for file type detection.
//   - macOS: brew install libmagic
//   - Debian/Ubuntu: apt-get install libmagic1
//   - RHEL/CentOS: yum install file-libs
func installLibmagicIfNeeded() {
	// Quick check: if libmagic is loadable, we're good.
	// The file command uses libmagic, so checking for it is a reasonable proxy.
	// On macOS, Homebrew installs to /opt/homebrew/lib or /usr/local/lib.
	if runtime.GOOS == "darwin" {
		if _, err := exec.LookPath("brew"); err == nil {
			// Check if already installed via brew.
			check := exec.Command("brew", "list", "libmagic")
			if check.Run() != nil {
				fmt.Println("Installing libmagic via Homebrew (required by scancode)...")
				cmd := exec.Command("brew", "install", "libmagic")
				cmd.Stdout = os.Stdout
				cmd.Stderr = os.Stderr
				if err := cmd.Run(); err != nil {
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
func ensurePythonUserBinOnPath() {
	// Determine the Python user bin directory.
	var binDir string
	for _, py := range []string{"python3", "python"} {
		pyPath, err := exec.LookPath(py)
		if err != nil {
			continue
		}
		out, err := exec.Command(pyPath, "-m", "site", "--user-base").Output()
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
