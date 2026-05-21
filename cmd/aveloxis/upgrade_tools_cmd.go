// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"

	"github.com/aveloxis/aveloxis/internal/collector"
)

// upgradeToolsCmd (v0.23.6) re-runs the install pipeline against any
// already-installed tools to pull updated versions, and re-injects the
// typecode-libmagic plugin into scancode's pipx venv.
//
// Distinct from `aveloxis install-tools` which short-circuits when a
// tool is already present. The upgrade command intentionally re-runs
// through the install path for each tool, even when already installed.
//
// Per-tool upgrade strategy:
//
//   - scc / scorecard — re-run their install function. scc uses
//     `go install ...@latest` so the install IS the upgrade. scorecard
//     re-downloads the latest tarball and overwrites the existing
//     binary in $GOPATH/bin.
//
//   - scancode — `pipx upgrade scancode-toolkit-mini` rather than
//     uninstall + reinstall, to preserve any operator customizations
//     of the venv (additional injected plugins, Python version
//     override, etc.). Then re-inject typecode-libmagic; pipx upgrade
//     may have rebuilt the venv in some pipx versions, and the
//     injection from the original install-tools wouldn't survive.
//
// Operators on existing deployments that hit the v0.23.3 stderr noise
// (the libmagic UserWarning dominating every "scancode subprocess
// failed" log line) run this command once to silence the warning
// without reinstalling from scratch.
func upgradeToolsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "upgrade-tools",
		Short: "Upgrade scc, scorecard, scancode to latest versions and re-inject typecode-libmagic",
		Long: `Upgrades each optional analysis tool to its latest release.

Tools handled:
  scc        — go install github.com/boyter/scc/v3@latest (idempotent re-install)
  scorecard  — re-download latest tarball from GitHub releases
  scancode   — pipx upgrade scancode-toolkit-mini (preserves venv customizations)

Side effect: after scancode upgrade, runs ` + "`pipx inject scancode-toolkit-mini" +
			" typecode-libmagic`" + ` to ensure the Python libmagic binding is in
place. Without this, scancode emits a UserWarning on every scan that
dominates stderr capture for diagnostics.

Idempotent: re-running is safe. Tools not yet installed are reported but
not installed — use ` + "`aveloxis install-tools`" + ` for fresh installs.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tools := collector.ExternalTools()
			upgraded := 0
			skipped := 0
			failed := 0

			for _, tool := range tools {
				path, err := exec.LookPath(tool.CheckBinary)
				if err != nil {
					fmt.Printf("- %s not installed; run `aveloxis install-tools` first\n", tool.Name)
					skipped++
					continue
				}
				fmt.Printf("Upgrading %s (currently at %s)...\n", tool.Name, path)

				if upgradeErr := upgradeOne(tool); upgradeErr != nil {
					fmt.Printf("x %s upgrade failed: %v\n", tool.Name, upgradeErr)
					failed++
					continue
				}

				if newPath, err := exec.LookPath(tool.CheckBinary); err == nil {
					fmt.Printf("ok %s upgraded: %s\n", tool.Name, newPath)
				} else {
					fmt.Printf("warning: %s upgrade succeeded but binary not on PATH\n", tool.Name)
				}
				upgraded++
			}

			fmt.Printf("\nupgrade-tools summary: %d upgraded, %d skipped (not installed), %d failed\n",
				upgraded, skipped, failed)
			if failed > 0 {
				return fmt.Errorf("%d tool(s) failed to upgrade", failed)
			}
			return nil
		},
	}
}

// upgradeOne dispatches per-tool upgrade logic. scancode uses pipx upgrade
// + libmagic re-inject; everything else just re-runs the install pipeline
// (which uses @latest / fetches the newest tarball).
func upgradeOne(tool collector.ExternalTool) error {
	if tool.Name == "scancode" {
		return upgradeScancode()
	}
	// scc, scorecard, and any future tools: re-run the install. The
	// install functions / commands already use @latest or fetch the
	// newest release, so a fresh install IS the upgrade.
	return collector.RunToolInstall(tool)
}

// upgradeScancode runs `pipx upgrade scancode-toolkit-mini` then
// re-injects typecode-libmagic. Falls back to the standard install
// path if pipx isn't available (in which case the tool was likely
// installed via pip --user and the upgrade is a fresh install).
func upgradeScancode() error {
	const pkg = "scancode-toolkit-mini"

	pipxPath, err := exec.LookPath("pipx")
	if err != nil {
		// No pipx — fall back to the standard install path.
		fmt.Println("pipx not found; falling back to fresh install via pip...")
		for _, t := range collector.ExternalTools() {
			if t.Name == "scancode" {
				return collector.RunToolInstall(t)
			}
		}
		return fmt.Errorf("scancode tool definition missing")
	}

	upgrade := exec.Command(pipxPath, "upgrade", pkg)
	upgrade.Stdout = os.Stdout
	upgrade.Stderr = os.Stderr
	if err := upgrade.Run(); err != nil {
		return fmt.Errorf("pipx upgrade %s: %w", pkg, err)
	}

	// Re-inject typecode-libmagic. Non-fatal if it fails: the upgrade
	// succeeded; the warning will just continue to print until the
	// operator fixes it manually.
	if err := collector.InjectTypecodeLibmagic(pipxPath, pkg); err != nil {
		fmt.Printf("warning: typecode-libmagic re-injection failed: %v\n", err)
		fmt.Println("  scancode upgrade succeeded; libmagic UserWarning may continue to print.")
		fmt.Println("  to retry: pipx inject scancode-toolkit-mini typecode-libmagic")
	}
	return nil
}
