// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.23.6 — `aveloxis upgrade-tools` subcommand. Re-runs the
// install pipeline against any already-installed tools to pull
// updated versions, and re-runs the libmagic injection so existing
// deployments stop emitting the UserWarning on every scan.
//
// Distinct from `aveloxis install-tools`, which short-circuits
// when a tool is already present. upgrade-tools intentionally
// re-runs through the install path even on already-installed
// tools.

func TestUpgradeToolsCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "upgradeToolsCmd()") {
		t.Error("main.go must wire upgradeToolsCmd() into root.AddCommand " +
			"or the root commands slice — without this, `aveloxis " +
			"upgrade-tools` is not callable.")
	}
}

func TestUpgradeToolsCmdFileExists(t *testing.T) {
	_, err := os.Stat("upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal("v0.23.6 introduces cmd/aveloxis/upgrade_tools_cmd.go. " +
			"See CLAUDE.md `Changes in v0.23.6`.")
	}
}

func TestUpgradeToolsCmdHasCorrectShape(t *testing.T) {
	src, err := os.ReadFile("upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "upgrade-tools") {
		t.Error("upgrade_tools_cmd.go must register the Use field as " +
			"`upgrade-tools` (kebab-case) following the data-test / " +
			"staging-stats / install-tools convention")
	}
	if !strings.Contains(code, "func upgradeToolsCmd") {
		t.Error("must define upgradeToolsCmd() *cobra.Command — pinned " +
			"by name so the wiring in main.go has a stable target")
	}
}

func TestUpgradeToolsCallsPipxUpgrade(t *testing.T) {
	src, err := os.ReadFile("upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The scancode upgrade path uses `pipx upgrade scancode-toolkit-mini`
	// rather than uninstall+reinstall. Pinned because uninstall+reinstall
	// would break operator overrides (custom Python version, custom
	// scancode plugins they may have injected) — pipx upgrade preserves
	// the venv.
	if !strings.Contains(code, "pipx") || !strings.Contains(code, "upgrade") {
		t.Error("scancode upgrade must use `pipx upgrade scancode-toolkit-mini` " +
			"to preserve operator customizations of the venv. Uninstall+reinstall " +
			"is NOT acceptable.")
	}
}

func TestUpgradeToolsReInjectsLibmagic(t *testing.T) {
	src, err := os.ReadFile("upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// After upgrading scancode, re-inject typecode-libmagic. Necessary
	// because pipx upgrade may have rebuilt the venv (depending on the
	// pipx version) and the injection from install-tools may not have
	// survived.
	if !strings.Contains(code, "InjectTypecodeLibmagic") &&
		!strings.Contains(code, "injectTypecodeLibmagic") {
		t.Error("upgrade-tools must call the libmagic injection helper " +
			"after scancode upgrade — pipx upgrade may have rebuilt the " +
			"venv, losing the prior injection")
	}
}

func TestUpgradeToolsReinstallsScorecard(t *testing.T) {
	src, err := os.ReadFile("upgrade_tools_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Scorecard isn't go-installable (we use the v5 tarball release).
	// Upgrade path: re-download the latest tarball and overwrite the
	// existing binary.
	if !strings.Contains(code, "Scorecard") && !strings.Contains(code, "scorecard") {
		t.Error("upgrade-tools must handle scorecard — pinned because " +
			"scorecard's upgrade path differs from scancode's (tarball " +
			"re-download, not pipx upgrade)")
	}
}
