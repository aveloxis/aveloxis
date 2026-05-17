// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.22.8 introduces `aveloxis data-test` — an operator-driven
// shadow-database verification harness for schema changes. Compares
// the local working-tree binary against a GitHub-tagged release
// binary by collecting the same repo into two scratch DBs and
// diffing row counts table-by-table.
//
// Tests in this file pin the command's source-level contract.
// Behavioral tests for the row-count diff function live in
// internal/db/rowcount_diff_test.go (also v0.22.8).

func TestDataTestCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "dataTestCmd(") {
		t.Error("main.go must call dataTestCmd() and add it to root via root.AddCommand — " +
			"without registration the subcommand can't be invoked from the CLI")
	}
}

func TestDataTestCmdHasRequiredFlags(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatalf("data_test_cmd.go does not exist — v0.22.8 command source file required: %v", err)
	}
	code := string(src)

	// Required flags
	for _, flag := range []string{"released-tag", "repo"} {
		if !strings.Contains(code, `"`+flag+`"`) {
			t.Errorf("data-test command must define --%s flag", flag)
		}
	}

	// Optional flags
	for _, flag := range []string{"keep-dbs", "work-dir"} {
		if !strings.Contains(code, `"`+flag+`"`) {
			t.Errorf("data-test command must define --%s flag (optional)", flag)
		}
	}

	// Must use cobra.Command
	if !strings.Contains(code, "cobra.Command") {
		t.Error("data-test command must be a cobra.Command (matches the rest of the CLI)")
	}
}

func TestDataTestCmdInvokesAllPhases(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Each phase is a separate helper function, called in order.
	// The names pin the operational sequence so a future refactor
	// can't accidentally skip a phase.
	phases := []string{
		"resolveReleasedBinary",
		"resolveLocalBinary",
		"provisionScratchDB",
		"generateScratchConfig",
		"runMigrate",
		"copyAPIKeys",
		"dtRunAddRepo",
		"dtRunCollect",
		"runRowCountDiff",
		"writeReport",
		"cleanupScratchDBs",
	}
	for _, p := range phases {
		if !strings.Contains(code, p) {
			t.Errorf("data-test command must invoke phase helper %s — without it, the "+
				"corresponding phase of the harness doesn't run", p)
		}
	}
}

func TestDataTestUsesGitWorktreeNotFreshClone(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// The released-binary resolver uses `git worktree add` (fast,
	// reuses local clone's objects) rather than a fresh `git clone`
	// from the remote (slow, redundant network fetch). Empirically
	// chosen for v0.22.8.
	if !strings.Contains(code, "worktree") {
		t.Error("resolveReleasedBinary must use `git worktree add` to materialize the " +
			"tagged release source — fresh `git clone` would re-fetch the entire repo " +
			"from origin, slow and redundant when the local working copy already has " +
			"the tag")
	}
}

func TestDataTestStreamsSubprocessOutput(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Collection takes ~30 minutes per side. Operator needs live
	// progress, not a 30-minute silence. The subprocess wrappers
	// must wire stdout/stderr through.
	if !strings.Contains(code, "Stdout") || !strings.Contains(code, "Stderr") {
		t.Error("runCollect (and runMigrate) must wire subprocess Stdout/Stderr to the " +
			"parent so the operator sees live progress during the ~30-min collection " +
			"window")
	}
}

func TestDataTestDropsScratchDBsByDefault(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Per operator decision on 2026-05-17: drop scratch DBs by
	// default; --keep-dbs to retain. The cleanup function must
	// honor the flag.
	if !strings.Contains(code, "keepDBs") && !strings.Contains(code, "KeepDBs") {
		t.Error("data-test must thread a keepDBs boolean from the --keep-dbs flag " +
			"through to cleanupScratchDBs so the operator can opt to retain DBs for " +
			"ad-hoc SQL inspection after a FAIL")
	}
}

func TestScratchDBNamesAreConventional(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	// Operator-mandated DB names so the harness output is
	// predictable from invocation to invocation: aveloxis_released
	// and aveloxis_new.
	if !strings.Contains(code, "aveloxis_released") {
		t.Error("data-test must use the conventional name `aveloxis_released` for the " +
			"released-version scratch DB so output is predictable across runs")
	}
	if !strings.Contains(code, "aveloxis_new") {
		t.Error("data-test must use the conventional name `aveloxis_new` for the " +
			"local-version scratch DB so output is predictable across runs")
	}
}
