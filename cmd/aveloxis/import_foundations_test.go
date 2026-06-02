// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestLoadFoundationCoreReposCmdRegistered — source-contract: the foundation
// core-repo importer must be wired up in main.go so it's discoverable via
// `aveloxis --help`. (v0.26.0: renamed from import-foundations to
// load-foundation-core-repos to say what it does — one core/primary repo per
// project — alongside the new load-foundation-orgs command.)
func TestLoadFoundationCoreReposCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "loadFoundationCoreReposCmd(") {
		t.Error("main.go must invoke loadFoundationCoreReposCmd(...) inside the root command setup so it is discoverable via `aveloxis --help`")
	}
}

// TestLoadFoundationCoreReposUseAndAlias — the user-facing slug is now
// "load-foundation-core-repos", but "import-foundations" MUST remain as a
// cobra alias so shipped operator scripts and docs don't break. This is the
// v0.26.0 deprecation-not-removal contract.
func TestLoadFoundationCoreReposUseAndAlias(t *testing.T) {
	data, err := os.ReadFile("import_foundations.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, `"load-foundation-core-repos"`) {
		t.Error(`command must use "load-foundation-core-repos" as its Use string — the new user-facing name`)
	}
	if !strings.Contains(src, `"import-foundations"`) {
		t.Error(`"import-foundations" must remain as a cobra alias (Aliases) so existing operator scripts keep working — deprecate, don't remove`)
	}
	if !strings.Contains(src, "Aliases:") {
		t.Error("command must declare Aliases: []string{...} carrying the legacy import-foundations name")
	}
}

// TestLoadFoundationCoreReposCmdFlags — the documented operator flags must
// survive the rename: `--dry-run` previews only, `--cncf-only`/`--apache-only`
// scope the run.
func TestLoadFoundationCoreReposCmdFlags(t *testing.T) {
	data, err := os.ReadFile("import_foundations.go")
	if err != nil {
		t.Skip("import_foundations.go not yet created")
	}
	src := string(data)
	for _, flag := range []string{"dry-run", "cncf-only", "apache-only", "priority"} {
		if !strings.Contains(src, flag) {
			t.Errorf("import_foundations.go must expose --%s flag — part of the documented operator contract", flag)
		}
	}
}
