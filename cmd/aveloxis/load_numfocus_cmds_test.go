// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// Source-contract tests for the v0.25.4 NumFocus import commands.
// These pin command surface (registration, flag names, defaults,
// group names) — operator-facing identifiers that should not
// silently drift under a refactor.

func TestLoadNumfocusCommandsRegisteredInMain(t *testing.T) {
	src := readSource(t, "main.go")
	for _, needle := range []string{
		"loadNumfocusProjectsCmd(&cfgPath)",
		"loadNumfocusOrgsCmd(&cfgPath)",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("main.go must register %s on the root command — without it the subcommand is unreachable via `aveloxis --help`", needle)
		}
	}
}

func TestLoadNumfocusProjectsCmdSurface(t *testing.T) {
	src := readSource(t, "load_numfocus_projects.go")
	for _, needle := range []string{
		`Use:   "load-numfocus-projects"`,
		`cmd.Flags().IntVar(&userID, "user-id", 1`,
		`cmd.Flags().BoolVar(&dryRun, "dry-run"`,
		`cmd.Flags().BoolVar(&detectNew, "detect-new"`,
		`cmd.Flags().StringVar(&catalogFile, "catalog-file"`,
		`"NumFocus Sponsored"`,
		`"NumFocus Affiliated"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("load-numfocus-projects command surface missing needle: %q", needle)
		}
	}
}

func TestLoadNumfocusOrgsCmdSurface(t *testing.T) {
	src := readSource(t, "load_numfocus_orgs.go")
	for _, needle := range []string{
		`Use:   "load-numfocus-orgs"`,
		`cmd.Flags().IntVar(&userID, "user-id", 1`,
		`cmd.Flags().BoolVar(&dryRun, "dry-run"`,
		`cmd.Flags().BoolVar(&detectNew, "detect-new"`,
		`cmd.Flags().StringVar(&catalogFile, "catalog-file"`,
		`"NumFocus Sponsored Orgs"`,
		`"NumFocus Affiliated Orgs"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("load-numfocus-orgs command surface missing needle: %q", needle)
		}
	}
}

// TestLoadNumfocusProjectsUsesAddRepoToGroup pins the load-projects
// path goes through the same web-UI insert function (AddRepoToGroup)
// so the v0.19.0 group-approval gate applies uniformly. A regression
// that bypasses AddRepoToGroup would skip the gate and insert
// repos for pending groups, breaking the approval workflow.
func TestLoadNumfocusProjectsUsesAddRepoToGroup(t *testing.T) {
	src := readSource(t, "load_numfocus_projects.go")
	if !strings.Contains(src, "store.AddRepoToGroup(") {
		t.Error("load-numfocus-projects must call store.AddRepoToGroup so the v0.19.0 group-approval gate applies. A path that bypasses this function would let non-admin user_groups insert repos without approval.")
	}
}

// TestLoadNumfocusOrgsUsesAddOrgToGroup pins the load-orgs path
// goes through AddOrgToGroup which inserts into
// aveloxis_ops.user_org_requests — the v0.19.x refreshUserOrgs
// ticker reads from there to discover orgs to scan periodically.
// A regression that bypasses this would lose the auto-pickup of
// new repos within the org.
func TestLoadNumfocusOrgsUsesAddOrgToGroup(t *testing.T) {
	src := readSource(t, "load_numfocus_orgs.go")
	if !strings.Contains(src, "store.AddOrgToGroup(") {
		t.Error("load-numfocus-orgs must call store.AddOrgToGroup — this is what inserts into user_org_requests, which the v0.19.x refreshUserOrgs ticker reads. Without it, the 'pick up new repos in the org automatically' contract is broken.")
	}
}

// TestLoadNumfocusProjectsDoesNotMigrate pins the v0.21.5 contract:
// migration only runs in `aveloxis migrate` and `aveloxis serve`,
// NOT in operator one-shot commands. A re-introduction of
// store.Migrate(ctx) in this command would silently mutate the
// schema on `--dry-run`, defeating the dry-run contract.
func TestLoadNumfocusProjectsDoesNotMigrate(t *testing.T) {
	src := readSource(t, "load_numfocus_projects.go")
	if strings.Contains(src, "store.Migrate(ctx)") {
		t.Error("load_numfocus_projects.go must NOT call store.Migrate(ctx). v0.21.5 reserves migration for `aveloxis migrate` and `aveloxis serve`. Calling Migrate here would silently mutate schema during --dry-run, defeating the dry-run contract.")
	}
}

func TestLoadNumfocusOrgsDoesNotMigrate(t *testing.T) {
	src := readSource(t, "load_numfocus_orgs.go")
	if strings.Contains(src, "store.Migrate(ctx)") {
		t.Error("load_numfocus_orgs.go must NOT call store.Migrate(ctx) — same v0.21.5 reasoning as load_numfocus_projects.go.")
	}
}

func readSource(t *testing.T, file string) string {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("read %s: %v", file, err)
	}
	return string(data)
}
