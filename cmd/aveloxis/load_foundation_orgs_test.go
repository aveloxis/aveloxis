// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestLoadFoundationOrgsCmdRegistered — the new org-tracking command must be
// wired into main.go (companion to load-foundation-core-repos).
func TestLoadFoundationOrgsCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "loadFoundationOrgsCmd(") {
		t.Error("main.go must invoke loadFoundationOrgsCmd(...) so the command is discoverable via `aveloxis --help`")
	}
}

// TestLoadFoundationOrgsCmdShape — Use slug + the operator-facing flags.
// `--yes` is the scale gate: tracking the apache org pulls ALL ~3,000
// apache/* repos into collection, so the command must require explicit
// confirmation rather than doing it silently.
func TestLoadFoundationOrgsCmdShape(t *testing.T) {
	data, err := os.ReadFile("load_foundation_orgs.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, `"load-foundation-orgs"`) {
		t.Error(`command Use must be "load-foundation-orgs"`)
	}
	for _, flag := range []string{"user-id", "dry-run", "yes", "apache-only", "cncf-only"} {
		if !strings.Contains(src, `"`+flag+`"`) {
			t.Errorf("load-foundation-orgs must expose --%s flag", flag)
		}
	}
}

// TestLoadFoundationOrgsTracksViaAddOrgToGroup — the whole point is to
// leverage the EXISTING org-tracking feature: register each foundation org
// via AddOrgToGroup → user_org_requests, so refreshUserOrgs continuously
// discovers new repos. A regression to one-shot repo adds would defeat the
// "catch future repos" requirement.
func TestLoadFoundationOrgsTracksViaAddOrgToGroup(t *testing.T) {
	data, err := os.ReadFile("load_foundation_orgs.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "AddOrgToGroup(") {
		t.Error("load-foundation-orgs must call store.AddOrgToGroup (the tracked-org feature), not one-shot repo adds")
	}
	// It must derive the org owner from project repo URLs.
	if !strings.Contains(src, "ParseRepoURL(") {
		t.Error("load-foundation-orgs must derive distinct org owners from project repo URLs via platform.ParseRepoURL")
	}
}

// TestLoadFoundationOrgsRequiresYesGate — without --yes (and not --dry-run)
// the command must NOT write; it must abort with guidance. This is the
// operator-mandated scale guard.
func TestLoadFoundationOrgsRequiresYesGate(t *testing.T) {
	data, err := os.ReadFile("load_foundation_orgs.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The gate must reference the yes flag and short-circuit before AddOrgToGroup.
	yesIdx := strings.Index(src, "opts.Yes")
	addIdx := strings.Index(src, "AddOrgToGroup(")
	if yesIdx < 0 {
		t.Error("command must consult the --yes flag (opts.Yes) before tracking orgs")
	}
	if yesIdx >= 0 && addIdx >= 0 && yesIdx > addIdx {
		t.Error("the --yes scale gate must be checked BEFORE AddOrgToGroup is called")
	}
}
