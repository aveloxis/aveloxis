// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_cmds_test.go — C2/C3 operator CLIs: register-jira-projects
// (derive registrations from the synthetics we already hold) and
// backfill-jira-identities (the one-shot identity + state pass that
// banks the perishable Server-era usernames — Jira Cloud's API has no
// stable username, so the collection window is bounded).
package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestJiraCommandsRegistered(t *testing.T) {
	mainSrc := srctest.Read(t, "cmd/aveloxis/main.go")
	for _, needle := range []string{"registerJiraProjectsCmd(", "backfillJiraIdentitiesCmd("} {
		if !strings.Contains(mainSrc, needle) {
			t.Errorf("main.go must register %s", needle)
		}
	}
}

func TestRegisterJiraProjectsShape(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/register_jira_projects.go")
	for _, needle := range []string{
		`"register-jira-projects"`,
		`"dry-run"`,
		`"base-url"`,
		"DeriveJiraProjectsFromSynthetics(",
		"RegisterJiraProject(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("register_jira_projects.go must contain %s", needle)
		}
	}
	code := srctest.StripGoComments(src)
	if strings.Contains(code, ".Migrate(") {
		t.Error("register-jira-projects must NOT call store.Migrate (v0.21.5)")
	}
}

func TestBackfillJiraIdentitiesShape(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/backfill_jira_identities.go")
	for _, needle := range []string{
		`"backfill-jira-identities"`,
		`"dry-run"`,
		`"project"`,
		`"limit"`,
		"ResolveJiraIdentity(",
		"UpsertJiraIssueFromAPI(",
		"MintJiraContributor(",
		// Copilot round 2 on PR #193 (#1): projects come from the LIVE
		// registrations (operator-correctable repo mapping + per-
		// registration base URL), never a synthetics re-derivation.
		"ListJiraProjectRegistrations(",
		// (#4): identity resolve/mint ERRORS fail the issue and the
		// run — a transient DB failure must not report success with
		// attribution permanently missing.
		"failedIssues",
		// (suppressed #1): the assignee identity is banked too — the
		// one-shot is the banking vehicle for the perishable username.
		"is.Fields.Assignee",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("backfill_jira_identities.go must contain %s", needle)
		}
	}
	code := srctest.StripGoComments(src)
	if strings.Contains(code, ".Migrate(") {
		t.Error("backfill-jira-identities must NOT call store.Migrate (v0.21.5)")
	}
	if strings.Contains(code, "DeriveJiraProjectsFromSynthetics(") {
		t.Error("backfill-jira-identities must NOT re-derive projects from synthetics — the registrations are the operator-correctable mapping (Copilot round 2 on PR #193, #1)")
	}
	if strings.Contains(code, `"base-url"`) {
		t.Error("backfill-jira-identities must NOT carry a command-wide base-url flag — each registration's base_url wins")
	}
}
