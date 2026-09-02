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

// TestBackfillJiraReporterMintCountedBeforeAssignee (Copilot round 19
// on PR #193): the reporter mint must be counted BEFORE the assignee
// resolve — the assignee block `continue`s on a transient error, and
// mint is idempotent, so a reporter mint counted after that continue
// is lost forever (a rerun's reporterMinted is false). Position pin:
// the `if reporterMinted {` increment precedes the assignee resolve.
// TestBackfillJiraLimitBoundsAttemptsNotSuccesses (Copilot round 21 on
// PR #193): --limit must bound IDENTITY MUTATIONS, not successful
// upserts. Reporter/assignee resolve+mint commit contributor rows
// BEFORE the issue upsert, and a failed issue never increments the
// success count — so a failure-heavy canary would mutate the whole
// corpus while `processed` never hit the limit. The `attempted >= limit`
// break must precede the reporter identity write, and the old
// success-based `processed >= limit` check must be gone.
func TestBackfillJiraLimitBoundsAttemptsNotSuccesses(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/backfill_jira_identities.go")
	stripped := srctest.StripGoComments(src)
	limitCheck := strings.Index(stripped, "attempted >= limit")
	reporter := strings.Index(stripped, "is.Fields.Reporter")
	if limitCheck < 0 || reporter < 0 {
		t.Fatalf("anchors missing: attempted-limit=%d reporter=%d", limitCheck, reporter)
	}
	if limitCheck > reporter {
		t.Error("the `attempted >= limit` break must precede the reporter identity write — --limit bounds attempts, not successes (round 21)")
	}
	if strings.Contains(stripped, "processed >= limit") {
		t.Error("the post-success `processed >= limit` check must be removed — it never bounds a failure-heavy canary (round 21)")
	}
}

func TestBackfillJiraReporterMintCountedBeforeAssignee(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/backfill_jira_identities.go")
	mint := strings.Index(src, "if reporterMinted {")
	assignee := strings.Index(src, "is.Fields.Assignee")
	if mint < 0 || assignee < 0 {
		t.Fatalf("anchors missing: reporterMinted=%d assignee=%d", mint, assignee)
	}
	if mint > assignee {
		t.Error("the reporter mint (if reporterMinted { minted++ }) must be counted BEFORE the assignee resolve — else a transient assignee failure permanently under-reports the mint (Copilot round 19)")
	}
}

// TestBackfillJiraIdentitiesUsesTheSharedDriftSafeWalk (Copilot round
// 3 on PR #193, #2): the full-history pass walks
// jira.WalkProjectByUpdated — the ONE drift-safe walk (SR-17). A
// hand-spelled offset walk over `ORDER BY updated ASC` is the
// permanent-skip class the round-2 C2 fix removed from the worker;
// this round found the CLI re-spelling it.
func TestBackfillJiraIdentitiesUsesTheSharedDriftSafeWalk(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/backfill_jira_identities.go")
	code := srctest.StripGoComments(src)
	if !strings.Contains(code, "WalkProjectByUpdated(") {
		t.Error("backfill-jira-identities must walk jira.WalkProjectByUpdated")
	}
	for _, banned := range []string{
		"startAt +=",           // the offset walk
		"ORDER BY updated ASC", // a hand-built jql (the walk owns it)
		".SearchPage(",         // raw paging (ProjectTotal covers dry-run)
	} {
		if strings.Contains(code, banned) {
			t.Errorf("backfill_jira_identities.go must not contain %q — the shared walk owns the paging", banned)
		}
	}
	// Round 3 #3: the --limit message must tell the truth — there is
	// no persisted cursor, so a rerun restarts from the beginning.
	if !strings.Contains(src, "WITHOUT --limit") {
		t.Error("the --limit message must direct the operator to rerun WITHOUT --limit (no resume marker exists; 'rerun to continue' was a lie)")
	}
}
