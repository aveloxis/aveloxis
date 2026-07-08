// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for HealRepoCaseDrift (v0.25.32) — the Phase 0
// self-heal that corrects a repo's stored URL casing when the forge
// reports a canonical spelling that differs ONLY by case. Real renames
// (more than case) stay prelim's job: the forge treats case variants as
// the same resource, so a case-only change does not change identity —
// unlike a rename, where mutating repo identity mid-job risks
// split-identity data (the documented prelim-owns-renames decision).

package db

import (
	"strings"
	"testing"
)

func TestHealRepoCaseDriftContract(t *testing.T) {
	body := extractFunctionBody(t, "repo_case_heal.go", "HealRepoCaseDrift")

	if !strings.Contains(body, "EqualFold") {
		t.Error("HealRepoCaseDrift must gate on strings.EqualFold — anything that " +
			"differs by MORE than case is a real rename and stays prelim's job.")
	}
	if !strings.Contains(body, "UpdateRepoURLs(") {
		t.Error("HealRepoCaseDrift must reuse UpdateRepoURLs — the existing rename " +
			"machinery that also rewrites stored issue/PR html_urls; do not invent a " +
			"parallel URL-rewrite path.")
	}
	if !strings.Contains(body, "repo_id <>") && !strings.Contains(body, "repo_id !=") {
		t.Error("HealRepoCaseDrift must run an exact-match occupancy check excluding " +
			"the repo itself (repo_git = $newURL AND repo_id <> $repoID) — NOT the " +
			"case-insensitive FindRepoByURL, which would match the very row being " +
			"healed.")
	}
	if !strings.Contains(body, "dedup-repos") {
		t.Error("when the canonical URL is already occupied by ANOTHER row (an " +
			"unmerged duplicate pair), HealRepoCaseDrift must WARN with the " +
			"`aveloxis dedup-repos` hint and skip — never collide two rows itself.")
	}
}
