// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot review 5268977585: `run-scorecard` took `r.GitURL` verbatim, so a
// legacy row with an empty repo_git sent `--repo ""`, while the scheduler's
// phase kept the synthesised fallback — two spellings of one rule. The job
// loop takes its URL from collector.ScorecardRepoURL and nothing else.
func TestRunScorecardTakesItsURLFromTheSharedRule(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/run_scorecard.go"), "func runRunScorecard("))
	if !strings.Contains(body, "repoURL := collector.ScorecardRepoURL(r.GitURL, model.Platform(r.Platform), r.Owner, r.Name)") {
		t.Error("run-scorecard must take its repo URL from collector.ScorecardRepoURL (stored URL, else the synthesised fallback)")
	}
	if strings.Contains(body, "repoURL := r.GitURL") || strings.Contains(body, "RepoURL:         r.GitURL") {
		t.Error("run-scorecard must not hand scorecard the raw stored URL — an empty repo_git becomes --repo \"\"")
	}
	if strings.Contains(body, "repoURL =") {
		t.Error("repoURL is reassigned after the shared rule chose it")
	}
}
