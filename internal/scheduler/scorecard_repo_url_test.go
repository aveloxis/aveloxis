// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.57, Copilot review 5261384568: the per-cycle scorecard phase built
// its repo URL as https://github.com/<owner>/<name> from the platform id,
// while `aveloxis run-scorecard` had already switched to the stored URL and
// its comment claimed the phase "has always passed the stored URL". On a
// deployment whose GitHub is not github.com that URL names the wrong
// repository (or, in local mode, rewrites a retained clone's origin to the
// wrong host). The facade phase made the same change in v0.25.38 for the
// same reason; the scorecard phase now shares its rule: the row's own URL,
// and the synthesised one ONLY for a row that has none.
func TestScorecardRepoURLPrefersTheStoredURL(t *testing.T) {
	// The rule itself is collector.ScorecardRepoURL (one spelling since
	// Copilot review 5268977585, when run-scorecard was found to have lost
	// the fallback); this pins the delegation and one row of each arm.
	for _, tc := range []struct {
		repo *model.Repo
		want string
	}{
		{&model.Repo{Platform: model.PlatformGitHub, Owner: "o", Name: "n", GitURL: "https://ghe.example.invalid/o/n"}, "https://ghe.example.invalid/o/n"},
		{&model.Repo{Platform: model.PlatformGitLab, Owner: "g", Name: "p"}, "https://gitlab.com/g/p"},
	} {
		if got := scorecardRepoURL(tc.repo); got != tc.want {
			t.Errorf("scorecardRepoURL(%+v) = %q, want %q", tc.repo, got, tc.want)
		}
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func scorecardRepoURL("))
	if !strings.Contains(body, "collector.ScorecardRepoURL(repo.GitURL, repo.Platform, repo.Owner, repo.Name)") || strings.Contains(body, "https://") {
		t.Error("scorecardRepoURL must delegate to collector.ScorecardRepoURL and synthesise nothing itself")
	}
}

// The helper is worth nothing unless the phase USES it for the URL it hands
// scorecard (the v0.29.57 lesson: a fix at one site while a sibling keeps
// the literal). The pin reads the argument, not a token: the option's value
// must be the helper's result, and the phase body must not synthesise a URL
// of its own.
func TestRunScorecardPhaseHandsScorecardTheStoredURL(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) runScorecardPhase("))
	if !strings.Contains(body, "repoURL := scorecardRepoURL(repo)") {
		t.Error("runScorecardPhase must take its repo URL from scorecardRepoURL(repo)")
	}
	if !strings.Contains(body, "RepoURL:         repoURL,") && !strings.Contains(body, "RepoURL: repoURL,") {
		t.Error("ScorecardOptions.RepoURL in runScorecardPhase must be the repoURL scorecardRepoURL returned")
	}
	if strings.Contains(body, "PlatformHost(") || strings.Contains(body, `"https://%s/%s/%s"`) {
		t.Error("runScorecardPhase must not synthesise a repo URL of its own — that is scorecardRepoURL's one job")
	}
	// A reassignment between the helper and the option would pass the two
	// checks above (fix-review round 1 mutation): the helper's result must be
	// the ONLY value repoURL ever holds.
	if strings.Contains(body, "repoURL =") {
		t.Error("runScorecardPhase reassigns repoURL after scorecardRepoURL — the option would carry a different URL than the helper chose")
	}
}
