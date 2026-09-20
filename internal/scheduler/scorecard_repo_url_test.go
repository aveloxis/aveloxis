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
	for _, tc := range []struct {
		repo *model.Repo
		want string
	}{
		{&model.Repo{Platform: model.PlatformGitHub, Owner: "o", Name: "n", GitURL: "https://ghe.example.invalid/o/n"}, "https://ghe.example.invalid/o/n"},
		{&model.Repo{Platform: model.PlatformGitLab, Owner: "g/sub", Name: "p", GitURL: "https://gitlab.example.invalid/g/sub/p"}, "https://gitlab.example.invalid/g/sub/p"},
		// no stored URL: the pre-v0.29.57 synthesis, unchanged
		{&model.Repo{Platform: model.PlatformGitHub, Owner: "o", Name: "n"}, "https://github.com/o/n"},
		{&model.Repo{Platform: model.PlatformGitLab, Owner: "g", Name: "p"}, "https://gitlab.com/g/p"},
	} {
		if got := scorecardRepoURL(tc.repo); got != tc.want {
			t.Errorf("scorecardRepoURL(%+v) = %q, want %q", tc.repo, got, tc.want)
		}
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
	if strings.Contains(body, "platformHostForModel(") || strings.Contains(body, `"https://%s/%s/%s"`) {
		t.Error("runScorecardPhase must not synthesise a repo URL of its own — that is scorecardRepoURL's one job")
	}
}
