// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.30.0 (multi-instance GitLab): a repo's URL is its stored repo_git.
// platformHostForModel (scheduler) and platformHost (collector) rebuilt it
// as https://<github.com|gitlab.com>/owner/name from the platform id, which
// sends a self-hosted GitLab repo's scorecard run and the collect CLI's
// clone to gitlab.com (v0.25.38 already moved serve's facade clone off it
// for the same reason). Both helpers are gone; this pins that they stay
// gone and that the two former callers use the repo's own URL.
func TestReposAreAddressedByTheirStoredURL(t *testing.T) {
	sched := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	coll := srctest.StripGoComments(srctest.Read(t, "internal/collector/collector.go"))
	for file, src := range map[string]string{"internal/scheduler/scheduler.go": sched, "internal/collector/collector.go": coll} {
		for _, banned := range []string{"platformHostForModel(", "platformHost("} {
			if strings.Contains(src, banned) {
				t.Errorf("%s calls or defines %s — rebuild no repo URL from its platform id; use the stored repo_git", file, strings.TrimSuffix(banned, "("))
			}
		}
	}

	scorecard := srctest.FuncBody(t, sched, "func (s *Scheduler) runScorecardPhase(")
	if !strings.Contains(scorecard, "repoURL := collector.ScorecardRepoURL(repo.Platform, repo.GitURL, repo.Owner, repo.Name)") {
		t.Error("runScorecardPhase must score the repo's stored URL through collector.ScorecardRepoURL")
	}
	runSC := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/run_scorecard.go"))
	if !strings.Contains(runSC, "repoURL := collector.ScorecardRepoURL(model.Platform(r.Platform), r.GitURL, r.Owner, r.Name)") || strings.Contains(runSC, `fmt.Sprintf("https://github.com/%s/%s"`) {
		t.Error("run-scorecard must build its URL through collector.ScorecardRepoURL (one spelling, SR-17)")
	}
	collect := srctest.FuncBody(t, coll, "func (c *Collector) CollectRepo(")
	if !strings.Contains(collect, "c.facade.CollectRepo(ctx, repoID, gitURL)") ||
		!strings.Contains(collect[:strings.Index(collect, "{")], "gitURL, owner, repo string") {
		t.Error("Collector.CollectRepo must take the repo's URL as a parameter and clone that, not a URL rebuilt from the platform id")
	}
	mainSrc := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/main.go"))
	if !strings.Contains(mainSrc, "coll.CollectRepo(ctx, repoID, stored.GitURL, owner, repo, since)") ||
		!strings.Contains(mainSrc, "stored, err := store.GetRepoByID(ctx, repoID)") {
		t.Error("the collect CLI must clone the stored repo_git (read back after UpsertRepo), not the raw argument")
	}
}
