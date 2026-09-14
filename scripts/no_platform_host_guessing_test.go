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
	if !strings.Contains(mainSrc, "c, rerr := clients.gl.ForRepo(stored.Platform, stored.GitURL)") {
		t.Error("the collect CLI must route a stored GitLab row by its platform_id and URL (clients.gl.ForRepo), as serve does")
	}
	if !strings.Contains(mainSrc, "coll.CollectRepo(ctx, repoID, stored.GitURL, owner, repo, since)") ||
		!strings.Contains(mainSrc, "stored, err := store.GetRepoByID(ctx, repoID)") {
		t.Error("the collect CLI must clone the stored repo_git (read back after UpsertRepo), not the raw argument")
	}
}

// v0.30.0 review of B1–B5 (finding 1): commands that parse a repository URL
// pass the configured GitLab instances' web URLs as hints, so a self-hosted
// instance without "gitlab" in its hostname is accepted and a sub-path
// prefix is not taken for an owner.
func TestCommandsParseRepoURLsWithInstanceHints(t *testing.T) {
	if !strings.Contains(srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/load_foundation_orgs.go")), "orgURLForRepo(rurl, configuredGitLabWebBases(cfg))") {
		t.Error("load-foundation-orgs must pass the configured GitLab instances' web URLs to orgURLForRepo")
	}
	allowed := map[string]int{}
	for _, f := range []string{"cmd/aveloxis/main.go", "cmd/aveloxis/import_foundations.go", "cmd/aveloxis/load_foundation_orgs.go"} {
		n := strings.Count(srctest.StripGoComments(srctest.Read(t, f)), "platform.ParseRepoURL(")
		if n != allowed[f] {
			t.Errorf("%s has %d unhinted platform.ParseRepoURL calls (allowed %d) — use ParseRepoURLWithHints with configuredGitLabWebBases(cfg)", f, n, allowed[f])
		}
	}
}
