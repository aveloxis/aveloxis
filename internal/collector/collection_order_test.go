package collector

import (
	"os"
	"strings"
	"testing"
)

// TestRepoInfoCollectedBeforeIssuesAndPRs verifies the staged collection
// order has repo_info, releases, and clone stats BEFORE contributors, issues,
// and PRs. This enables large-repo detection (>10K commits) before the heavy
// collection phases begin.
func TestRepoInfoCollectedBeforeIssuesAndPRs(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the CollectRepo function.
	idx := strings.Index(code, "func (sc *StagedCollector) CollectRepo(")
	if idx < 0 {
		t.Fatal("cannot find CollectRepo function")
	}
	fnBody := code[idx:]

	// Repo info (FetchRepoInfo) must appear BEFORE issues (ListIssues).
	repoInfoIdx := strings.Index(fnBody, "FetchRepoInfo")
	issuesIdx := strings.Index(fnBody, "ListIssues")
	if repoInfoIdx < 0 {
		t.Fatal("cannot find FetchRepoInfo in CollectRepo")
	}
	if issuesIdx < 0 {
		t.Fatal("cannot find ListIssues in CollectRepo")
	}
	if repoInfoIdx > issuesIdx {
		t.Error("FetchRepoInfo must be called BEFORE ListIssues in CollectRepo — " +
			"repo_info provides commit count metadata needed for large-repo " +
			"detection before the heavy collection phases begin")
	}

	// Repo info must also appear before ListPullRequests.
	prsIdx := strings.Index(fnBody, "ListPullRequests")
	if prsIdx < 0 {
		t.Fatal("cannot find ListPullRequests in CollectRepo")
	}
	if repoInfoIdx > prsIdx {
		t.Error("FetchRepoInfo must be called BEFORE ListPullRequests")
	}

	// Repo info must appear before ListContributors too (it's the very first phase now).
	contribIdx := strings.Index(fnBody, "ListContributors")
	if contribIdx < 0 {
		t.Fatal("cannot find ListContributors in CollectRepo")
	}
	if repoInfoIdx > contribIdx {
		t.Error("FetchRepoInfo must be called BEFORE ListContributors — " +
			"metadata collection is now Phase 0, contributors is Phase 1")
	}
}

// TestRepoInfoProcessedBeforeContributors verifies the processing order has
// repo_info BEFORE contributors. This ensures metadata counts survive even
// if processing is interrupted (crash/restart). Repo_info has no FK deps
// on any other entity — it's safe to process first.
func TestRepoInfoProcessedBeforeContributors(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the entityTypes slice in ProcessRepo.
	idx := strings.Index(code, "entityTypes := []string{")
	if idx < 0 {
		t.Fatal("cannot find entityTypes slice in staged.go")
	}
	slice := code[idx : idx+500]

	repoInfoIdx := strings.Index(slice, "EntityRepoInfo")
	contribIdx := strings.Index(slice, "EntityContributor")
	if repoInfoIdx < 0 || contribIdx < 0 {
		t.Fatal("cannot find EntityRepoInfo or EntityContributor in entityTypes")
	}
	if repoInfoIdx > contribIdx {
		t.Error("EntityRepoInfo must be processed BEFORE EntityContributor — " +
			"metadata counts need to survive interrupted processing so the " +
			"monitor shows correct gathered vs metadata columns")
	}
}

// TestCollectResultHasCommitCount verifies CollectResult exposes the commit
// count from repo_info for large-repo detection.
func TestCollectResultHasCommitCount(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "CommitCount") {
		t.Error("CollectResult must include CommitCount from repo_info " +
			"for large-repo detection (>10K commits triggers parallel collection)")
	}
}
