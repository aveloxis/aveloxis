package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestRefreshGitHubOrgBridgesToUserRepos pins that the legacy org-refresh
// path (operating on aveloxis_data.repo_groups via GetOrgRepoGroups) ALSO
// links every discovered repo into aveloxis_ops.user_repos for any
// user_group tracking the same org_url. Without this bridge, repos added
// via `aveloxis add-repo <orgURL>` (legacy repo_groups path) or
// discovered by the periodic refreshGitHubOrg ticker landed in the
// catalog but never reached user_repos — drift the operator observed
// clustering on forks of bioconductor/ohdsi/genepattern/kiharalab repos
// (2026-05-07 diagnostic).
//
// The fix uses GetUserGroupIDsForOrgURL to map the legacy
// repo_groups.rg_website to any matching user_org_requests.org_url
// rows, then calls AddRepoToGroupByID for every repo (including forks
// — the GitHub API call uses ?type=all which already includes them).
func TestRefreshGitHubOrgBridgesToUserRepos(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnStart := strings.Index(src, "func (s *Scheduler) refreshGitHubOrg(")
	if fnStart < 0 {
		t.Fatal("cannot find refreshGitHubOrg in scheduler.go")
	}
	// Function body extends to the next top-level `func ` after fnStart.
	// Find the end by locating the next `\nfunc ` after fnStart.
	rest := src[fnStart:]
	nextFn := strings.Index(rest[1:], "\nfunc ")
	if nextFn < 0 {
		t.Fatal("cannot find end of refreshGitHubOrg function")
	}
	body := rest[:nextFn+1]

	if !strings.Contains(body, "GetUserGroupIDsForOrgURL") {
		t.Error("refreshGitHubOrg must call GetUserGroupIDsForOrgURL to find " +
			"user_groups tracking this org. Without this bridge, repos discovered " +
			"by the legacy refresh ticker land in the catalog but never reach " +
			"user_repos — the fork-clustering drift the operator diagnosed " +
			"on 2026-05-07.")
	}
	if !strings.Contains(body, "AddRepoToGroupByID") {
		t.Error("refreshGitHubOrg must call AddRepoToGroupByID for every repo " +
			"(including forks) so user_repos linkage stays in sync with the catalog.")
	}
}

// TestRefreshGitLabGroupBridgesToUserRepos — same invariant for GitLab.
func TestRefreshGitLabGroupBridgesToUserRepos(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnStart := strings.Index(src, "func (s *Scheduler) refreshGitLabGroup(")
	if fnStart < 0 {
		t.Fatal("cannot find refreshGitLabGroup in scheduler.go")
	}
	rest := src[fnStart:]
	nextFn := strings.Index(rest[1:], "\nfunc ")
	if nextFn < 0 {
		t.Fatal("cannot find end of refreshGitLabGroup function")
	}
	body := rest[:nextFn+1]

	if !strings.Contains(body, "GetUserGroupIDsForOrgURL") {
		t.Error("refreshGitLabGroup must call GetUserGroupIDsForOrgURL to find " +
			"user_groups tracking this org. Mirrors the GitHub fix for symmetry.")
	}
	if !strings.Contains(body, "AddRepoToGroupByID") {
		t.Error("refreshGitLabGroup must call AddRepoToGroupByID for every repo.")
	}
}

// TestRefreshGitHubOrgLinksExistingReposNotJustNew pins that the
// AddRepoToGroupByID call sits OUTSIDE the "if existing > 0 { continue }"
// skip branch — i.e., existing repos get the linkage step too, not just
// newly-discovered ones. This makes the periodic refresh self-healing
// for any drift accumulated before the bridge was added.
func TestRefreshGitHubOrgLinksExistingReposNotJustNew(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnStart := strings.Index(src, "func (s *Scheduler) refreshGitHubOrg(")
	if fnStart < 0 {
		t.Fatal("cannot find refreshGitHubOrg in scheduler.go")
	}
	rest := src[fnStart:]
	nextFn := strings.Index(rest[1:], "\nfunc ")
	body := rest[:nextFn+1]

	// The legacy code had `if existing > 0 { continue }` which short-circuits
	// past the linkage step. The fix must NOT have a `continue` that skips
	// AddRepoToGroupByID for existing repos. Easiest invariant to pin: the
	// call to AddRepoToGroupByID must appear AFTER the per-item FindRepoByURL
	// in the same iteration scope (not gated behind a continue).
	addIdx := strings.Index(body, "AddRepoToGroupByID")
	if addIdx < 0 {
		t.Fatal("AddRepoToGroupByID call missing — covered by TestRefreshGitHubOrgBridgesToUserRepos")
	}
	// Pin the absence of the legacy "continue past the rest of the iteration"
	// pattern between FindRepoByURL and AddRepoToGroupByID. The legacy code
	// was: existing, _ := FindRepoByURL(...); if existing > 0 { continue }.
	// The fixed code keeps repoID and falls through to the link step.
	findIdx := strings.Index(body, "FindRepoByURL")
	if findIdx < 0 || findIdx >= addIdx {
		t.Error("FindRepoByURL must precede AddRepoToGroupByID inside refreshGitHubOrg " +
			"so the per-item flow is: lookup → (create if missing) → link to user_repos.")
	}
	// Specifically: the substring between FindRepoByURL and AddRepoToGroupByID
	// must NOT contain `continue` followed only by closing braces (which would
	// indicate the linkage step is unreachable for existing repos).
	between := body[findIdx:addIdx]
	if strings.Contains(between, "if existing > 0 {\n\t\t\t\tcontinue\n\t\t\t}") {
		t.Error("refreshGitHubOrg still has the legacy 'if existing > 0 { continue }' " +
			"that skips AddRepoToGroupByID for repos already in the catalog. " +
			"Existing repos must be linked too, otherwise pre-bridge drift " +
			"never heals.")
	}
}
