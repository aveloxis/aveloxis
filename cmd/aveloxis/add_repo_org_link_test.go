package main

import (
	"os"
	"strings"
	"testing"
)

// TestAddRepoOrgExpansionBridgesToUserRepos pins that
// `aveloxis add-repo <orgURL>` (the runAddRepo CLI path that expands an
// org URL into per-repo upserts) calls GetUserGroupIDsForOrgURL and
// AddRepoToGroupByID for every discovered repo. Without this, CLI-added
// orgs land in aveloxis_data.repos with the legacy repo_group_id set
// but never reach aveloxis_ops.user_repos for any user_group tracking
// the same org_url — the fork-clustering drift the operator diagnosed
// on 2026-05-07.
//
// Source-contract test (no DB needed): reads main.go and asserts the
// runAddRepo expansion loop contains both calls.
func TestAddRepoOrgExpansionBridgesToUserRepos(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnStart := strings.Index(src, "func runAddRepo(")
	if fnStart < 0 {
		t.Fatal("cannot find runAddRepo in main.go")
	}
	rest := src[fnStart:]
	nextFn := strings.Index(rest[1:], "\nfunc ")
	if nextFn < 0 {
		t.Fatal("cannot find end of runAddRepo function")
	}
	body := rest[:nextFn+1]

	if !strings.Contains(body, "GetUserGroupIDsForOrgURL") {
		t.Error("runAddRepo's org-expansion branch must call " +
			"GetUserGroupIDsForOrgURL(ctx, repoURL) so user_groups tracking " +
			"this org get every discovered repo (including forks) linked " +
			"into user_repos. Without this bridge, `aveloxis add-repo " +
			"<orgURL>` is a one-shot legacy-only insert.")
	}
	if !strings.Contains(body, "AddRepoToGroupByID") {
		t.Error("runAddRepo's org-expansion branch must call " +
			"AddRepoToGroupByID for every discovered repo so user_repos " +
			"linkage stays in sync with the catalog.")
	}
}
