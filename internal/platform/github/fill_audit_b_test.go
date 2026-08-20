// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.104 — Workstream B of the 2026-08-19 fill audit: free-data
// captures on the GitHub client (fork-owner ref, repo topics, repo
// creation date). All of it is data the responses already carry (or one
// selection token away in queries we already run).
package github

import (
	"strings"
	"testing"
)

// B5 — the fork owner was decoded on BOTH rails and dropped before
// storage, leaving pull_request_repo.pr_cntrb_id at 0 of 41.2M rows.
func TestFetchPRReposCapturesOwnerRef(t *testing.T) {
	body := region(t, readSrc(t, "client.go"), "func (c *Client) FetchPRRepos")
	if strings.Count(body, "OwnerRef:") < 2 {
		t.Error("FetchPRRepos must set OwnerRef via ghUserToRef on BOTH head and base repos")
	}
	if !strings.Contains(body, "ghUserToRef(raw.Head.Repo.Owner)") ||
		!strings.Contains(body, "ghUserToRef(raw.Base.Repo.Owner)") {
		t.Error("FetchPRRepos owner refs must come from the already-decoded ghPRBranchRepo.Owner")
	}
}

func TestRepoFromGraphQLCapturesOwnerRef(t *testing.T) {
	body := region(t, readSrc(t, "graphql_pr_batch.go"), "func repoFromGraphQL")
	if !strings.Contains(body, "OwnerRef") {
		t.Error("repoFromGraphQL must map the already-selected owner{login,databaseId,__typename} into OwnerRef")
	}
}

// B7 + B8 — repositoryTopics and createdAt are one selection away in the
// repo-info query we already run every Phase 0 cycle.
func TestRepoInfoQuerySelectsTopicsAndCreatedAt(t *testing.T) {
	body := region(t, readSrc(t, "client.go"), "func repoInfoGraphQL")
	if !strings.Contains(body, "repositoryTopics(first: 20)") {
		t.Error("repoInfoGraphQL must select repositoryTopics — repo_info.keywords was bound-but-never-set")
	}
	if !strings.Contains(body, "createdAt") {
		t.Error("repoInfoGraphQL must select createdAt — repos.created_at was 0% populated")
	}
}

func TestBothRepoInfoTransportsMapKeywordsAndCreatedAt(t *testing.T) {
	src := readSrc(t, "client.go")
	if strings.Count(src, "Keywords:") < 2 {
		t.Error("both FetchRepoInfo mappings (GraphQL + REST fallback) must populate RepoInfo.Keywords (the v0.27.78 both-transports lesson)")
	}
	if strings.Count(src, "CreatedAt:") < 2 {
		t.Error("both FetchRepoInfo mappings must populate RepoInfo.CreatedAt")
	}
	types := readSrc(t, "types.go")
	if !strings.Contains(types, "`json:\"topics\"`") {
		t.Error("ghRepoInfo must decode REST `topics` for the fallback transport")
	}
	if !strings.Contains(types, "`json:\"created_at\"`") {
		t.Error("ghRepoInfo must decode REST `created_at` for the fallback transport")
	}
}
