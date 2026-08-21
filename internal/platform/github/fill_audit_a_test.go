// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.103 — Workstream A of the 2026-08-19 fill audit: values the
// GitHub client decoded (or could select for free) but dropped before
// storage.
package github

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func readSrc(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func region(t *testing.T, src, decl string) string {
	t.Helper()
	i := strings.Index(src, decl)
	if i < 0 {
		t.Fatalf("declaration not found: %s", decl)
	}
	rest := src[i+len(decl):]
	if j := strings.Index(rest, "\nfunc "); j > 0 {
		rest = rest[:j]
	}
	return decl + rest
}

// A2 — the REST fallback dropped `archived`: fetchRepoInfoREST set no
// Status, so staged.go's `info.Status == "Archived"` was always false on
// every GraphQL→REST fallback and repos.repo_archived went silently
// false (the v0.27.50 class, reintroduced by transport asymmetry).
func TestRESTFallbackMapsArchivedStatus(t *testing.T) {
	types := readSrc(t, "types.go")
	if !strings.Contains(types, "`json:\"disabled\"`") {
		t.Error("ghRepoInfo must decode `disabled` so statusStr gets both inputs on the REST fallback")
	}
	body := region(t, readSrc(t, "client.go"), "func (c *Client) fetchRepoInfoREST")
	if !strings.Contains(body, "statusStr(raw.Archived") {
		t.Error("fetchRepoInfoREST must set Status via statusStr(raw.Archived, raw.Disabled) — parity with the GraphQL mapping")
	}
}

// A3 — GitHub releases carried no Origin: releases.data_source was empty
// on all 1,051,111 production rows.
func TestListReleasesSetsOrigin(t *testing.T) {
	body := region(t, readSrc(t, "client.go"), "func (c *Client) ListReleases")
	if !strings.Contains(body, `DataSource: "GitHub API"`) {
		t.Error(`ListReleases must set Origin.DataSource "GitHub API" (the GitLab side's convention)`)
	}
}

// A4a — userRefFromGraphQL dropped the already-decoded node ID, leaving
// contributor_identities.node_id dark on the whole GraphQL rail (26%
// fill = REST-era rows only).
func TestUserRefFromGraphQLMapsNodeID(t *testing.T) {
	body := region(t, readSrc(t, "graphql_pr_batch.go"), "func userRefFromGraphQL")
	if !strings.Contains(body, "NodeID:") {
		t.Error("userRefFromGraphQL must map prBatchUser.ID into UserRef.NodeID (REST's ghUserToRef already does)")
	}
}

// A4b — every `... on User {` inline in the batch + listing queries must
// select `id` (the GraphQL node ID) so A4a has data to map. One token
// per fragment, zero extra request cost.
func TestUserInlinesSelectNodeID(t *testing.T) {
	re := regexp.MustCompile(`\.\.\. on User \{([^}]*)\}`)
	for _, f := range []string{"graphql_pr_batch.go", "graphql_listing.go"} {
		src := readSrc(t, f)
		for _, m := range re.FindAllStringSubmatch(src, -1) {
			fields := " " + strings.Join(strings.Fields(m[1]), " ") + " "
			if !strings.Contains(fields, " id ") {
				t.Errorf("%s: User inline %q must select `id`", f, strings.TrimSpace(m[0]))
			}
		}
	}
}

// v0.27.119 (Copilot round 12, suppressed — real): the assignee
// connections are bare `nodes { ... }` selections, NOT `... on User`
// inlines, so TestUserInlinesSelectNodeID structurally could not see
// them — and both (initial page at prNodeFragment + paginatePRAssignees)
// omitted `id`, leaving assignee-sourced identities without node_id
// despite userRefFromGraphQL promising it since v0.27.103. Pin every
// assignees connection's node selection carries `id`.
func TestAssigneeSelectionsCarryNodeID(t *testing.T) {
	src := readSrc(t, "graphql_pr_batch.go")
	re := regexp.MustCompile(`assignees\(first: \d+[^)]*\) \{\s*\n\s*nodes \{([^}]*)\}`)
	matches := re.FindAllStringSubmatch(src, -1)
	if len(matches) < 2 {
		t.Fatalf("expected >= 2 assignees selections (initial page + pagination), found %d — the regex or the query moved", len(matches))
	}
	for _, m := range matches {
		fields := " " + strings.Join(strings.Fields(m[1]), " ") + " "
		if !strings.Contains(fields, " id ") {
			t.Errorf("assignees node selection %q must select `id` — without it every assignee-sourced identity lands with empty node_id", strings.TrimSpace(m[1]))
		}
	}
}

// A4c — FetchIssueClosers selected __typename but decoded neither it nor
// `id`, so every closer identity landed with empty user_type/node_id.
func TestIssueClosersCaptureTypeAndNodeID(t *testing.T) {
	src := readSrc(t, "issue_closers.go")
	if !strings.Contains(src, "`json:\"__typename\"`") {
		t.Error("issue closer actor decode must capture __typename → UserRef.Type")
	}
	if !strings.Contains(src, "`json:\"id\"`") {
		t.Error("issue closer actor decode must capture id → UserRef.NodeID (select it in the User inline)")
	}
	body := region(t, src, "func (c *Client) FetchIssueClosers")
	if !strings.Contains(body, "NodeID:") || !strings.Contains(body, "Type:") {
		t.Error("FetchIssueClosers must map Type and NodeID onto the returned UserRef")
	}
}

// v0.27.111 (round 6, wrongly-suppressed finding): every Bot inline must
// select `id` (and databaseId) — review/comment authors and issue
// closers are VERY often bots (dependabot et al.), and without these
// selections they stayed login-only refs with empty node_id, resolving
// through the login fallback instead of the deterministic platform-ID
// path.
func TestBotInlinesSelectNodeID(t *testing.T) {
	re := regexp.MustCompile(`\.\.\. on Bot \{([^}]*)\}`)
	for _, f := range []string{"graphql_pr_batch.go", "graphql_listing.go", "issue_closers.go"} {
		src := readSrc(t, f)
		hits := re.FindAllStringSubmatch(src, -1)
		if f != "issue_closers.go" && len(hits) < 3 {
			t.Errorf("%s: expected Bot inlines on the author fragments (found %d)", f, len(hits))
		}
		for _, m := range hits {
			fields := " " + strings.Join(strings.Fields(m[1]), " ") + " "
			if !strings.Contains(fields, " id ") || !strings.Contains(fields, " databaseId ") {
				t.Errorf("%s: Bot inline %q must select id AND databaseId", f, strings.TrimSpace(m[0]))
			}
		}
	}
}

// v0.27.112 (wrongly-suppressed finding): the reviewer PAGINATION path
// dropped UserRef while the initial-page path mapped it — reviewers past
// the first page lost contributor identity.
func TestPaginatedReviewersKeepUserRef(t *testing.T) {
	body := region(t, readSrc(t, "graphql_pr_batch.go"), "func (c *Client) paginatePRReviewers")
	if !strings.Contains(body, "userRefFromGraphQL(r.RequestedReviewer)") {
		t.Error("paginatePRReviewers must map UserRef exactly as the initial-page path does")
	}
}
