// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.26.4: closes the four REST-vs-GraphQL parity gaps surfaced by the
// 2026-07-09 data-test column-fill diff (REST released-side vs GraphQL
// new-side). All four were dark on production too — pre-existing gaps
// in the GraphQL path, not regressions:
//
//   pull_requests.pr_diff_url          (REST: diff_url; GraphQL: none — synthesize html+".diff")
//   pull_request_meta.meta_label       (REST: "owner:branch"; GraphQL: synthesize from repo owner + ref)
//   pull_request_commits.pr_cmt_node_id (GraphQL Commit has `id` — was never selected)
//   issue_assignees.platform_node_id   (GraphQL User has `id` — was never selected; REST node_id IS
//                                       the GraphQL global ID, same string)
//
// NOT closable and deliberately absent here: platform_label_id (GraphQL
// Label exposes no databaseId — the two documented data-test entries).

import (
	"os"
	"strings"
	"testing"
)

func TestDiffURLSynthesis(t *testing.T) {
	if got := diffURL("https://github.com/o/r/pull/7"); got != "https://github.com/o/r/pull/7.diff" {
		t.Errorf("diffURL: got %q", got)
	}
	if got := diffURL(""); got != "" {
		t.Errorf("diffURL must map empty to empty (no bare \".diff\"), got %q", got)
	}
}

func TestOwnerFromNameWithOwner(t *testing.T) {
	if got := ownerFromNameWithOwner("augurlabs/augur"); got != "augurlabs" {
		t.Errorf("got %q", got)
	}
	if got := ownerFromNameWithOwner("noslash"); got != "" {
		t.Errorf("malformed nameWithOwner must yield empty owner, got %q", got)
	}
	if got := ownerFromNameWithOwner(""); got != "" {
		t.Errorf("empty must yield empty, got %q", got)
	}
}

func TestGraphQLQueriesSelectParityFields(t *testing.T) {
	batch, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	listing, err := os.ReadFile("graphql_listing.go")
	if err != nil {
		t.Fatal(err)
	}

	// Commit node id: both the main batch fragment and the oversized-
	// commits pagination query must select `id` on the commit object.
	if n := strings.Count(string(batch), "id oid message committedDate"); n < 2 {
		t.Errorf("expected the commit selections (main fragment + pagination) to "+
			"select `id` before oid, found %d occurrences of the id-first form", n)
	}

	// Assignee node id: the issue listing fragment and the assignee
	// pagination helper must select `id` on the User nodes.
	if n := strings.Count(string(listing), "nodes { id databaseId login avatarUrl url name email }"); n < 2 {
		t.Errorf("expected the issue-assignee selections (listing + paginate) to "+
			"select `id`, found %d occurrences", n)
	}
}

func TestGraphQLMappingsPopulateParityFields(t *testing.T) {
	batch, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	listing, err := os.ReadFile("graphql_listing.go")
	if err != nil {
		t.Fatal(err)
	}
	b, l := string(batch), string(listing)

	if !strings.Contains(b, "c.Commit.ID") {
		t.Error("PR-commit mapping must populate NodeID from c.Commit.ID (pr_cmt_node_id)")
	}
	if !strings.Contains(b, "DiffURL:") || !strings.Contains(l, "DiffURL:") {
		t.Error("both PR mappings (batch envelope + listing) must synthesize DiffURL — " +
			"GraphQL has no diff_url field; REST parity requires html_url + \".diff\"")
	}
	if !strings.Contains(b, "ownerFromNameWithOwner(") {
		t.Error("head/base meta mapping must synthesize Label (\"owner:branch\") via " +
			"ownerFromNameWithOwner — meta_label was dark on the GraphQL path")
	}
	if !strings.Contains(l, "PlatformNodeID: a.ID") {
		t.Error("issue-assignee mappings must populate PlatformNodeID from the GraphQL " +
			"User id — REST's node_id IS the GraphQL global ID (same string)")
	}
}
