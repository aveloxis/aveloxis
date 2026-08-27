// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.26.5 source-contract pins for identity capture on the
// assignment-class rows (2026-07-09 audit: issue_assignees,
// pull_request_assignees, pull_request_reviewers, pull_request_meta
// all had cntrb_id = 0% populated since inception; issues.closed_by_id
// 0.015%). The GraphQL responses already carried login/name/email —
// the mappings discarded them at the model layer.

import (
	"os"
	"strings"
	"testing"
)

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestAssignmentModelsCarryIdentity(t *testing.T) {
	issue := mustRead(t, "../model/issue.go")
	pr := mustRead(t, "../model/pullrequest.go")

	for _, tc := range []struct{ src, structName string }{
		{issue, "type IssueAssignee struct"},
		{pr, "type PullRequestAssignee struct"},
		{pr, "type PullRequestReviewer struct"},
	} {
		idx := strings.Index(tc.src, tc.structName)
		if idx < 0 {
			t.Fatalf("cannot find %s", tc.structName)
		}
		body := tc.src[idx:]
		if end := strings.Index(body, "}"); end > 0 {
			body = body[:end]
		}
		flat := strings.Join(strings.Fields(body), " ")
		if !strings.Contains(flat, "UserRef") {
			t.Errorf("%s must carry a UserRef — the identity the API already "+
				"delivers; without it the processor cannot resolve cntrb_id", tc.structName)
		}
		if !strings.Contains(flat, "ContributorID *string") {
			t.Errorf("%s must carry ContributorID *string (resolved cntrb_id UUID), "+
				"not the never-written int64 vestige", tc.structName)
		}
	}

	idx := strings.Index(pr, "type PullRequestMeta struct")
	body := pr[idx:]
	body = body[:strings.Index(body, "}")]
	if !strings.Contains(body, "AuthorRef") {
		t.Error("PullRequestMeta must carry AuthorRef (the head/base ref owner) — " +
			"operator decision 2026-07-09: populate pr_meta.cntrb_id")
	}
}

func TestProcessorResolvesAssignmentIdentities(t *testing.T) {
	code := mustRead(t, "staged.go")
	for _, needle := range []string{
		// issue assignees + PR assignees + PR reviewers resolved in place
		"env.Assignees[i].ContributorID = p.resolveUser",
		"env.Reviewers[i].ContributorID = p.resolveUser",
		// head/base meta owner
		"env.MetaHead.AuthorID = p.resolveUser",
		"env.MetaBase.AuthorID = p.resolveUser",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("staged.go processor must contain %q — the resolveUser contract "+
				"that gives reporter/author their 99%%+ fill", needle)
		}
	}
}

func TestProcessRepoDerivesClosedBy(t *testing.T) {
	code := mustRead(t, "staged.go")
	if !strings.Contains(code, "DeriveIssueClosedByFromEvents(") {
		t.Error("ProcessRepo must invoke DeriveIssueClosedByFromEvents — closed_by is " +
			"structurally absent from every list endpoint (REST list has no closed_by; " +
			"GraphQL Issue has no closedBy field); the closer is derived from the " +
			"issue's latest 'closed' event, platform- and mode-agnostic")
	}
}

func TestMappingsPopulateAssignmentUserRefs(t *testing.T) {
	ghClient := mustRead(t, "../platform/github/client.go")
	ghListing := mustRead(t, "../platform/github/graphql_listing.go")
	ghBatch := mustRead(t, "../platform/github/graphql_pr_batch.go")
	glClient := mustRead(t, "../platform/gitlab/client.go")
	// v0.28.15: the GitLab PR assignee/reviewer mappings moved to mr_map.go
	// (one fetched MR feeds ListPRAssignees/ListPRReviewers AND FetchPRBatch).
	glMap := mustRead(t, "../platform/gitlab/mr_map.go")

	type pin struct{ src, name, needle string }
	for _, p := range []pin{
		{ghClient, "github REST ListIssueAssignees", "func (c *Client) ListIssueAssignees("},
		{ghClient, "github REST ListPRAssignees", "func (c *Client) ListPRAssignees("},
		{ghClient, "github REST ListPRReviewers", "func (c *Client) ListPRReviewers("},
		{glClient, "gitlab ListIssueAssignees", "func (c *Client) ListIssueAssignees("},
		{glMap, "gitlab mrAssignees (ListPRAssignees + FetchPRBatch)", "func mrAssignees("},
		{glMap, "gitlab mrReviewers (ListPRReviewers + FetchPRBatch)", "func mrReviewers("},
	} {
		idx := strings.Index(p.src, p.needle)
		if idx < 0 {
			t.Fatalf("cannot find %s", p.name)
		}
		body := p.src[idx:]
		if end := strings.Index(body[1:], "\nfunc "); end > 0 {
			body = body[:end+1]
		}
		if !strings.Contains(body, "UserRef:") {
			t.Errorf("%s must populate UserRef — the user object is already in the "+
				"payload and was being discarded", p.name)
		}
	}

	// GraphQL sides: the selections already fetch the identity; the
	// mappings must stop discarding it.
	if n := strings.Count(ghListing, "UserRef:"); n < 2 {
		t.Errorf("graphql_listing.go issue-assignee mappings (listing + paginate) must "+
			"populate UserRef, found %d", n)
	}
	for _, needle := range []string{
		"UserRef:", // batch assignees + reviewers + paginate
	} {
		if !strings.Contains(ghBatch, needle) {
			t.Errorf("graphql_pr_batch.go must contain %q", needle)
		}
	}
	// Meta owner ref, synthesized from the repo owner alongside the label.
	if !strings.Contains(ghBatch, "MetaHead.AuthorRef") && !strings.Contains(ghBatch, "AuthorRef: ") {
		t.Error("graphql_pr_batch.go must populate the head/base meta AuthorRef " +
			"(owner login) — operator decision: pr_meta.cntrb_id is high-value")
	}
}
