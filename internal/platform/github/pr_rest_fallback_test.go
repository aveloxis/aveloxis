// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"os"
	"strings"
	"testing"
)

// v0.20.20 (Fix L): when fetchPRBatchWithSubdivide narrows
// down to a single PR that STILL fails with a transient
// classification, fall back to a per-PR REST fetch instead of
// bubbling. Production diagnostic on 2026-05-13 traced the
// dominant failure pattern to PRs with 60K+ character bodies
// — GitHub's GraphQL gateway has a strict 10-second internal
// timeout, and assembling a tree with a massive body in
// memory often exceeds it, returning HTTP 502. The REST API
// streams list results sequentially and isn't subject to the
// same aggregation timeout.
//
// Fallback granularity: only at size 1 (one PR). At that
// point subdivision can go no further. The user-observed
// pattern is that a single "expensive" PR in a 10-PR batch
// trips the whole batch; subdivision isolates it; then REST
// completes that one PR. Higher-size sub-batches keep using
// GraphQL.

func TestFetchPRBatchHasRESTFallbackHelper(t *testing.T) {
	src, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	// The fallback function name and the call site from
	// fetchPRBatchWithSubdivide must both exist. We pin both
	// so a future refactor can't silently break the size-1
	// recovery path.
	if !strings.Contains(body, "fetchPRBatchOneREST") {
		t.Error("graphql_pr_batch.go must define fetchPRBatchOneREST — the REST-per-PR fallback that runs when GraphQL subdivision can't reduce further. Without it, single PRs with 60K+ character bodies will keep failing the GraphQL gateway's 10-second timeout indefinitely.")
	}
	// The subdivide function must invoke the fallback at
	// size 1. Anchor on the function-definition line so we
	// don't false-match earlier doc-comment references to
	// the function name.
	subdivIdx := strings.Index(body, "func (c *Client) fetchPRBatchWithSubdivide(")
	if subdivIdx < 0 {
		t.Fatal("fetchPRBatchWithSubdivide definition not found")
	}
	fnTail := body[subdivIdx:]
	endRel := strings.Index(fnTail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of fetchPRBatchWithSubdivide")
	}
	fnBody := fnTail[:1+endRel]
	if !strings.Contains(fnBody, "fetchPRBatchOneREST") {
		t.Error("fetchPRBatchWithSubdivide must call fetchPRBatchOneREST at size 1 — this is the whole point of Fix L. Calling it elsewhere wastes REST budget; not calling it leaves single huge-body PRs unrecoverable.")
	}
}

func TestFetchPRBatchRESTFallbackGatedOnSize1(t *testing.T) {
	src, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	// We don't want the REST fallback to fire mid-recursion
	// (size > 1) — that would burn ~8 REST calls per PR for
	// failures the subdivision should have handled itself.
	// Pin via the requirement that the fallback call site sits
	// inside an `if len(numbers) <= 1` (or equivalent) guard.
	// Pull the body of fetchPRBatchWithSubdivide and verify
	// the call to fetchPRBatchOneREST is positionally INSIDE
	// the `if len(numbers) <= 1` block — i.e., the size
	// guard appears textually before the call within the
	// function. Anchoring on the function definition (not the
	// first stray substring) so doc-comment mentions don't
	// throw us off.
	subdivIdx := strings.Index(body, "func (c *Client) fetchPRBatchWithSubdivide(")
	if subdivIdx < 0 {
		t.Skip("fetchPRBatchWithSubdivide not yet defined")
	}
	fnTail := body[subdivIdx:]
	endRel := strings.Index(fnTail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of fetchPRBatchWithSubdivide")
	}
	fnBody := fnTail[:1+endRel]

	callIdx := strings.Index(fnBody, "c.fetchPRBatchOneREST(")
	if callIdx < 0 {
		t.Skip("fetchPRBatchOneREST not yet called from fetchPRBatchWithSubdivide; helper-presence test covers this")
	}
	guardIdx := strings.Index(fnBody, "if len(numbers) <= 1")
	if guardIdx < 0 || guardIdx >= callIdx {
		t.Error("fetchPRBatchOneREST call must sit inside an `if len(numbers) <= 1` guard — the bigger-than-1 path is supposed to subdivide further, not burn REST budget. Reorder so the guard appears textually before the call.")
	}
}

func TestFetchPRBatchRESTFallbackFetchesAllChildren(t *testing.T) {
	src, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)

	idx := strings.Index(body, "func (c *Client) fetchPRBatchOneREST(")
	if idx < 0 {
		t.Skip("fetchPRBatchOneREST not yet defined")
	}
	fnTail := body[idx:]
	endRel := strings.Index(fnTail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of fetchPRBatchOneREST")
	}
	fnBody := fnTail[:1+endRel]

	// The REST fallback must populate every StagedPR field
	// that the GraphQL path populates, otherwise the resulting
	// envelope is missing data downstream (labels, reviewers,
	// review bodies, commits, files, head/base meta).
	required := []string{
		"FetchPRByNumber",
		"ListPRLabels",
		"ListPRAssignees",
		"ListPRReviewers",
		"ListPRReviews",
		"ListPRCommits",
		"ListPRFiles",
		"FetchPRMeta",
		"FetchPRRepos",
		// Conversation comments: in full-GraphQL mode the
		// collector's collectMessages SKIPS /issues/comments
		// entirely, so the inline-via-GraphQL comments are
		// the only conversation source. REST fallback must
		// populate StagedPR.Comments so the data isn't lost.
		"ListCommentsForPR",
	}
	for _, m := range required {
		if !strings.Contains(fnBody, m) {
			t.Errorf("fetchPRBatchOneREST must call %s to populate the same StagedPR fields the GraphQL path delivers. Without this, the REST-rescued PR would be missing children downstream.", m)
		}
	}
}
