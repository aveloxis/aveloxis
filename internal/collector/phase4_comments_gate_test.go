// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestPhase4CommentsGateInStagedCollector — source-contract test for the
// phase 4 partial redundancy gate.
//
// When pr_child_mode=graphql AND listing_mode=graphql on GitHub, the
// issue conversation comments arrive inline via ListIssuesAndPRs and the
// PR conversation comments arrive inline via FetchPRBatch. In that mode
// the repo-wide REST /issues/comments iterator is skipped in
// collectMessages — it would be pure duplicate work. The /pulls/comments
// iterator is deliberately NOT skipped: GitHub's GraphQL
// PullRequestReviewComment omits the side / startSide fields the REST
// schema carries, so review-comment row fidelity depends on the REST
// call continuing to run.
//
// If a future refactor removes the fullGraphQLMode gate, skips
// /pulls/comments in full-GraphQL mode, or stops staging the inline PR /
// issue comments, this test fails loudly.
func TestPhase4CommentsGateInStagedCollector(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "fullGraphQLMode") {
		t.Error("staged.go must define a fullGraphQLMode check that gates the " +
			"/issues/comments iterator in collectMessages — without it, the repo-wide " +
			"REST comment fetch runs in full-GraphQL mode and duplicates data that " +
			"FetchPRBatch / ListIssuesAndPRs already delivered")
	}
	if !strings.Contains(code, "sc.fullGraphQLMode()") {
		t.Error("collectMessages must call sc.fullGraphQLMode() to decide whether to skip " +
			"the /issues/comments iterator")
	}
	if !strings.Contains(code, "stageInlineIssueComments") {
		t.Error("staged.go must stage the inline issue comments delivered by " +
			"ListIssuesAndPRs via stageInlineIssueComments — otherwise listing-mode graphql " +
			"silently loses issue comments when /issues/comments is skipped")
	}
	if !strings.Contains(code, "s.Comments") {
		t.Error("stagePRBatch must stage StagedPR.Comments — otherwise " +
			"pr_child_mode=graphql silently drops every PR conversation comment " +
			"when /issues/comments is skipped")
	}
	// Review-inline comments (/pulls/comments) must always be fetched via REST
	// so side / startSide stay populated. The gate must NOT short-circuit that
	// iterator. Confirmed by: after the gate, sc.client.ListReviewComments is
	// unconditionally ranged.
	if !strings.Contains(code, "sc.client.ListReviewComments(ctx, owner, repo, since)") {
		t.Error("collectMessages must always range sc.client.ListReviewComments — " +
			"GraphQL's PullRequestReviewComment omits side / startSide, so phase 4 " +
			"keeps the repo-wide /pulls/comments REST call alive in all modes for " +
			"byte-for-byte REST fidelity on review_comments.pr_cmt_side / pr_cmt_start_side")
	}
}

// TestPhase4StagedPRCarriesInlineComments — the platform.StagedPR envelope
// must carry the inline PR conversation comments delivered by
// FetchPRBatch, and platform.IssueAndPRBatch must carry the inline issue
// conversation comments delivered by ListIssuesAndPRs. Without these
// fields the mappers have nowhere to put comment data and the staged
// collector has nothing to stage, turning phase 4 into a silent drop of
// every PR / issue conversation comment when /issues/comments is skipped.
func TestPhase4StagedPRCarriesInlineComments(t *testing.T) {
	src, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Use whitespace-tolerant regex: gofmt aligns struct fields in
	// columns, so adding a longer-named field later shifts the spacing
	// on every existing field. The contract is "name + type", not
	// "exact spacing".
	commentsRE := regexp.MustCompile(`\bComments\s+\[\]MessageWithRef\b`)
	if !commentsRE.MatchString(code) {
		t.Error("platform.StagedPR must declare Comments []MessageWithRef for inline " +
			"PR conversation comments delivered by phase 4's GraphQL PR batch")
	}
	issueCommentsRE := regexp.MustCompile(`\bIssueComments\s+\[\]MessageWithRef\b`)
	if !issueCommentsRE.MatchString(code) {
		t.Error("platform.IssueAndPRBatch must declare IssueComments []MessageWithRef for " +
			"inline issue conversation comments delivered by phase 4's unified listing")
	}
}

// v0.27.128: collectParallel's goroutines must merge the inline-comment
// counters — without this the v0.22.4 collectMessages diagnostic line
// always logged issue_inline_comments=0 pr_inline_comments=0 in
// parallel mode (observed on the 2026-08-21 data-test run; staging was
// never affected — a "log the effective value" bug).
func TestParallelMergesInlineCommentCounters(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"result.InlineIssueComments += localResult.InlineIssueComments",
		"result.InlinePRComments += localResult.InlinePRComments",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("collectParallel must merge %q — the collectMessages diagnostic reads these counters", needle)
		}
	}
}
