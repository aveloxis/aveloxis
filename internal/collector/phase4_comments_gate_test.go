package collector

import (
	"os"
	"strings"
	"testing"
)

// TestPhase4CommentsGateInStagedCollector — source-contract test for the
// phase 4 redundancy gate.
//
// When pr_child_mode=graphql AND listing_mode=graphql on GitHub, the
// PR conversation comments and inline review comments arrive inline via
// FetchPRBatch, and issue conversation comments arrive inline via
// ListIssuesAndPRs. The two repo-wide REST iterators that used to fetch
// these (/issues/comments and /pulls/comments) become pure duplicate
// work. Phase 4 skips collectMessages in that configuration.
//
// The gate is implemented by a `fullGraphQLMode` helper that gatekeeps
// collectMessages. If a future refactor removes or bypasses it, every
// full-GraphQL collection cycle will double-fetch (and double-cost)
// every comment the GraphQL path already delivered. This test pins the
// contract at the source level so the regression can't slip through.
func TestPhase4CommentsGateInStagedCollector(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "fullGraphQLMode") {
		t.Error("staged.go must define a fullGraphQLMode check that gates collectMessages — " +
			"without it, the repo-wide REST comment fetch runs in full-GraphQL mode and " +
			"duplicates data that FetchPRBatch / ListIssuesAndPRs already delivered")
	}
	if !strings.Contains(code, "sc.fullGraphQLMode()") {
		t.Error("collectMessages must call sc.fullGraphQLMode() to decide whether to skip " +
			"the repo-wide REST comment iterators")
	}
	if !strings.Contains(code, "stageInlineIssueComments") {
		t.Error("staged.go must stage the inline issue comments delivered by " +
			"ListIssuesAndPRs via stageInlineIssueComments — otherwise listing-mode graphql " +
			"silently loses issue comments when collectMessages is skipped")
	}
	if !strings.Contains(code, "s.Comments") || !strings.Contains(code, "s.ReviewComments") {
		t.Error("stagePRBatch must stage StagedPR.Comments and StagedPR.ReviewComments — " +
			"otherwise pr_child_mode=graphql silently drops every PR / review comment " +
			"when collectMessages is skipped")
	}
}

// TestPhase4StagedPRCarriesInlineComments — the platform.StagedPR envelope
// must carry the inline comments delivered by FetchPRBatch. Without these
// fields the mapper has nowhere to put comment data and the staged
// collector has nothing to stage, turning phase 4 into a silent drop of
// every PR conversation and inline review comment.
func TestPhase4StagedPRCarriesInlineComments(t *testing.T) {
	src, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "Comments       []MessageWithRef") {
		t.Error("platform.StagedPR must declare Comments []MessageWithRef for inline " +
			"PR conversation comments delivered by phase 4's GraphQL PR batch")
	}
	if !strings.Contains(code, "ReviewComments []ReviewCommentWithRef") {
		t.Error("platform.StagedPR must declare ReviewComments []ReviewCommentWithRef for " +
			"inline diff-anchored review comments delivered by phase 4's GraphQL PR batch")
	}
	if !strings.Contains(code, "IssueComments []MessageWithRef") {
		t.Error("platform.IssueAndPRBatch must declare IssueComments []MessageWithRef for " +
			"inline issue conversation comments delivered by phase 4's unified listing")
	}
}
