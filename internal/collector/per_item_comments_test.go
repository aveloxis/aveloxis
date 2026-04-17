package collector

import (
	"os"
	"strings"
	"testing"
)

// Background
// ==========
// Main-path `StagedCollector.collectMessages` uses repo-wide, since-filtered
// comment endpoints (ListIssueComments / ListReviewComments). That works for
// the first-ever collection (since=zero) but on incremental cycles it only
// picks up comments within the last `days_until_recollect` window. Two
// downstream paths need per-item comment fetch to stay complete:
//
//  1. Gap fill (fillIssueGaps/fillPRGaps) backfills *historical* missing
//     issues/PRs whose comments are outside any since window. Without a
//     per-item fetch here, gap-filled items land in the DB with parent rows
//     but zero comments, forever.
//
//  2. Open-item refresh (refreshIssues/refreshPRs) is a safety net for new
//     comments on items that were already collected. If a prior cycle's
//     repo-wide collectMessages failed (rate limit, transient error, flush
//     bug), those comments are lost unless the refresh re-fetches them.
//
// These source-contract tests enforce that each of the four functions
// actually calls the per-item comment APIs and stages messages.

// TestFillIssueGapsFetchesIssueComments — gap-filled issues must have their
// comments re-fetched via a per-item endpoint and staged as EntityMessage.
func TestFillIssueGapsFetchesIssueComments(t *testing.T) {
	body := extractFuncBody(t, "gap_fill.go", "func (gf *GapFiller) fillIssueGaps(")
	if !strings.Contains(body, "ListCommentsForIssue") {
		t.Error("fillIssueGaps must call client.ListCommentsForIssue(ctx, owner, repo, num) " +
			"for every gap-filled issue — otherwise historical issues backfilled via gap " +
			"fill land with zero comments because their age is outside any incremental " +
			"since window in the main-path collectMessages.")
	}
	if !strings.Contains(body, "EntityMessage") {
		t.Error("fillIssueGaps must stage fetched issue comments as EntityMessage " +
			"so the same Processor branch (case EntityMessage in staged.go) writes them " +
			"into aveloxis_data.messages with the correct issue_message_ref row.")
	}
}

// TestFillPRGapsFetchesPRComments — gap-filled PRs must have both their
// conversation comments and their inline review comments re-fetched.
func TestFillPRGapsFetchesPRComments(t *testing.T) {
	body := extractFuncBody(t, "gap_fill.go", "func (gf *GapFiller) fillPRGaps(")
	if !strings.Contains(body, "ListCommentsForPR") {
		t.Error("fillPRGaps must call client.ListCommentsForPR(ctx, owner, repo, num) " +
			"for every gap-filled PR — this covers the conversation tab (the /issues/{n}/comments " +
			"endpoint on GitHub; /merge_requests/{iid}/notes on GitLab). Without it, " +
			"gap-filled PR rows land with no conversation comments.")
	}
	if !strings.Contains(body, "ListReviewCommentsForPR") {
		t.Error("fillPRGaps must call client.ListReviewCommentsForPR(ctx, owner, repo, num) " +
			"for every gap-filled PR — this covers inline code review comments " +
			"(/pulls/{n}/comments on GitHub; discussions with position on GitLab). " +
			"Without it, review comments on historical PRs are permanently missing.")
	}
	if !strings.Contains(body, "EntityMessage") {
		t.Error("fillPRGaps must stage PR conversation comments as EntityMessage")
	}
	if !strings.Contains(body, "EntityReviewComment") {
		t.Error("fillPRGaps must stage inline review comments as EntityReviewComment")
	}
}

// TestRefreshIssuesFetchesIssueComments — open-item refresh must also fetch
// per-issue comments as a safety net against prior-cycle collectMessages
// failures (rate limit, transient error, or the pre-v0.16.11 flush bug).
func TestRefreshIssuesFetchesIssueComments(t *testing.T) {
	body := extractFuncBody(t, "refresh_open.go", "func (r *OpenItemRefresher) refreshIssues(")
	if !strings.Contains(body, "ListCommentsForIssue") {
		t.Error("refreshIssues must call client.ListCommentsForIssue for every open issue — " +
			"this is the safety net that catches comments missed when a prior cycle's " +
			"repo-wide collectMessages failed. Without it, a single broken cycle can " +
			"permanently drop comments on still-open items.")
	}
	if !strings.Contains(body, "EntityMessage") {
		t.Error("refreshIssues must stage fetched issue comments as EntityMessage")
	}
}

// TestRefreshPRsFetchesPRComments — open-item refresh for PRs must fetch
// both conversation comments and inline review comments.
func TestRefreshPRsFetchesPRComments(t *testing.T) {
	body := extractFuncBody(t, "refresh_open.go", "func (r *OpenItemRefresher) refreshPRs(")
	if !strings.Contains(body, "ListCommentsForPR") {
		t.Error("refreshPRs must call client.ListCommentsForPR for every open PR — " +
			"same safety-net role as refreshIssues but for the PR conversation tab.")
	}
	if !strings.Contains(body, "ListReviewCommentsForPR") {
		t.Error("refreshPRs must call client.ListReviewCommentsForPR for every open PR — " +
			"inline review comments can appear well after a PR is opened and the prior " +
			"cycle's repo-wide ListReviewComments may have missed them.")
	}
	if !strings.Contains(body, "EntityMessage") {
		t.Error("refreshPRs must stage PR conversation comments as EntityMessage")
	}
	if !strings.Contains(body, "EntityReviewComment") {
		t.Error("refreshPRs must stage inline review comments as EntityReviewComment")
	}
}

// TestPlatformInterfaceExposesPerItemCommentMethods — source contract on the
// interface itself. Without these methods, the four functions above cannot
// compile, but naming them wrong in the interface would still compile against
// an incomplete concrete type embedding — catch the contract here too.
func TestPlatformInterfaceExposesPerItemCommentMethods(t *testing.T) {
	data, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	for _, name := range []string{
		"ListCommentsForIssue(",
		"ListCommentsForPR(",
		"ListReviewCommentsForPR(",
	} {
		if !strings.Contains(src, name) {
			t.Errorf("platform.Client (via MessageCollector) must declare %s as a per-item "+
				"comment-fetch method. Repo-wide since-filtered listings alone don't cover "+
				"gap-filled historical items or safety-net refresh on open items.", name)
		}
	}
}
