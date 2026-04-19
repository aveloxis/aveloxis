// Package platform defines the abstraction layer that both GitHub and GitLab
// implement. This is the core interface contract that ensures feature parity.
package platform

import (
	"context"
	"iter"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
)

// Client is the top-level interface every forge must implement.
// Both GitHub and GitLab provide concrete implementations.
type Client interface {
	// Platform returns the platform identifier.
	Platform() model.Platform

	// ParseRepoURL parses a repository URL into owner and repo name.
	// For GitHub: "https://github.com/owner/repo" -> ("owner", "repo")
	// For GitLab: "https://gitlab.com/group/subgroup/project" -> ("group/subgroup", "project")
	ParseRepoURL(url string) (owner, repo string, err error)

	// OnPermanentRedirect installs a callback invoked by the underlying
	// HTTP client whenever it observes a 301 or 308. Used by the scheduler
	// to detect repo renames (see internal/platform/httpclient.go).
	// Pass nil to clear a previously installed hook.
	OnPermanentRedirect(hook func(from, to string))

	// ListIssuesAndPRs enumerates every issue and PR in the repo updated
	// since the given time, in one call. Replaces two separate iterator
	// loops (ListIssues + ListPullRequests) with a single batch return.
	//
	// Pass zero time for full collection. GitHub's implementation uses
	// GraphQL (issues + pullRequests connections, cursor paginated).
	// GitLab's composes the existing REST iterators. Both produce the
	// same model types the legacy iterators produce, so callers can
	// substitute one for the other without column-level drift.
	ListIssuesAndPRs(ctx context.Context, owner, repo string, since time.Time) (*IssueAndPRBatch, error)

	// Repo metadata
	RepoCollector
	// Issues and their related data
	IssueCollector
	// Pull requests / merge requests and their related data
	PullRequestCollector
	// Events on issues and PRs/MRs
	EventCollector
	// Comments on issues, PRs/MRs, and reviews
	MessageCollector
	// Releases and tags
	ReleaseCollector
	// Contributors
	ContributorCollector
}

// RepoCollector fetches repository-level metadata.
type RepoCollector interface {
	// FetchRepoInfo returns a point-in-time metadata snapshot.
	FetchRepoInfo(ctx context.Context, owner, repo string) (*model.RepoInfo, error)

	// FetchCloneStats returns clone/traffic data (may require elevated perms).
	FetchCloneStats(ctx context.Context, owner, repo string) ([]model.RepoClone, error)
}

// IssueAndPRBatch bundles the result of a unified issue + PR enumeration
// (phase 2 of the REST→GraphQL refactor). Replaces two separate iterator
// calls (ListIssues + ListPullRequests) with a single batch return.
//
// On GitHub: populated by a pair of GraphQL queries (one per connection)
// that eliminate REST's /issues-returns-PRs double-count.
// On GitLab: populated by composing the existing REST iterators — GitLab's
// GraphQL API is too weak on merge_request fields to use here. Parity is
// preserved at the row/column level; the only observable difference is
// the call pattern inside the platform package.
//
// Phase 4 adds IssueComments: per-issue conversation comments delivered
// inline with the issue listing. Each entry carries an IssueRef so the
// staged collector can skip the repo-wide /issues/comments REST call
// when running in full-GraphQL mode. IssueComments is nil when the
// platform implementation doesn't support inline comments (GitLab REST
// composition); callers fall back to the MessageCollector interface in
// that case.
type IssueAndPRBatch struct {
	Issues        []model.Issue
	PullRequests  []model.PullRequest
	IssueComments []MessageWithRef
}

// IssueCollector fetches issues and related entities.
type IssueCollector interface {
	// ListIssues returns all issues updated since the given time.
	// Pass zero time for full collection.
	ListIssues(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.Issue, error]

	// ListIssueLabels returns labels for a specific issue.
	ListIssueLabels(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueLabel, error]

	// ListIssueAssignees returns assignees for a specific issue.
	ListIssueAssignees(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueAssignee, error]

	// FetchIssueByNumber fetches a single issue by number for targeted gap filling.
	FetchIssueByNumber(ctx context.Context, owner, repo string, number int) (*model.Issue, error)
}

// StagedPR bundles a PullRequest with every child entity the collector
// stages alongside it. Produced by FetchPRBatch, which in one call
// fetches a parent PR plus its labels, assignees, reviewers, reviews,
// commits, files, head/base meta, and head/base repositories.
//
// Shape mirrors the collector's internal stagedPR envelope so callers
// can hand results straight to the staging writer without re-shaping.
// Pointer fields (MetaHead, MetaBase, RepoHead, RepoBase) may be nil
// when the source is null — typical for merged PRs whose branches
// were deleted.
type StagedPR struct {
	PR        model.PullRequest
	Labels    []model.PullRequestLabel
	Assignees []model.PullRequestAssignee
	Reviewers []model.PullRequestReviewer
	Reviews   []model.PullRequestReview
	Commits   []model.PullRequestCommit
	Files     []model.PullRequestFile
	MetaHead  *model.PullRequestMeta
	MetaBase  *model.PullRequestMeta
	RepoHead  *model.PullRequestRepo
	RepoBase  *model.PullRequestRepo

	// Phase 4: inline comment payloads. Populated by GraphQL FetchPRBatch
	// (PR.comments and PullRequestReview.comments connections); nil when
	// the implementation doesn't fetch comments inline (GitLab REST
	// composition). Each Comments entry carries a PRRef so the staged
	// collector can write to pull_request_message_ref; each ReviewComments
	// entry carries the parent's platform review ID so msg_id / pr_review_id
	// resolution happens the same way it does on the REST path.
	Comments       []MessageWithRef
	ReviewComments []ReviewCommentWithRef
}

// PullRequestCollector fetches pull requests / merge requests and related entities.
type PullRequestCollector interface {
	// ListPullRequests returns all PRs/MRs updated since the given time.
	ListPullRequests(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequest, error]

	// ListPRLabels returns labels for a specific PR/MR.
	ListPRLabels(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestLabel, error]

	// ListPRAssignees returns assignees for a specific PR/MR.
	ListPRAssignees(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestAssignee, error]

	// ListPRReviewers returns requested reviewers for a specific PR/MR.
	ListPRReviewers(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestReviewer, error]

	// ListPRReviews returns completed reviews for a specific PR/MR.
	ListPRReviews(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestReview, error]

	// ListPRCommits returns commits in a PR/MR.
	ListPRCommits(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestCommit, error]

	// ListPRFiles returns files changed in a PR/MR.
	ListPRFiles(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestFile, error]

	// FetchPRMeta returns head and base metadata for a PR/MR.
	FetchPRMeta(ctx context.Context, owner, repo string, prNumber int) (head, base *model.PullRequestMeta, err error)

	// FetchPRRepos returns fork repo details for a PR's head and base branches.
	// Returns nil for either if the repo data is unavailable (e.g., deleted fork).
	FetchPRRepos(ctx context.Context, owner, repo string, prNumber int) (headRepo, baseRepo *model.PullRequestRepo, err error)

	// FetchPRByNumber fetches a single PR by number for targeted gap filling.
	FetchPRByNumber(ctx context.Context, owner, repo string, number int) (*model.PullRequest, error)

	// FetchPRBatch fetches a batch of PRs by number and returns each one
	// with all its children populated (labels, assignees, reviewers,
	// reviews, commits, files, head/base meta, head/base repo). The
	// implementation may split a large number list into multiple
	// underlying requests; callers pass the full list and receive the
	// flattened result. Replaces the per-PR REST child waterfall with
	// a single GraphQL call per batch on GitHub.
	//
	// Returns an empty slice (no error) for an empty input. PRs that
	// have been deleted or are inaccessible are silently skipped; the
	// returned length may be less than len(numbers).
	FetchPRBatch(ctx context.Context, owner, repo string, numbers []int) ([]StagedPR, error)
}

// EventCollector fetches timeline events on issues and PRs/MRs.
type EventCollector interface {
	// ListIssueEvents returns events for issues in the repo since the given time.
	ListIssueEvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.IssueEvent, error]

	// ListPREvents returns events for PRs/MRs in the repo since the given time.
	ListPREvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequestEvent, error]
}

// MessageCollector fetches comments/notes.
type MessageCollector interface {
	// ListIssueComments returns comments on issues since the given time.
	ListIssueComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[MessageWithRef, error]

	// ListPRComments returns comments on PRs/MRs since the given time.
	ListPRComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[MessageWithRef, error]

	// ListReviewComments returns inline review comments since the given time.
	ListReviewComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[ReviewCommentWithRef, error]

	// ListCommentsForIssue returns all comments on a single issue. Used by
	// gap fill (to backfill comments on historical issues whose age is
	// outside any since window) and by open-item refresh (as a safety net
	// against prior cycles' repo-wide collectMessages failures).
	ListCommentsForIssue(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[MessageWithRef, error]

	// ListCommentsForPR returns conversation comments for a single PR/MR.
	// On GitHub this hits the same /issues/{n}/comments endpoint as issue
	// comments (PRs are issues on GitHub); on GitLab it hits
	// /merge_requests/:iid/notes.
	ListCommentsForPR(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[MessageWithRef, error]

	// ListReviewCommentsForPR returns inline (diff-line-anchored) review
	// comments for a single PR/MR. On GitHub: /pulls/{n}/comments. On
	// GitLab: /merge_requests/:iid/discussions filtered to notes with a
	// position (diff anchor).
	ListReviewCommentsForPR(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[ReviewCommentWithRef, error]
}

// ReleaseCollector fetches releases and tags.
type ReleaseCollector interface {
	// ListReleases returns all releases. Falls back to tags if no releases exist.
	ListReleases(ctx context.Context, owner, repo string) iter.Seq2[model.Release, error]
}

// ContributorCollector fetches and enriches contributor profiles.
type ContributorCollector interface {
	// ListContributors returns contributors to the repository.
	ListContributors(ctx context.Context, owner, repo string) iter.Seq2[model.Contributor, error]

	// EnrichContributor fills in profile details for a contributor by login.
	EnrichContributor(ctx context.Context, login string) (*model.Contributor, error)
}

// MessageWithRef pairs a message with its parent reference (issue or PR).
type MessageWithRef struct {
	Message    model.Message
	IssueRef   *model.IssueMessageRef
	PRRef      *model.PullRequestMessageRef
}

// ReviewCommentWithRef pairs a review comment with its message.
type ReviewCommentWithRef struct {
	Message model.Message
	Comment model.ReviewComment
}
