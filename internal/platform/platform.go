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
