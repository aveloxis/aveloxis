// Package db provides the database storage interface and PostgreSQL implementation.
package db

import (
	"context"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Store is the interface for persisting collected data. All methods use
// upsert semantics (INSERT ... ON CONFLICT UPDATE) so re-collection is safe.
type Store interface {
	// Close releases database resources.
	Close()

	// Migrate runs schema migrations to bring the database up to date.
	Migrate(ctx context.Context) error

	// Repos
	UpsertRepo(ctx context.Context, r *model.Repo) (repoID int64, err error)

	// Issues
	UpsertIssue(ctx context.Context, issue *model.Issue) (issueID int64, err error)
	UpsertIssueLabels(ctx context.Context, issueID, repoID int64, labels []model.IssueLabel) error
	UpsertIssueAssignees(ctx context.Context, issueID, repoID int64, assignees []model.IssueAssignee) error

	// Pull Requests
	UpsertPullRequest(ctx context.Context, pr *model.PullRequest) (prID int64, err error)
	UpsertPRLabels(ctx context.Context, prID, repoID int64, labels []model.PullRequestLabel) error
	UpsertPRAssignees(ctx context.Context, prID, repoID int64, assignees []model.PullRequestAssignee) error
	UpsertPRReviewers(ctx context.Context, prID, repoID int64, reviewers []model.PullRequestReviewer) error
	UpsertPRReview(ctx context.Context, review *model.PullRequestReview) error
	UpsertPRCommit(ctx context.Context, commit *model.PullRequestCommit) error
	UpsertPRFile(ctx context.Context, file *model.PullRequestFile) error
	UpsertPRMeta(ctx context.Context, meta *model.PullRequestMeta) (metaID int64, err error)
	UpsertPRRepo(ctx context.Context, repo *model.PullRequestRepo) error

	// Events
	UpsertIssueEvent(ctx context.Context, event *model.IssueEvent) error
	UpsertPREvent(ctx context.Context, event *model.PullRequestEvent) error

	// Messages
	UpsertMessage(ctx context.Context, msg *model.Message) (msgID int64, err error)
	UpsertIssueMessageRef(ctx context.Context, ref *model.IssueMessageRef) error
	UpsertPRMessageRef(ctx context.Context, ref *model.PullRequestMessageRef) error
	UpsertReviewComment(ctx context.Context, comment *model.ReviewComment) error

	// Releases
	UpsertRelease(ctx context.Context, release *model.Release) error

	// Contributors
	UpsertContributor(ctx context.Context, contrib *model.Contributor) error

	// Commits (facade/git)
	UpsertCommit(ctx context.Context, commit *model.Commit) error
	UpsertCommitMessage(ctx context.Context, msg *model.CommitMessage) error
	InsertCommitParent(ctx context.Context, repoID int64, commitHash, parentHash string) error

	// Repo metadata
	InsertRepoInfo(ctx context.Context, info *model.RepoInfo) error
	UpsertRepoClone(ctx context.Context, clone *model.RepoClone) error

	// Collection status
	GetCollectionStatus(ctx context.Context, repoID int64) (*CollectionState, error)
	UpdateCollectionStatus(ctx context.Context, state *CollectionState) error

	// Batch convenience — upserts a slice of messages + their refs in one transaction.
	UpsertMessageBatch(ctx context.Context, msgs []platform.MessageWithRef) error
	UpsertReviewCommentBatch(ctx context.Context, comments []platform.ReviewCommentWithRef) error
}

// CollectionState tracks per-phase status for a repo.
type CollectionState struct {
	RepoID                    int64
	CoreStatus                string
	CoreTaskID                string
	CoreDataLastCollected     *string // RFC3339 timestamp or nil
	CoreWeight                *int64
	SecondaryStatus           string
	SecondaryTaskID           string
	SecondaryDataLastCollected *string
	SecondaryWeight           *int64
	FacadeStatus              string
	FacadeTaskID              string
	FacadeDataLastCollected   *string
	FacadeWeight              *int64
	EventLastCollected        *string
	IssuePRSum                *int64
	CommitSum                 *int64
	MLStatus                  string
	MLTaskID                  string
	MLDataLastCollected       *string
	MLWeight                  *int64
}

// Convenience aliases used by the collector.
func (s *CollectionState) CoreLastCollected() *string   { return s.CoreDataLastCollected }
func (s *CollectionState) SetCoreLastCollected(v *string) { s.CoreDataLastCollected = v }
