// Package collector — staged.go implements the two-phase staged collection pipeline.
//
// At 400K repos, direct upserts create massive contention on the contributors
// table because every worker is doing concurrent contributor resolution.
// The staged approach decouples collection from persistence:
//
//	Phase 1 (Collect): Raw API responses are written to aveloxis_ops.staging
//	  as JSONB. No FK lookups, no contributor resolution, just fast inserts.
//	  Multiple workers can blast data in concurrently with zero contention.
//
//	Phase 2 (Process): A single-threaded processor drains staged rows in
//	  batches. Contributors are resolved in bulk across the batch (deduplicating
//	  by platform ID, then email, then login) before inserting into the
//	  relational schema. This eliminates the contributor table hot-spot.
//
// Child entities (labels, assignees, reviewers, files, meta) are bundled into
// their parent's staged payload via envelope types (stagedIssue, stagedPR).
// This ensures the parent DB ID is available when processing children.
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Entity type constants for the staging table.
const (
	EntityIssue         = "issue"
	EntityPullRequest   = "pull_request"
	EntityIssueEvent    = "issue_event"
	EntityPREvent       = "pr_event"
	EntityMessage       = "message"
	EntityReviewComment = "review_comment"
	EntityRelease       = "release"
	EntityContributor   = "contributor"
	EntityRepoInfo      = "repo_info"
	EntityCloneStats    = "clone_stats"
)

// Envelope types that bundle a parent entity with its children.
// These are what get JSON-serialized into the staging table.

type stagedIssue struct {
	Issue     model.Issue           `json:"issue"`
	Labels    []model.IssueLabel    `json:"labels,omitempty"`
	Assignees []model.IssueAssignee `json:"assignees,omitempty"`
}

type stagedPR struct {
	PR        model.PullRequest           `json:"pr"`
	Labels    []model.PullRequestLabel    `json:"labels,omitempty"`
	Assignees []model.PullRequestAssignee `json:"assignees,omitempty"`
	Reviewers []model.PullRequestReviewer `json:"reviewers,omitempty"`
	Reviews   []model.PullRequestReview   `json:"reviews,omitempty"`
	Commits   []model.PullRequestCommit   `json:"commits,omitempty"`
	Files     []model.PullRequestFile     `json:"files,omitempty"`
	MetaHead  *model.PullRequestMeta      `json:"meta_head,omitempty"`
	MetaBase  *model.PullRequestMeta      `json:"meta_base,omitempty"`
}

// StagedCollector writes raw API data to the staging table instead of directly
// into the relational schema. This is the fast path for high-throughput collection.
type StagedCollector struct {
	client platform.Client
	store  *db.PostgresStore
	logger *slog.Logger
	platID int16
}

// NewStagedCollector creates a staged collector.
func NewStagedCollector(client platform.Client, store *db.PostgresStore, logger *slog.Logger) *StagedCollector {
	return &StagedCollector{
		client: client,
		store:  store,
		logger: logger,
		platID: int16(client.Platform()),
	}
}

// CollectRepo stages all API data for a repo. Does NOT resolve contributors or
// write to relational tables. Call Processor.ProcessRepo() after this.
func (sc *StagedCollector) CollectRepo(ctx context.Context, repoID int64, owner, repo string, since time.Time) (*CollectResult, error) {
	result := &CollectResult{}

	// Purge any old unprocessed staging rows for this repo from a previous
	// interrupted run. Without this, stale child entities (events, messages)
	// reference parent rows (issues, PRs) that were never inserted, causing
	// massive FK constraint violations during processing.
	sc.store.PurgeStagedForRepo(ctx, repoID)

	sw := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)

	sc.logger.Info("staged collection starting",
		"platform", sc.client.Platform(),
		"owner", owner, "repo", repo, "repoID", repoID)

	_ = sc.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:     repoID,
		CoreStatus: string(StatusCollecting),
	})

	// Phase 0: Contributors.
	sc.logger.Info("collecting contributors", "owner", owner, "repo", repo)
	for contrib, err := range sc.client.ListContributors(ctx, owner, repo) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("contributors: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityContributor, contrib); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.Contributors++
	}
	sc.logger.Info("contributors staged", "count", result.Contributors)

	// Phase 1: Issues — bundle each issue with its labels and assignees.
	sc.logger.Info("collecting issues", "owner", owner, "repo", repo)
	for issue, err := range sc.client.ListIssues(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("issues: %w", err))
			break
		}

		envelope := stagedIssue{Issue: issue}
		for label, err := range sc.client.ListIssueLabels(ctx, owner, repo, issue.Number) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for assignee, err := range sc.client.ListIssueAssignees(ctx, owner, repo, issue.Number) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, assignee)
		}

		if err := sw.Stage(ctx, EntityIssue, envelope); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.Issues++
		if result.Issues%100 == 0 {
			sc.logger.Info("issues progress", "owner", owner, "repo", repo, "staged", result.Issues)
		}
	}
	sc.logger.Info("issues staged", "count", result.Issues)

	// Phase 1: Pull Requests — bundle each PR with all its children.
	sc.logger.Info("collecting pull requests", "owner", owner, "repo", repo)
	for pr, err := range sc.client.ListPullRequests(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("pull requests: %w", err))
			break
		}

		envelope := stagedPR{PR: pr}
		for label, err := range sc.client.ListPRLabels(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for a, err := range sc.client.ListPRAssignees(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, a)
		}
		for r, err := range sc.client.ListPRReviewers(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Reviewers = append(envelope.Reviewers, r)
		}
		for review, err := range sc.client.ListPRReviews(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Reviews = append(envelope.Reviews, review)
		}
		for commit, err := range sc.client.ListPRCommits(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Commits = append(envelope.Commits, commit)
		}
		for file, err := range sc.client.ListPRFiles(ctx, owner, repo, pr.Number) {
			if err != nil {
				break
			}
			envelope.Files = append(envelope.Files, file)
		}
		head, base, err := sc.client.FetchPRMeta(ctx, owner, repo, pr.Number)
		if err == nil {
			envelope.MetaHead = head
			envelope.MetaBase = base
		}

		if err := sw.Stage(ctx, EntityPullRequest, envelope); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.PullRequests++
		if result.PullRequests%100 == 0 {
			sc.logger.Info("pull requests progress", "staged", result.PullRequests)
		}
	}
	sc.logger.Info("pull requests staged", "count", result.PullRequests)

	// Phase 2: Events.
	sc.logger.Info("collecting events", "owner", owner, "repo", repo)
	for event, err := range sc.client.ListIssueEvents(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("issue events: %w", err))
			break
		}
		sw.Stage(ctx, EntityIssueEvent, event)
		result.Events++
	}
	for event, err := range sc.client.ListPREvents(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("pr events: %w", err))
			break
		}
		sw.Stage(ctx, EntityPREvent, event)
		result.Events++
	}

	sc.logger.Info("events staged", "count", result.Events)

	// Phase 2: Messages.
	sc.logger.Info("collecting messages", "owner", owner, "repo", repo)
	for msg, err := range sc.client.ListIssueComments(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("issue comments: %w", err))
			break
		}
		sw.Stage(ctx, EntityMessage, msg)
		result.Messages++
	}
	for rc, err := range sc.client.ListReviewComments(ctx, owner, repo, since) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("review comments: %w", err))
			break
		}
		sw.Stage(ctx, EntityReviewComment, rc)
		result.Messages++
	}

	sc.logger.Info("messages staged", "count", result.Messages)

	// Phase 3: Metadata.
	sc.logger.Info("collecting metadata", "owner", owner, "repo", repo)
	info, err := sc.client.FetchRepoInfo(ctx, owner, repo)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("repo info: %w", err))
	} else {
		sw.Stage(ctx, EntityRepoInfo, info)
	}

	for rel, err := range sc.client.ListReleases(ctx, owner, repo) {
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("releases: %w", err))
			break
		}
		sw.Stage(ctx, EntityRelease, rel)
		result.Releases++
	}

	clones, err := sc.client.FetchCloneStats(ctx, owner, repo)
	if err == nil {
		for _, clone := range clones {
			sw.Stage(ctx, EntityCloneStats, clone)
		}
	}

	// Final flush.
	if err := sw.Flush(ctx); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("staging flush: %w", err))
	}

	sc.logger.Info("staged collection complete",
		"repoID", repoID, "staged_issues", result.Issues,
		"staged_prs", result.PullRequests, "staged_messages", result.Messages,
		"staged_events", result.Events, "staged_releases", result.Releases)

	return result, nil
}

// Processor drains the staging table and writes to the relational schema.
// Contributor resolution happens here, in bulk, to minimize contention.
type Processor struct {
	store    *db.PostgresStore
	resolver *db.ContributorResolver
	logger   *slog.Logger
	errors   int // count of individual row processing failures
}

// NewProcessor creates a staging processor.
func NewProcessor(store *db.PostgresStore, logger *slog.Logger) *Processor {
	return &Processor{
		store:    store,
		resolver: db.NewContributorResolver(store),
		logger:   logger,
	}
}

const processBatchSize = 500

// ProcessRepo drains all staged data for a repo into the relational schema.
// Entity types are processed in dependency order: contributors first, then
// parent entities (issues, PRs), then events/messages, then metadata.
func (p *Processor) ProcessRepo(ctx context.Context, repoID int64, platID int16) error {
	p.logger.Info("processing staged data", "repo_id", repoID)

	// Order matters: contributors must exist before FK resolution.
	// Issues/PRs (with bundled children) come next.
	// Events/messages reference issues/PRs by platform ID, not DB ID, so order is less strict.
	entityTypes := []string{
		EntityContributor,
		EntityIssue,
		EntityPullRequest,
		EntityIssueEvent,
		EntityPREvent,
		EntityMessage,
		EntityReviewComment,
		EntityRelease,
		EntityRepoInfo,
		EntityCloneStats,
	}

	for _, entityType := range entityTypes {
		if err := p.store.ProcessStaged(ctx, repoID, entityType, processBatchSize, func(rows []db.StagedRow) error {
			return p.processBatch(ctx, repoID, platID, entityType, rows)
		}); err != nil {
			p.logger.Error("failed to process entity type", "type", entityType, "error", err)
			return err
		}
	}

	// Update status based on whether any rows failed.
	now := time.Now().Format(time.RFC3339)
	status := string(StatusSuccess)
	if p.errors > 0 {
		status = string(StatusError)
		p.logger.Warn("processing completed with errors", "repo_id", repoID, "error_count", p.errors)
	}
	_ = p.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:                repoID,
		CoreStatus:            status,
		CoreDataLastCollected: &now,
	})

	p.logger.Info("processing complete", "repo_id", repoID, "errors", p.errors)
	return nil
}

func (p *Processor) processBatch(ctx context.Context, repoID int64, platID int16, entityType string, rows []db.StagedRow) error {
	// Contributors get special batch handling: deserialize all, dedup in memory,
	// then upsert in one transaction. This eliminates contention.
	if entityType == EntityContributor {
		var contribs []model.Contributor
		for _, row := range rows {
			var c model.Contributor
			if err := json.Unmarshal(row.Payload, &c); err != nil {
				p.logger.Warn("failed to unmarshal contributor", "staging_id", row.ID, "error", err)
				p.errors++
				continue
			}
			contribs = append(contribs, c)
		}
		if len(contribs) > 0 {
			if err := p.store.UpsertContributorBatch(ctx, contribs); err != nil {
				p.logger.Warn("failed to upsert contributor batch", "count", len(contribs), "error", err)
				p.errors += len(contribs)
			}
		}
		return nil
	}

	// All other entity types: process one at a time.
	var errCount int
	for _, row := range rows {
		if err := p.processOne(ctx, repoID, platID, entityType, row.Payload); err != nil {
			p.logger.Warn("failed to process staged row",
				"type", entityType, "staging_id", row.ID, "error", err)
			errCount++
		}
	}
	p.errors += errCount
	return nil
}

// resolveUser resolves a UserRef to a contributor UUID via the cache/DB.
func (p *Processor) resolveUser(ctx context.Context, platID int16, ref model.UserRef) *string {
	if ref.IsZero() {
		return nil
	}
	cid, err := p.resolver.Resolve(ctx, platID, ref.PlatformID,
		ref.Login, ref.Name, ref.Email,
		ref.AvatarURL, ref.URL, ref.NodeID, ref.Type)
	if err != nil {
		// Log the error — the original silent nil return hid a SQL syntax bug
		// that caused 131K+ messages to lose contributor attribution.
		p.logger.Warn("failed to resolve contributor",
			"login", ref.Login, "platform_id", ref.PlatformID, "error", err)
		return nil
	}
	return &cid
}

func (p *Processor) processOne(ctx context.Context, repoID int64, platID int16, entityType string, payload json.RawMessage) error {
	switch entityType {
	case EntityContributor:
		// Should not reach here — contributors are batched in processBatch.
		// Fallback just in case.
		var c model.Contributor
		if err := json.Unmarshal(payload, &c); err != nil {
			return err
		}
		return p.store.UpsertContributor(ctx, &c)

	case EntityIssue:
		var env stagedIssue
		if err := json.Unmarshal(payload, &env); err != nil {
			return err
		}
		issue := &env.Issue
		issue.RepoID = repoID
		issue.ReporterID = p.resolveUser(ctx, platID, issue.ReporterRef)
		issue.ClosedByID = p.resolveUser(ctx, platID, issue.ClosedByRef)

		issueID, err := p.store.UpsertIssue(ctx, issue)
		if err != nil {
			return err
		}

		// Process bundled children using the parent's DB ID.
		if len(env.Labels) > 0 {
			if err := p.store.UpsertIssueLabels(ctx, issueID, repoID, env.Labels); err != nil {
				p.logger.Warn("failed to upsert issue labels", "issue_id", issueID, "error", err)
			}
		}
		if len(env.Assignees) > 0 {
			if err := p.store.UpsertIssueAssignees(ctx, issueID, repoID, env.Assignees); err != nil {
				p.logger.Warn("failed to upsert issue assignees", "issue_id", issueID, "error", err)
			}
		}
		return nil

	case EntityPullRequest:
		var env stagedPR
		if err := json.Unmarshal(payload, &env); err != nil {
			return err
		}
		pr := &env.PR
		pr.RepoID = repoID
		pr.AuthorID = p.resolveUser(ctx, platID, pr.AuthorRef)

		prID, err := p.store.UpsertPullRequest(ctx, pr)
		if err != nil {
			return err
		}

		// Process all bundled children using the parent's DB ID.
		if len(env.Labels) > 0 {
			if err := p.store.UpsertPRLabels(ctx, prID, repoID, env.Labels); err != nil {
				p.logger.Warn("failed to upsert PR labels", "pr_id", prID, "error", err)
			}
		}
		if len(env.Assignees) > 0 {
			if err := p.store.UpsertPRAssignees(ctx, prID, repoID, env.Assignees); err != nil {
				p.logger.Warn("failed to upsert PR assignees", "pr_id", prID, "error", err)
			}
		}
		if len(env.Reviewers) > 0 {
			if err := p.store.UpsertPRReviewers(ctx, prID, repoID, env.Reviewers); err != nil {
				p.logger.Warn("failed to upsert PR reviewers", "pr_id", prID, "error", err)
			}
		}
		for _, review := range env.Reviews {
			review.PRID = prID
			review.RepoID = repoID
			review.ContributorID = p.resolveUser(ctx, platID, review.AuthorRef)
			if err := p.store.UpsertPRReview(ctx, &review); err != nil {
				p.logger.Warn("failed to upsert PR review", "pr_id", prID, "error", err)
			}
		}
		for _, commit := range env.Commits {
			commit.PRID = prID
			commit.RepoID = repoID
			commit.AuthorID = p.resolveUser(ctx, platID, commit.AuthorRef)
			if err := p.store.UpsertPRCommit(ctx, &commit); err != nil {
				p.logger.Warn("failed to upsert PR commit", "pr_id", prID, "error", err)
			}
		}
		for _, file := range env.Files {
			file.PRID = prID
			file.RepoID = repoID
			if err := p.store.UpsertPRFile(ctx, &file); err != nil {
				p.logger.Warn("failed to upsert PR file", "pr_id", prID, "error", err)
			}
		}
		if env.MetaHead != nil {
			env.MetaHead.PRID = prID
			env.MetaHead.RepoID = repoID
			p.store.UpsertPRMeta(ctx, env.MetaHead)
		}
		if env.MetaBase != nil {
			env.MetaBase.PRID = prID
			env.MetaBase.RepoID = repoID
			p.store.UpsertPRMeta(ctx, env.MetaBase)
		}
		return nil

	case EntityIssueEvent:
		var event model.IssueEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		event.RepoID = repoID
		// Resolve platform issue number to DB issue_id.
		if event.PlatformIssueID != 0 {
			dbID, err := p.store.FindIssueDBID(ctx, repoID, event.PlatformIssueID)
			if err != nil || dbID == 0 {
				return nil // parent issue not in DB — skip silently
			}
			event.IssueID = dbID
		}
		if event.IssueID == 0 {
			return nil // no parent issue — skip
		}
		event.ContributorID = p.resolveUser(ctx, platID, event.ActorRef)
		return p.store.UpsertIssueEvent(ctx, &event)

	case EntityPREvent:
		var event model.PullRequestEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return err
		}
		event.RepoID = repoID
		// Resolve platform PR number to DB pull_request_id.
		if event.PlatformPRID != 0 {
			dbID, err := p.store.FindPRDBID(ctx, repoID, event.PlatformPRID)
			if err != nil || dbID == 0 {
				return nil // parent PR not in DB — skip silently
			}
			event.PRID = dbID
		}
		if event.PRID == 0 {
			return nil // no parent PR — skip
		}
		event.ContributorID = p.resolveUser(ctx, platID, event.ActorRef)
		return p.store.UpsertPREvent(ctx, &event)

	case EntityMessage:
		var msg platform.MessageWithRef
		if err := json.Unmarshal(payload, &msg); err != nil {
			return err
		}
		msg.Message.RepoID = repoID
		msg.Message.ContributorID = p.resolveUser(ctx, platID, msg.Message.AuthorRef)
		// Resolve platform issue/PR numbers to DB IDs for message refs.
		if msg.IssueRef != nil {
			msg.IssueRef.RepoID = repoID
			num := int64(msg.IssueRef.PlatformIssueNumber)
			if num == 0 {
				num = msg.IssueRef.IssueID // fallback to IssueID if set
			}
			if num != 0 {
				dbID, err := p.store.FindIssueDBID(ctx, repoID, num)
				if err != nil || dbID == 0 {
					return nil // parent issue not in DB — skip
				}
				msg.IssueRef.IssueID = dbID
			} else {
				return nil // no way to resolve parent — skip
			}
		}
		if msg.PRRef != nil {
			msg.PRRef.RepoID = repoID
			num := int64(msg.PRRef.PlatformPRNumber)
			if num == 0 {
				num = msg.PRRef.PRID // fallback
			}
			if num != 0 {
				dbID, err := p.store.FindPRDBID(ctx, repoID, num)
				if err != nil || dbID == 0 {
					return nil // parent PR not in DB — skip
				}
				msg.PRRef.PRID = dbID
			} else {
				return nil // no way to resolve parent — skip
			}
		}
		return p.store.UpsertMessageBatch(ctx, []platform.MessageWithRef{msg})

	case EntityReviewComment:
		var rc platform.ReviewCommentWithRef
		if err := json.Unmarshal(payload, &rc); err != nil {
			return err
		}
		rc.Message.RepoID = repoID
		rc.Comment.RepoID = repoID
		rc.Message.ContributorID = p.resolveUser(ctx, platID, rc.Message.AuthorRef)
		// Resolve platform review ID to DB pr_review_id.
		if rc.Comment.PlatformReviewID != 0 {
			dbID, err := p.store.FindReviewDBID(ctx, rc.Comment.PlatformReviewID)
			if err == nil && dbID != 0 {
				rc.Comment.ReviewID = dbID
			}
		}
		return p.store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{rc})

	case EntityRelease:
		var rel model.Release
		if err := json.Unmarshal(payload, &rel); err != nil {
			return err
		}
		rel.RepoID = repoID
		return p.store.UpsertRelease(ctx, &rel)

	case EntityRepoInfo:
		var info model.RepoInfo
		if err := json.Unmarshal(payload, &info); err != nil {
			return err
		}
		info.RepoID = repoID
		// Rotate previous snapshot to history before inserting the latest.
		_ = p.store.RotateRepoInfoToHistory(ctx, repoID)
		return p.store.InsertRepoInfo(ctx, &info)

	case EntityCloneStats:
		var clone model.RepoClone
		if err := json.Unmarshal(payload, &clone); err != nil {
			return err
		}
		clone.RepoID = repoID
		return p.store.UpsertRepoClone(ctx, &clone)

	default:
		return fmt.Errorf("unknown entity type: %s", entityType)
	}
}
