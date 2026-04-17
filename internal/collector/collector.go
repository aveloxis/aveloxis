package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Collector orchestrates data collection for a single repository.
type Collector struct {
	client         platform.Client
	store          *db.PostgresStore
	resolver       *db.ContributorResolver
	logger         *slog.Logger
	platID         int16
	facade         *FacadeCollector
	ghKeys         *platform.KeyPool // for commit resolution (GitHub only)
}

// New creates a collector for the given platform client and database store.
// Uses the default clone directory ($HOME/aveloxis-repos).
func New(client platform.Client, store *db.PostgresStore, logger *slog.Logger) *Collector {
	home, _ := os.UserHomeDir()
	defaultDir := home + "/aveloxis-repos"
	if home == "" {
		defaultDir = os.TempDir() + "/aveloxis-repos"
	}
	return NewWithOptions(client, store, logger, nil, defaultDir)
}

// NewWithKeys creates a collector with GitHub keys for commit resolution.
func NewWithKeys(client platform.Client, store *db.PostgresStore, logger *slog.Logger, ghKeys *platform.KeyPool) *Collector {
	home, _ := os.UserHomeDir()
	defaultDir := home + "/aveloxis-repos"
	if home == "" {
		defaultDir = os.TempDir() + "/aveloxis-repos"
	}
	return NewWithOptions(client, store, logger, ghKeys, defaultDir)
}

// NewWithOptions creates a collector with all options specified.
func NewWithOptions(client platform.Client, store *db.PostgresStore, logger *slog.Logger, ghKeys *platform.KeyPool, repoCloneDir string) *Collector {
	platID := int16(client.Platform())
	return &Collector{
		client:   client,
		store:    store,
		resolver: db.NewContributorResolver(store),
		logger:   logger,
		platID:   platID,
		facade:   NewFacadeCollector(store, logger, repoCloneDir),
		ghKeys:   ghKeys,
	}
}

// CollectResult summarizes the outcome of a collection run.
type CollectResult struct {
	Issues       int
	PullRequests int
	Messages     int
	Events       int
	Releases     int
	Contributors int
	CommitCount  int // from repo_info metadata, used for large-repo detection
	Errors       []error
}

// CollectRepo runs a full collection for the given repository.
// The since parameter controls incremental vs full collection (zero = full).
func (c *Collector) CollectRepo(ctx context.Context, repoID int64, owner, repo string, since time.Time) (*CollectResult, error) {
	result := &CollectResult{}
	c.logger.Info("starting collection",
		"platform", c.client.Platform(),
		"owner", owner,
		"repo", repo,
		"repoID", repoID,
		"since", since,
	)

	// Update status to Collecting.
	if err := c.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:     repoID,
		CoreStatus: string(StatusCollecting),
	}); err != nil {
		c.logger.Warn("failed to update collection status", "repo_id", repoID, "error", err)
	}

	// Phase 0: Seed contributor cache from repo's known contributors.
	if err := c.collectContributors(ctx, repoID, owner, repo, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("contributors: %w", err))
	}

	// Phase 1: Primary data — issues and PRs.
	if err := c.collectIssues(ctx, repoID, owner, repo, since, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("issues: %w", err))
	}
	if err := c.collectPullRequests(ctx, repoID, owner, repo, since, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("pull requests: %w", err))
	}

	// Phase 2: Secondary data — events, messages.
	if err := c.collectEvents(ctx, repoID, owner, repo, since, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("events: %w", err))
	}
	if err := c.collectMessages(ctx, repoID, owner, repo, since, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("messages: %w", err))
	}

	// Phase 3: Metadata — repo info, releases.
	if err := c.collectRepoInfo(ctx, repoID, owner, repo, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("repo info: %w", err))
	}
	if err := c.collectReleases(ctx, repoID, owner, repo, result); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("releases: %w", err))
	}
	if err := c.collectCloneStats(ctx, repoID, owner, repo); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("clone stats: %w", err))
	}

	// Phase 4: Facade — git clone + log for commit data.
	// Runs AFTER API phases so contributor emails can be resolved.
	gitURL := fmt.Sprintf("https://%s/%s/%s.git",
		platformHost(c.client.Platform()), owner, repo)
	if err := c.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:       repoID,
		FacadeStatus: string(StatusCollecting),
	}); err != nil {
		c.logger.Warn("failed to update facade status", "repo_id", repoID, "error", err)
	}
	facadeResult, err := c.facade.CollectRepo(ctx, repoID, gitURL)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("facade: %w", err))
	} else if facadeResult != nil {
		for _, e := range facadeResult.Errors {
			result.Errors = append(result.Errors, fmt.Errorf("facade: %w", e))
		}
		c.logger.Info("facade complete",
			"commits", facadeResult.Commits,
			"commit_messages", facadeResult.CommitMessages)
	}

	// Phase 5: Commit author resolution (GitHub only).
	if c.client.Platform() == model.PlatformGitHub && c.ghKeys != nil {
		commitResolver := NewCommitResolver(c.store, c.ghKeys, c.logger)
		resolveResult, resolveErr := commitResolver.ResolveCommits(ctx, repoID, owner, repo)
		if resolveErr != nil {
			result.Errors = append(result.Errors, fmt.Errorf("commit resolution: %w", resolveErr))
		} else if resolveResult != nil {
			c.logger.Info("commit resolution complete",
				"resolved_api", resolveResult.ResolvedAPI,
				"resolved_noreply", resolveResult.ResolvedNoreply,
				"unresolved", resolveResult.Unresolved)
		}
	}

	// Update status to Success or Error.
	status := string(StatusSuccess)
	if len(result.Errors) > 0 {
		status = string(StatusError)
	}
	now := time.Now().Format(time.RFC3339)
	facadeStatus := string(StatusSuccess)
	if facadeResult == nil || len(facadeResult.Errors) > 0 {
		facadeStatus = string(StatusError)
	}
	if err := c.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:                  repoID,
		CoreStatus:              status,
		CoreDataLastCollected:   &now,
		FacadeStatus:            facadeStatus,
		FacadeDataLastCollected: &now,
	}); err != nil {
		c.logger.Warn("failed to update final collection status", "repo_id", repoID, "error", err)
	}

	c.logger.Info("collection complete",
		"platform", c.client.Platform(),
		"owner", owner, "repo", repo,
		"issues", result.Issues,
		"prs", result.PullRequests,
		"messages", result.Messages,
		"events", result.Events,
		"releases", result.Releases,
		"contributors", result.Contributors,
		"errors", len(result.Errors),
	)

	return result, nil
}

func (c *Collector) collectIssues(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) error {
	c.logger.Info("collecting issues", "owner", owner, "repo", repo)
	for issue, err := range c.client.ListIssues(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		issue.RepoID = repoID

		// Resolve contributor FKs.
		if !issue.ReporterRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, issue.ReporterRef.PlatformID,
				issue.ReporterRef.Login, issue.ReporterRef.Name, issue.ReporterRef.Email,
				issue.ReporterRef.AvatarURL, issue.ReporterRef.URL, issue.ReporterRef.NodeID, issue.ReporterRef.Type); err == nil {
				issue.ReporterID = &cid
			}
		}
		if !issue.ClosedByRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, issue.ClosedByRef.PlatformID,
				issue.ClosedByRef.Login, issue.ClosedByRef.Name, issue.ClosedByRef.Email,
				issue.ClosedByRef.AvatarURL, issue.ClosedByRef.URL, issue.ClosedByRef.NodeID, issue.ClosedByRef.Type); err == nil {
				issue.ClosedByID = &cid
			}
		}

		issueID, err := c.store.UpsertIssue(ctx, &issue)
		if err != nil {
			c.logger.Warn("failed to upsert issue", "number", issue.Number, "error", err)
			continue
		}

		// Collect labels and assignees for this issue.
		var labels []model.IssueLabel
		for label, err := range c.client.ListIssueLabels(ctx, owner, repo, issue.Number) {
			if err != nil {
				c.logger.Warn("failed to list issue labels", "issue", issue.Number, "error", err)
				break
			}
			labels = append(labels, label)
		}
		if len(labels) > 0 {
			if err := c.store.UpsertIssueLabels(ctx, issueID, repoID, labels); err != nil {
				c.logger.Warn("failed to upsert issue labels", "issue", issue.Number, "error", err)
			}
		}

		var assignees []model.IssueAssignee
		for assignee, err := range c.client.ListIssueAssignees(ctx, owner, repo, issue.Number) {
			if err != nil {
				c.logger.Warn("failed to list issue assignees", "issue", issue.Number, "error", err)
				break
			}
			assignees = append(assignees, assignee)
		}
		if len(assignees) > 0 {
			if err := c.store.UpsertIssueAssignees(ctx, issueID, repoID, assignees); err != nil {
				c.logger.Warn("failed to upsert issue assignees", "issue", issue.Number, "error", err)
			}
		}

		result.Issues++
	}
	return nil
}

func (c *Collector) collectPullRequests(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) error {
	c.logger.Info("collecting pull requests", "owner", owner, "repo", repo)
	for pr, err := range c.client.ListPullRequests(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		pr.RepoID = repoID

		// Resolve author contributor FK.
		if !pr.AuthorRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, pr.AuthorRef.PlatformID,
				pr.AuthorRef.Login, pr.AuthorRef.Name, pr.AuthorRef.Email,
				pr.AuthorRef.AvatarURL, pr.AuthorRef.URL, pr.AuthorRef.NodeID, pr.AuthorRef.Type); err == nil {
				pr.AuthorID = &cid
			}
		}

		prID, err := c.store.UpsertPullRequest(ctx, &pr)
		if err != nil {
			c.logger.Warn("failed to upsert PR", "number", pr.Number, "error", err)
			continue
		}

		// Labels
		var labels []model.PullRequestLabel
		for label, err := range c.client.ListPRLabels(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to list PR labels", "pr", pr.Number, "error", err)
				break
			}
			labels = append(labels, label)
		}
		if len(labels) > 0 {
			if err := c.store.UpsertPRLabels(ctx, prID, repoID, labels); err != nil {
				c.logger.Warn("failed to upsert PR labels", "pr", pr.Number, "error", err)
			}
		}

		// Assignees
		var assignees []model.PullRequestAssignee
		for a, err := range c.client.ListPRAssignees(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to list PR assignees", "pr", pr.Number, "error", err)
				break
			}
			assignees = append(assignees, a)
		}
		if len(assignees) > 0 {
			if err := c.store.UpsertPRAssignees(ctx, prID, repoID, assignees); err != nil {
				c.logger.Warn("failed to upsert PR assignees", "pr", pr.Number, "error", err)
			}
		}

		// Reviewers
		var reviewers []model.PullRequestReviewer
		for r, err := range c.client.ListPRReviewers(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to list PR reviewers", "pr", pr.Number, "error", err)
				break
			}
			reviewers = append(reviewers, r)
		}
		if len(reviewers) > 0 {
			if err := c.store.UpsertPRReviewers(ctx, prID, repoID, reviewers); err != nil {
				c.logger.Warn("failed to upsert PR reviewers", "pr", pr.Number, "error", err)
			}
		}

		// Reviews
		for review, err := range c.client.ListPRReviews(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to collect PR reviews", "pr", pr.Number, "error", err)
				break
			}
			review.PRID = prID
			review.RepoID = repoID
			if !review.AuthorRef.IsZero() {
				if cid, err := c.resolver.Resolve(ctx, c.platID, review.AuthorRef.PlatformID,
					review.AuthorRef.Login, review.AuthorRef.Name, review.AuthorRef.Email,
					review.AuthorRef.AvatarURL, review.AuthorRef.URL, review.AuthorRef.NodeID, review.AuthorRef.Type); err == nil {
					review.ContributorID = &cid
				}
			}
			if err := c.store.UpsertPRReview(ctx, &review); err != nil {
				c.logger.Warn("failed to upsert PR review", "pr", pr.Number, "error", err)
			}
		}

		// Commits
		for commit, err := range c.client.ListPRCommits(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to collect PR commits", "pr", pr.Number, "error", err)
				break
			}
			commit.PRID = prID
			commit.RepoID = repoID
			if !commit.AuthorRef.IsZero() {
				if cid, err := c.resolver.Resolve(ctx, c.platID, commit.AuthorRef.PlatformID,
					commit.AuthorRef.Login, commit.AuthorRef.Name, commit.AuthorRef.Email,
					commit.AuthorRef.AvatarURL, commit.AuthorRef.URL, commit.AuthorRef.NodeID, commit.AuthorRef.Type); err == nil {
					commit.AuthorID = &cid
				}
			}
			if err := c.store.UpsertPRCommit(ctx, &commit); err != nil {
				c.logger.Warn("failed to upsert PR commit", "pr", pr.Number, "error", err)
			}
		}

		// Files
		for file, err := range c.client.ListPRFiles(ctx, owner, repo, pr.Number) {
			if err != nil {
				c.logger.Warn("failed to collect PR files", "pr", pr.Number, "error", err)
				break
			}
			file.PRID = prID
			file.RepoID = repoID
			if err := c.store.UpsertPRFile(ctx, &file); err != nil {
				c.logger.Warn("failed to upsert PR file", "pr", pr.Number, "error", err)
			}
		}

		// Head/base metadata
		head, base, err := c.client.FetchPRMeta(ctx, owner, repo, pr.Number)
		var headMetaID, baseMetaID int64
		if err == nil {
			head.PRID = prID
			head.RepoID = repoID
			base.PRID = prID
			base.RepoID = repoID
			var metaErr error
			headMetaID, metaErr = c.store.UpsertPRMeta(ctx, head)
			if metaErr != nil {
				c.logger.Warn("failed to upsert PR meta (head)", "pr", pr.Number, "error", metaErr)
			}
			baseMetaID, metaErr = c.store.UpsertPRMeta(ctx, base)
			if metaErr != nil {
				c.logger.Warn("failed to upsert PR meta (base)", "pr", pr.Number, "error", metaErr)
			}
		} else {
			c.logger.Warn("failed to fetch PR meta", "pr", pr.Number, "error", err)
		}
		// Fork/upstream repo details from head.repo and base.repo
		headRepo, baseRepo, repoErr := c.client.FetchPRRepos(ctx, owner, repo, pr.Number)
		if repoErr != nil {
			c.logger.Warn("failed to fetch PR repos", "pr", pr.Number, "error", repoErr)
		} else {
			if headRepo != nil && headMetaID != 0 {
				headRepo.MetaID = headMetaID
				if err := c.store.UpsertPRRepo(ctx, headRepo); err != nil {
					c.logger.Warn("failed to upsert PR repo (head)", "pr", pr.Number, "error", err)
				}
			}
			if baseRepo != nil && baseMetaID != 0 {
				baseRepo.MetaID = baseMetaID
				if err := c.store.UpsertPRRepo(ctx, baseRepo); err != nil {
					c.logger.Warn("failed to upsert PR repo (base)", "pr", pr.Number, "error", err)
				}
			}
		}

		result.PullRequests++
	}
	return nil
}

func (c *Collector) collectEvents(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) error {
	c.logger.Info("collecting events", "owner", owner, "repo", repo)
	for event, err := range c.client.ListIssueEvents(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		event.RepoID = repoID
		if !event.ActorRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, event.ActorRef.PlatformID,
				event.ActorRef.Login, event.ActorRef.Name, event.ActorRef.Email,
				event.ActorRef.AvatarURL, event.ActorRef.URL, event.ActorRef.NodeID, event.ActorRef.Type); err == nil {
				event.ContributorID = &cid
			}
		}
		if err := c.store.UpsertIssueEvent(ctx, &event); err != nil {
			c.logger.Warn("failed to upsert issue event", "error", err)
			continue
		}
		result.Events++
	}
	for event, err := range c.client.ListPREvents(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		event.RepoID = repoID
		if !event.ActorRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, event.ActorRef.PlatformID,
				event.ActorRef.Login, event.ActorRef.Name, event.ActorRef.Email,
				event.ActorRef.AvatarURL, event.ActorRef.URL, event.ActorRef.NodeID, event.ActorRef.Type); err == nil {
				event.ContributorID = &cid
			}
		}
		if err := c.store.UpsertPREvent(ctx, &event); err != nil {
			c.logger.Warn("failed to upsert PR event", "error", err)
			continue
		}
		result.Events++
	}
	return nil
}

func (c *Collector) collectMessages(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) error {
	c.logger.Info("collecting messages", "owner", owner, "repo", repo)

	// Collect issue comments with batching.
	var batch []platform.MessageWithRef
	const batchSize = 500

	for msg, err := range c.client.ListIssueComments(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		msg.Message.RepoID = repoID
		if msg.IssueRef != nil {
			msg.IssueRef.RepoID = repoID
		}
		if msg.PRRef != nil {
			msg.PRRef.RepoID = repoID
		}
		if !msg.Message.AuthorRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, msg.Message.AuthorRef.PlatformID,
				msg.Message.AuthorRef.Login, msg.Message.AuthorRef.Name, msg.Message.AuthorRef.Email,
				msg.Message.AuthorRef.AvatarURL, msg.Message.AuthorRef.URL, msg.Message.AuthorRef.NodeID, msg.Message.AuthorRef.Type); err == nil {
				msg.Message.ContributorID = &cid
			}
		}
		batch = append(batch, msg)
		result.Messages++

		if len(batch) >= batchSize {
			if err := c.store.UpsertMessageBatch(ctx, batch); err != nil {
				c.logger.Warn("failed to upsert message batch", "error", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := c.store.UpsertMessageBatch(ctx, batch); err != nil {
			c.logger.Warn("failed to upsert message batch", "error", err)
		}
	}

	// Collect review comments with batching.
	var rcBatch []platform.ReviewCommentWithRef
	for rc, err := range c.client.ListReviewComments(ctx, owner, repo, since) {
		if err != nil {
			return err
		}
		rc.Message.RepoID = repoID
		rc.Comment.RepoID = repoID
		if !rc.Message.AuthorRef.IsZero() {
			if cid, err := c.resolver.Resolve(ctx, c.platID, rc.Message.AuthorRef.PlatformID,
				rc.Message.AuthorRef.Login, rc.Message.AuthorRef.Name, rc.Message.AuthorRef.Email,
				rc.Message.AuthorRef.AvatarURL, rc.Message.AuthorRef.URL, rc.Message.AuthorRef.NodeID, rc.Message.AuthorRef.Type); err == nil {
				rc.Message.ContributorID = &cid
			}
		}
		rcBatch = append(rcBatch, rc)
		result.Messages++

		if len(rcBatch) >= batchSize {
			if err := c.store.UpsertReviewCommentBatch(ctx, rcBatch); err != nil {
				c.logger.Warn("failed to upsert review comment batch", "error", err)
			}
			rcBatch = rcBatch[:0]
		}
	}
	if len(rcBatch) > 0 {
		if err := c.store.UpsertReviewCommentBatch(ctx, rcBatch); err != nil {
			c.logger.Warn("failed to upsert review comment batch", "error", err)
		}
	}

	return nil
}

func (c *Collector) collectRepoInfo(ctx context.Context, repoID int64, owner, repo string, result *CollectResult) error {
	c.logger.Info("collecting repo info", "owner", owner, "repo", repo)
	info, err := c.client.FetchRepoInfo(ctx, owner, repo)
	if err != nil {
		return err
	}
	info.RepoID = repoID
	return c.store.InsertRepoInfo(ctx, info)
}

func (c *Collector) collectReleases(ctx context.Context, repoID int64, owner, repo string, result *CollectResult) error {
	c.logger.Info("collecting releases", "owner", owner, "repo", repo)
	for rel, err := range c.client.ListReleases(ctx, owner, repo) {
		if err != nil {
			// 404 on /releases is a normal state — not every repo has cut a
			// release. Don't propagate it as a collection error.
			if errors.Is(err, platform.ErrNotFound) {
				c.logger.Info("no releases endpoint (404) — treating as zero releases",
					"owner", owner, "repo", repo)
				return nil
			}
			return err
		}
		rel.RepoID = repoID
		if err := c.store.UpsertRelease(ctx, &rel); err != nil {
			c.logger.Warn("failed to upsert release", "tag", rel.TagName, "error", err)
			continue
		}
		result.Releases++
	}
	return nil
}

func (c *Collector) collectContributors(ctx context.Context, repoID int64, owner, repo string, result *CollectResult) error {
	c.logger.Info("collecting contributors", "owner", owner, "repo", repo)
	for contrib, err := range c.client.ListContributors(ctx, owner, repo) {
		if err != nil {
			return err
		}
		if err := c.store.UpsertContributor(ctx, &contrib); err != nil {
			c.logger.Warn("failed to upsert contributor", "login", contrib.Login, "error", err)
			continue
		}
		result.Contributors++
	}
	return nil
}

func platformHost(p model.Platform) string {
	switch p {
	case model.PlatformGitHub:
		return "github.com"
	case model.PlatformGitLab:
		return "gitlab.com"
	default:
		return "unknown"
	}
}

func (c *Collector) collectCloneStats(ctx context.Context, repoID int64, owner, repo string) error {
	clones, err := c.client.FetchCloneStats(ctx, owner, repo)
	if err != nil {
		return err
	}
	for _, clone := range clones {
		clone.RepoID = repoID
		if err := c.store.UpsertRepoClone(ctx, &clone); err != nil {
			c.logger.Warn("failed to upsert clone stat", "error", err)
		}
	}
	return nil
}

// ClientForRepo returns the appropriate platform client given a repository URL.
func ClientForRepo(repoURL string, ghClient, glClient platform.Client) (platform.Client, string, string, error) {
	parsed, err := platform.ParseRepoURL(repoURL)
	if err != nil {
		return nil, "", "", err
	}
	switch parsed.Platform {
	case model.PlatformGitHub:
		return ghClient, parsed.Owner, parsed.Repo, nil
	case model.PlatformGitLab:
		return glClient, parsed.Owner, parsed.Repo, nil
	default:
		return nil, "", "", fmt.Errorf("unsupported platform for URL: %s", repoURL)
	}
}
