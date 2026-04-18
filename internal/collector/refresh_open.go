// Package collector — refresh_open.go re-fetches all open issues and PRs for a
// repo to capture status changes (closed, merged), label additions, assignee
// changes, new reviews, etc. that occurred since the last collection pass.
//
// Incremental collection (since-based) only picks up items updated within the
// recollect window. But open items can change at any time — a PR may get merged,
// an issue may get new labels, assignees may change. This refresh ensures the
// database reflects the current state of all open items.
//
// The refresh uses the same staging pipeline as gap fill: fetches individual
// items by number via FetchIssueByNumber/FetchPRByNumber, bundles them into
// stagedIssue/stagedPR envelopes with all children, and processes via the
// standard Processor. The ON CONFLICT upserts update existing rows.
//
// This is a reusable module — the same pattern can be applied to refresh items
// matching any criteria (e.g., recently commented, specific labels).
package collector

import (
	"context"
	"log/slog"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// OpenItemRefresher re-fetches open issues and PRs to capture state changes.
type OpenItemRefresher struct {
	store       *db.PostgresStore
	client      platform.Client
	logger      *slog.Logger
	prChildMode string // see CollectionConfig.PRChildMode
}

// NewOpenItemRefresher creates an open item refresher using the REST
// per-PR child waterfall. For GraphQL-mode, call NewOpenItemRefresherWithMode.
func NewOpenItemRefresher(store *db.PostgresStore, client platform.Client, logger *slog.Logger) *OpenItemRefresher {
	return NewOpenItemRefresherWithMode(store, client, logger, "rest")
}

// NewOpenItemRefresherWithMode is the explicit-mode constructor.
// Unknown modes collapse to "rest".
func NewOpenItemRefresherWithMode(store *db.PostgresStore, client platform.Client, logger *slog.Logger, mode string) *OpenItemRefresher {
	if mode != "graphql" {
		mode = "rest"
	}
	return &OpenItemRefresher{store: store, client: client, logger: logger, prChildMode: mode}
}

// RefreshOpenItems re-fetches all open issues and PRs for a repo, updating
// their state, labels, assignees, reviews, etc. in the database.
// Returns the total number of items refreshed.
func (r *OpenItemRefresher) RefreshOpenItems(ctx context.Context, repoID int64, owner, repo string) int {
	totalRefreshed := 0

	// Refresh open issues.
	openIssues, err := r.store.GetOpenIssueNumbers(ctx, repoID)
	if err != nil {
		r.logger.Warn("failed to query open issues", "repo_id", repoID, "error", err)
	} else if len(openIssues) > 0 {
		r.logger.Info("refreshing open issues", "repo_id", repoID, "count", len(openIssues))
		refreshed := r.refreshIssues(ctx, repoID, owner, repo, openIssues)
		totalRefreshed += refreshed
		r.logger.Info("open issues refreshed", "repo_id", repoID, "refreshed", refreshed)
	}

	// Refresh open PRs (includes re-fetching reviews, commits, files, etc.).
	openPRs, err := r.store.GetOpenPRNumbers(ctx, repoID)
	if err != nil {
		r.logger.Warn("failed to query open PRs", "repo_id", repoID, "error", err)
	} else if len(openPRs) > 0 {
		r.logger.Info("refreshing open PRs", "repo_id", repoID, "count", len(openPRs))
		refreshed := r.refreshPRs(ctx, repoID, owner, repo, openPRs)
		totalRefreshed += refreshed
		r.logger.Info("open PRs refreshed", "repo_id", repoID, "refreshed", refreshed)
	}

	return totalRefreshed
}

// refreshIssues re-fetches specific issues by number and stages them.
// Uses the same envelope pattern as the staged collector and gap filler.
func (r *OpenItemRefresher) refreshIssues(ctx context.Context, repoID int64, owner, repo string, numbers []int) int {
	refreshed := 0
	sw := db.NewStagingWriter(r.store, repoID, int16(r.client.Platform()), r.logger)

	for _, num := range numbers {
		issue, err := r.client.FetchIssueByNumber(ctx, owner, repo, num)
		if err != nil {
			// Issue may have been deleted or made private.
			r.logger.Debug("failed to fetch open issue", "number", num, "error", err)
			continue
		}

		envelope := stagedIssue{Issue: *issue}
		for label, err := range r.client.ListIssueLabels(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for assignee, err := range r.client.ListIssueAssignees(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, assignee)
		}

		if err := sw.Stage(ctx, EntityIssue, envelope); err != nil {
			continue
		}
		refreshed++

		// Fetch and stage this issue's comments. Acts as a safety net against
		// prior cycles' repo-wide ListIssueComments failures (rate limit,
		// transient error, or the pre-v0.16.11 flush bug): a single broken
		// cycle would otherwise permanently drop comments on still-open
		// items because the since-window of future cycles won't cover them.
		for cref, cerr := range r.client.ListCommentsForIssue(ctx, owner, repo, num) {
			if cerr != nil {
				if isOptionalEndpointSkip(cerr) {
					break
				}
				r.logger.Debug("refresh open-issue comments error", "number", num, "error", cerr)
				break
			}
			if err := sw.Stage(ctx, EntityMessage, cref); err != nil {
				r.logger.Debug("failed to stage refreshed issue comment", "number", num, "error", err)
				break
			}
		}
	}

	if refreshed > 0 {
		// Flush the pgx.Batch to Postgres before the processor reads from
		// staging. Without this, refreshed issues would sit in the in-memory
		// batch and be lost when the StagingWriter goes out of scope — the
		// open-item status refresh would become a silent no-op for any repo
		// with fewer than stagingFlushSize (500) open items.
		if err := sw.Flush(ctx); err != nil {
			r.logger.Warn("failed to flush refreshed issue staging batch",
				"repo_id", repoID, "refreshed", refreshed, "error", err)
			return refreshed
		}
		proc := NewProcessor(r.store, r.logger)
		if err := proc.ProcessRepo(ctx, repoID, int16(r.client.Platform())); err != nil {
			r.logger.Warn("failed to process refreshed issues", "error", err)
		}
	}
	return refreshed
}

// refreshPRs re-fetches specific PRs by number with all children.
// Uses the same envelope pattern as the staged collector and gap filler.
// Branches on PRChildMode: "rest" (per-PR waterfall, pre-v0.18.1
// behavior) or "graphql" (batched FetchPRBatch). Message/review-comment
// collection remains per-PR REST in both modes — phase 1 only
// consolidates the PR child fetch.
func (r *OpenItemRefresher) refreshPRs(ctx context.Context, repoID int64, owner, repo string, numbers []int) int {
	refreshed := 0
	sw := db.NewStagingWriter(r.store, repoID, int16(r.client.Platform()), r.logger)

	// Fetch PR cores + children. In graphql mode, one batch call replaces
	// the per-PR waterfall; in rest mode, fetch each one sequentially
	// via the existing methods.
	envelopes := r.fetchPRsForRefresh(ctx, owner, repo, numbers)

	for _, env := range envelopes {
		num := env.PR.Number
		if err := sw.Stage(ctx, EntityPullRequest, env); err != nil {
			continue
		}
		refreshed++

		// Fetch and stage this PR's conversation comments (safety net — see
		// refreshIssues above for the full rationale).
		for cref, cerr := range r.client.ListCommentsForPR(ctx, owner, repo, num) {
			if cerr != nil {
				if isOptionalEndpointSkip(cerr) {
					break
				}
				r.logger.Debug("refresh open-PR comments error", "number", num, "error", cerr)
				break
			}
			if err := sw.Stage(ctx, EntityMessage, cref); err != nil {
				r.logger.Debug("failed to stage refreshed PR comment", "number", num, "error", err)
				break
			}
		}

		// Fetch and stage this PR's inline review comments.
		for rc, rerr := range r.client.ListReviewCommentsForPR(ctx, owner, repo, num) {
			if rerr != nil {
				if isOptionalEndpointSkip(rerr) {
					break
				}
				r.logger.Debug("refresh open-PR review comments error", "number", num, "error", rerr)
				break
			}
			if err := sw.Stage(ctx, EntityReviewComment, rc); err != nil {
				r.logger.Debug("failed to stage refreshed PR review comment", "number", num, "error", err)
				break
			}
		}
	}

	if refreshed > 0 {
		// Flush the pgx.Batch to Postgres before the processor reads from
		// staging (see refreshIssues above for the full rationale — same
		// buffering bug).
		if err := sw.Flush(ctx); err != nil {
			r.logger.Warn("failed to flush refreshed PR staging batch",
				"repo_id", repoID, "refreshed", refreshed, "error", err)
			return refreshed
		}
		proc := NewProcessor(r.store, r.logger)
		if err := proc.ProcessRepo(ctx, repoID, int16(r.client.Platform())); err != nil {
			r.logger.Warn("failed to process refreshed PRs", "error", err)
		}
	}
	return refreshed
}

// fetchPRsForRefresh returns a slice of stagedPR envelopes ready for
// staging, using either the REST waterfall or the GraphQL batch
// depending on r.prChildMode. PRs that can't be fetched are skipped
// cleanly — the caller appends only what comes back.
func (r *OpenItemRefresher) fetchPRsForRefresh(ctx context.Context, owner, repo string, numbers []int) []stagedPR {
	if r.prChildMode == "graphql" {
		return r.fetchPRsForRefreshGraphQL(ctx, owner, repo, numbers)
	}
	return r.fetchPRsForRefreshREST(ctx, owner, repo, numbers)
}

func (r *OpenItemRefresher) fetchPRsForRefreshREST(ctx context.Context, owner, repo string, numbers []int) []stagedPR {
	out := make([]stagedPR, 0, len(numbers))
	for _, num := range numbers {
		pr, err := r.client.FetchPRByNumber(ctx, owner, repo, num)
		if err != nil {
			r.logger.Debug("failed to fetch open PR", "number", num, "error", err)
			continue
		}
		envelope := stagedPR{PR: *pr}
		for label, err := range r.client.ListPRLabels(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for assignee, err := range r.client.ListPRAssignees(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, assignee)
		}
		for reviewer, err := range r.client.ListPRReviewers(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Reviewers = append(envelope.Reviewers, reviewer)
		}
		for review, err := range r.client.ListPRReviews(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Reviews = append(envelope.Reviews, review)
		}
		for commit, err := range r.client.ListPRCommits(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Commits = append(envelope.Commits, commit)
		}
		for file, err := range r.client.ListPRFiles(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Files = append(envelope.Files, file)
		}
		out = append(out, envelope)
	}
	return out
}

func (r *OpenItemRefresher) fetchPRsForRefreshGraphQL(ctx context.Context, owner, repo string, numbers []int) []stagedPR {
	batch, err := r.client.FetchPRBatch(ctx, owner, repo, numbers)
	if err != nil {
		r.logger.Debug("FetchPRBatch in refresh failed", "count", len(numbers), "error", err)
		return nil
	}
	out := make([]stagedPR, 0, len(batch))
	for _, s := range batch {
		out = append(out, stagedPR{
			PR:        s.PR,
			Labels:    s.Labels,
			Assignees: s.Assignees,
			Reviewers: s.Reviewers,
			Reviews:   s.Reviews,
			Commits:   s.Commits,
			Files:     s.Files,
			MetaHead:  s.MetaHead,
			MetaBase:  s.MetaBase,
			RepoHead:  s.RepoHead,
			RepoBase:  s.RepoBase,
		})
	}
	return out
}
