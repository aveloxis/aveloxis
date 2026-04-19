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
	"sync"
	"sync/atomic"
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

// LargeRepoCommitThreshold is the commit count above which parallel collection
// kicks in. Repos with more commits than this typically also have many issues,
// PRs, and events — collecting them in parallel significantly speeds up the
// initial collection pass.
const LargeRepoCommitThreshold = 10000

// isOptionalEndpointSkip returns true when err represents a routine
// "can't collect this item, continue the loop" condition — 404, 403-private,
// 410, or entity-kind mismatches (issue-number-that-is-a-PR).
//
// Since v0.18.0 this is a thin delegate to platform.ClassifyError so that
// new error shapes (e.g. platform.ErrWrongEntityKind, GraphQL-layer errors)
// flow through a single source of truth. Callers should prefer
// `platform.ClassifyError(err) == platform.ClassSkip` in new code; this
// helper remains for the 20+ existing call sites to keep the migration
// diff small.
func isOptionalEndpointSkip(err error) bool {
	return platform.ClassifyError(err) == platform.ClassSkip
}

// ParallelSlots is a global counter tracking how many extra parallel goroutines
// are active for large-repo collection. The scheduler's fillWorkerSlots checks
// this to avoid starting new jobs while large repos consume extra capacity.
var ParallelSlots atomic.Int32

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
	RepoHead  *model.PullRequestRepo      `json:"repo_head,omitempty"`
	RepoBase  *model.PullRequestRepo      `json:"repo_base,omitempty"`
}

// StagedCollector writes raw API data to the staging table instead of directly
// into the relational schema. This is the fast path for high-throughput collection.
type StagedCollector struct {
	client        platform.Client
	store         *db.PostgresStore
	logger        *slog.Logger
	platID        int16
	prChildMode   string // "rest" (default) or "graphql" — see CollectionConfig.PRChildMode
	listingMode   string // "rest" (default) or "graphql" — see CollectionConfig.ListingMode
	threadingMode string // "single" (default) or "sharded" — see CollectionConfig.ThreadingMode
	shardSize     int    // item-count threshold for sharded fan-out (default 3000)
}

// NewStagedCollector creates a staged collector in the fully-default
// mode: REST per-PR child waterfall, REST issue/PR listing,
// single-goroutine PR batch execution.
func NewStagedCollector(client platform.Client, store *db.PostgresStore, logger *slog.Logger) *StagedCollector {
	return NewStagedCollectorWithAllModes(client, store, logger, "rest", "rest", "single", defaultShardSize)
}

// NewStagedCollectorWithMode is the phase-1 single-field constructor
// that sets pr_child_mode only. Preserved for backward compatibility.
func NewStagedCollectorWithMode(client platform.Client, store *db.PostgresStore, logger *slog.Logger, mode string) *StagedCollector {
	return NewStagedCollectorWithAllModes(client, store, logger, mode, "rest", "single", defaultShardSize)
}

// NewStagedCollectorWithModes is the phase-2 two-field constructor
// (prChildMode + listingMode). Preserved for backward compatibility.
func NewStagedCollectorWithModes(client platform.Client, store *db.PostgresStore, logger *slog.Logger, prChildMode, listingMode string) *StagedCollector {
	return NewStagedCollectorWithAllModes(client, store, logger, prChildMode, listingMode, "single", defaultShardSize)
}

// NewStagedCollectorWithAllModes is the explicit dual-mode-plus-
// threading constructor added in phase 3. prChildMode selects the
// REST vs GraphQL per-PR child path. listingMode selects the unified
// vs split issue/PR listing path. threadingMode selects single-
// goroutine vs sharded PR batch execution. shardSize sets the
// item-count threshold above which sharded mode fans out.
//
// Unknown string modes collapse to their safest defaults (rest,
// rest, single). shardSize <= 0 collapses to defaultShardSize.
func NewStagedCollectorWithAllModes(client platform.Client, store *db.PostgresStore, logger *slog.Logger, prChildMode, listingMode, threadingMode string, shardSize int) *StagedCollector {
	if prChildMode != "graphql" {
		prChildMode = "rest"
	}
	if listingMode != "graphql" {
		listingMode = "rest"
	}
	if threadingMode != "sharded" {
		threadingMode = "single"
	}
	if shardSize <= 0 {
		shardSize = defaultShardSize
	}
	return &StagedCollector{
		client:        client,
		store:         store,
		logger:        logger,
		platID:        int16(client.Platform()),
		prChildMode:   prChildMode,
		listingMode:   listingMode,
		threadingMode: threadingMode,
		shardSize:     shardSize,
	}
}

// defaultShardSize matches the "1 additional worker per 3,000 items"
// rule from the refactor plan in CLAUDE.md.
const defaultShardSize = 3000

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

	if err := sc.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:     repoID,
		CoreStatus: string(StatusCollecting),
	}); err != nil {
		sc.logger.Warn("failed to update collection status", "repo_id", repoID, "error", err)
	}

	// Phase 0: Metadata — collected AND PROCESSED first so metadata counts
	// appear in the monitor immediately, even while the heavy collection
	// phases (issues, PRs, events) are still running. Without immediate
	// processing, repo_info sits unprocessed in staging for the entire
	// duration of collection, and a crash/restart loses the metadata.
	sc.logger.Info("collecting metadata", "owner", owner, "repo", repo)
	info, infoErr := sc.client.FetchRepoInfo(ctx, owner, repo)
	if infoErr != nil {
		result.Errors = append(result.Errors, fmt.Errorf("repo info: %w", infoErr))
	} else {
		if err := sw.Stage(ctx, EntityRepoInfo, info); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage repo info: %w", err))
		}
		result.CommitCount = info.CommitCount
	}

	for rel, relErr := range sc.client.ListReleases(ctx, owner, repo) {
		if relErr != nil {
			// 404/403 on /releases is normal for repos that never cut a release
			// or for private/unreachable resources. It must NOT fail the job.
			if isOptionalEndpointSkip(relErr) {
				sc.logger.Info("skipping releases endpoint",
					"owner", owner, "repo", repo, "reason", relErr)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("releases: %w", relErr))
			break
		}
		if err := sw.Stage(ctx, EntityRelease, rel); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage release: %w", err))
		}
		result.Releases++
	}

	clones, cloneErr := sc.client.FetchCloneStats(ctx, owner, repo)
	if cloneErr == nil {
		for _, clone := range clones {
			if err := sw.Stage(ctx, EntityCloneStats, clone); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage clone stats: %w", err))
			}
		}
	}

	// Flush and process metadata immediately so it's in the DB before
	// the minutes-long issue/PR/event collection begins. This ensures
	// the monitor shows metadata counts even during active collection,
	// and a crash/restart doesn't lose the metadata.
	if err := sw.Flush(ctx); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("metadata flush: %w", err))
	}
	proc := NewProcessor(sc.store, sc.logger)
	for _, et := range []string{EntityRepoInfo, EntityCloneStats, EntityRelease} {
		if err := sc.store.ProcessStaged(ctx, repoID, et, 500, func(rows []db.StagedRow) error {
			return proc.processBatch(ctx, repoID, sc.platID, et, rows)
		}); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("process staged %s: %w", et, err))
		}
	}
	sc.logger.Info("metadata processed", "commit_count", result.CommitCount, "releases", result.Releases)

	// Phase 1: Contributors.
	sc.logger.Info("collecting contributors", "owner", owner, "repo", repo)
	for contrib, err := range sc.client.ListContributors(ctx, owner, repo) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping contributors endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("contributors: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityContributor, contrib); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.Contributors++
	}
	sc.logger.Info("contributors staged", "count", result.Contributors)

	// Decide between parallel and sequential collection based on commit count.
	// Large repos (>10K commits) typically have many issues, PRs, and events.
	// Collecting them in parallel across 3 goroutines significantly speeds up
	// the initial collection pass.
	if result.CommitCount >= LargeRepoCommitThreshold {
		sc.logger.Info("large repo detected — using parallel collection",
			"repo_id", repoID, "commit_count", result.CommitCount)
		sc.collectParallel(ctx, repoID, owner, repo, since, result)
	} else {
		sc.collectSequential(ctx, sw, owner, repo, since, result)
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

// collectSequential runs issues, PRs, events, and messages one after another.
// Used for repos with fewer than LargeRepoCommitThreshold commits.
func (sc *StagedCollector) collectSequential(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult) {
	preIssues, prePRs, preIssueComments := sc.preEnumerateIfGraphQL(ctx, owner, repo, since, result)
	sc.collectIssues(ctx, sw, owner, repo, since, result, preIssues)
	sc.stageInlineIssueComments(ctx, sw, preIssueComments, result)
	sc.collectPRs(ctx, sw, owner, repo, since, result, prePRs)
	sc.collectEvents(ctx, sw, owner, repo, since, result)
	sc.collectMessages(ctx, sw, owner, repo, since, result)
}

// preEnumerateIfGraphQL does the unified GraphQL issue+PR listing once
// when listingMode=graphql, returning the issue and PR slices plus the
// inline issue comments delivered with the listing (phase 4). In
// listingMode=rest it returns (nil, nil, nil) — the legacy per-path
// iterators in collectIssues and collectPRs remain in charge, and the
// repo-wide collectMessages call keeps its job.
//
// Non-fatal errors from the unified listing are logged and the function
// returns (nil, nil, nil) so collection falls through to the legacy
// iterators. This way a transient GraphQL problem doesn't take down the
// entire repo's collection — we just lose the speedup for this cycle.
func (sc *StagedCollector) preEnumerateIfGraphQL(ctx context.Context, owner, repo string, since time.Time, _ *CollectResult) ([]model.Issue, []model.PullRequest, []platform.MessageWithRef) {
	if sc.listingMode != "graphql" {
		return nil, nil, nil
	}
	batch, err := sc.client.ListIssuesAndPRs(ctx, owner, repo, since)
	if err != nil {
		if isOptionalEndpointSkip(err) {
			sc.logger.Info("unified listing skipped — falling back to REST iterators",
				"owner", owner, "repo", repo, "reason", err)
			return nil, nil, nil
		}
		sc.logger.Warn("unified GraphQL listing failed — falling back to REST iterators",
			"owner", owner, "repo", repo, "error", err)
		return nil, nil, nil
	}
	sc.logger.Info("unified listing complete",
		"owner", owner, "repo", repo,
		"issues", len(batch.Issues), "prs", len(batch.PullRequests),
		"inline_issue_comments", len(batch.IssueComments))
	// Initialize to non-nil empty slices when the batch had zero items
	// so the caller can distinguish "pre-fetched, found none" from
	// "not pre-fetched, use iterator". Legacy rest-mode callers still
	// see (nil, nil, nil).
	issues := batch.Issues
	if issues == nil {
		issues = []model.Issue{}
	}
	prs := batch.PullRequests
	if prs == nil {
		prs = []model.PullRequest{}
	}
	return issues, prs, batch.IssueComments
}

// stageInlineIssueComments stages the issue comments that came inline
// with the phase-2 unified listing. No-op when the slice is empty
// (listingMode=rest, or a GitHub repo with zero issue comments, or a
// GitLab repo where the REST composition didn't deliver inline comments).
func (sc *StagedCollector) stageInlineIssueComments(ctx context.Context, sw *db.StagingWriter, comments []platform.MessageWithRef, result *CollectResult) {
	if len(comments) == 0 {
		return
	}
	for _, msg := range comments {
		if err := sw.Stage(ctx, EntityMessage, msg); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage inline issue message: %w", err))
			continue
		}
		result.Messages++
	}
	sc.logger.Info("staged inline issue comments", "count", len(comments))
}

// fullGraphQLMode reports whether this collector is set up to deliver
// PR conversation comments, issue conversation comments, and inline
// review comments through the phase 1+2 GraphQL batches — which makes
// the repo-wide REST collectMessages call redundant.
//
// Requires BOTH pr_child_mode AND listing_mode to be "graphql" AND the
// platform to be GitHub. GitLab's FetchPRBatch composes REST calls and
// does not populate StagedPR.Comments / StagedPR.ReviewComments, so
// GitLab repos must keep running the repo-wide REST fetch regardless
// of mode flags.
func (sc *StagedCollector) fullGraphQLMode() bool {
	return sc.prChildMode == "graphql" &&
		sc.listingMode == "graphql" &&
		sc.platID == int16(model.PlatformGitHub)
}

// collectParallel runs issues, PRs, and events concurrently in 3 goroutines,
// each with its own StagingWriter for thread safety. The parent waits for all
// three to complete before collecting messages. Claims 3 extra parallel slots
// from the global counter so fillWorkerSlots can respect the capacity.
func (sc *StagedCollector) collectParallel(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) {
	// Claim 3 extra parallel slots.
	ParallelSlots.Add(3)
	defer ParallelSlots.Add(-3)

	// Pre-enumerate once before forking goroutines when listingMode is
	// graphql. Calling ListIssuesAndPRs in each child goroutine would
	// double-fetch the entire data set.
	preIssues, prePRs, preIssueComments := sc.preEnumerateIfGraphQL(ctx, owner, repo, since, result)

	var wg sync.WaitGroup
	var mu sync.Mutex // protects result.Errors and counts

	// Each goroutine gets its own StagingWriter and CollectResult for
	// thread-safe staging. Results are merged under the mutex.
	wg.Add(3)

	// Goroutine 1: Issues + inline issue comments from the unified listing.
	go func() {
		defer wg.Done()
		issueSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
		localResult := &CollectResult{}
		sc.collectIssues(ctx, issueSW, owner, repo, since, localResult, preIssues)
		sc.stageInlineIssueComments(ctx, issueSW, preIssueComments, localResult)
		if err := issueSW.Flush(ctx); err != nil {
			localResult.Errors = append(localResult.Errors, fmt.Errorf("issue flush: %w", err))
		}
		mu.Lock()
		result.Issues += localResult.Issues
		result.Messages += localResult.Messages
		result.Errors = append(result.Errors, localResult.Errors...)
		mu.Unlock()
	}()

	// Goroutine 2: Pull Requests + inline PR / review comments (phase 4).
	go func() {
		defer wg.Done()
		prSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
		localResult := &CollectResult{}
		sc.collectPRs(ctx, prSW, owner, repo, since, localResult, prePRs)
		if err := prSW.Flush(ctx); err != nil {
			localResult.Errors = append(localResult.Errors, fmt.Errorf("pr flush: %w", err))
		}
		mu.Lock()
		result.PullRequests += localResult.PullRequests
		result.Messages += localResult.Messages
		result.Errors = append(result.Errors, localResult.Errors...)
		mu.Unlock()
	}()

	// Goroutine 3: Events
	go func() {
		defer wg.Done()
		eventSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
		localResult := &CollectResult{}
		sc.collectEvents(ctx, eventSW, owner, repo, since, localResult)
		if err := eventSW.Flush(ctx); err != nil {
			localResult.Errors = append(localResult.Errors, fmt.Errorf("event flush: %w", err))
		}
		mu.Lock()
		result.Events += localResult.Events
		result.Errors = append(result.Errors, localResult.Errors...)
		mu.Unlock()
	}()

	// Wait for all three parallel goroutines to finish.
	wg.Wait()
	sc.logger.Info("parallel collection complete",
		"issues", result.Issues, "prs", result.PullRequests, "events", result.Events)

	// Messages collect sequentially after parallel phase.
	// They need a fresh StagingWriter.
	msgSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
	sc.collectMessages(ctx, msgSW, owner, repo, since, result)
	if err := msgSW.Flush(ctx); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("message flush: %w", err))
	}
}

// collectIssues stages all issues with their labels and assignees.
// When preEnumerated is non-nil, it's the issue slice from an earlier
// unified ListIssuesAndPRs call (phase 2, listingMode=graphql); the
// iterator path is bypassed. When nil, the legacy ListIssues iterator
// drives enumeration (listingMode=rest, the default).
func (sc *StagedCollector) collectIssues(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult, preEnumerated []model.Issue) {
	mode := sc.listingMode
	if preEnumerated == nil && mode == "graphql" {
		// Pre-enumeration failed or wasn't done. Fall back to REST.
		mode = "rest"
	}
	sc.logger.Info("collecting issues", "owner", owner, "repo", repo, "listing_mode", mode)

	issues := preEnumerated
	if preEnumerated == nil {
		// Enumerate via the legacy REST iterator.
		for issue, err := range sc.client.ListIssues(ctx, owner, repo, since) {
			if err != nil {
				if isOptionalEndpointSkip(err) {
					sc.logger.Info("skipping issues endpoint",
						"owner", owner, "repo", repo, "reason", err)
					break
				}
				result.Errors = append(result.Errors, fmt.Errorf("issues: %w", err))
				break
			}
			issues = append(issues, issue)
		}
	}

	for _, issue := range issues {
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
			sc.logger.Info("issues progress", "owner", owner, "repo", repo, "staged", result.Issues, "listing_mode", mode)
		}
	}
	sc.logger.Info("issues staged", "count", result.Issues, "listing_mode", mode)
}

// collectPRs stages all pull requests with their children.
// When preEnumerated is non-nil (listingMode=graphql pre-fetched), it's
// used directly. When nil, the legacy ListPullRequests iterator drives
// enumeration.
func (sc *StagedCollector) collectPRs(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult, preEnumerated []model.PullRequest) {
	listMode := sc.listingMode
	if preEnumerated == nil && listMode == "graphql" {
		listMode = "rest" // pre-enumeration failed, fell back
	}
	sc.logger.Info("collecting pull requests", "owner", owner, "repo", repo, "mode", sc.prChildMode, "listing_mode", listMode)

	// Collect PR numbers either from the pre-enumerated slice (phase 2
	// graphql listing) or by iterating ListPullRequests (legacy REST).
	// Sharing this enumeration between rest and graphql child modes
	// eliminates a source of equivalence drift.
	var prs []model.PullRequest
	if preEnumerated != nil {
		prs = preEnumerated
	} else {
		for pr, err := range sc.client.ListPullRequests(ctx, owner, repo, since) {
			if err != nil {
				if isOptionalEndpointSkip(err) {
					sc.logger.Info("skipping pull requests endpoint",
						"owner", owner, "repo", repo, "reason", err)
					break
				}
				result.Errors = append(result.Errors, fmt.Errorf("pull requests: %w", err))
				break
			}
			prs = append(prs, pr)
		}
	}

	switch sc.prChildMode {
	case "graphql":
		sc.collectPRsGraphQL(ctx, sw, owner, repo, prs, result)
	default:
		sc.collectPRsREST(ctx, sw, owner, repo, prs, result)
	}
	sc.logger.Info("pull requests staged", "count", result.PullRequests, "mode", sc.prChildMode)
}

// collectPRsREST stages PRs using the per-PR REST child waterfall — 8
// HTTP calls per PR. The pre-v0.18.1 behavior, preserved as the default
// until the GraphQL path is validated in production.
func (sc *StagedCollector) collectPRsREST(ctx context.Context, sw *db.StagingWriter, owner, repo string, prs []model.PullRequest, result *CollectResult) {
	for _, pr := range prs {
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
		headRepo, baseRepo, err := sc.client.FetchPRRepos(ctx, owner, repo, pr.Number)
		if err == nil {
			envelope.RepoHead = headRepo
			envelope.RepoBase = baseRepo
		}
		if err := sw.Stage(ctx, EntityPullRequest, envelope); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.PullRequests++
		if result.PullRequests%100 == 0 {
			sc.logger.Info("pull requests progress", "owner", owner, "repo", repo, "staged", result.PullRequests, "mode", "rest")
		}
	}
}

// collectPRsGraphQL stages PRs using platform.Client.FetchPRBatch — one
// GraphQL query per batch of 25 PRs, children populated inline.
// Equivalent column-for-column with collectPRsREST.
//
// Branches on threadingMode. The default "single" path runs FetchPRBatch
// in the calling goroutine — pre-phase-3 behavior. When threadingMode
// is "sharded" AND len(prs) exceeds shardSize, the PR list is
// partitioned across computeShardCount(len(prs), shardSize) goroutines,
// each running its own FetchPRBatch chain. ParallelSlots is claimed for
// the extra goroutines so the scheduler's worker budget is respected.
//
// If FetchPRBatch returns an error classified as ClassSkip we swallow
// it (same policy as the REST path); any other error is surfaced in
// result.Errors to fail the job.
func (sc *StagedCollector) collectPRsGraphQL(ctx context.Context, sw *db.StagingWriter, owner, repo string, prs []model.PullRequest, result *CollectResult) {
	// Guard: sharded mode only fans out when there's enough work to
	// justify the goroutine overhead AND the operator has opted in.
	if sc.threadingMode != "sharded" || len(prs) <= sc.shardSize {
		sc.runPRBatchSingle(ctx, sw, owner, repo, prs, result)
		return
	}
	sc.runPRBatchSharded(ctx, sw, owner, repo, prs, result)
}

// runPRBatchSingle is the pre-phase-3 path: one goroutine, one
// FetchPRBatch chain driven by the platform client's internal
// prBatchSize (25). Kept as a distinct function so the shard workers
// can reuse it and so the source-contract test's single-mode guard
// has a clear target.
func (sc *StagedCollector) runPRBatchSingle(ctx context.Context, sw *db.StagingWriter, owner, repo string, prs []model.PullRequest, result *CollectResult) {
	numbers := make([]int, 0, len(prs))
	for _, pr := range prs {
		numbers = append(numbers, pr.Number)
	}

	batch, err := sc.client.FetchPRBatch(ctx, owner, repo, numbers)
	if err != nil {
		if isOptionalEndpointSkip(err) {
			sc.logger.Info("skipping pull requests graphql batch",
				"owner", owner, "repo", repo, "reason", err)
			return
		}
		result.Errors = append(result.Errors, fmt.Errorf("pull requests graphql batch: %w", err))
		return
	}
	stagePRBatch(ctx, sw, batch, result, sc.logger, owner, repo)
}

// runPRBatchSharded fans out PR batch fetching across N goroutines,
// each owning a disjoint slice of the enumerated PR list. Every shard
// gets its own StagingWriter for thread-safety; results are merged
// under a mutex at the end.
//
// Calls ParallelSlots.Add(shards-1) / -(shards-1) to inform the
// scheduler that this job is consuming (shards-1) additional worker
// slots beyond the one already granted — consistent with how
// collectParallel handles its 3-way fan-out.
func (sc *StagedCollector) runPRBatchSharded(ctx context.Context, sw *db.StagingWriter, owner, repo string, prs []model.PullRequest, result *CollectResult) {
	shardCount := computeShardCount(len(prs), sc.shardSize)
	if shardCount <= 1 {
		// Safety: the guard in collectPRsGraphQL above should have
		// short-circuited this case. If we got here anyway, fall
		// back to single-mode rather than spin up one goroutine.
		sc.runPRBatchSingle(ctx, sw, owner, repo, prs, result)
		return
	}

	sc.logger.Info("sharding pull request collection",
		"owner", owner, "repo", repo,
		"prs", len(prs), "shard_size", sc.shardSize, "shards", shardCount)

	// Claim (shardCount-1) extra parallel slots. The 1 slot this job
	// already has covers the first shard; each additional shard gets
	// its own.
	ParallelSlots.Add(int32(shardCount - 1))
	defer ParallelSlots.Add(-int32(shardCount - 1))

	partitions := partitionShards(prs, shardCount)

	var wg sync.WaitGroup
	var mu sync.Mutex
	// stagedNumbers accumulates the PR numbers that successfully made
	// it into the staging writer across all shards. After the join,
	// we diff this against the full enumerated list (prs); any PR in
	// the enumeration that didn't land in staging gets a second-chance
	// FetchPRBatch call — the "reconcile-by-set-diff" pass from the
	// phase 3 design.
	var stagedNumbers []int

	wg.Add(shardCount)
	for shardIdx, shardPRs := range partitions {
		go func(idx int, prs []model.PullRequest) {
			defer wg.Done()
			if len(prs) == 0 {
				return
			}
			numbers := make([]int, 0, len(prs))
			for _, pr := range prs {
				numbers = append(numbers, pr.Number)
			}
			batch, err := sc.client.FetchPRBatch(ctx, owner, repo, numbers)
			if err != nil {
				mu.Lock()
				if isOptionalEndpointSkip(err) {
					sc.logger.Info("shard skipping pull requests graphql batch",
						"owner", owner, "repo", repo, "shard", idx, "reason", err)
				} else {
					result.Errors = append(result.Errors, fmt.Errorf("pull requests graphql batch shard %d: %w", idx, err))
				}
				mu.Unlock()
				return
			}
			// Merge this shard's results under the mutex so result
			// counts and log lines are coherent.
			mu.Lock()
			defer mu.Unlock()
			stagePRBatch(ctx, sw, batch, result, sc.logger, owner, repo)
			for _, s := range batch {
				stagedNumbers = append(stagedNumbers, s.PR.Number)
			}
		}(shardIdx, shardPRs)
	}
	wg.Wait()

	// Reconcile: identify any enumerated PR that failed to land in
	// staging and re-fetch in one corrective pass. Phase 3's completeness
	// safety net — shard errors, partial FetchPRBatch returns, or even a
	// transient rate-limit mid-shard all get a second chance here.
	enumerated := make([]int, 0, len(prs))
	for _, pr := range prs {
		enumerated = append(enumerated, pr.Number)
	}
	missing := missingPRsFromSet(enumerated, stagedNumbers)
	// Some of the "missing" PRs may legitimately have been returned null
	// from GitHub (deleted/inaccessible mid-collection). A single retry
	// with a fresh FetchPRBatch call is enough — if they're still null,
	// they're truly gone. We log the final missing count for ops.
	if len(missing) > 0 {
		sc.logger.Info("reconcile: refetching PRs missed by shards",
			"owner", owner, "repo", repo, "missing_count", len(missing))
		batch, err := sc.client.FetchPRBatch(ctx, owner, repo, missing)
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("reconcile: skippable error on refetch",
					"owner", owner, "repo", repo, "reason", err)
			} else {
				result.Errors = append(result.Errors, fmt.Errorf("pull requests reconcile refetch: %w", err))
			}
			return
		}
		stagePRBatch(ctx, sw, batch, result, sc.logger, owner, repo)
		// Compute final residual — PRs that didn't even come back on
		// the retry. These are almost certainly deleted; log and move on.
		retryStaged := make([]int, 0, len(batch))
		for _, s := range batch {
			retryStaged = append(retryStaged, s.PR.Number)
		}
		stillMissing := missingPRsFromSet(missing, retryStaged)
		if len(stillMissing) > 0 {
			sc.logger.Warn("reconcile: PRs still missing after refetch (likely deleted on GitHub)",
				"owner", owner, "repo", repo, "count", len(stillMissing),
				"sample_numbers", sampleInts(stillMissing, 10))
		}
	}
}

// sampleInts returns up to n items from the front of v for log output —
// keeps log lines bounded when the missing list is large.
func sampleInts(v []int, n int) []int {
	if len(v) <= n {
		return v
	}
	return v[:n]
}

// stagePRBatch is the shared tail of both single-shard and per-shard
// PR-batch processing: takes the fetched []platform.StagedPR, stages
// each into the supplied writer, updates result counts, and logs
// progress. Caller owns any locking around result / sw.
func stagePRBatch(ctx context.Context, sw *db.StagingWriter, batch []platform.StagedPR, result *CollectResult, logger *slog.Logger, owner, repo string) {
	for _, s := range batch {
		envelope := stagedPR{
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
		}
		if err := sw.Stage(ctx, EntityPullRequest, envelope); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.PullRequests++
		if result.PullRequests%100 == 0 {
			logger.Info("pull requests progress", "owner", owner, "repo", repo, "staged", result.PullRequests, "mode", "graphql")
		}

		// Phase 4: stage inline conversation comments and inline review
		// comments delivered with the PR node. GitLab's FetchPRBatch
		// returns empty slices here (its REST composition doesn't
		// populate them), so this is a no-op unless the caller routed
		// through GitHub's GraphQL path.
		for _, cm := range s.Comments {
			if err := sw.Stage(ctx, EntityMessage, cm); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage inline pr comment: %w", err))
				continue
			}
			result.Messages++
		}
		for _, rc := range s.ReviewComments {
			if err := sw.Stage(ctx, EntityReviewComment, rc); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage inline review comment: %w", err))
				continue
			}
			result.Messages++
		}
	}
}

// collectEvents stages issue and PR events.
func (sc *StagedCollector) collectEvents(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult) {
	sc.logger.Info("collecting events", "owner", owner, "repo", repo)
	for event, err := range sc.client.ListIssueEvents(ctx, owner, repo, since) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping issue events endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("issue events: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityIssueEvent, event); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage issue event: %w", err))
		}
		result.Events++
	}
	for event, err := range sc.client.ListPREvents(ctx, owner, repo, since) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping pr events endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("pr events: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityPREvent, event); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage pr event: %w", err))
		}
		result.Events++
	}
	sc.logger.Info("events staged", "count", result.Events)
}

// collectMessages stages issue comments, PR comments, and review comments
// via the repo-wide REST endpoints (/issues/comments + /pulls/comments).
//
// Phase 4 skip: when both pr_child_mode AND listing_mode are "graphql"
// on GitHub, the comments arrive inline with the phase-1 PR batch and
// the phase-2 issue listing and are already staged by stagePRBatch +
// stageInlineIssueComments. The two repo-wide REST iterators become
// duplicate work, so we skip them entirely. On GitLab (or when either
// mode is rest), the REST path remains the authoritative comment source.
func (sc *StagedCollector) collectMessages(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult) {
	if sc.fullGraphQLMode() {
		sc.logger.Info("collectMessages skipped — comments delivered inline by graphql phases",
			"owner", owner, "repo", repo)
		return
	}
	sc.logger.Info("collecting messages", "owner", owner, "repo", repo)
	for msg, err := range sc.client.ListIssueComments(ctx, owner, repo, since) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping issue comments endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("issue comments: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityMessage, msg); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage message: %w", err))
		}
		result.Messages++
	}
	for rc, err := range sc.client.ListReviewComments(ctx, owner, repo, since) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping review comments endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("review comments: %w", err))
			break
		}
		if err := sw.Stage(ctx, EntityReviewComment, rc); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("stage review comment: %w", err))
		}
		result.Messages++
	}
	sc.logger.Info("messages staged", "count", result.Messages)
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

	// Order matters: repo_info is processed FIRST so metadata counts
	// (used by the monitor and gap fill) survive even if processing is
	// interrupted. Contributors must exist before FK resolution in
	// issues/PRs/events/messages.
	entityTypes := []string{
		EntityRepoInfo,
		EntityCloneStats,
		EntityRelease,
		EntityContributor,
		EntityIssue,
		EntityPullRequest,
		EntityIssueEvent,
		EntityPREvent,
		EntityMessage,
		EntityReviewComment,
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
	if err := p.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:                repoID,
		CoreStatus:            status,
		CoreDataLastCollected: &now,
	}); err != nil {
		p.logger.Warn("failed to update final processing status", "repo_id", repoID, "error", err)
	}

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
		var headMetaID, baseMetaID int64
		if env.MetaHead != nil {
			env.MetaHead.PRID = prID
			env.MetaHead.RepoID = repoID
			var metaErr error
			headMetaID, metaErr = p.store.UpsertPRMeta(ctx, env.MetaHead)
			if metaErr != nil {
				p.logger.Warn("failed to upsert PR meta (head)", "pr_id", prID, "error", metaErr)
			}
		}
		if env.MetaBase != nil {
			env.MetaBase.PRID = prID
			env.MetaBase.RepoID = repoID
			var metaErr error
			baseMetaID, metaErr = p.store.UpsertPRMeta(ctx, env.MetaBase)
			if metaErr != nil {
				p.logger.Warn("failed to upsert PR meta (base)", "pr_id", prID, "error", metaErr)
			}
		}
		// Insert fork repo details linked to their corresponding meta rows.
		if env.RepoHead != nil && headMetaID != 0 {
			env.RepoHead.MetaID = headMetaID
			if err := p.store.UpsertPRRepo(ctx, env.RepoHead); err != nil {
				p.logger.Warn("failed to upsert PR repo (head)", "pr_id", prID, "error", err)
			}
		}
		if env.RepoBase != nil && baseMetaID != 0 {
			env.RepoBase.MetaID = baseMetaID
			if err := p.store.UpsertPRRepo(ctx, env.RepoBase); err != nil {
				p.logger.Warn("failed to upsert PR repo (base)", "pr_id", prID, "error", err)
			}
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
		if err := p.store.RotateRepoInfoToHistory(ctx, repoID); err != nil {
			p.logger.Warn("failed to rotate repo info to history", "repo_id", repoID, "error", err)
		}
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
