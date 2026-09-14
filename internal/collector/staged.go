// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/safego"
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
	client         platform.Client
	store          *db.PostgresStore
	logger         *slog.Logger
	platID         int16
	prChildMode    string // "rest" (default) or "graphql" — see CollectionConfig.PRChildMode
	listingMode    string // "rest" (default) or "graphql" — see CollectionConfig.ListingMode
	threadingMode  string // "single" (default) or "sharded" — see CollectionConfig.ThreadingMode
	shardSize      int    // item-count threshold for sharded fan-out (default 3000)
	issueChildMode string // "rest" (default) or "graphql" — phase 5: inline issue label+assignee fetch via the GraphQL listing
	workers        int    // scheduler worker pool size — feeds parallelSlotsForWorkers
}

// WithWorkers records the scheduler's worker-pool size on a
// StagedCollector so collectParallel can scale its ParallelSlots
// claim (see parallelSlotsForWorkers). Returns the same collector
// for chaining. A zero or negative value leaves the legacy 3-slot
// fallback in place.
func (sc *StagedCollector) WithWorkers(n int) *StagedCollector {
	if n < 0 {
		n = 0
	}
	sc.workers = n
	return sc
}

// WithIssueChildMode sets the phase 5 mode controlling whether issue
// labels + assignees are fetched via per-issue REST iterators (the
// legacy path) or drained from the inline maps delivered by the
// GraphQL listing. Unknown values collapse to "rest". Returns the
// same collector for chaining. Kept as a chainable setter (rather
// than a new constructor) because the phase 1/2/3 constructor
// signature is already at 7 parameters.
func (sc *StagedCollector) WithIssueChildMode(mode string) *StagedCollector {
	if mode != "graphql" {
		mode = "rest"
	}
	sc.issueChildMode = mode
	return sc
}

// CollectionModes bundles the per-instance collection-path selections
// (v0.27.42, summary/18 Phase 4 — replaces the four-constructor ladder
// that grew one positional parameter per phase). Zero value = the
// safest defaults: REST everywhere, single-goroutine, default shard
// size. Unknown strings collapse to those defaults in
// NewStagedCollectorWithOptions, so a misspelled config fails safe.
type CollectionModes struct {
	PRChildMode    string // "rest" (default) | "graphql"
	ListingMode    string // "rest" (default) | "graphql"
	ThreadingMode  string // "single" (default) | "sharded"
	IssueChildMode string // "rest" (default) | "graphql"
	ShardSize      int    // <= 0 → defaultShardSize
}

// NewStagedCollector creates a staged collector in the fully-default
// mode: REST per-PR child waterfall, REST issue/PR listing,
// single-goroutine PR batch execution.
func NewStagedCollector(client platform.Client, store *db.PostgresStore, logger *slog.Logger) *StagedCollector {
	return NewStagedCollectorWithOptions(client, store, logger, CollectionModes{})
}

// NewStagedCollectorWithAllModes is the legacy positional constructor
// (phases 1–3). Prefer NewStagedCollectorWithOptions; kept because the
// mode semantics are pinned by a broad test surface.
func NewStagedCollectorWithAllModes(client platform.Client, store *db.PostgresStore, logger *slog.Logger, prChildMode, listingMode, threadingMode string, shardSize int) *StagedCollector {
	return NewStagedCollectorWithOptions(client, store, logger, CollectionModes{
		PRChildMode:   prChildMode,
		ListingMode:   listingMode,
		ThreadingMode: threadingMode,
		ShardSize:     shardSize,
	})
}

// NewStagedCollectorWithOptions is THE constructor. Unknown string
// modes collapse to their safest defaults (rest, rest, single, rest);
// ShardSize <= 0 collapses to defaultShardSize.
func NewStagedCollectorWithOptions(client platform.Client, store *db.PostgresStore, logger *slog.Logger, m CollectionModes) *StagedCollector {
	if m.PRChildMode != "graphql" {
		m.PRChildMode = "rest"
	}
	if m.ListingMode != "graphql" {
		m.ListingMode = "rest"
	}
	if m.ThreadingMode != "sharded" {
		m.ThreadingMode = "single"
	}
	if m.IssueChildMode != "graphql" {
		m.IssueChildMode = "rest"
	}
	if m.ShardSize <= 0 {
		m.ShardSize = defaultShardSize
	}
	return &StagedCollector{
		client:         client,
		store:          store,
		logger:         logger,
		platID:         int16(client.Platform()),
		prChildMode:    m.PRChildMode,
		listingMode:    m.ListingMode,
		threadingMode:  m.ThreadingMode,
		shardSize:      m.ShardSize,
		issueChildMode: m.IssueChildMode,
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
		if errors.Is(err, context.Canceled) {
			return nil, err // shutdown before the first phase (pass 35)
		}
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

		// v0.23.0: mirror description + primary_language + full
		// language breakdown to the repos row. The repos table is
		// the canonical "what is this repo" reference; repo_info
		// holds per-cycle metrics, but description/language are
		// slow-changing reference data so they live on repos.
		// Non-fatal — if the UPDATE fails we log and continue;
		// the next cycle will retry.
		if updateErr := sc.store.UpdateRepoMetadata(ctx, repoID, info.Description, info.PrimaryLanguage, info.Languages, info.Status == "Archived", info.ForkedFrom(), info.PlatformRepoID, info.CreatedAt, info.LastUpdated); updateErr != nil {
			// Round-8 burn-down: a cancelled context is a `stop serve`, not a
			// defect. Only the log is suppressed — surrounding behaviour is
			// unchanged and the work is retried on the next cycle.
			if !errors.Is(updateErr, context.Canceled) {
				sc.logger.Warn("failed to update repos.repo_description/primary_language/languages",
					"owner", owner, "repo", repo, "error", updateErr)
			}
		}

		// v0.25.32: case self-heal. The forge returns the CANONICAL
		// owner/name spelling regardless of the casing we queried with;
		// when the stored value differs only by case, correct repo_git/
		// repo_owner/repo_name via the rename machinery. Real renames
		// stay prelim's job (HealRepoCaseDrift enforces the case-only
		// gate). Non-fatal: cosmetic case drift must never fail a
		// collection job, so failures log and continue.
		if info.FullName != "" {
			if healed, healErr := sc.store.HealRepoCaseDrift(ctx, repoID, info.FullName); healErr != nil {
				// Round-8 burn-down: a cancelled context is a `stop serve`, not a
				// defect. Only the log is suppressed — surrounding behaviour is
				// unchanged and the work is retried on the next cycle.
				if !errors.Is(healErr, context.Canceled) {
					sc.logger.Warn("case-drift self-heal failed",
						"owner", owner, "repo", repo, "error", healErr)
				}
			} else if healed {
				sc.logger.Info("healed repo case drift",
					"repo_id", repoID, "canonical", info.FullName)
			}
		}
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
		if result.Contributors%100 == 0 {
			sc.logger.Info("contributors progress",
				"owner", owner, "repo", repo, "staged", result.Contributors)
		}
	}
	sc.logger.Info("contributors staged", "owner", owner, "repo", repo, "count", result.Contributors)

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

	if ctx.Err() != nil {
		return result, ctx.Err() // shutdown: never "complete" with zeros (pass 37)
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
	pre := sc.preEnumerateIfGraphQL(ctx, owner, repo, since, result)
	sc.collectIssues(ctx, sw, owner, repo, since, result, pre.issues, pre.issueLabels, pre.issueAssignees)
	sc.stageInlineIssueComments(ctx, sw, pre.issueComments, result)
	sc.collectPRs(ctx, sw, owner, repo, since, result, pre.prs)
	sc.collectEvents(ctx, sw, owner, repo, since, result)
	sc.collectMessages(ctx, sw, owner, repo, since, result)
}

// preEnumerateBatch carries the result of the unified GraphQL
// issue+PR listing. issues and prs are non-nil empty slices when the
// listing succeeded with zero items, so the caller can distinguish
// "pre-fetched, found none" from "not pre-fetched, use iterator".
// issueLabels and issueAssignees are nil unless phase 5's
// IssueChildMode is "graphql" (kept nil otherwise so collectIssues
// can short-circuit cheaply).
type preEnumerateBatch struct {
	issues         []model.Issue
	prs            []model.PullRequest
	issueComments  []platform.MessageWithRef
	issueLabels    map[int][]model.IssueLabel
	issueAssignees map[int][]model.IssueAssignee
}

// preEnumerateIfGraphQL does the unified GraphQL issue+PR listing once
// when listingMode=graphql, returning the issue and PR slices plus the
// inline issue comments (phase 4) and the inline issue labels +
// assignees (phase 5) delivered with the listing. In listingMode=rest
// it returns an empty batch — the legacy per-path iterators in
// collectIssues and collectPRs remain in charge.
//
// Non-fatal errors from the unified listing are logged and the function
// returns an empty batch so collection falls through to the legacy
// iterators. This way a transient GraphQL problem doesn't take down the
// entire repo's collection — we just lose the speedup for this cycle.
func (sc *StagedCollector) preEnumerateIfGraphQL(ctx context.Context, owner, repo string, since time.Time, _ *CollectResult) preEnumerateBatch {
	if sc.listingMode != "graphql" {
		return preEnumerateBatch{}
	}
	batch, err := sc.client.ListIssuesAndPRs(ctx, owner, repo, since)
	if errors.Is(err, context.Canceled) {
		// Shutdown mid-listing (the common shape on a big repo): no
		// partial staging on the dead ctx, no fallback WARN — the REST
		// fallback fails fast and the job ends unrecorded (pass 36).
		return preEnumerateBatch{}
	}
	if err != nil {
		// Surface partial results even on error: ListIssuesAndPRs (phase 5)
		// returns the batch with whatever issues/labels/assignees made it
		// before the error, so we still stage them rather than discarding
		// all the work done so far.
		if batch != nil && (len(batch.Issues) > 0 || len(batch.PullRequests) > 0) {
			sc.logger.Warn("unified GraphQL listing errored after partial results — staging what we got, then falling back to REST iterators for the rest",
				"owner", owner, "repo", repo, "error", err,
				"partial_issues", len(batch.Issues), "partial_prs", len(batch.PullRequests))
			return preEnumerateBatch{
				issues:         batch.Issues,
				prs:            batch.PullRequests,
				issueComments:  batch.IssueComments,
				issueLabels:    batch.IssueLabels,
				issueAssignees: batch.IssueAssignees,
			}
		}
		if isOptionalEndpointSkip(err) {
			sc.logger.Info("unified listing skipped — falling back to REST iterators",
				"owner", owner, "repo", repo, "reason", err)
			return preEnumerateBatch{}
		}
		sc.logger.Warn("unified GraphQL listing failed — falling back to REST iterators",
			"owner", owner, "repo", repo, "error", err)
		return preEnumerateBatch{}
	}
	sc.logger.Info("unified listing complete",
		"owner", owner, "repo", repo,
		"issues", len(batch.Issues), "prs", len(batch.PullRequests),
		"inline_issue_comments", len(batch.IssueComments),
		"inline_issue_labels_buckets", len(batch.IssueLabels),
		"inline_issue_assignees_buckets", len(batch.IssueAssignees))
	issues := batch.Issues
	if issues == nil {
		issues = []model.Issue{}
	}
	prs := batch.PullRequests
	if prs == nil {
		prs = []model.PullRequest{}
	}
	return preEnumerateBatch{
		issues:         issues,
		prs:            prs,
		issueComments:  batch.IssueComments,
		issueLabels:    batch.IssueLabels,
		issueAssignees: batch.IssueAssignees,
	}
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
		result.InlineIssueComments++
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
// three to complete before collecting messages. Claims ParallelSlots so
// fillWorkerSlots can respect the reserved capacity — see
// parallelSlotsForWorkers for why the claim scales with worker count.
func (sc *StagedCollector) collectParallel(ctx context.Context, repoID int64, owner, repo string, since time.Time, result *CollectResult) {
	// Claim extra parallel slots scaled to the worker pool so the
	// scheduler's throttle rule actually engages on large fleets.
	slots := int32(parallelSlotsForWorkers(sc.workers))
	ParallelSlots.Add(slots)
	defer ParallelSlots.Add(-slots)

	// Pre-enumerate once before forking goroutines when listingMode is
	// graphql. Calling ListIssuesAndPRs in each child goroutine would
	// double-fetch the entire data set.
	pre := sc.preEnumerateIfGraphQL(ctx, owner, repo, since, result)

	var wg sync.WaitGroup
	var mu sync.Mutex // protects result.Errors and counts

	// Each goroutine gets its own StagingWriter and CollectResult for
	// thread-safe staging. Results are merged under the mutex.
	wg.Add(3)

	// Goroutine 1: Issues + inline issue comments from the unified listing.
	go func() {
		defer safego.Recover(sc.logger, "collect-issues")
		defer wg.Done()
		issueSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
		localResult := &CollectResult{}
		sc.collectIssues(ctx, issueSW, owner, repo, since, localResult, pre.issues, pre.issueLabels, pre.issueAssignees)
		sc.stageInlineIssueComments(ctx, issueSW, pre.issueComments, localResult)
		if err := issueSW.Flush(ctx); err != nil {
			localResult.Errors = append(localResult.Errors, fmt.Errorf("issue flush: %w", err))
		}
		mu.Lock()
		result.Issues += localResult.Issues
		result.Messages += localResult.Messages
		// v0.27.128: the inline-comment counters must merge too — the
		// v0.22.4 collectMessages diagnostic line read them and always
		// logged 0 in parallel mode (a "log the effective value" bug;
		// staging itself was never affected).
		result.InlineIssueComments += localResult.InlineIssueComments
		result.Errors = append(result.Errors, localResult.Errors...)
		mu.Unlock()
	}()

	// Goroutine 2: Pull Requests + inline PR / review comments (phase 4).
	go func() {
		defer safego.Recover(sc.logger, "collect-prs")
		defer wg.Done()
		prSW := db.NewStagingWriter(sc.store, repoID, sc.platID, sc.logger)
		localResult := &CollectResult{}
		sc.collectPRs(ctx, prSW, owner, repo, since, localResult, pre.prs)
		if err := prSW.Flush(ctx); err != nil {
			localResult.Errors = append(localResult.Errors, fmt.Errorf("pr flush: %w", err))
		}
		mu.Lock()
		result.PullRequests += localResult.PullRequests
		result.Messages += localResult.Messages
		result.InlinePRComments += localResult.InlinePRComments
		result.Errors = append(result.Errors, localResult.Errors...)
		mu.Unlock()
	}()

	// Goroutine 3: Events
	go func() {
		defer safego.Recover(sc.logger, "collect-messages")
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
//
// Phase 5: when issueChildMode=="graphql" AND the preLabels /
// preAssignees maps are non-nil (delivered by ListIssuesAndPRs on
// GitHub), labels and assignees are drained from the maps by issue
// number — the per-issue ListIssueLabels and ListIssueAssignees REST
// calls are skipped. In every other configuration (rest mode, GitLab
// REST composition, GraphQL listing fell back) the legacy per-issue
// REST iterators run as before.
func (sc *StagedCollector) collectIssues(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult, preEnumerated []model.Issue, preLabels map[int][]model.IssueLabel, preAssignees map[int][]model.IssueAssignee) {
	mode := sc.listingMode
	if preEnumerated == nil && mode == "graphql" {
		// Pre-enumeration failed or wasn't done. Fall back to REST.
		mode = "rest"
	}
	// Phase 5 inline-child mode fires only when explicitly enabled AND
	// the inline maps are present (GitHub graphql path). On GitLab
	// FetchPRBatch-style REST composition the maps stay nil and we
	// fall through to the legacy REST iterators below.
	inlineChildren := sc.issueChildMode == "graphql" && (preLabels != nil || preAssignees != nil)
	sc.logger.Info("collecting issues",
		"owner", owner, "repo", repo,
		"listing_mode", mode,
		"issue_child_mode", sc.issueChildMode,
		"inline_children", inlineChildren)

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
		if inlineChildren {
			// Phase 5: labels + assignees came inline with the listing.
			// preLabels and preAssignees are nil-safe to index (a nil map
			// returns the zero value, which is a nil slice — fine).
			envelope.Labels = preLabels[issue.Number]
			envelope.Assignees = preAssignees[issue.Number]
		} else {
			// Legacy REST path: two per-issue iterators.
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
		}
		if err := sw.Stage(ctx, EntityIssue, envelope); err != nil {
			result.Errors = append(result.Errors, err)
		}
		result.Issues++
		if result.Issues%100 == 0 {
			sc.logger.Info("issues progress", "owner", owner, "repo", repo, "staged", result.Issues, "listing_mode", mode)
		}
	}
	sc.logger.Info("issues staged", "owner", owner, "repo", repo, "count", result.Issues, "listing_mode", mode)
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
	sc.logger.Info("pull requests staged", "owner", owner, "repo", repo, "count", result.PullRequests, "mode", sc.prChildMode)
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
			defer safego.Recover(sc.logger, "pr-shard")
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

		// Phase 4: stage inline PR conversation comments delivered with
		// the PR node. Inline diff-anchored review comments are NOT
		// fetched via GraphQL (see platform.StagedPR.Comments godoc for
		// why) — they continue to arrive through the repo-wide REST
		// /pulls/comments endpoint in collectMessages. GitLab's
		// FetchPRBatch leaves s.Comments empty, so this is a no-op on
		// GitLab repos.
		for _, cm := range s.Comments {
			if err := sw.Stage(ctx, EntityMessage, cm); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage inline pr comment: %w", err))
				continue
			}
			result.Messages++
			result.InlinePRComments++
		}
	}
}

// collectEvents stages issue and PR events.
func (sc *StagedCollector) collectEvents(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult) {
	sc.logger.Info("collecting events", "owner", owner, "repo", repo)
	// Single pass over the unified feed (v0.26.3). The previous two
	// sequential iterations (issue events, then PR events) each
	// paginated the SAME GitHub endpoint; the second pass 304'd
	// against the ETag the first pass primed and silently dropped the
	// entire PR-event history on quiet repos.
	for ev, err := range sc.client.ListRepoEvents(ctx, owner, repo, since) {
		if err != nil {
			if isOptionalEndpointSkip(err) {
				sc.logger.Info("skipping events endpoint",
					"owner", owner, "repo", repo, "reason", err)
				break
			}
			result.Errors = append(result.Errors, fmt.Errorf("repo events: %w", err))
			break
		}
		switch {
		case ev.PR != nil:
			if err := sw.Stage(ctx, EntityPREvent, *ev.PR); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage pr event: %w", err))
			}
			result.Events++
		case ev.Issue != nil:
			if err := sw.Stage(ctx, EntityIssueEvent, *ev.Issue); err != nil {
				result.Errors = append(result.Errors, fmt.Errorf("stage issue event: %w", err))
			}
			result.Events++
		}
	}
	sc.logger.Info("events staged", "owner", owner, "repo", repo, "count", result.Events)
}

// collectMessages stages issue + PR conversation comments (via repo-wide
// /issues/comments) and diff-anchored review comments (via repo-wide
// /pulls/comments).
//
// Phase 4 partial skip: when pr_child_mode AND listing_mode are both
// "graphql" on GitHub, the issue + PR conversation comments arrive inline
// from the phase-1 PR batch and the phase-2 issue listing and are already
// staged by stagePRBatch + stageInlineIssueComments. In that mode the
// /issues/comments iterator is redundant and gets skipped.
//
// The /pulls/comments (review-inline) iterator is NOT skipped even in
// full-GraphQL mode. GitHub's GraphQL `PullRequestReviewComment` omits
// the `side` / `startSide` fields the REST schema carries, and deriving
// them from `line`/`originalLine` is not bijective on context-line
// comments. Running ListReviewComments here preserves byte-for-byte
// fidelity on `review_comments.pr_cmt_side` / `pr_cmt_start_side` and
// gives shadow-diff a clean comparison against the REST shadow. The
// trade-off is one extra REST call per collection — /pulls/comments
// returns far fewer rows than /issues/comments so the net phase-4
// speedup is preserved.
func (sc *StagedCollector) collectMessages(ctx context.Context, sw *db.StagingWriter, owner, repo string, since time.Time, result *CollectResult) {
	full := sc.fullGraphQLMode()
	if full {
		sc.logger.Info("collectMessages phase plan: issue + PR conversation comments arrived inline via GraphQL listing; "+
			"repo-wide /issues/comments REST iterator intentionally skipped to avoid duplicate work; "+
			"/pulls/comments REST iterator IS running here for review_comments.pr_cmt_side / pr_cmt_start_side fidelity "+
			"(GitHub GraphQL PullRequestReviewComment omits those fields)",
			"owner", owner, "repo", repo,
			"issue_inline_comments", result.InlineIssueComments,
			"pr_inline_comments", result.InlinePRComments)
	} else {
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
		if sc.platID == int16(model.PlatformGitLab) {
			// GitLab keeps MR conversation notes on a per-MR endpoint
			// (GitHub's repo-wide /issues/comments covers PRs too, so its
			// ListPRComments delegates and would duplicate here). Pass 31
			// (v0.28.18): this walk had NO production caller — merged and
			// closed MRs' conversation threads were never collected on the
			// main path; only the open-item refresher and gap fill read them.
			for msg, err := range sc.client.ListPRComments(ctx, owner, repo, since) {
				if err != nil {
					if isOptionalEndpointSkip(err) {
						sc.logger.Info("skipping MR comments endpoint",
							"owner", owner, "repo", repo, "reason", err)
						break
					}
					result.Errors = append(result.Errors, fmt.Errorf("mr comments: %w", err))
					break
				}
				if err := sw.Stage(ctx, EntityMessage, msg); err != nil {
					result.Errors = append(result.Errors, fmt.Errorf("stage message: %w", err))
				}
				result.Messages++
			}
		}
	}
	reviewCount := 0
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
		reviewCount++
		if reviewCount%1000 == 0 {
			sc.logger.Info("review comments progress",
				"owner", owner, "repo", repo, "staged", reviewCount)
		}
	}
	sc.logger.Info("messages staged", "owner", owner, "repo", repo, "count", result.Messages)
}

// Processor drains the staging table and writes to the relational schema.
// Contributor resolution happens here, in bulk, to minimize contention.
type Processor struct {
	store    *db.PostgresStore
	resolver *db.ContributorResolver
	logger   *slog.Logger
	errors   int // count of individual row processing failures
	// unresolvableRefs counts messages whose parent could not be
	// resolved (no number, no id). v0.27.37 (summary/18 Phase 1b):
	// this skip path silently dropped EVERY GitLab conversation
	// comment on the main path for the product's whole life because
	// the client never set the parent number — one aggregate WARN per
	// repo makes the class impossible to lose silently again.
	unresolvableRefs int
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
		// v0.27.53: progress visibility only — the tracker counts rows
		// AFTER a successful processBatch and never affects the error
		// path. Before this, a pytorch-class repo (1.32M staged
		// messages) spent 4+ days here with zero log output.
		prog := newProcessProgress(p.logger, repoID, entityType, processProgressEvery)
		if err := p.store.ProcessStaged(ctx, repoID, entityType, processBatchSize, func(rows []db.StagedRow) error {
			if err := p.processBatch(ctx, repoID, platID, entityType, rows); err != nil {
				return err
			}
			prog.add(len(rows))
			return nil
		}); err != nil {
			// v0.27.28: cancellation is the shutdown asking us to stop
			// mid-flush — expected, resumable (staging rows stay
			// unprocessed and drain on restart), and not an ERROR.
			// These two lines were the only ERROR-level entries in the
			// 2026-07-21 shutdown's 600-line noise burst.
			if errors.Is(err, context.Canceled) {
				p.logger.Info("entity processing aborted by shutdown", "type", entityType)
			} else {
				p.logger.Error("failed to process entity type", "type", entityType, "error", err)
			}
			return err
		}
		prog.finish()
	}

	// v0.26.5: derive issues.closed_by_id from the latest 'closed'
	// event per issue. closed_by is structurally absent from every LIST
	// endpoint (REST list has no closed_by; GraphQL Issue has no
	// closedBy field) — production had 775 of 5.26M closed issues
	// attributed. Events are processed by this point, so the derivation
	// is platform- and mode-agnostic with zero API cost.
	if n, err := p.store.DeriveIssueClosedByFromEvents(ctx, repoID); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			p.logger.Warn("closed_by derivation failed", "repo_id", repoID, "error", err)
		}
	} else if n > 0 {
		p.logger.Info("derived issue closed_by from events", "repo_id", repoID, "count", n)
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
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			p.logger.Warn("failed to update final processing status", "repo_id", repoID, "error", err)
		}
	}

	if p.unresolvableRefs > 0 {
		p.logger.Warn("messages skipped: parent reference unresolvable (no number, no id) — a platform client is emitting refs the processor cannot link",
			"repo_id", repoID, "skipped", p.unresolvableRefs)
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
				if errors.Is(err, context.Canceled) {
					return err // shutdown: ProcessRepo reports it once
				}
				p.logger.Warn("failed to upsert contributor batch", "count", len(contribs), "error", err)
				p.errors += len(contribs)
			}
		}
		return nil
	}

	// All other entity types: process one at a time.
	var errCount int
	for _, row := range rows {
		if ctx.Err() != nil {
			return ctx.Err() // shutdown mid-batch: one exit, not one WARN per remaining row (pass 36)
		}
		if err := p.processOne(ctx, repoID, platID, entityType, row.Payload); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
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
	if errors.Is(err, context.Canceled) {
		return nil // shutdown: the row's processing fails right after and ProcessRepo reports it once
	}
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
	// v0.27.42 (summary/18 Phase 4): pure dispatcher — one method per
	// entity type, extracted verbatim from the former 272-line switch.
	switch entityType {
	case EntityContributor:
		return p.processStagedContributor(ctx, repoID, platID, payload)
	case EntityIssue:
		return p.processStagedIssue(ctx, repoID, platID, payload)
	case EntityPullRequest:
		return p.processStagedPR(ctx, repoID, platID, payload)
	case EntityIssueEvent:
		return p.processStagedIssueEvent(ctx, repoID, platID, payload)
	case EntityPREvent:
		return p.processStagedPREvent(ctx, repoID, platID, payload)
	case EntityMessage:
		return p.processStagedMessage(ctx, repoID, platID, payload)
	case EntityReviewComment:
		return p.processStagedReviewComment(ctx, repoID, platID, payload)
	case EntityRelease:
		return p.processStagedRelease(ctx, repoID, platID, payload)
	case EntityRepoInfo:
		return p.processStagedRepoInfo(ctx, repoID, platID, payload)
	case EntityCloneStats:
		return p.processStagedCloneStats(ctx, repoID, platID, payload)
	default:
		return fmt.Errorf("unknown entity type: %s", entityType)
	}
}

// processStagedContributor handles one staged EntityContributor row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedContributor(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
	// Should not reach here — contributors are batched in processBatch.
	// Fallback just in case.
	var c model.Contributor
	if err := json.Unmarshal(payload, &c); err != nil {
		return err
	}
	return p.store.UpsertContributor(ctx, &c)
}

// processStagedIssue handles one staged EntityIssue row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedIssue(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
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
			// Copilot round 8 (class sweep): all 12 child-upsert arms
			// below are warn-and-continue, so a cancel mid-row emitted
			// one WARN per remaining child — the v0.27.91 flood class.
			// Shutdown ends the row; the staging row stays unprocessed
			// and the next cycle redoes it (idempotent upserts).
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert issue labels", "issue_id", issueID, "error", err)
		}
	}
	if len(env.Assignees) > 0 {
		// v0.26.5: resolve each assignee's identity into cntrb_id —
		// the same contract as reporter/author above. Before this,
		// issue_assignees.cntrb_id was 0% populated since inception.
		for i := range env.Assignees {
			env.Assignees[i].ContributorID = p.resolveUser(ctx, platID, env.Assignees[i].UserRef)
		}
		if err := p.store.UpsertIssueAssignees(ctx, issueID, repoID, env.Assignees); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert issue assignees", "issue_id", issueID, "error", err)
		}
	}
	return nil
}

// processStagedPR handles one staged EntityPullRequest row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedPR(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
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
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR labels", "pr_id", prID, "error", err)
		}
	}
	if len(env.Assignees) > 0 {
		for i := range env.Assignees {
			env.Assignees[i].ContributorID = p.resolveUser(ctx, platID, env.Assignees[i].UserRef)
		}
		if err := p.store.UpsertPRAssignees(ctx, prID, repoID, env.Assignees); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR assignees", "pr_id", prID, "error", err)
		}
	}
	if len(env.Reviewers) > 0 {
		for i := range env.Reviewers {
			env.Reviewers[i].ContributorID = p.resolveUser(ctx, platID, env.Reviewers[i].UserRef)
		}
		if err := p.store.UpsertPRReviewers(ctx, prID, repoID, env.Reviewers); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR reviewers", "pr_id", prID, "error", err)
		}
	}
	for _, review := range env.Reviews {
		review.PRID = prID
		review.RepoID = repoID
		review.ContributorID = p.resolveUser(ctx, platID, review.AuthorRef)
		if err := p.store.UpsertPRReview(ctx, &review); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR review", "pr_id", prID, "error", err)
		}
	}
	for _, commit := range env.Commits {
		commit.PRID = prID
		commit.RepoID = repoID
		commit.AuthorID = p.resolveUser(ctx, platID, commit.AuthorRef)
		if err := p.store.UpsertPRCommit(ctx, &commit); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR commit", "pr_id", prID, "error", err)
		}
	}
	for _, file := range env.Files {
		file.PRID = prID
		file.RepoID = repoID
		if err := p.store.UpsertPRFile(ctx, &file); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR file", "pr_id", prID, "error", err)
		}
	}
	var headMetaID, baseMetaID int64
	if env.MetaHead != nil {
		env.MetaHead.PRID = prID
		env.MetaHead.RepoID = repoID
		var metaErr error
		env.MetaHead.AuthorID = p.resolveUser(ctx, platID, env.MetaHead.AuthorRef)
		headMetaID, metaErr = p.store.UpsertPRMeta(ctx, env.MetaHead)
		if metaErr != nil {
			if errors.Is(metaErr, context.Canceled) {
				return metaErr
			}
			p.logger.Warn("failed to upsert PR meta (head)", "pr_id", prID, "error", metaErr)
		}
	}
	if env.MetaBase != nil {
		env.MetaBase.PRID = prID
		env.MetaBase.RepoID = repoID
		var metaErr error
		env.MetaBase.AuthorID = p.resolveUser(ctx, platID, env.MetaBase.AuthorRef)
		baseMetaID, metaErr = p.store.UpsertPRMeta(ctx, env.MetaBase)
		if metaErr != nil {
			if errors.Is(metaErr, context.Canceled) {
				return metaErr
			}
			p.logger.Warn("failed to upsert PR meta (base)", "pr_id", prID, "error", metaErr)
		}
	}
	// v0.27.104: persist the meta-row PKs onto the PR row. They were
	// computed here since inception and discarded — meta_head_id/
	// meta_base_id were 100% dark (2026-08-19 fill audit). The PR row
	// is upserted before the meta rows (FK order), so this is a
	// targeted follow-up UPDATE, warn-don't-fail like the siblings.
	if headMetaID != 0 || baseMetaID != 0 {
		if err := p.store.SetPRMetaLinks(ctx, prID, headMetaID, baseMetaID); err != nil {
			// L10 pass: the one child-write arm in this function the
			// round-8 sweep missed — its verb is "link", and the pin
			// that was supposed to derive the sites only matched
			// upsert/insert/clear/set. meta_head_id/meta_base_id are
			// the columns v0.27.104 un-darkened after being 100% dark
			// on 41.2M rows; a shutdown here left them unset for the
			// PR while the row still stamped processed.
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to link PR meta ids", "pr_id", prID, "error", err)
		}
	}
	// Insert fork repo details linked to their corresponding meta rows.
	// v0.27.104: resolve the fork OWNER identity (the v0.26.5
	// resolveUser contract) — pull_request_repo.pr_cntrb_id was 0 of
	// 41.2M rows because OwnerRef was dropped at the mappers.
	if env.RepoHead != nil && headMetaID != 0 {
		env.RepoHead.MetaID = headMetaID
		env.RepoHead.ContribID = p.resolveUser(ctx, platID, env.RepoHead.OwnerRef)
		if err := p.store.UpsertPRRepo(ctx, env.RepoHead); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR repo (head)", "pr_id", prID, "error", err)
		}
	}
	if env.RepoBase != nil && baseMetaID != 0 {
		env.RepoBase.MetaID = baseMetaID
		env.RepoBase.ContribID = p.resolveUser(ctx, platID, env.RepoBase.OwnerRef)
		if err := p.store.UpsertPRRepo(ctx, env.RepoBase); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			p.logger.Warn("failed to upsert PR repo (base)", "pr_id", prID, "error", err)
		}
	}
	return nil
}

// processStagedIssueEvent handles one staged EntityIssueEvent row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedIssueEvent(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
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
}

// processStagedPREvent handles one staged EntityPREvent row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedPREvent(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
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
}

// processStagedMessage handles one staged EntityMessage row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedMessage(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
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
			p.unresolvableRefs++
			return nil // no way to resolve parent — skip (counted; WARN at repo end)
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
			p.unresolvableRefs++
			return nil // no way to resolve parent — skip (counted; WARN at repo end)
		}
	}
	return p.store.UpsertMessageBatch(ctx, []platform.MessageWithRef{msg})
}

// processStagedReviewComment handles one staged EntityReviewComment row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedReviewComment(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
	var rc platform.ReviewCommentWithRef
	if err := json.Unmarshal(payload, &rc); err != nil {
		return err
	}
	rc.Message.RepoID = repoID
	rc.Comment.RepoID = repoID
	rc.Message.ContributorID = p.resolveUser(ctx, platID, rc.Message.AuthorRef)
	// Resolve platform review ID to DB pr_review_id — repo-scoped
	// (v0.25.33): the parent review must belong to this repo, or a
	// case-variant duplicate pair gets cross-linked bridge rows.
	if rc.Comment.PlatformReviewID != 0 {
		dbID, err := p.store.FindReviewDBID(ctx, repoID, rc.Comment.PlatformReviewID)
		if err == nil && dbID != 0 {
			rc.Comment.ReviewID = dbID
		}
	}
	return p.store.UpsertReviewCommentBatch(ctx, []platform.ReviewCommentWithRef{rc})
}

// processStagedRelease handles one staged EntityRelease row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedRelease(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
	var rel model.Release
	if err := json.Unmarshal(payload, &rel); err != nil {
		return err
	}
	rel.RepoID = repoID
	return p.store.UpsertRelease(ctx, &rel)
}

// processStagedRepoInfo handles one staged EntityRepoInfo row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedRepoInfo(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
	var info model.RepoInfo
	if err := json.Unmarshal(payload, &info); err != nil {
		return err
	}
	info.RepoID = repoID
	// Rotate previous snapshot to history before inserting the latest.
	if err := p.store.RotateRepoInfoToHistory(ctx, repoID); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			p.logger.Warn("failed to rotate repo info to history", "repo_id", repoID, "error", err)
		}
	}
	return p.store.InsertRepoInfo(ctx, &info)
}

// processStagedCloneStats handles one staged EntityCloneStats row (v0.27.42 — extracted from
// the former 272-line processOne switch; behavior identical).
func (p *Processor) processStagedCloneStats(ctx context.Context, repoID int64, platID int16, payload json.RawMessage) error {
	var clone model.RepoClone
	if err := json.Unmarshal(payload, &clone); err != nil {
		return err
	}
	clone.RepoID = repoID
	return p.store.UpsertRepoClone(ctx, &clone)
}
