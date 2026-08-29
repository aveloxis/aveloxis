// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Collector orchestrates a one-shot collection for a single repository
// (`aveloxis collect`). Since v0.26.2 its API phases DELEGATE to the
// same staged pipeline `aveloxis serve` uses (StagedCollector →
// Processor), so the one-shot path honors pr_child_mode / listing_mode
// / issue_child_mode and resolves event/message parent FKs correctly.
// The legacy direct-write phases it replaced never resolved event
// numbers into local-serial FKs — every event failed FK 23503 and was
// silently dropped (2026-07-09 data-test incident).
type Collector struct {
	client platform.Client
	store  *db.PostgresStore
	logger *slog.Logger
	platID int16
	facade *FacadeCollector
	ghKeys *platform.KeyPool // for commit resolution (GitHub only)

	// Staged-pipeline mode knobs, threaded from CollectionConfig by
	// runCollect via WithCollectionModes — the same source of truth
	// the scheduler reads. Constructor values are safe REST fallbacks
	// for direct construction in tests.
	prChildMode    string
	listingMode    string
	threadingMode  string
	shardSize      int
	issueChildMode string
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
		client:         client,
		store:          store,
		logger:         logger,
		platID:         platID,
		facade:         NewFacadeCollector(store, logger, repoCloneDir),
		ghKeys:         ghKeys,
		prChildMode:    "rest",
		listingMode:    "rest",
		threadingMode:  "single",
		shardSize:      defaultShardSize,
		issueChildMode: "rest",
	}
}

// WithCollectionModes threads the staged-pipeline mode knobs
// (pr_child_mode, listing_mode, threading_mode, shard_size,
// issue_child_mode) into the collector. runCollect passes the
// CollectionConfig values so `aveloxis collect` and `aveloxis serve`
// see identical modes and cannot diverge.
func (c *Collector) WithCollectionModes(prChild, listing, threading string, shardSize int, issueChild string) *Collector {
	if prChild != "" {
		c.prChildMode = prChild
	}
	if listing != "" {
		c.listingMode = listing
	}
	if threading != "" {
		c.threadingMode = threading
	}
	if shardSize > 0 {
		c.shardSize = shardSize
	}
	if issueChild != "" {
		c.issueChildMode = issueChild
	}
	return c
}

// collectAndProcess mirrors scheduler.collectAndProcess: stage raw API
// data via the StagedCollector, then process staged rows into
// relational tables via the Processor (which resolves event/message
// parent NUMBERS into local-serial FKs — the step whose absence made
// the legacy direct-write path drop every event with FK 23503).
// Processing is gated on a nil collect error, exactly like serve — a
// hard collect error means incomplete staging.
func (c *Collector) collectAndProcess(ctx context.Context, repoID int64, owner, repo string, since time.Time) (*CollectResult, error) {
	sc := NewStagedCollectorWithAllModes(c.client, c.store, c.logger,
		c.prChildMode, c.listingMode, c.threadingMode, c.shardSize).
		WithIssueChildMode(c.issueChildMode)
	result, err := sc.CollectRepo(ctx, repoID, owner, repo, since)
	if err == nil {
		proc := NewProcessor(c.store, c.logger)
		if procErr := proc.ProcessRepo(ctx, repoID, c.platID); procErr != nil {
			err = procErr
		}
	}
	return result, err
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
	// InlineIssueComments and InlinePRComments are per-cycle counters
	// of conversation comments staged inline from the phase-2 unified
	// GraphQL listing and the phase-1 PR batch respectively. They are
	// SUBSETS of Messages, surfaced separately so collectMessages can
	// emit a "phase plan" log line that explains why /issues/comments
	// is being skipped (v0.22.4 item 4).
	InlineIssueComments int
	InlinePRComments    int
	Errors              []error
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
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			c.logger.Warn("failed to update collection status", "repo_id", repoID, "error", err)
		}
	}

	// API phases: contributors, issues, PRs, events, messages, repo
	// info, releases, clone stats — all via the staged pipeline, the
	// exact path production serve runs.
	stagedResult, stagedErr := c.collectAndProcess(ctx, repoID, owner, repo, since)
	if stagedResult != nil {
		result = stagedResult
	}
	if stagedErr != nil {
		result.Errors = append(result.Errors, fmt.Errorf("staged collection: %w", stagedErr))
	}

	// Phase 4: Facade — git clone + log for commit data.
	// Runs AFTER API phases so contributor emails can be resolved.
	gitURL := fmt.Sprintf("https://%s/%s/%s.git",
		platformHost(c.client.Platform()), owner, repo)
	if err := c.store.UpdateCollectionStatus(ctx, &db.CollectionState{
		RepoID:       repoID,
		FacadeStatus: string(StatusCollecting),
	}); err != nil {
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			c.logger.Warn("failed to update facade status", "repo_id", repoID, "error", err)
		}
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
		// Round-8 burn-down: a cancelled context is a `stop serve`, not a
		// defect. Only the log is suppressed — surrounding behaviour is
		// unchanged and the work is retried on the next cycle.
		if !errors.Is(err, context.Canceled) {
			c.logger.Warn("failed to update final collection status", "repo_id", repoID, "error", err)
		}
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
