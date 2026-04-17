// Package collector — gap_fill.go implements smart gap detection and targeted
// re-collection for repos where gathered data is incomplete.
//
// After each collection pass, gathered counts are compared against metadata.
// If the gap exceeds GapThreshold (5%), the gap filler:
//  1. Lists all issue/PR numbers from the API (lightweight — just extracts numbers)
//  2. Queries the DB for numbers we already have
//  3. Computes the exact missing numbers, grouped into contiguous gaps
//  4. Fetches only the missing items + 2 edge items per gap side
//
// Edge items (2 on each side of each gap) are items we already collected but
// re-fetch to ensure their associated data (comments, events, reviews) is
// complete — the original collection may have been interrupted after the
// parent entity was staged but before its children were fetched.
//
// This handles multiple distinct gaps (e.g., gaps at items 100-200, 5000-5500,
// and 30000-31000 in the same repo). Each gap is processed independently.
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// GapThreshold is the minimum gap percentage (gathered vs metadata) to trigger
// gap filling. 5% allows for minor counting differences between API listing
// and metadata counts (due to deleted items, permission changes, etc.).
const GapThreshold = 0.05

// GapEdgeCount is how many already-collected items on each side of a gap
// to re-fetch, ensuring their associated data (comments, events, etc.) is complete.
const GapEdgeCount = 2

// Gap represents a contiguous range of missing issue/PR numbers.
type Gap struct {
	Start int // first missing number (inclusive)
	End   int // last missing number (inclusive)
}

// GapFiller detects and fills collection gaps for a repo.
type GapFiller struct {
	store  *db.PostgresStore
	client platform.Client
	logger *slog.Logger
}

// NewGapFiller creates a gap filler.
func NewGapFiller(store *db.PostgresStore, client platform.Client, logger *slog.Logger) *GapFiller {
	return &GapFiller{store: store, client: client, logger: logger}
}

// AssessAndFillGaps checks for collection gaps and fills them if needed.
// Called after each collection pass completes. Returns the number of items filled.
func (gf *GapFiller) AssessAndFillGaps(ctx context.Context, repoID int64, owner, repo string, metaIssues, metaPRs int64) (int, error) {
	totalFilled := 0

	// Check issue gaps.
	collectedIssues, err := gf.store.GetCollectedIssueNumbers(ctx, repoID)
	if err != nil {
		return 0, fmt.Errorf("querying collected issues: %w", err)
	}

	if gapExceedsThreshold(int64(len(collectedIssues)), metaIssues) {
		gf.logger.Info("issue gap detected",
			"repo_id", repoID,
			"collected", len(collectedIssues),
			"metadata", metaIssues,
			"gap_pct", fmt.Sprintf("%.1f%%", gapPercent(int64(len(collectedIssues)), metaIssues)))

		// List all issue numbers from the API.
		apiIssueNumbers, err := gf.listAPIIssueNumbers(ctx, owner, repo)
		if err != nil {
			gf.logger.Warn("failed to list API issue numbers for gap fill", "error", err)
		} else {
			gaps := ComputeGaps(collectedIssues, apiIssueNumbers)
			if len(gaps) > 0 {
				toFetch := ExpandGapsWithEdges(gaps, collectedIssues, GapEdgeCount)
				gf.logger.Info("filling issue gaps",
					"repo_id", repoID,
					"gaps", len(gaps),
					"items_to_fetch", len(toFetch))
				filled, err := gf.fillIssueGaps(ctx, repoID, owner, repo, toFetch)
				if err != nil {
					gf.logger.Warn("issue gap fill error", "error", err)
				}
				totalFilled += filled
			}
		}
	}

	// Check PR gaps.
	collectedPRs, err := gf.store.GetCollectedPRNumbers(ctx, repoID)
	if err != nil {
		return totalFilled, fmt.Errorf("querying collected PRs: %w", err)
	}

	if gapExceedsThreshold(int64(len(collectedPRs)), metaPRs) {
		gf.logger.Info("PR gap detected",
			"repo_id", repoID,
			"collected", len(collectedPRs),
			"metadata", metaPRs,
			"gap_pct", fmt.Sprintf("%.1f%%", gapPercent(int64(len(collectedPRs)), metaPRs)))

		apiPRNumbers, err := gf.listAPIPRNumbers(ctx, owner, repo)
		if err != nil {
			gf.logger.Warn("failed to list API PR numbers for gap fill", "error", err)
		} else {
			gaps := ComputeGaps(collectedPRs, apiPRNumbers)
			if len(gaps) > 0 {
				toFetch := ExpandGapsWithEdges(gaps, collectedPRs, GapEdgeCount)
				gf.logger.Info("filling PR gaps",
					"repo_id", repoID,
					"gaps", len(gaps),
					"items_to_fetch", len(toFetch))
				filled, err := gf.fillPRGaps(ctx, repoID, owner, repo, toFetch)
				if err != nil {
					gf.logger.Warn("PR gap fill error", "error", err)
				}
				totalFilled += filled
			}
		}
	}

	if totalFilled > 0 {
		gf.logger.Info("gap fill complete", "repo_id", repoID, "total_filled", totalFilled)
	}
	return totalFilled, nil
}

// listAPIIssueNumbers iterates the platform's issue listing to extract all
// issue numbers. Uses since=zero to get the complete set.
func (gf *GapFiller) listAPIIssueNumbers(ctx context.Context, owner, repo string) ([]int, error) {
	var numbers []int
	for issue, err := range gf.client.ListIssues(ctx, owner, repo, time.Time{}) {
		if err != nil {
			return numbers, err
		}
		numbers = append(numbers, issue.Number)
	}
	sort.Ints(numbers)
	return numbers, nil
}

// listAPIPRNumbers iterates the platform's PR listing to extract all PR numbers.
func (gf *GapFiller) listAPIPRNumbers(ctx context.Context, owner, repo string) ([]int, error) {
	var numbers []int
	for pr, err := range gf.client.ListPullRequests(ctx, owner, repo, time.Time{}) {
		if err != nil {
			return numbers, err
		}
		numbers = append(numbers, pr.Number)
	}
	sort.Ints(numbers)
	return numbers, nil
}

// fillIssueGaps fetches specific issues by number and stages them with children.
// Uses the same envelope pattern as the staged collector (stagedIssue + Stage).
func (gf *GapFiller) fillIssueGaps(ctx context.Context, repoID int64, owner, repo string, numbers []int) (int, error) {
	filled := 0
	sw := db.NewStagingWriter(gf.store, repoID, int16(gf.client.Platform()), gf.logger)

	for _, num := range numbers {
		issue, err := gf.client.FetchIssueByNumber(ctx, owner, repo, num)
		if err != nil {
			gf.logger.Debug("issue not found or error", "number", num, "error", err)
			continue
		}

		// Build the same envelope the staged collector uses.
		envelope := stagedIssue{Issue: *issue}
		for label, err := range gf.client.ListIssueLabels(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for assignee, err := range gf.client.ListIssueAssignees(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, assignee)
		}

		if err := sw.Stage(ctx, EntityIssue, envelope); err != nil {
			gf.logger.Debug("failed to stage issue", "number", num, "error", err)
			continue
		}
		filled++

		if filled%100 == 0 {
			gf.logger.Info("gap fill issues progress", "staged", filled, "of", len(numbers))
		}
	}

	// Flush the in-memory pgx.Batch to Postgres BEFORE invoking the processor.
	// StagingWriter.Stage only auto-sends to the database when the batch reaches
	// stagingFlushSize (500). Gap fills are usually smaller than that, so
	// without an explicit flush the buffered rows would sit in memory, the
	// processor would read an empty staging table, and every staged row would
	// be silently discarded when the writer goes out of scope.
	if filled > 0 {
		if err := sw.Flush(ctx); err != nil {
			gf.logger.Warn("failed to flush gap-fill issue staging batch",
				"repo_id", repoID, "staged", filled, "error", err)
			return filled, fmt.Errorf("flushing gap-fill issue staging: %w", err)
		}
		proc := NewProcessor(gf.store, gf.logger)
		if err := proc.ProcessRepo(ctx, repoID, int16(gf.client.Platform())); err != nil {
			return filled, fmt.Errorf("processing gap-fill staging: %w", err)
		}
	}
	return filled, nil
}

// fillPRGaps fetches specific PRs by number and stages them with all children.
// Uses the same envelope pattern as the staged collector (stagedPR + Stage).
func (gf *GapFiller) fillPRGaps(ctx context.Context, repoID int64, owner, repo string, numbers []int) (int, error) {
	filled := 0
	sw := db.NewStagingWriter(gf.store, repoID, int16(gf.client.Platform()), gf.logger)

	for _, num := range numbers {
		pr, err := gf.client.FetchPRByNumber(ctx, owner, repo, num)
		if err != nil {
			gf.logger.Debug("PR not found or error", "number", num, "error", err)
			continue
		}

		// Build the same envelope the staged collector uses.
		envelope := stagedPR{PR: *pr}
		for label, err := range gf.client.ListPRLabels(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Labels = append(envelope.Labels, label)
		}
		for assignee, err := range gf.client.ListPRAssignees(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Assignees = append(envelope.Assignees, assignee)
		}
		for reviewer, err := range gf.client.ListPRReviewers(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Reviewers = append(envelope.Reviewers, reviewer)
		}
		for review, err := range gf.client.ListPRReviews(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Reviews = append(envelope.Reviews, review)
		}
		for commit, err := range gf.client.ListPRCommits(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Commits = append(envelope.Commits, commit)
		}
		for file, err := range gf.client.ListPRFiles(ctx, owner, repo, num) {
			if err != nil {
				break
			}
			envelope.Files = append(envelope.Files, file)
		}

		if err := sw.Stage(ctx, EntityPullRequest, envelope); err != nil {
			gf.logger.Debug("failed to stage PR", "number", num, "error", err)
			continue
		}
		filled++

		if filled%100 == 0 {
			gf.logger.Info("gap fill PRs progress", "staged", filled, "of", len(numbers))
		}
	}

	// Flush the in-memory pgx.Batch to Postgres BEFORE invoking the processor
	// (see fillIssueGaps above for the full rationale — same buffering bug).
	if filled > 0 {
		if err := sw.Flush(ctx); err != nil {
			gf.logger.Warn("failed to flush gap-fill PR staging batch",
				"repo_id", repoID, "staged", filled, "error", err)
			return filled, fmt.Errorf("flushing gap-fill PR staging: %w", err)
		}
		proc := NewProcessor(gf.store, gf.logger)
		if err := proc.ProcessRepo(ctx, repoID, int16(gf.client.Platform())); err != nil {
			return filled, fmt.Errorf("processing gap-fill staging: %w", err)
		}
	}
	return filled, nil
}

// ComputeGaps returns contiguous gaps between collected and expected number sets.
// Both inputs must be sorted. Returns nil if no gaps.
func ComputeGaps(collected, expected []int) []Gap {
	if len(expected) == 0 {
		return nil
	}

	collectedSet := make(map[int]bool, len(collected))
	for _, n := range collected {
		collectedSet[n] = true
	}

	var gaps []Gap
	var current *Gap

	for _, n := range expected {
		if collectedSet[n] {
			// We have this number — close any open gap.
			if current != nil {
				gaps = append(gaps, *current)
				current = nil
			}
		} else {
			// Missing — extend or start a gap.
			if current == nil {
				current = &Gap{Start: n, End: n}
			} else {
				current.End = n
			}
		}
	}
	// Close final gap if open.
	if current != nil {
		gaps = append(gaps, *current)
	}

	return gaps
}

// ExpandGapsWithEdges returns the full set of numbers to fetch: all gap numbers
// plus edgeCount items from the collected set on each side of each gap.
// Edge items are already-collected items re-fetched to verify their children.
// Handles multiple distinct gaps with proper deduplication.
func ExpandGapsWithEdges(gaps []Gap, collected []int, edgeCount int) []int {
	if len(gaps) == 0 {
		return nil
	}

	// Sort collected for binary search.
	sort.Ints(collected)

	fetchSet := make(map[int]bool)

	for _, g := range gaps {
		// Add all gap numbers.
		for n := g.Start; n <= g.End; n++ {
			fetchSet[n] = true
		}

		// Add edge items BEFORE the gap (from collected set).
		beforeIdx := sort.SearchInts(collected, g.Start) - 1
		for i := 0; i < edgeCount && beforeIdx-i >= 0; i++ {
			fetchSet[collected[beforeIdx-i]] = true
		}

		// Add edge items AFTER the gap (from collected set).
		afterIdx := sort.SearchInts(collected, g.End+1)
		for i := 0; i < edgeCount && afterIdx+i < len(collected); i++ {
			fetchSet[collected[afterIdx+i]] = true
		}
	}

	// Convert to sorted slice.
	result := make([]int, 0, len(fetchSet))
	for n := range fetchSet {
		result = append(result, n)
	}
	sort.Ints(result)
	return result
}

func gapExceedsThreshold(gathered, metadata int64) bool {
	if metadata == 0 {
		return false
	}
	return float64(metadata-gathered)/float64(metadata) > GapThreshold
}

func gapPercent(gathered, metadata int64) float64 {
	if metadata == 0 {
		return 0
	}
	return float64(metadata-gathered) / float64(metadata) * 100
}
