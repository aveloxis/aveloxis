package gitlab

import (
	"context"
	"fmt"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// FetchPRBatch is GitLab's implementation of platform.Client.FetchPRBatch.
//
// GitLab's GraphQL API is weaker on MergeRequest fields than GitHub's is
// on PullRequest — notably missing a clean cursor-based commits connection
// and lacking state-change timeline items. Rather than half-implement a
// GraphQL path and diverge from GitHub at column-level, this fallback
// composes GitLab's existing REST methods (ListPRLabels, ListPRAssignees,
// ListPRReviewers, ListPRReviews, ListPRCommits, ListPRFiles,
// FetchPRMeta, FetchPRRepos, FetchPRByNumber) to produce the same
// platform.StagedPR shape the GraphQL path returns on GitHub.
//
// Parity is preserved at the row/column level: both the REST-via-batch
// and REST-direct paths on GitLab populate identical database rows.
// The only observable difference is the call pattern inside the
// platform package, invisible to collector code.
//
// This means GitLab collection gets no speedup from the feature gate
// flipping to "graphql" mode — but the contract of FetchPRBatch (one
// call, all children populated) works uniformly across both forges,
// so the collector doesn't need client-type branching.
func (c *Client) FetchPRBatch(ctx context.Context, owner, repo string, numbers []int) ([]platform.StagedPR, error) {
	if len(numbers) == 0 {
		return nil, nil
	}
	out := make([]platform.StagedPR, 0, len(numbers))
	for _, n := range numbers {
		staged, err := c.fetchOnePRWithChildren(ctx, owner, repo, n)
		if err != nil {
			// Skip individual PRs that are inaccessible (ClassSkip) — the
			// ClassifyError contract matches the GitHub path's behavior
			// for deleted/private items.
			if platform.ClassifyError(err) == platform.ClassSkip {
				continue
			}
			return out, fmt.Errorf("gitlab pr batch at #%d: %w", n, err)
		}
		if staged != nil {
			out = append(out, *staged)
		}
	}
	return out, nil
}

// fetchOnePRWithChildren composes the per-PR REST methods for a single
// merge request number. Returns nil, nil when the PR is inaccessible
// (ClassSkip-classifiable error from FetchPRByNumber); the caller appends
// only non-nil results.
func (c *Client) fetchOnePRWithChildren(ctx context.Context, owner, repo string, number int) (*platform.StagedPR, error) {
	pr, err := c.FetchPRByNumber(ctx, owner, repo, number)
	if err != nil {
		return nil, err
	}
	if pr == nil {
		return nil, nil
	}

	staged := platform.StagedPR{PR: *pr}

	// Labels — iterator breaks on non-skip error; skip-class errors
	// stop collection for this child and leave the slice empty (parity
	// with GitHub path's "no labels rather than fail").
	for label, err := range c.ListPRLabels(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("labels: %w", err)
		}
		staged.Labels = append(staged.Labels, label)
	}

	for a, err := range c.ListPRAssignees(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("assignees: %w", err)
		}
		staged.Assignees = append(staged.Assignees, a)
	}

	for r, err := range c.ListPRReviewers(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("reviewers: %w", err)
		}
		staged.Reviewers = append(staged.Reviewers, r)
	}

	for rv, err := range c.ListPRReviews(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("reviews: %w", err)
		}
		staged.Reviews = append(staged.Reviews, rv)
	}

	for cm, err := range c.ListPRCommits(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("commits: %w", err)
		}
		staged.Commits = append(staged.Commits, cm)
	}

	for f, err := range c.ListPRFiles(ctx, owner, repo, number) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return nil, fmt.Errorf("files: %w", err)
		}
		staged.Files = append(staged.Files, f)
	}

	head, base, err := c.FetchPRMeta(ctx, owner, repo, number)
	if err == nil {
		staged.MetaHead = head
		staged.MetaBase = base
	} else if platform.ClassifyError(err) != platform.ClassSkip {
		return nil, fmt.Errorf("meta: %w", err)
	}

	headRepo, baseRepo, err := c.FetchPRRepos(ctx, owner, repo, number)
	if err == nil {
		staged.RepoHead = headRepo
		staged.RepoBase = baseRepo
	} else if platform.ClassifyError(err) != platform.ClassSkip {
		return nil, fmt.Errorf("repos: %w", err)
	}

	return &staged, nil
}
