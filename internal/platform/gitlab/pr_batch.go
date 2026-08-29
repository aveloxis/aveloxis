// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// FetchPRBatch is GitLab's implementation of platform.Client.FetchPRBatch.
//
// GitLab's GraphQL API is weaker on MergeRequest fields than GitHub's is
// on PullRequest — notably missing a clean cursor-based commits connection
// and lacking state-change timeline items. Rather than half-implement a
// GraphQL path and diverge from GitHub at column-level, this fallback
// composes REST: ONE GET of each merge request (whose payload carries the
// labels, assignees, reviewers, branch/SHA meta, and source/target
// project ids — mr_map.go derives them) plus the distinct child endpoints
// (approvals, commits, diffs) and the two /projects/:id lookups, to
// produce the same platform.StagedPR shape the GraphQL path returns on
// GitHub. v0.28.15: pre-fix it re-GET the same MR URL six times per MR via
// the public per-child methods, and the ETag cache's 304 on the second
// hit aborted every batch — see fetchOnePRWithChildren.
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

// fetchOnePRWithChildren composes the per-MR data for a single merge
// request number. Any error from the MR GET is returned verbatim;
// FetchPRBatch classifies it — a ClassSkip error (deleted/private MR)
// skips that MR, anything else aborts the batch. There is no nil, nil
// path (the old comment claiming one described a branch that was dead
// even then — Copilot round on PR #191).
//
// v0.28.15: ONE GET of the merge request. Labels, assignees, reviewers,
// branch/SHA meta, and the source/target project ids all live in that
// payload (mr_map.go derives them), so the six identical GETs the
// pre-v0.28.15 composition issued per MR are gone. The batch also runs
// ETag-free (platform.WithoutETag, the v0.25.0 precedent): the HTTP
// client's ETag cache turned every repeat hit on an MR URL into a 304
// that a single-object reader cannot use ("labels: not modified (304)"
// aborted every GitLab MR batch — petsc/petsc: 0 of 9,450 MRs stored),
// and the paginated children under a 304 would silently yield ZERO
// items on the repo's next cycle (the v0.26.3 class). A batch needs the
// current body every time; the 304 saving does not apply to it.
func (c *Client) fetchOnePRWithChildren(ctx context.Context, owner, repo string, number int) (*platform.StagedPR, error) {
	ctx = platform.WithoutETag(ctx)
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, number)
	var raw glMergeRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}

	staged := platform.StagedPR{PR: *mrToPullRequest(raw, prDataSourceBatch)}
	staged.Labels = mrLabels(raw)
	staged.Assignees = mrAssignees(raw)
	staged.Reviewers = mrReviewers(raw)

	// Reviews, commits, and files are distinct endpoints; skip-class
	// errors leave the slice empty (parity with the GitHub path's "no
	// children rather than fail").
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

	staged.MetaHead, staged.MetaBase = mrMeta(raw)
	staged.RepoHead, staged.RepoBase = c.mrRepos(ctx, raw)
	return &staged, nil
}
