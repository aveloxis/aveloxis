package gitlab

import (
	"context"
	"fmt"
	"time"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// ListIssuesAndPRs is GitLab's implementation of phase 2's unified
// listing method. Composes the existing REST iterators (ListIssues +
// ListPullRequests) into the same platform.IssueAndPRBatch shape that
// GitHub's GraphQL implementation returns.
//
// GitLab's GraphQL API is too weak on merge_request fields to use here
// (no server-side since filter on MRs, no per-MR children connection
// aligned with what Aveloxis populates). The REST composition preserves
// column-level parity at the cost of the call-count reduction GitHub
// sees. Column parity is the contract, not uniform call patterns.
//
// Non-skippable errors from either iterator abort the whole listing —
// the caller needs a consistent failure mode regardless of whether
// the underlying implementation is one GraphQL query or two REST loops.
func (c *Client) ListIssuesAndPRs(ctx context.Context, owner, repo string, since time.Time) (*platform.IssueAndPRBatch, error) {
	batch := &platform.IssueAndPRBatch{}

	for issue, err := range c.ListIssues(ctx, owner, repo, since) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return batch, fmt.Errorf("gitlab list issues: %w", err)
		}
		batch.Issues = append(batch.Issues, issue)
	}

	for pr, err := range c.ListPullRequests(ctx, owner, repo, since) {
		if err != nil {
			if platform.ClassifyError(err) == platform.ClassSkip {
				break
			}
			return batch, fmt.Errorf("gitlab list merge requests: %w", err)
		}
		batch.PullRequests = append(batch.PullRequests, pr)
	}

	return batch, nil
}
