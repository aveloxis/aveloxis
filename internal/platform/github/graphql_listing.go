package github

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// ListIssuesAndPRs enumerates issues and PRs via two paginated GraphQL
// queries — one per connection — and returns the combined batch. Phase
// 2 of the REST→GraphQL refactor; callers that previously iterated
// ListIssues and ListPullRequests separately can now get both in one
// call with one GraphQL POST per page instead of two REST GETs.
//
// Why two queries instead of one combined query with both connections:
//   - Issues and PRs have independent page counts (typically very
//     different — augur has 989 issues vs 2623 PRs). A combined query
//     keeps both cursors in one request and wastes bandwidth once one
//     connection is exhausted.
//   - The since-filter mechanism differs per connection: `issues`
//     supports `filterBy: {since: $since}` server-side; `pullRequests`
//     has no equivalent and must rely on orderBy UPDATED_AT DESC + a
//     client-side break when we see an item older than since.
//   - Cursor management is trivial with one cursor per query.
//
// Eliminates REST's /issues-returns-PRs double-count: the legacy REST
// path listed PRs twice (once through /issues with a client-side
// filter-out, once through /pulls). GraphQL's separate connections
// never return PRs via the issues connection.
func (c *Client) ListIssuesAndPRs(ctx context.Context, owner, repo string, since time.Time) (*platform.IssueAndPRBatch, error) {
	batch := &platform.IssueAndPRBatch{}

	issues, err := c.listIssuesGraphQL(ctx, owner, repo, since)
	if err != nil {
		return batch, fmt.Errorf("graphql issues listing: %w", err)
	}
	batch.Issues = issues

	prs, err := c.listPullRequestsGraphQL(ctx, owner, repo, since)
	if err != nil {
		return batch, fmt.Errorf("graphql pullRequests listing: %w", err)
	}
	batch.PullRequests = prs

	return batch, nil
}

// listIssuesGraphQL paginates the repository.issues connection using the
// server-side since filter. ORDER BY UPDATED_AT DESC so pages arrive
// most-recent-first — matches REST's /issues?sort=updated&direction=desc
// ordering.
func (c *Client) listIssuesGraphQL(ctx context.Context, owner, repo string, since time.Time) ([]model.Issue, error) {
	const query = `query IssuesList($owner: String!, $repo: String!, $cursor: String, $since: DateTime) {
  repository(owner: $owner, name: $repo) {
    issues(first: 100, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}, filterBy: {since: $since}) {
      nodes {
        databaseId id number title body state url
        createdAt updatedAt closedAt
        comments { totalCount }
        author {
          __typename login
          ... on User { databaseId avatarUrl url name email }
          ... on Bot { databaseId avatarUrl url }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

	var out []model.Issue
	var cursor string
	for {
		vars := map[string]any{"owner": owner, "repo": repo}
		if cursor != "" {
			vars["cursor"] = cursor
		} else {
			vars["cursor"] = nil
		}
		if !since.IsZero() {
			vars["since"] = since.UTC().Format(time.RFC3339)
		} else {
			vars["since"] = nil
		}

		var resp struct {
			Repository struct {
				Issues struct {
					Nodes []struct {
						DatabaseID int64      `json:"databaseId"`
						ID         string     `json:"id"`
						Number     int        `json:"number"`
						Title      string     `json:"title"`
						Body       string     `json:"body"`
						State      string     `json:"state"`
						URL        string     `json:"url"`
						CreatedAt  time.Time  `json:"createdAt"`
						UpdatedAt  time.Time  `json:"updatedAt"`
						ClosedAt   *time.Time `json:"closedAt"`
						Comments   struct {
							TotalCount int `json:"totalCount"`
						} `json:"comments"`
						Author *prBatchUser `json:"author"`
					} `json:"nodes"`
					PageInfo prBatchPageInfo `json:"pageInfo"`
				} `json:"issues"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}

		for _, n := range resp.Repository.Issues.Nodes {
			issue := model.Issue{
				PlatformID:   n.DatabaseID,
				NodeID:       n.ID,
				Number:       n.Number,
				Title:        n.Title,
				Body:         n.Body,
				State:        strings.ToLower(n.State),
				URL:          n.URL,
				HTMLURL:      n.URL,
				CreatedAt:    n.CreatedAt,
				UpdatedAt:    n.UpdatedAt,
				ClosedAt:     n.ClosedAt,
				CommentCount: n.Comments.TotalCount,
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitHub GraphQL (listing)",
				},
			}
			if n.Author != nil {
				issue.ReporterRef = userRefFromGraphQL(n.Author)
			}
			out = append(out, issue)
		}
		if !resp.Repository.Issues.PageInfo.HasNextPage {
			return out, nil
		}
		cursor = resp.Repository.Issues.PageInfo.EndCursor
	}
}

// listPullRequestsGraphQL paginates the repository.pullRequests connection.
// The connection has no server-side since filter, so pages come ordered
// DESC and the loop breaks as soon as a PR's updatedAt falls on or before
// the since cutoff. Matches the legacy REST ListPullRequests behavior.
func (c *Client) listPullRequestsGraphQL(ctx context.Context, owner, repo string, since time.Time) ([]model.PullRequest, error) {
	const query = `query PRList($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(first: 100, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}, states: [OPEN, CLOSED, MERGED]) {
      nodes {
        databaseId id number title body state locked
        url createdAt updatedAt closedAt mergedAt authorAssociation
        mergeCommit { oid }
        author {
          __typename login
          ... on User { databaseId avatarUrl url name email }
          ... on Bot { databaseId avatarUrl url }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}`

	var out []model.PullRequest
	var cursor string
	for {
		vars := map[string]any{"owner": owner, "repo": repo}
		if cursor != "" {
			vars["cursor"] = cursor
		} else {
			vars["cursor"] = nil
		}

		var resp struct {
			Repository struct {
				PullRequests struct {
					Nodes []struct {
						DatabaseID        int64        `json:"databaseId"`
						ID                string       `json:"id"`
						Number            int          `json:"number"`
						Title             string       `json:"title"`
						Body              string       `json:"body"`
						State             string       `json:"state"`
						Locked            bool         `json:"locked"`
						URL               string       `json:"url"`
						CreatedAt         time.Time    `json:"createdAt"`
						UpdatedAt         time.Time    `json:"updatedAt"`
						ClosedAt          *time.Time   `json:"closedAt"`
						MergedAt          *time.Time   `json:"mergedAt"`
						AuthorAssociation string       `json:"authorAssociation"`
						MergeCommit       *struct {
							OID string `json:"oid"`
						} `json:"mergeCommit"`
						Author *prBatchUser `json:"author"`
					} `json:"nodes"`
					PageInfo prBatchPageInfo `json:"pageInfo"`
				} `json:"pullRequests"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}

		// Iterate in order. Because the connection is ordered UPDATED_AT
		// DESC, once we see a PR whose updatedAt is on/before `since`,
		// every subsequent PR is also older — stop paginating.
		for _, n := range resp.Repository.PullRequests.Nodes {
			if !since.IsZero() && !n.UpdatedAt.After(since) {
				return out, nil
			}
			pr := model.PullRequest{
				PlatformSrcID:     n.DatabaseID,
				NodeID:            n.ID,
				Number:            n.Number,
				Title:             n.Title,
				Body:              n.Body,
				State:             mapPRState(n.State, n.MergedAt),
				Locked:            n.Locked,
				URL:               n.URL,
				HTMLURL:           n.URL,
				CreatedAt:         n.CreatedAt,
				UpdatedAt:         n.UpdatedAt,
				ClosedAt:          n.ClosedAt,
				MergedAt:          n.MergedAt,
				AuthorAssociation: n.AuthorAssociation,
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitHub GraphQL (listing)",
				},
			}
			if n.MergeCommit != nil {
				pr.MergeCommitSHA = n.MergeCommit.OID
			}
			if n.Author != nil {
				pr.AuthorRef = userRefFromGraphQL(n.Author)
			}
			out = append(out, pr)
		}
		if !resp.Repository.PullRequests.PageInfo.HasNextPage {
			return out, nil
		}
		cursor = resp.Repository.PullRequests.PageInfo.EndCursor
	}
}
