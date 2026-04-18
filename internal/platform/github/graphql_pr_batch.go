package github

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// StagedPR is a package-local alias for platform.StagedPR so existing
// test code in this package keeps compiling without a wider rename.
// All new code should use platform.StagedPR directly; this alias will
// be removed once the test files migrate.
type StagedPR = platform.StagedPR

// prBatchSize is how many PRs go into one GraphQL query. Sized so the
// response payload stays under ~1 MB on a typical repo and the query's
// point cost (floor(nodes/100)+1) stays under ~50 points — well below
// one key's hourly GraphQL budget of 5000 points.
//
// Can be tuned later; exported via this const so benchmarks can change
// it without re-reading the file.
const prBatchSize = 25

// FetchPRBatch fetches up to prBatchSize PRs and all their children in a
// single GraphQL query (repeating for each batch when len(numbers) > 25).
//
// Replaces the per-PR REST waterfall (ListPRLabels, ListPRAssignees,
// ListPRReviewers, ListPRReviews, ListPRCommits, ListPRFiles, FetchPRMeta,
// FetchPRRepos — 8 REST calls per PR) with one GraphQL call per batch,
// a ~200× reduction in HTTP round trips on a 500-PR repo.
//
// Oversized children (any connection with hasNextPage=true after the
// initial page) are paginated via follow-up GraphQL queries, keeping
// the whole operation on the GraphQL rail. Each follow-up is tiny
// (~2 points) because it requests just the one oversized connection
// with a cursor.
//
// Returns an error classified via platform.ClassifyError so callers in
// staged.go / refresh_open.go / gap_fill.go apply the right retry/skip
// semantics (ClassSkip for NOT_FOUND, ClassRateLimit for RATE_LIMITED,
// etc.).
func (c *Client) FetchPRBatch(ctx context.Context, owner, repo string, numbers []int) ([]StagedPR, error) {
	if len(numbers) == 0 {
		return nil, nil
	}

	var out []StagedPR
	for start := 0; start < len(numbers); start += prBatchSize {
		end := start + prBatchSize
		if end > len(numbers) {
			end = len(numbers)
		}
		batch, err := c.fetchPRBatchOne(ctx, owner, repo, numbers[start:end])
		if err != nil {
			return out, err
		}
		out = append(out, batch...)
	}
	return out, nil
}

// fetchPRBatchOne handles a single batch (≤ prBatchSize PRs).
func (c *Client) fetchPRBatchOne(ctx context.Context, owner, repo string, numbers []int) ([]StagedPR, error) {
	query := buildPRBatchQuery(len(numbers))
	vars := map[string]any{"owner": owner, "repo": repo}
	for i, n := range numbers {
		vars[fmt.Sprintf("n%d", i)] = n
	}

	var resp prBatchResponse
	if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
		return nil, fmt.Errorf("graphql PR batch: %w", err)
	}

	out := make([]StagedPR, 0, len(numbers))
	repoData := resp.Repository
	if repoData == nil {
		// Whole repository field came back null — unusual, typically means
		// the repository was deleted mid-collection. Treat as empty batch;
		// the collector will log and move on.
		return out, nil
	}
	for i, n := range numbers {
		raw, ok := repoData[fmt.Sprintf("pr%d", i)]
		if !ok || raw == nil {
			// Alias missing or PR was null (deleted / inaccessible mid-query).
			// Skip cleanly — the enumerator will catch truly missing items
			// on its next pass.
			continue
		}
		staged := mapPRNodeToStagedPR(raw, n)
		// Paginate oversized children. Happens rarely (most PRs have
		// well under 100 of any child type), but must be correct when
		// it does — a 500-file refactor PR cannot silently become a
		// 100-file record.
		if err := c.paginateOversizedChildren(ctx, owner, repo, n, raw, &staged); err != nil {
			return out, fmt.Errorf("paginating children for PR #%d: %w", n, err)
		}
		out = append(out, staged)
	}
	return out, nil
}

// buildPRBatchQuery generates the aliased GraphQL query for N PRs.
// GraphQL doesn't have a native "fetch these specific PRs by number"
// connection, so each PR is requested under a distinct alias (pr0, pr1,
// ...). The client decodes them by alias.
//
// Variable names pair with aliases ($n0 with pr0, etc.) so one query
// handles any N ≤ prBatchSize without the client generating literal
// numbers into the query string (which would defeat GitHub's query
// fingerprint caching and bloat logs).
func buildPRBatchQuery(n int) string {
	var sb strings.Builder
	sb.WriteString("query PRBatch($owner: String!, $repo: String!")
	for i := 0; i < n; i++ {
		sb.WriteString(", $n")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(": Int!")
	}
	sb.WriteString(") {\n")
	sb.WriteString("  repository(owner: $owner, name: $repo) {\n")
	for i := 0; i < n; i++ {
		sb.WriteString("    pr")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(": pullRequest(number: $n")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(") {\n")
		sb.WriteString(prNodeFragment)
		sb.WriteString("    }\n")
	}
	sb.WriteString("  }\n")
	sb.WriteString("}\n")
	return sb.String()
}

// prNodeFragment is the selection set for a single pullRequest node.
// Field names must stay in sync with prBatchPRNode below; a drift between
// them shows up as zero-valued fields in the parsed output.
//
// childPageSize is baked in as 100 (GitHub's max); future tuning can
// switch to a template.
const prNodeFragment = `
      databaseId id number title body state locked
      createdAt updatedAt closedAt mergedAt url authorAssociation
      mergeCommit { oid }
      author {
        __typename login
        ... on User { databaseId avatarUrl url name email }
        ... on Bot { databaseId avatarUrl url }
        ... on Organization { databaseId avatarUrl url name }
      }
      labels(first: 100) {
        nodes { id name color description isDefault }
        pageInfo { hasNextPage endCursor }
      }
      assignees(first: 100) {
        nodes { databaseId login avatarUrl url name email }
        pageInfo { hasNextPage endCursor }
      }
      reviewRequests(first: 100) {
        nodes {
          requestedReviewer {
            __typename
            ... on User { databaseId login avatarUrl url }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
      reviews(first: 100) {
        nodes {
          databaseId id state body submittedAt authorAssociation url
          author { __typename login ... on User { databaseId avatarUrl url } }
          commit { oid }
        }
        pageInfo { hasNextPage endCursor }
      }
      commits(first: 100) {
        nodes {
          commit {
            oid message committedDate
            author {
              email name date
              user { databaseId login avatarUrl url name email }
            }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
      files(first: 100) {
        nodes { path additions deletions }
        pageInfo { hasNextPage endCursor }
      }
      headRef { name target { oid } }
      baseRef { name target { oid } }
      headRepository {
        databaseId id nameWithOwner name isPrivate
        owner { __typename login ... on User { databaseId } ... on Organization { databaseId } }
      }
      baseRepository {
        databaseId id nameWithOwner name isPrivate
        owner { __typename login ... on User { databaseId } ... on Organization { databaseId } }
      }
`

// --- response decoding ------------------------------------------------

// prBatchResponse is the top-level decoded response. The repository field
// is a map so we can pluck aliases by name (pr0, pr1, ...); null values
// decode to nil.
type prBatchResponse struct {
	Repository map[string]*prBatchPRNode `json:"repository"`
}

type prBatchPRNode struct {
	DatabaseID        int64      `json:"databaseId"`
	ID                string     `json:"id"`
	Number            int        `json:"number"`
	Title             string     `json:"title"`
	Body              string     `json:"body"`
	State             string     `json:"state"` // OPEN, CLOSED, MERGED
	Locked            bool       `json:"locked"`
	CreatedAt         time.Time  `json:"createdAt"`
	UpdatedAt         time.Time  `json:"updatedAt"`
	ClosedAt          *time.Time `json:"closedAt"`
	MergedAt          *time.Time `json:"mergedAt"`
	URL               string     `json:"url"`
	AuthorAssociation string     `json:"authorAssociation"`

	MergeCommit *struct {
		OID string `json:"oid"`
	} `json:"mergeCommit"`

	Author *prBatchUser `json:"author"`

	Labels         prBatchLabels      `json:"labels"`
	Assignees      prBatchUserConn    `json:"assignees"`
	ReviewRequests prBatchReviewReqs  `json:"reviewRequests"`
	Reviews        prBatchReviews     `json:"reviews"`
	Commits        prBatchCommits     `json:"commits"`
	Files          prBatchFiles       `json:"files"`

	HeadRef *prBatchRef `json:"headRef"`
	BaseRef *prBatchRef `json:"baseRef"`

	HeadRepository *prBatchRepo `json:"headRepository"`
	BaseRepository *prBatchRepo `json:"baseRepository"`
}

type prBatchUser struct {
	Typename   string `json:"__typename"`
	Login      string `json:"login"`
	DatabaseID int64  `json:"databaseId"`
	AvatarURL  string `json:"avatarUrl"`
	URL        string `json:"url"`
	Name       string `json:"name,omitempty"`
	Email      string `json:"email,omitempty"`
}

type prBatchPageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// prBatchLabels deliberately omits databaseId. GitHub's GraphQL Label
// type exposes only the global node ID — there's no databaseId field
// (REST returns one, but it's a backwards-compat REST-only artifact).
// Mapping consequence: PullRequestLabel.PlatformID is 0 on the GraphQL
// path. The shadow-diff harness will surface this as a content
// mismatch on aveloxis_data.pull_request_labels.platform_label_id;
// it's a known parity gap, not a regression.
type prBatchLabels struct {
	Nodes []struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Color       string `json:"color"`
		Description string `json:"description"`
		IsDefault   bool   `json:"isDefault"`
	} `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchUserConn struct {
	Nodes    []prBatchUser   `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchReviewReqs struct {
	Nodes []struct {
		RequestedReviewer *prBatchUser `json:"requestedReviewer"`
	} `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchReviews struct {
	Nodes []struct {
		DatabaseID        int64        `json:"databaseId"`
		ID                string       `json:"id"`
		State             string       `json:"state"`
		Body              string       `json:"body"`
		SubmittedAt       time.Time    `json:"submittedAt"`
		AuthorAssociation string       `json:"authorAssociation"`
		URL               string       `json:"url"`
		Author            *prBatchUser `json:"author"`
		Commit            *struct {
			OID string `json:"oid"`
		} `json:"commit"`
	} `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchCommits struct {
	Nodes []struct {
		Commit struct {
			OID           string    `json:"oid"`
			Message       string    `json:"message"`
			CommittedDate time.Time `json:"committedDate"`
			Author        struct {
				Email string       `json:"email"`
				Name  string       `json:"name"`
				Date  time.Time    `json:"date"`
				User  *prBatchUser `json:"user"`
			} `json:"author"`
		} `json:"commit"`
	} `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchFiles struct {
	Nodes []struct {
		Path      string `json:"path"`
		Additions int    `json:"additions"`
		Deletions int    `json:"deletions"`
	} `json:"nodes"`
	PageInfo prBatchPageInfo `json:"pageInfo"`
}

type prBatchRef struct {
	Name   string `json:"name"`
	Target *struct {
		OID string `json:"oid"`
	} `json:"target"`
}

type prBatchRepo struct {
	DatabaseID    int64  `json:"databaseId"`
	ID            string `json:"id"`
	NameWithOwner string `json:"nameWithOwner"`
	Name          string `json:"name"`
	IsPrivate     bool   `json:"isPrivate"`
	Owner         *struct {
		Typename   string `json:"__typename"`
		Login      string `json:"login"`
		DatabaseID int64  `json:"databaseId"`
	} `json:"owner"`
}

// --- mapping to collector model types ---------------------------------

// mapPRNodeToStagedPR converts a decoded GraphQL PR node into the
// collector's StagedPR envelope. Field-for-field, the output must match
// what the REST path produces so the equivalence test shows zero
// content-level deltas on the intersection.
func mapPRNodeToStagedPR(n *prBatchPRNode, number int) StagedPR {
	pr := model.PullRequest{
		PlatformSrcID:     n.DatabaseID,
		NodeID:            n.ID,
		Number:            n.Number,
		URL:               n.URL,
		HTMLURL:           n.URL, // GraphQL conflates these; REST has both
		Title:             n.Title,
		Body:              n.Body,
		State:             mapPRState(n.State, n.MergedAt),
		Locked:            n.Locked,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
		ClosedAt:          n.ClosedAt,
		MergedAt:          n.MergedAt,
		AuthorAssociation: n.AuthorAssociation,
	}
	if n.Number == 0 {
		pr.Number = number
	}
	if n.MergeCommit != nil {
		pr.MergeCommitSHA = n.MergeCommit.OID
	}
	if n.Author != nil {
		pr.AuthorRef = userRefFromGraphQL(n.Author)
	}
	pr.Origin = model.DataOrigin{
		ToolSource: "aveloxis",
		DataSource: "GitHub GraphQL (pr batch)",
	}

	staged := StagedPR{PR: pr}

	for _, l := range n.Labels.Nodes {
		// PlatformID stays 0 — GitHub's GraphQL Label has no databaseId.
		// See prBatchLabels comment for the parity-gap rationale.
		staged.Labels = append(staged.Labels, model.PullRequestLabel{
			NodeID:      l.ID,
			Name:        l.Name,
			Description: l.Description,
			Color:       l.Color,
			IsDefault:   l.IsDefault,
			Origin:      pr.Origin,
		})
	}

	for _, a := range n.Assignees.Nodes {
		staged.Assignees = append(staged.Assignees, model.PullRequestAssignee{
			PlatformSrcID: a.DatabaseID,
			Origin:        pr.Origin,
		})
	}

	for _, r := range n.ReviewRequests.Nodes {
		if r.RequestedReviewer == nil {
			continue
		}
		staged.Reviewers = append(staged.Reviewers, model.PullRequestReviewer{
			PlatformSrcID: r.RequestedReviewer.DatabaseID,
			Origin:        pr.Origin,
		})
	}

	for _, rv := range n.Reviews.Nodes {
		review := model.PullRequestReview{
			PlatformReviewID:  rv.DatabaseID,
			NodeID:            rv.ID,
			State:             rv.State,
			Body:              rv.Body,
			SubmittedAt:       rv.SubmittedAt,
			AuthorAssociation: rv.AuthorAssociation,
			HTMLURL:           rv.URL,
			Origin:            pr.Origin,
		}
		if rv.Author != nil {
			review.AuthorRef = userRefFromGraphQL(rv.Author)
		}
		if rv.Commit != nil {
			review.CommitID = rv.Commit.OID
		}
		staged.Reviews = append(staged.Reviews, review)
	}

	for _, c := range n.Commits.Nodes {
		cm := model.PullRequestCommit{
			SHA:         c.Commit.OID,
			Message:     c.Commit.Message,
			AuthorEmail: c.Commit.Author.Email,
			Timestamp:   c.Commit.CommittedDate,
			Origin:      pr.Origin,
		}
		if c.Commit.Author.User != nil {
			cm.AuthorRef = userRefFromGraphQL(c.Commit.Author.User)
			// The User object on a commit does not carry email (GraphQL
			// splits "author identity" from "GitHub user"), so re-copy
			// the raw email to stay in sync with what REST provides.
			cm.AuthorRef.Email = c.Commit.Author.Email
			if cm.AuthorRef.Name == "" {
				cm.AuthorRef.Name = c.Commit.Author.Name
			}
		} else {
			// No linked GitHub user (anonymous commit). Preserve raw
			// name/email so contributor resolution can still try.
			cm.AuthorRef = model.UserRef{
				Name:  c.Commit.Author.Name,
				Email: c.Commit.Author.Email,
			}
		}
		staged.Commits = append(staged.Commits, cm)
	}

	for _, f := range n.Files.Nodes {
		staged.Files = append(staged.Files, model.PullRequestFile{
			Path:      f.Path,
			Additions: f.Additions,
			Deletions: f.Deletions,
			Origin:    pr.Origin,
		})
	}

	if n.HeadRef != nil {
		meta := &model.PullRequestMeta{
			HeadOrBase: "head",
			Ref:        n.HeadRef.Name,
			Origin:     pr.Origin,
		}
		if n.HeadRef.Target != nil {
			meta.SHA = n.HeadRef.Target.OID
		}
		staged.MetaHead = meta
	}
	if n.BaseRef != nil {
		meta := &model.PullRequestMeta{
			HeadOrBase: "base",
			Ref:        n.BaseRef.Name,
			Origin:     pr.Origin,
		}
		if n.BaseRef.Target != nil {
			meta.SHA = n.BaseRef.Target.OID
		}
		staged.MetaBase = meta
	}

	if n.HeadRepository != nil {
		staged.RepoHead = repoFromGraphQL(n.HeadRepository, "head", pr.Origin)
	}
	if n.BaseRepository != nil {
		staged.RepoBase = repoFromGraphQL(n.BaseRepository, "base", pr.Origin)
	}

	return staged
}

// mapPRState turns GraphQL's state enum (OPEN/CLOSED/MERGED) into the
// lowercase string the existing schema stores. MERGED dominates CLOSED
// when mergedAt is set; GraphQL returns the right enum but we re-check
// because REST mapping did, and parity matters.
func mapPRState(state string, mergedAt *time.Time) string {
	if mergedAt != nil {
		return "merged"
	}
	return strings.ToLower(state)
}

// userRefFromGraphQL builds a model.UserRef from a GraphQL author object.
// The __typename distinguishes User / Bot / Organization so contributor
// resolution can dedupe bot accounts the same way REST does (via the
// "type" field on /users/{login}).
func userRefFromGraphQL(u *prBatchUser) model.UserRef {
	if u == nil {
		return model.UserRef{}
	}
	t := "User"
	if u.Typename != "" {
		t = u.Typename
	}
	return model.UserRef{
		PlatformID: u.DatabaseID,
		Login:      u.Login,
		Name:       u.Name,
		Email:      u.Email,
		AvatarURL:  u.AvatarURL,
		URL:        u.URL,
		Type:       t,
	}
}

func repoFromGraphQL(r *prBatchRepo, headOrBase string, origin model.DataOrigin) *model.PullRequestRepo {
	out := &model.PullRequestRepo{
		HeadOrBase:   headOrBase,
		SrcRepoID:    r.DatabaseID,
		SrcNodeID:    r.ID,
		RepoName:     r.Name,
		RepoFullName: r.NameWithOwner,
		Private:      r.IsPrivate,
		Origin:       origin,
	}
	return out
}

// paginateOversizedChildren is invoked for every PR after the main batch
// query. For each connection whose pageInfo.HasNextPage is true, it fires
// follow-up GraphQL queries (one connection per query) until the
// connection is exhausted.
//
// Kept as a separate pass rather than interleaved with the main query
// because (a) the common case is zero oversized connections — a nop; and
// (b) when it does fire, each follow-up is cheap (~2 points) so the
// overhead of a separate query is negligible.
func (c *Client) paginateOversizedChildren(ctx context.Context, owner, repo string, number int, n *prBatchPRNode, staged *StagedPR) error {
	if n.Commits.PageInfo.HasNextPage {
		extra, err := c.paginatePRCommits(ctx, owner, repo, number, n.Commits.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Commits = append(staged.Commits, extra...)
	}
	if n.Files.PageInfo.HasNextPage {
		extra, err := c.paginatePRFiles(ctx, owner, repo, number, n.Files.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Files = append(staged.Files, extra...)
	}
	if n.Reviews.PageInfo.HasNextPage {
		extra, err := c.paginatePRReviews(ctx, owner, repo, number, n.Reviews.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Reviews = append(staged.Reviews, extra...)
	}
	if n.Labels.PageInfo.HasNextPage {
		extra, err := c.paginatePRLabels(ctx, owner, repo, number, n.Labels.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Labels = append(staged.Labels, extra...)
	}
	if n.Assignees.PageInfo.HasNextPage {
		extra, err := c.paginatePRAssignees(ctx, owner, repo, number, n.Assignees.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Assignees = append(staged.Assignees, extra...)
	}
	if n.ReviewRequests.PageInfo.HasNextPage {
		extra, err := c.paginatePRReviewers(ctx, owner, repo, number, n.ReviewRequests.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Reviewers = append(staged.Reviewers, extra...)
	}
	return nil
}

// paginatePRCommits follows the commits connection cursor until exhausted,
// accumulating every page into one slice. The returned slice is appended
// to whatever the initial batch query produced.
func (c *Client) paginatePRCommits(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestCommit, error) {
	query := `query PagPRCommits($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      commits(first: 100, after: $after) {
        nodes { commit {
          oid message committedDate
          author { email name date user { databaseId login avatarUrl url name email } }
        } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestCommit
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Commits prBatchCommits `json:"commits"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, c := range resp.Repository.PullRequest.Commits.Nodes {
			cm := model.PullRequestCommit{
				SHA:         c.Commit.OID,
				Message:     c.Commit.Message,
				AuthorEmail: c.Commit.Author.Email,
				Timestamp:   c.Commit.CommittedDate,
				Origin:      origin,
			}
			if c.Commit.Author.User != nil {
				cm.AuthorRef = userRefFromGraphQL(c.Commit.Author.User)
				cm.AuthorRef.Email = c.Commit.Author.Email
				if cm.AuthorRef.Name == "" {
					cm.AuthorRef.Name = c.Commit.Author.Name
				}
			} else {
				cm.AuthorRef = model.UserRef{Name: c.Commit.Author.Name, Email: c.Commit.Author.Email}
			}
			out = append(out, cm)
		}
		pi := resp.Repository.PullRequest.Commits.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

func (c *Client) paginatePRFiles(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestFile, error) {
	query := `query PagPRFiles($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      files(first: 100, after: $after) {
        nodes { path additions deletions }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestFile
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Files prBatchFiles `json:"files"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, f := range resp.Repository.PullRequest.Files.Nodes {
			out = append(out, model.PullRequestFile{
				Path:      f.Path,
				Additions: f.Additions,
				Deletions: f.Deletions,
				Origin:    origin,
			})
		}
		pi := resp.Repository.PullRequest.Files.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

func (c *Client) paginatePRReviews(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestReview, error) {
	query := `query PagPRReviews($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviews(first: 100, after: $after) {
        nodes {
          databaseId id state body submittedAt authorAssociation url
          author { __typename login ... on User { databaseId avatarUrl url } }
          commit { oid }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestReview
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Reviews prBatchReviews `json:"reviews"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, rv := range resp.Repository.PullRequest.Reviews.Nodes {
			r := model.PullRequestReview{
				PlatformReviewID:  rv.DatabaseID,
				NodeID:            rv.ID,
				State:             rv.State,
				Body:              rv.Body,
				SubmittedAt:       rv.SubmittedAt,
				AuthorAssociation: rv.AuthorAssociation,
				HTMLURL:           rv.URL,
				Origin:            origin,
			}
			if rv.Author != nil {
				r.AuthorRef = userRefFromGraphQL(rv.Author)
			}
			if rv.Commit != nil {
				r.CommitID = rv.Commit.OID
			}
			out = append(out, r)
		}
		pi := resp.Repository.PullRequest.Reviews.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

func (c *Client) paginatePRLabels(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestLabel, error) {
	query := `query PagPRLabels($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      labels(first: 100, after: $after) {
        nodes { id name color description isDefault }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestLabel
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Labels prBatchLabels `json:"labels"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, l := range resp.Repository.PullRequest.Labels.Nodes {
			// PlatformID stays 0 — see prBatchLabels comment.
			out = append(out, model.PullRequestLabel{
				NodeID:      l.ID,
				Name:        l.Name,
				Description: l.Description,
				Color:       l.Color,
				IsDefault:   l.IsDefault,
				Origin:      origin,
			})
		}
		pi := resp.Repository.PullRequest.Labels.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

func (c *Client) paginatePRAssignees(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestAssignee, error) {
	query := `query PagPRAssignees($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      assignees(first: 100, after: $after) {
        nodes { databaseId login avatarUrl url name email }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestAssignee
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Assignees prBatchUserConn `json:"assignees"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, a := range resp.Repository.PullRequest.Assignees.Nodes {
			out = append(out, model.PullRequestAssignee{
				PlatformSrcID: a.DatabaseID,
				Origin:        origin,
			})
		}
		pi := resp.Repository.PullRequest.Assignees.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

func (c *Client) paginatePRReviewers(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]model.PullRequestReviewer, error) {
	query := `query PagPRReviewers($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewRequests(first: 100, after: $after) {
        nodes { requestedReviewer { __typename ... on User { databaseId login avatarUrl url } } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []model.PullRequestReviewer
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					ReviewRequests prBatchReviewReqs `json:"reviewRequests"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, r := range resp.Repository.PullRequest.ReviewRequests.Nodes {
			if r.RequestedReviewer == nil {
				continue
			}
			out = append(out, model.PullRequestReviewer{
				PlatformSrcID: r.RequestedReviewer.DatabaseID,
				Origin:        origin,
			})
		}
		pi := resp.Repository.PullRequest.ReviewRequests.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}
