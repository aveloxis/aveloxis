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

// prBatchSize is how many PRs go into one GraphQL query. Lowered from 25
// to 10 in v0.18.22 (Fix B) after production repos like apache/spark and
// grpc/grpc hit GitHub's server-side "Timeout on validation of query" and
// mid-body stream-CANCEL limits. A 25-alias query with 8 child
// connections × (pre-Fix-A) 100 items could produce responses with tens
// of thousands of sub-records — too expensive for GitHub's planner.
// The smaller batch amortizes poorly on tiny repos but eliminates the
// expensive-query failure mode for active ones; net throughput was still
// better than REST in benchmarks because the per-query cost is bounded.
// Point budget is not a concern: each batch is ~2–10 points and the
// per-key GraphQL quota is 5000/hour.
const prBatchSize = 10

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
      labels(first: 50) {
        nodes { id name color description isDefault }
        pageInfo { hasNextPage endCursor }
      }
      assignees(first: 50) {
        nodes { databaseId login avatarUrl url name email }
        pageInfo { hasNextPage endCursor }
      }
      reviewRequests(first: 50) {
        nodes {
          requestedReviewer {
            __typename
            ... on User { databaseId login avatarUrl url }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
      reviews(first: 50) {
        nodes {
          databaseId id state body submittedAt authorAssociation url
          author { __typename login ... on User { databaseId avatarUrl url } }
          commit { oid }
        }
        pageInfo { hasNextPage endCursor }
      }
      comments(first: 50) {
        nodes {
          databaseId id body createdAt updatedAt url authorAssociation
          author { __typename login ... on User { databaseId avatarUrl url name email } }
        }
        pageInfo { hasNextPage endCursor }
      }
      commits(first: 50) {
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
      files(first: 50) {
        nodes { path additions deletions }
        pageInfo { hasNextPage endCursor }
      }
      # Persistent scalar fields — stay populated even after branch deletion.
      # The headRef/baseRef object pointers go null once the branch is gone;
      # the *Name and *Oid scalars are frozen at PR-open time and live forever.
      # Without this distinction, meta rows (and by extension repo rows, which
      # require a meta_id) were missing for ~65% of PRs in phase 1's first
      # equivalence run against augurlabs/augur.
      headRefName
      headRefOid
      baseRefName
      baseRefOid
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
	Comments       prBatchComments    `json:"comments"`

	// Persistent scalar fields — see prNodeFragment comment. Always
	// populated from GitHub as long as the PR exists, even after the
	// branch/fork is deleted.
	HeadRefName string `json:"headRefName"`
	HeadRefOid  string `json:"headRefOid"`
	BaseRefName string `json:"baseRefName"`
	BaseRefOid  string `json:"baseRefOid"`

	// Pointer-valued. Null if the branch/fork was deleted. Used only for
	// the head/base repository (fork-source) snapshot which has no
	// persistent-scalar equivalent.
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
	Nodes    []prBatchReviewNode `json:"nodes"`
	PageInfo prBatchPageInfo     `json:"pageInfo"`
}

// prBatchReviewNode carries everything we need from a PullRequestReview
// GraphQL node: review metadata + body. Inline review comments are NOT
// fetched via GraphQL — GitHub's GraphQL `PullRequestReviewComment` type
// does not expose the `side` / `startSide` fields the REST schema carries,
// and deriving them from `line`/`originalLine` is not bijective with
// REST's values on context-line comments. Phase 4 keeps the repo-wide
// REST `/pulls/comments` endpoint running (via `ListReviewComments`) so
// `review_comments.pr_cmt_side` / `pr_cmt_start_side` stay populated and
// the phase-4 shadow-diff has clean comparability against the REST shadow.
type prBatchReviewNode struct {
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
}

// prBatchComments decodes the PullRequest.comments connection (conversation
// comments — NOT inline review comments). On GitHub these are IssueComment
// nodes, identical in shape to Issue.comments. Mapped to MessageWithRef
// with a PRRef so the staged collector writes to pull_request_message_ref.
type prBatchComments struct {
	Nodes []struct {
		DatabaseID        int64        `json:"databaseId"`
		ID                string       `json:"id"`
		Body              string       `json:"body"`
		CreatedAt         time.Time    `json:"createdAt"`
		UpdatedAt         time.Time    `json:"updatedAt"`
		URL               string       `json:"url"`
		AuthorAssociation string       `json:"authorAssociation"`
		Author            *prBatchUser `json:"author"`
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
			// PlatformID is an FK to the platform table (1=GitHub, 2=GitLab,
			// 3=GenericGit). Leaving it 0 would FK-violate the reviews
			// upsert and silently drop every review from the DB —
			// exactly what the v0.18.1 phase 1 first run did.
			PlatformID:        model.PlatformGitHub,
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

	// Always emit head/base meta from the persistent scalar fields. The
	// REST path's pull_request_meta table has exactly head+base rows per
	// PR (2× row count); matching that parity requires we never skip meta
	// emission just because the live branch pointer went null.
	if n.HeadRefName != "" || n.HeadRefOid != "" {
		staged.MetaHead = &model.PullRequestMeta{
			HeadOrBase: "head",
			Ref:        n.HeadRefName,
			SHA:        n.HeadRefOid,
			Origin:     pr.Origin,
		}
	}
	if n.BaseRefName != "" || n.BaseRefOid != "" {
		staged.MetaBase = &model.PullRequestMeta{
			HeadOrBase: "base",
			Ref:        n.BaseRefName,
			SHA:        n.BaseRefOid,
			Origin:     pr.Origin,
		}
	}

	if n.HeadRepository != nil {
		staged.RepoHead = repoFromGraphQL(n.HeadRepository, "head", pr.Origin)
	}
	if n.BaseRepository != nil {
		staged.RepoBase = repoFromGraphQL(n.BaseRepository, "base", pr.Origin)
	}

	// PR conversation comments: IssueComment nodes on the PullRequest.
	// Each becomes one message + one pull_request_message_ref row.
	// PRRef.PlatformPRNumber ties the comment back to the parent PR
	// without relying on the local pull_request_id serial, same contract
	// as the REST ListIssueComments path.
	for _, c := range n.Comments.Nodes {
		msg := model.Message{
			PlatformMsgID: c.DatabaseID,
			PlatformID:    model.PlatformGitHub,
			NodeID:        c.ID,
			Text:          c.Body,
			Timestamp:     c.CreatedAt,
		}
		if c.Author != nil {
			msg.AuthorRef = userRefFromGraphQL(c.Author)
		}
		msg.Origin = pr.Origin
		staged.Comments = append(staged.Comments, platform.MessageWithRef{
			Message: msg,
			PRRef: &model.PullRequestMessageRef{
				PlatformSrcID:    c.DatabaseID,
				PlatformNodeID:   c.ID,
				PlatformPRNumber: pr.Number,
			},
		})
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
	if n.Comments.PageInfo.HasNextPage {
		extra, err := c.paginatePRComments(ctx, owner, repo, number, n.Comments.PageInfo.EndCursor, staged.PR.Origin)
		if err != nil {
			return err
		}
		staged.Comments = append(staged.Comments, extra...)
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
				PlatformID:        model.PlatformGitHub, // FK to platforms — see mapPRNodeToStagedPR for why required
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

// paginatePRComments follows the PR.comments connection (conversation
// comments — the equivalent of REST's /issues/{n}/comments). Each comment
// lands in aveloxis_data.messages via the staged collector, paired with a
// PullRequestMessageRef so it joins back to the PR.
func (c *Client) paginatePRComments(ctx context.Context, owner, repo string, number int, cursor string, origin model.DataOrigin) ([]platform.MessageWithRef, error) {
	query := `query PagPRComments($owner: String!, $repo: String!, $number: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      comments(first: 100, after: $after) {
        nodes {
          databaseId id body createdAt updatedAt url authorAssociation
          author { __typename login ... on User { databaseId avatarUrl url name email } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
	var out []platform.MessageWithRef
	for {
		vars := map[string]any{"owner": owner, "repo": repo, "number": number, "after": cursor}
		var resp struct {
			Repository struct {
				PullRequest struct {
					Comments prBatchComments `json:"comments"`
				} `json:"pullRequest"`
			} `json:"repository"`
		}
		if err := c.http.GraphQL(ctx, query, vars, &resp); err != nil {
			return out, err
		}
		for _, cm := range resp.Repository.PullRequest.Comments.Nodes {
			msg := model.Message{
				PlatformMsgID: cm.DatabaseID,
				PlatformID:    model.PlatformGitHub,
				NodeID:        cm.ID,
				Text:          cm.Body,
				Timestamp:     cm.CreatedAt,
				Origin:        origin,
			}
			if cm.Author != nil {
				msg.AuthorRef = userRefFromGraphQL(cm.Author)
			}
			out = append(out, platform.MessageWithRef{
				Message: msg,
				PRRef: &model.PullRequestMessageRef{
					PlatformSrcID:    cm.DatabaseID,
					PlatformNodeID:   cm.ID,
					PlatformPRNumber: number,
				},
			})
		}
		pi := resp.Repository.PullRequest.Comments.PageInfo
		if !pi.HasNextPage {
			return out, nil
		}
		cursor = pi.EndCursor
	}
}

