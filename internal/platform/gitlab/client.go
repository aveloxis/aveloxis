package gitlab

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Client implements platform.Client for GitLab (API v4).
// Supports both gitlab.com and self-hosted instances.
type Client struct {
	http   *platform.HTTPClient
	logger *slog.Logger
	host   string // e.g. "gitlab.com"
}

// New creates a GitLab client. baseURL should be like "https://gitlab.com/api/v4".
func New(baseURL string, keys *platform.KeyPool, logger *slog.Logger) *Client {
	host := "gitlab.com"
	if u, err := url.Parse(baseURL); err == nil {
		host = u.Host
	}
	return &Client{
		http:   platform.NewHTTPClient(baseURL, keys, logger),
		logger: logger,
		host:   host,
	}
}

func (c *Client) Platform() model.Platform {
	return model.PlatformGitLab
}

func (c *Client) ParseRepoURL(rawURL string) (owner, repo string, err error) {
	parsed, err := platform.ParseRepoURLWithHints(rawURL, map[string]bool{c.host: true})
	if err != nil {
		return "", "", err
	}
	if parsed.Platform != model.PlatformGitLab {
		return "", "", fmt.Errorf("URL %q is not a GitLab URL", rawURL)
	}
	return parsed.Owner, parsed.Repo, nil
}

// glUserToRef converts a GitLab user to a model.UserRef for contributor resolution.
func glUserToRef(u glUser) model.UserRef {
	email := u.Email
	if email == "" {
		email = u.PublicEmail
	}
	return model.UserRef{
		PlatformID: u.ID,
		Login:      u.Username,
		Name:       u.Name,
		Email:      email,
		AvatarURL:  u.AvatarURL,
		URL:        u.WebURL,
	}
}

// projectPath returns the URL-encoded full path for GitLab API calls.
// e.g. "group/subgroup" + "project" -> "group%2Fsubgroup%2Fproject"
func projectPath(owner, repo string) string {
	return url.PathEscape(owner + "/" + repo)
}

// --- IssueCollector ---

func (c *Client) ListIssues(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.Issue, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/issues?scope=all&sort=desc&order_by=updated_at", pp)
	if !since.IsZero() {
		path += "&updated_after=" + since.Format(time.RFC3339)
	}

	return func(yield func(model.Issue, error) bool) {
		for raw, err := range platform.PaginateGitLab[glIssue](ctx, c.http, path) {
			if err != nil {
				yield(model.Issue{}, err)
				return
			}
			// GitLab state "opened" -> normalized "open"
			state := raw.State
			if state == "opened" {
				state = "open"
			}
			issue := model.Issue{
				PlatformID:   raw.ID,
				Number:       raw.IID,
				Title:        raw.Title,
				Body:         raw.Description,
				State:        state,
				HTMLURL:      raw.WebURL,
				CreatedAt:    raw.CreatedAt,
				UpdatedAt:    raw.UpdatedAt,
				ClosedAt:     raw.ClosedAt,
				CommentCount: raw.UserNotesCount,
				ReporterRef:  glUserToRef(raw.Author),
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitLab API",
				},
			}
			if raw.ClosedBy != nil {
				issue.ClosedByRef = glUserToRef(*raw.ClosedBy)
			}
			if !yield(issue, nil) {
				return
			}
		}
	}
}

func (c *Client) ListIssueLabels(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueLabel, error] {
	// GitLab embeds label names in the issue response but not full label objects.
	// Fetch the issue to get label names, then look up full details.
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/issues/%d", pp, issueNumber)

	return func(yield func(model.IssueLabel, error) bool) {
		var raw glIssue
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.IssueLabel{}, err)
			return
		}
		for _, name := range raw.Labels {
			if !yield(model.IssueLabel{
				Text: name,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListIssueAssignees(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueAssignee, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/issues/%d", pp, issueNumber)

	return func(yield func(model.IssueAssignee, error) bool) {
		var raw glIssue
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.IssueAssignee{}, err)
			return
		}
		for _, a := range raw.Assignees {
			if !yield(model.IssueAssignee{
				PlatformSrcID: a.ID,
			}, nil) {
				return
			}
		}
	}
}

// --- PullRequestCollector (GitLab Merge Requests) ---

func (c *Client) ListPullRequests(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequest, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests?scope=all&sort=desc&order_by=updated_at", pp)
	if !since.IsZero() {
		path += "&updated_after=" + since.Format(time.RFC3339)
	}

	return func(yield func(model.PullRequest, error) bool) {
		for raw, err := range platform.PaginateGitLab[glMergeRequest](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequest{}, err)
				return
			}
			// Normalize GitLab state to match model.
			state := raw.State
			if state == "opened" {
				state = "open"
			}

			mergeCommit := raw.MergeCommitSHA
			if mergeCommit == "" {
				mergeCommit = raw.SquashCommitSHA
			}

			pr := model.PullRequest{
				PlatformSrcID:  raw.ID,
				Number:         raw.IID,
				HTMLURL:        raw.WebURL,
				DiffURL:        raw.WebURL + ".diff",
				Title:          raw.Title,
				Body:           raw.Description,
				State:          state,
				Locked:         state == "locked",
				CreatedAt:      raw.CreatedAt,
				UpdatedAt:      raw.UpdatedAt,
				ClosedAt:       raw.ClosedAt,
				MergedAt:       raw.MergedAt,
				MergeCommitSHA: mergeCommit,
				AuthorRef:      glUserToRef(raw.Author),
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitLab API",
				},
			}
			if !yield(pr, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRLabels(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestLabel, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, prNumber)

	return func(yield func(model.PullRequestLabel, error) bool) {
		var raw glMergeRequest
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.PullRequestLabel{}, err)
			return
		}
		for _, name := range raw.Labels {
			if !yield(model.PullRequestLabel{
				Name: name,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRAssignees(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestAssignee, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, prNumber)

	return func(yield func(model.PullRequestAssignee, error) bool) {
		var raw glMergeRequest
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.PullRequestAssignee{}, err)
			return
		}
		for _, a := range raw.Assignees {
			if !yield(model.PullRequestAssignee{
				PlatformSrcID: a.ID,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRReviewers(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestReviewer, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, prNumber)

	return func(yield func(model.PullRequestReviewer, error) bool) {
		var raw glMergeRequest
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.PullRequestReviewer{}, err)
			return
		}
		for _, r := range raw.Reviewers {
			if !yield(model.PullRequestReviewer{
				PlatformSrcID: r.ID,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRReviews(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestReview, error] {
	// GitLab doesn't have a direct "reviews" concept like GitHub.
	// The closest equivalent is the approvals API.
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d/approvals", pp, prNumber)

	return func(yield func(model.PullRequestReview, error) bool) {
		var resp struct {
			ApprovedBy []glMRApproval `json:"approved_by"`
		}
		if err := c.http.GetJSON(ctx, path, &resp); err != nil {
			yield(model.PullRequestReview{}, err)
			return
		}
		for _, approval := range resp.ApprovedBy {
			if !yield(model.PullRequestReview{
				PlatformReviewID: approval.ID,
				PlatformID:       model.PlatformGitLab,
				State:            "APPROVED",
				AuthorRef:        glUserToRef(approval.User),
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRCommits(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestCommit, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d/commits", pp, prNumber)

	return func(yield func(model.PullRequestCommit, error) bool) {
		for raw, err := range platform.PaginateGitLab[glCommit](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestCommit{}, err)
				return
			}
			if !yield(model.PullRequestCommit{
				SHA:         raw.ID,
				Message:     raw.Message,
				AuthorEmail: raw.AuthorEmail,
				Timestamp:   raw.AuthoredDate,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRFiles(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestFile, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d/diffs", pp, prNumber)

	return func(yield func(model.PullRequestFile, error) bool) {
		for raw, err := range platform.PaginateGitLab[glDiff](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestFile{}, err)
				return
			}
			// GitLab doesn't provide addition/deletion counts per file in the diffs endpoint.
			// Count from the diff text as an approximation.
			adds, dels := countDiffLines(raw.Diff)
			filePath := raw.NewPath
			if raw.DeletedFile {
				filePath = raw.OldPath
			}
			if !yield(model.PullRequestFile{
				Path:      filePath,
				Additions: adds,
				Deletions: dels,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) FetchPRMeta(ctx context.Context, owner, repo string, prNumber int) (head, base *model.PullRequestMeta, err error) {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, prNumber)
	var raw glMergeRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, nil, err
	}

	head = &model.PullRequestMeta{
		HeadOrBase: "head",
		Ref:        raw.SourceBranch,
		SHA:        raw.SHA,
	}
	base = &model.PullRequestMeta{
		HeadOrBase: "base",
		Ref:        raw.TargetBranch,
	}
	return head, base, nil
}

// --- EventCollector ---

func (c *Client) ListIssueEvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.IssueEvent, error] {
	pp := projectPath(owner, repo)
	// GitLab uses resource state events per-issue. We iterate all issues and fetch events.
	// For bulk collection, we use the project events endpoint filtered to issues.
	path := fmt.Sprintf("/projects/%s/events?target_type=issue&sort=desc", pp)
	if !since.IsZero() {
		path += "&after=" + since.Format("2006-01-02")
	}

	return func(yield func(model.IssueEvent, error) bool) {
		for raw, err := range platform.PaginateGitLab[glResourceEvent](ctx, c.http, path) {
			if err != nil {
				yield(model.IssueEvent{}, err)
				return
			}
			action := raw.Action
			if action == "" {
				action = raw.State
			}
			if !yield(model.IssueEvent{
				PlatformEventID: raw.ID,
				PlatformID:      model.PlatformGitLab,
				Action:          action,
				CreatedAt:       raw.CreatedAt,
				ActorRef:        glUserToRef(raw.User),
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPREvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequestEvent, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/events?target_type=merge_request&sort=desc", pp)
	if !since.IsZero() {
		path += "&after=" + since.Format("2006-01-02")
	}

	return func(yield func(model.PullRequestEvent, error) bool) {
		for raw, err := range platform.PaginateGitLab[glResourceEvent](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestEvent{}, err)
				return
			}
			action := raw.Action
			if action == "" {
				action = raw.State
			}
			if !yield(model.PullRequestEvent{
				PlatformEventID: raw.ID,
				PlatformID:      model.PlatformGitLab,
				Action:          action,
				CreatedAt:       raw.CreatedAt,
				ActorRef:        glUserToRef(raw.User),
			}, nil) {
				return
			}
		}
	}
}

// --- MessageCollector ---

func (c *Client) ListIssueComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.MessageWithRef, error] {
	pp := projectPath(owner, repo)

	// GitLab doesn't have a bulk notes endpoint across all issues.
	// We iterate issues and fetch their notes.
	issuesPath := fmt.Sprintf("/projects/%s/issues?scope=all&sort=desc&order_by=updated_at", pp)
	if !since.IsZero() {
		issuesPath += "&updated_after=" + since.Format(time.RFC3339)
	}

	return func(yield func(platform.MessageWithRef, error) bool) {
		for issue, err := range platform.PaginateGitLab[glIssue](ctx, c.http, issuesPath) {
			if err != nil {
				yield(platform.MessageWithRef{}, err)
				return
			}
			if issue.UserNotesCount == 0 {
				continue
			}
			noteP := fmt.Sprintf("/projects/%s/issues/%d/notes?sort=asc", pp, issue.IID)
			for note, err := range platform.PaginateGitLab[glNote](ctx, c.http, noteP) {
				if err != nil {
					yield(platform.MessageWithRef{}, err)
					return
				}
				if note.System {
					continue // Skip system notes (events); we capture those separately.
				}
				msg := model.Message{
					PlatformMsgID: note.ID,
					PlatformID:    model.PlatformGitLab,
					Text:          note.Body,
					Timestamp:     note.CreatedAt,
					AuthorRef:     glUserToRef(note.Author),
				}
				ref := platform.MessageWithRef{
					Message: msg,
					IssueRef: &model.IssueMessageRef{
						PlatformSrcID: note.ID,
					},
				}
				if !yield(ref, nil) {
					return
				}
			}
		}
	}
}

func (c *Client) ListPRComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.MessageWithRef, error] {
	pp := projectPath(owner, repo)
	mrsPath := fmt.Sprintf("/projects/%s/merge_requests?scope=all&sort=desc&order_by=updated_at", pp)
	if !since.IsZero() {
		mrsPath += "&updated_after=" + since.Format(time.RFC3339)
	}

	return func(yield func(platform.MessageWithRef, error) bool) {
		for mr, err := range platform.PaginateGitLab[glMergeRequest](ctx, c.http, mrsPath) {
			if err != nil {
				yield(platform.MessageWithRef{}, err)
				return
			}
			if mr.UserNotesCount == 0 {
				continue
			}
			noteP := fmt.Sprintf("/projects/%s/merge_requests/%d/notes?sort=asc", pp, mr.IID)
			for note, err := range platform.PaginateGitLab[glNote](ctx, c.http, noteP) {
				if err != nil {
					yield(platform.MessageWithRef{}, err)
					return
				}
				if note.System {
					continue
				}
				msg := model.Message{
					PlatformMsgID: note.ID,
					PlatformID:    model.PlatformGitLab,
					Text:          note.Body,
					Timestamp:     note.CreatedAt,
					AuthorRef:     glUserToRef(note.Author),
				}
				ref := platform.MessageWithRef{
					Message: msg,
					PRRef: &model.PullRequestMessageRef{
						PlatformSrcID: note.ID,
					},
				}
				if !yield(ref, nil) {
					return
				}
			}
		}
	}
}

func (c *Client) ListReviewComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.ReviewCommentWithRef, error] {
	pp := projectPath(owner, repo)
	mrsPath := fmt.Sprintf("/projects/%s/merge_requests?scope=all&sort=desc&order_by=updated_at", pp)
	if !since.IsZero() {
		mrsPath += "&updated_after=" + since.Format(time.RFC3339)
	}

	return func(yield func(platform.ReviewCommentWithRef, error) bool) {
		for mr, err := range platform.PaginateGitLab[glMergeRequest](ctx, c.http, mrsPath) {
			if err != nil {
				yield(platform.ReviewCommentWithRef{}, err)
				return
			}
			discPath := fmt.Sprintf("/projects/%s/merge_requests/%d/discussions", pp, mr.IID)
			for disc, err := range platform.PaginateGitLab[glDiscussion](ctx, c.http, discPath) {
				if err != nil {
					yield(platform.ReviewCommentWithRef{}, err)
					return
				}
				for _, note := range disc.Notes {
					if note.System || note.Position == nil {
						continue // skip non-diff notes and system notes
					}
					pos := note.Position
					side := "RIGHT"
					if pos.OldLine != nil && pos.NewLine == nil {
						side = "LEFT"
					}
					var line, origLine, startLine, origStartLine *int
					line = pos.NewLine
					origLine = pos.OldLine
					if pos.LineRange != nil {
						startLine = pos.LineRange.Start.NewLine
						origStartLine = pos.LineRange.Start.OldLine
					}
					msg := model.Message{
						PlatformMsgID: note.ID,
						PlatformID:    model.PlatformGitLab,
						Text:          note.Body,
						Timestamp:     note.CreatedAt,
						AuthorRef:     glUserToRef(note.Author),
					}
					comment := model.ReviewComment{
						PlatformSrcID:     note.ID,
						Path:              pos.NewPath,
						CommitID:          pos.HeadSHA,
						OriginalCommitID:  pos.BaseSHA,
						Line:              line,
						OriginalLine:      origLine,
						Side:              side,
						StartLine:         startLine,
						OriginalStartLine: origStartLine,
						HTMLURL:           "", // GitLab notes don't have direct URLs in the API
						UpdatedAt:         note.UpdatedAt,
					}
					if !yield(platform.ReviewCommentWithRef{Message: msg, Comment: comment}, nil) {
						return
					}
				}
			}
		}
	}
}

// --- ReleaseCollector ---

func (c *Client) ListReleases(ctx context.Context, owner, repo string) iter.Seq2[model.Release, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/releases?sort=desc&order_by=released_at", pp)

	return func(yield func(model.Release, error) bool) {
		for raw, err := range platform.PaginateGitLab[glRelease](ctx, c.http, path) {
			if err != nil {
				yield(model.Release{}, err)
				return
			}
			if !yield(model.Release{
				ID:          raw.TagName, // GitLab doesn't expose numeric release IDs
				Name:        raw.Name,
				Description: raw.Description,
				Author:      raw.Author.Username,
				TagName:     raw.TagName,
				URL:         raw.Links.Self,
				CreatedAt:   raw.CreatedAt,
				PublishedAt: raw.ReleasedAt,
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitLab API",
				},
			}, nil) {
				return
			}
		}
	}
}

// --- ContributorCollector ---

func (c *Client) ListContributors(ctx context.Context, owner, repo string) iter.Seq2[model.Contributor, error] {
	pp := projectPath(owner, repo)

	return func(yield func(model.Contributor, error) bool) {
		// First: project members (users with explicit access).
		membersPath := fmt.Sprintf("/projects/%s/members/all", pp)
		for raw, err := range platform.PaginateGitLab[glMember](ctx, c.http, membersPath) {
			if err != nil {
				yield(model.Contributor{}, err)
				return
			}
			if !yield(model.Contributor{
				Login: raw.Username,
				Identities: []model.ContributorIdentity{{
					Platform:  model.PlatformGitLab,
					UserID:    raw.ID,
					Login:     raw.Username,
					Name:      raw.Name,
					AvatarURL: raw.AvatarURL,
					URL:       raw.WebURL,
				}},
			}, nil) {
				return
			}
		}

		// Second: git contributors (from repository commits).
		contribPath := fmt.Sprintf("/projects/%s/repository/contributors", pp)
		for raw, err := range platform.PaginateGitLab[glContributor](ctx, c.http, contribPath) {
			if err != nil {
				// This endpoint may 403 for some repos; don't fail entirely.
				c.logger.Warn("failed to fetch git contributors", "error", err)
				return
			}
			if !yield(model.Contributor{
				Login:    raw.Name,
				Email:    raw.Email,
				FullName: raw.Name,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) EnrichContributor(ctx context.Context, login string) (*model.Contributor, error) {
	// GitLab: /users?username=login returns an array.
	path := fmt.Sprintf("/users?username=%s", url.QueryEscape(login))
	var users []glUser
	if err := c.http.GetJSON(ctx, path, &users); err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("user %q not found", login)
	}
	raw := users[0]
	return &model.Contributor{
		Login:    raw.Username,
		Email:    raw.PublicEmail,
		FullName: raw.Name,
		Company:  raw.Company,
		Location: raw.Location,
		Identities: []model.ContributorIdentity{{
			Platform:  model.PlatformGitLab,
			UserID:    raw.ID,
			Login:     raw.Username,
			Name:      raw.Name,
			Email:     raw.PublicEmail,
			AvatarURL: raw.AvatarURL,
			URL:       raw.WebURL,
		}},
	}, nil
}

// --- RepoCollector ---

func (c *Client) FetchRepoInfo(ctx context.Context, owner, repo string) (*model.RepoInfo, error) {
	pp := projectPath(owner, repo)

	// Primary project data with statistics (gives commit count, fork count, star count).
	path := fmt.Sprintf("/projects/%s?statistics=true", pp)
	var raw glProject
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}

	license := ""
	if raw.License != nil {
		license = raw.License.Name
	}
	commitCount := 0
	if raw.Statistics != nil {
		commitCount = raw.Statistics.CommitCount
	}

	// GitLab issues_statistics endpoint — gives total and by-state counts in one call.
	// GET /projects/:id/issues_statistics returns:
	//   { "statistics": { "counts": { "all": N, "closed": N, "opened": N } } }
	var issueStats struct {
		Statistics struct {
			Counts struct {
				All    int `json:"all"`
				Closed int `json:"closed"`
				Opened int `json:"opened"`
			} `json:"counts"`
		} `json:"statistics"`
	}
	issueStatsPath := fmt.Sprintf("/projects/%s/issues_statistics", pp)
	_ = c.http.GetJSON(ctx, issueStatsPath, &issueStats)

	// GitLab merge_requests count by state.
	// The /merge_requests endpoint returns X-Total header with per_page=1 for cheap counts.
	mrOpen := c.countGitLabResource(ctx, pp, "merge_requests", "opened")
	mrClosed := c.countGitLabResource(ctx, pp, "merge_requests", "closed")
	mrMerged := c.countGitLabResource(ctx, pp, "merge_requests", "merged")
	mrTotal := mrOpen + mrClosed + mrMerged

	status := "Active"
	if raw.Archived {
		status = "Archived"
	}

	return &model.RepoInfo{
		LastUpdated:   raw.LastActivityAt,
		IssuesEnabled: raw.IssuesEnabled,
		PRsEnabled:    raw.MergeRequestsEnabled,
		WikiEnabled:   raw.WikiEnabled,
		PagesEnabled:  raw.PagesAccessLevel != "disabled",
		ForkCount:     raw.ForksCount,
		StarCount:     raw.StarCount,
		OpenIssues:    raw.OpenIssuesCount,
		DefaultBranch: raw.DefaultBranch,
		License:       license,
		LicenseFile:   license,
		CommitCount:   commitCount,
		IssuesCount:   issueStats.Statistics.Counts.All,
		IssuesClosed:  issueStats.Statistics.Counts.Closed,
		PRCount:       mrTotal,
		PRsOpen:       mrOpen,
		PRsClosed:     mrClosed,
		PRsMerged:     mrMerged,
		Status:        status,
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitLab API",
		},
	}, nil
}

// countGitLabResource returns the total count for a filtered resource using
// per_page=1 and reading the X-Total response header. This is the cheapest way
// to get counts from GitLab without paginating through all results.
func (c *Client) countGitLabResource(ctx context.Context, projectPath, resource, state string) int {
	path := fmt.Sprintf("/projects/%s/%s?state=%s&per_page=1", projectPath, resource, state)
	resp, err := c.http.Get(ctx, path)
	if err != nil {
		return 0
	}
	resp.Body.Close()
	total := resp.Header.Get("X-Total")
	if total == "" {
		return 0
	}
	n, _ := strconv.Atoi(total)
	return n
}

func (c *Client) FetchCloneStats(ctx context.Context, owner, repo string) ([]model.RepoClone, error) {
	// GitLab doesn't expose clone statistics via the API for non-admins.
	// Return empty; the facade/git layer can provide this from git operations.
	return nil, nil
}

// countDiffLines counts added and removed lines from a unified diff string.
func countDiffLines(diff string) (adds, dels int) {
	for _, line := range strings.Split(diff, "\n") {
		if len(line) == 0 {
			continue
		}
		switch line[0] {
		case '+':
			if !strings.HasPrefix(line, "+++") {
				adds++
			}
		case '-':
			if !strings.HasPrefix(line, "---") {
				dels++
			}
		}
	}
	return
}

// FetchIssueByNumber fetches a single issue by IID for targeted gap filling.
func (c *Client) FetchIssueByNumber(ctx context.Context, owner, repo string, number int) (*model.Issue, error) {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/issues/%d", pp, number)
	var raw glIssue
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	state := raw.State
	if state == "opened" {
		state = "open"
	}
	issue := &model.Issue{
		PlatformID:   raw.ID,
		Number:       raw.IID,
		Title:        raw.Title,
		Body:         raw.Description,
		State:        state,
		HTMLURL:      raw.WebURL,
		CreatedAt:    raw.CreatedAt,
		UpdatedAt:    raw.UpdatedAt,
		ClosedAt:     raw.ClosedAt,
		CommentCount: raw.UserNotesCount,
		ReporterRef:  glUserToRef(raw.Author),
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitLab API (gap fill)",
		},
	}
	if raw.ClosedBy != nil {
		issue.ClosedByRef = glUserToRef(*raw.ClosedBy)
	}
	return issue, nil
}

// FetchPRByNumber fetches a single merge request by IID for targeted gap filling.
func (c *Client) FetchPRByNumber(ctx context.Context, owner, repo string, number int) (*model.PullRequest, error) {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, number)
	var raw glMergeRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	state := raw.State
	if state == "opened" {
		state = "open"
	}
	mergeCommit := raw.MergeCommitSHA
	if mergeCommit == "" {
		mergeCommit = raw.SquashCommitSHA
	}
	pr := &model.PullRequest{
		PlatformSrcID:  raw.ID,
		Number:         raw.IID,
		HTMLURL:        raw.WebURL,
		DiffURL:        raw.WebURL + ".diff",
		Title:          raw.Title,
		Body:           raw.Description,
		State:          state,
		Locked:         state == "locked",
		CreatedAt:      raw.CreatedAt,
		UpdatedAt:      raw.UpdatedAt,
		ClosedAt:       raw.ClosedAt,
		MergedAt:       raw.MergedAt,
		MergeCommitSHA: mergeCommit,
		AuthorRef:      glUserToRef(raw.Author),
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitLab API (gap fill)",
		},
	}
	return pr, nil
}
