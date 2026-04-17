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
		http:   platform.NewHTTPClient(baseURL, keys, logger, platform.AuthGitLab),
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

// FetchPRRepos returns fork repo details for a merge request's source and target projects.
func (c *Client) FetchPRRepos(ctx context.Context, owner, repo string, prNumber int) (headRepo, baseRepo *model.PullRequestRepo, err error) {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d", pp, prNumber)
	var raw glMergeRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, nil, err
	}

	// Source project (head/fork).
	if raw.SourceProjectID != 0 {
		headRepo = c.fetchGLProjectAsRepo(ctx, raw.SourceProjectID, "head")
	}
	// Target project (base/upstream).
	if raw.TargetProjectID != 0 {
		baseRepo = c.fetchGLProjectAsRepo(ctx, raw.TargetProjectID, "base")
	}
	return headRepo, baseRepo, nil
}

// fetchGLProjectAsRepo fetches a GitLab project and converts it to a PullRequestRepo.
func (c *Client) fetchGLProjectAsRepo(ctx context.Context, projectID int64, headOrBase string) *model.PullRequestRepo {
	path := fmt.Sprintf("/projects/%d", projectID)
	var proj struct {
		ID                int64  `json:"id"`
		Name              string `json:"name"`
		PathWithNamespace string `json:"path_with_namespace"`
		Visibility        string `json:"visibility"` // "public", "internal", "private"
	}
	if err := c.http.GetJSON(ctx, path, &proj); err != nil {
		return nil
	}
	return &model.PullRequestRepo{
		HeadOrBase:   headOrBase,
		SrcRepoID:    proj.ID,
		RepoName:     proj.Name,
		RepoFullName: proj.PathWithNamespace,
		Private:      proj.Visibility == "private",
		Origin:       model.DataOrigin{DataSource: "GitLab API"},
	}
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

// ListCommentsForIssue returns all user notes on a single GitLab issue via
// GET /projects/:id/issues/:iid/notes. System notes (timeline events like
// "assigned to bob") are filtered out — those are captured through the
// separate events pipeline, and including them here would double-count.
func (c *Client) ListCommentsForIssue(ctx context.Context, owner, repo string, issueIID int) iter.Seq2[platform.MessageWithRef, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/issues/%d/notes?sort=asc", pp, issueIID)
	return func(yield func(platform.MessageWithRef, error) bool) {
		for note, err := range platform.PaginateGitLab[glNote](ctx, c.http, path) {
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
				IssueRef: &model.IssueMessageRef{
					PlatformSrcID:       note.ID,
					PlatformIssueNumber: issueIID,
				},
			}
			if !yield(ref, nil) {
				return
			}
		}
	}
}

// ListCommentsForPR returns conversation notes on a single merge request via
// GET /projects/:id/merge_requests/:iid/notes. System notes are skipped (see
// ListCommentsForIssue for rationale).
func (c *Client) ListCommentsForPR(ctx context.Context, owner, repo string, mrIID int) iter.Seq2[platform.MessageWithRef, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d/notes?sort=asc", pp, mrIID)
	return func(yield func(platform.MessageWithRef, error) bool) {
		for note, err := range platform.PaginateGitLab[glNote](ctx, c.http, path) {
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
					PlatformSrcID:    note.ID,
					PlatformPRNumber: mrIID,
				},
			}
			if !yield(ref, nil) {
				return
			}
		}
	}
}

// ListReviewCommentsForPR returns inline (diff-line-anchored) review comments
// for a single merge request via GET /projects/:id/merge_requests/:iid/discussions,
// filtered to notes that carry a `position` (those are diff-anchored; notes
// without a position are conversation comments that belong in ListCommentsForPR).
func (c *Client) ListReviewCommentsForPR(ctx context.Context, owner, repo string, mrIID int) iter.Seq2[platform.ReviewCommentWithRef, error] {
	pp := projectPath(owner, repo)
	path := fmt.Sprintf("/projects/%s/merge_requests/%d/discussions", pp, mrIID)
	return func(yield func(platform.ReviewCommentWithRef, error) bool) {
		for disc, err := range platform.PaginateGitLab[glDiscussion](ctx, c.http, path) {
			if err != nil {
				yield(platform.ReviewCommentWithRef{}, err)
				return
			}
			for _, note := range disc.Notes {
				if note.System || note.Position == nil {
					continue
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
					UpdatedAt:         note.UpdatedAt,
				}
				if !yield(platform.ReviewCommentWithRef{Message: msg, Comment: comment}, nil) {
					return
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
			// GitLab access_level >= 50 (Owner) approximates GitHub's site_admin.
			// 50 = Owner, 40 = Maintainer, 30 = Developer, 20 = Reporter, 10 = Guest.
			isAdmin := raw.AccessLevel >= 50
			if !yield(model.Contributor{
				Login: raw.Username,
				Identities: []model.ContributorIdentity{{
					Platform:  model.PlatformGitLab,
					UserID:    raw.ID,
					Login:     raw.Username,
					Name:      raw.Name,
					AvatarURL: raw.AvatarURL,
					URL:       raw.WebURL,
					IsAdmin:   isAdmin,
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
	var createdAt time.Time
	if raw.CreatedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, raw.CreatedAt)
	}
	// Set canonical email from the public email if it's a real address
	// (not a noreply). This eliminates duplicate API calls from
	// ResolveEmailsToCanonical.
	var canonical string
	if raw.PublicEmail != "" && !strings.Contains(strings.ToLower(raw.PublicEmail), "noreply") {
		canonical = raw.PublicEmail
	}

	return &model.Contributor{
		Login:     raw.Username,
		Email:     raw.PublicEmail,
		FullName:  raw.Name,
		Company:   raw.Company,
		Location:  raw.Location,
		Canonical: canonical,
		CreatedAt: createdAt,
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
		if commitCount == 0 {
			// Common: GitLab populates statistics.commit_count via an async
			// background job, so freshly-imported / mirrored / recently-pushed
			// projects return 0 until the stats worker catches up. The facade
			// phase + BackfillGitLabCommitCount will patch repo_info later.
			c.logger.Info("GitLab reports commit_count=0; will backfill from facade if non-empty",
				"owner", owner, "repo", repo)
		}
	} else {
		// GitLab omits the statistics object entirely when the token lacks
		// Reporter+ access on a private project, or on some self-managed
		// instances with custom permission rules. Surface this so ops can
		// distinguish "real zero commits" from "token too narrow".
		c.logger.Warn("GitLab returned no statistics object; commit_count will be 0 until facade backfill",
			"owner", owner, "repo", repo,
			"hint", "token may lack Reporter+ access on private project")
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
	if err := c.http.GetJSON(ctx, issueStatsPath, &issueStats); err != nil {
		c.logger.Warn("failed to fetch issue statistics, counts will be zero", "owner", owner, "repo", repo, "error", err)
	}

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

	// Community profile files — detect CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY
	// from the repository root via the /repository/tree endpoint.
	community := c.fetchCommunityFiles(ctx, pp)

	return &model.RepoInfo{
		LastUpdated:        raw.LastActivityAt,
		IssuesEnabled:      raw.IssuesEnabled,
		PRsEnabled:         raw.MergeRequestsEnabled,
		WikiEnabled:        raw.WikiEnabled,
		PagesEnabled:       raw.PagesAccessLevel != "disabled",
		ForkCount:          raw.ForksCount,
		StarCount:          raw.StarCount,
		OpenIssues:         raw.OpenIssuesCount,
		DefaultBranch:      raw.DefaultBranch,
		License:            license,
		LicenseFile:        license,
		CommitCount:        commitCount,
		IssuesCount:        issueStats.Statistics.Counts.All,
		IssuesClosed:       issueStats.Statistics.Counts.Closed,
		PRCount:            mrTotal,
		PRsOpen:            mrOpen,
		PRsClosed:          mrClosed,
		PRsMerged:          mrMerged,
		ChangelogFile:      community.Changelog,
		ContributingFile:   community.Contributing,
		CodeOfConductFile:  community.CodeOfConduct,
		SecurityIssueFile:  community.Security,
		Status:             status,
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitLab API",
		},
	}, nil
}

// communityFiles holds the presence status of standard community profile files.
type communityFiles struct {
	Changelog     string // "present" or ""
	Contributing  string
	CodeOfConduct string
	Security      string
}

// fetchCommunityFiles checks the GitLab repository root for common community
// profile files (CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY).
// Uses the /projects/:id/repository/tree endpoint with a single API call.
// Returns empty strings on any error (graceful degradation — missing community
// file data is not worth failing the entire repo info collection).
func (c *Client) fetchCommunityFiles(ctx context.Context, projectPath string) communityFiles {
	var result communityFiles
	path := fmt.Sprintf("/projects/%s/repository/tree?per_page=100&ref=HEAD", projectPath)
	var tree []struct {
		Name string `json:"name"`
		Type string `json:"type"` // "blob" or "tree"
	}
	if err := c.http.GetJSON(ctx, path, &tree); err != nil {
		return result
	}

	for _, entry := range tree {
		if entry.Type != "blob" {
			continue
		}
		name := strings.ToLower(entry.Name)
		base := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, ".md"), ".rst"), ".txt")

		switch {
		case base == "changelog" || base == "changes" || base == "history":
			result.Changelog = "present"
		case base == "contributing":
			result.Contributing = "present"
		case base == "code_of_conduct" || base == "code-of-conduct":
			result.CodeOfConduct = "present"
		case base == "security":
			result.Security = "present"
		}
	}
	return result
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
