package github

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// Client implements platform.Client for GitHub.
type Client struct {
	http   *platform.HTTPClient
	logger *slog.Logger
}

// New creates a GitHub client. baseURL is typically "https://api.github.com"
// (or a GitHub Enterprise URL).
func New(baseURL string, keys *platform.KeyPool, logger *slog.Logger) *Client {
	return &Client{
		http:   platform.NewHTTPClient(baseURL, keys, logger),
		logger: logger,
	}
}

func (c *Client) Platform() model.Platform {
	return model.PlatformGitHub
}

// ghUserToRef converts a GitHub user to a model.UserRef for contributor resolution.
func ghUserToRef(u ghUser) model.UserRef {
	return model.UserRef{
		PlatformID: u.ID,
		Login:      u.Login,
		Name:       u.Name,
		Email:      u.Email,
		AvatarURL:  u.AvatarURL,
		URL:        u.HTMLURL,
		NodeID:     u.NodeID,
		Type:       u.Type,
	}
}

// parseTrailingNumber extracts the trailing integer from a URL path.
// e.g., "https://api.github.com/repos/o/r/issues/42" -> 42
func parseTrailingNumber(u string) int {
	if u == "" {
		return 0
	}
	// Find last slash.
	idx := strings.LastIndex(u, "/")
	if idx < 0 || idx >= len(u)-1 {
		return 0
	}
	n := 0
	for _, c := range u[idx+1:] {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func (c *Client) ParseRepoURL(url string) (owner, repo string, err error) {
	parsed, err := platform.ParseRepoURL(url)
	if err != nil {
		return "", "", err
	}
	if parsed.Platform != model.PlatformGitHub {
		return "", "", fmt.Errorf("URL %q is not a GitHub URL", url)
	}
	return parsed.Owner, parsed.Repo, nil
}

// --- IssueCollector ---

func (c *Client) ListIssues(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.Issue, error] {
	path := fmt.Sprintf("/repos/%s/%s/issues?state=all&sort=updated&direction=desc", owner, repo)
	if !since.IsZero() {
		path += "&since=" + since.Format(time.RFC3339)
	}

	return func(yield func(model.Issue, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghIssue](ctx, c.http, path) {
			if err != nil {
				yield(model.Issue{}, err)
				return
			}
			// GitHub returns PRs in the issues endpoint; skip them.
			if raw.PullRequest != nil && raw.PullRequest.URL != "" {
				continue
			}
			issue := model.Issue{
				PlatformID:   raw.ID,
				Number:       raw.Number,
				NodeID:       raw.NodeID,
				Title:        raw.Title,
				Body:         raw.Body,
				State:        raw.State,
				URL:          raw.URL,
				HTMLURL:      raw.HTMLURL,
				CreatedAt:    raw.CreatedAt,
				UpdatedAt:    raw.UpdatedAt,
				ClosedAt:     raw.ClosedAt,
				CommentCount: raw.Comments,
				ReporterRef:  ghUserToRef(raw.User),
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitHub API",
				},
			}
			if !yield(issue, nil) {
				return
			}
		}
	}
}

func (c *Client) ListIssueLabels(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueLabel, error] {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/labels", owner, repo, issueNumber)
	return func(yield func(model.IssueLabel, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghLabel](ctx, c.http, path) {
			if err != nil {
				yield(model.IssueLabel{}, err)
				return
			}
			if !yield(model.IssueLabel{
				PlatformID:  raw.ID,
				NodeID:      raw.NodeID,
				Text:        raw.Name,
				Description: raw.Description,
				Color:       raw.Color,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListIssueAssignees(ctx context.Context, owner, repo string, issueNumber int) iter.Seq2[model.IssueAssignee, error] {
	// Assignees come embedded in the issue response; this endpoint is for
	// explicit per-issue fetching if needed. For bulk collection, we extract
	// assignees during ListIssues and handle them in the collector.
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, issueNumber)
	return func(yield func(model.IssueAssignee, error) bool) {
		var raw ghIssue
		if err := c.http.GetJSON(ctx, path, &raw); err != nil {
			yield(model.IssueAssignee{}, err)
			return
		}
		for _, a := range raw.Assignees {
			if !yield(model.IssueAssignee{
				PlatformSrcID:  a.ID,
				PlatformNodeID: a.NodeID,
			}, nil) {
				return
			}
		}
	}
}

// --- PullRequestCollector ---

func (c *Client) ListPullRequests(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequest, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls?state=all&sort=updated&direction=desc", owner, repo)

	return func(yield func(model.PullRequest, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghPullRequest](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequest{}, err)
				return
			}
			if !since.IsZero() && raw.UpdatedAt.Before(since) {
				return // Past our cutoff; stop.
			}
			state := raw.State
			if raw.MergedAt != nil {
				state = "merged"
			}
			pr := model.PullRequest{
				PlatformSrcID:     raw.ID,
				NodeID:            raw.NodeID,
				Number:            raw.Number,
				URL:               raw.URL,
				HTMLURL:           raw.HTMLURL,
				DiffURL:           raw.DiffURL,
				Title:             raw.Title,
				Body:              raw.Body,
				State:             state,
				Locked:            raw.Locked,
				CreatedAt:         raw.CreatedAt,
				UpdatedAt:         raw.UpdatedAt,
				ClosedAt:          raw.ClosedAt,
				MergedAt:          raw.MergedAt,
				MergeCommitSHA:    raw.MergeCommitSHA,
				AuthorAssociation: raw.AuthorAssociation,
				AuthorRef:         ghUserToRef(raw.User),
				Origin: model.DataOrigin{
					ToolSource: "aveloxis",
					DataSource: "GitHub API",
				},
			}
			if !yield(pr, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRLabels(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestLabel, error] {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d/labels", owner, repo, prNumber)
	return func(yield func(model.PullRequestLabel, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghLabel](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestLabel{}, err)
				return
			}
			if !yield(model.PullRequestLabel{
				PlatformID:  raw.ID,
				NodeID:      raw.NodeID,
				Name:        raw.Name,
				Description: raw.Description,
				Color:       raw.Color,
				IsDefault:   raw.Default,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRAssignees(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestAssignee, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, prNumber)
	return func(yield func(model.PullRequestAssignee, error) bool) {
		var raw ghPullRequest
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
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/requested_reviewers", owner, repo, prNumber)
	return func(yield func(model.PullRequestReviewer, error) bool) {
		var resp struct {
			Users []ghUser `json:"users"`
		}
		if err := c.http.GetJSON(ctx, path, &resp); err != nil {
			yield(model.PullRequestReviewer{}, err)
			return
		}
		for _, u := range resp.Users {
			if !yield(model.PullRequestReviewer{
				PlatformSrcID: u.ID,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRReviews(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestReview, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/reviews", owner, repo, prNumber)
	return func(yield func(model.PullRequestReview, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghReview](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestReview{}, err)
				return
			}
			if !yield(model.PullRequestReview{
				PlatformReviewID:  raw.ID,
				NodeID:            raw.NodeID,
				PlatformID:        model.PlatformGitHub,
				State:             raw.State,
				Body:              raw.Body,
				SubmittedAt:       raw.SubmittedAt,
				AuthorAssociation: raw.AuthorAssociation,
				CommitID:          raw.CommitID,
				HTMLURL:           raw.HTMLURL,
				AuthorRef:         ghUserToRef(raw.User),
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRCommits(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestCommit, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/commits", owner, repo, prNumber)
	return func(yield func(model.PullRequestCommit, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghCommit](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestCommit{}, err)
				return
			}
			commit := model.PullRequestCommit{
				SHA:         raw.SHA,
				NodeID:      raw.NodeID,
				Message:     raw.Commit.Message,
				AuthorEmail: raw.Commit.Author.Email,
				Timestamp:   raw.Commit.Author.Date,
			}
			if raw.Author != nil {
				commit.AuthorRef = ghUserToRef(*raw.Author)
			}
			if !yield(commit, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRFiles(ctx context.Context, owner, repo string, prNumber int) iter.Seq2[model.PullRequestFile, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d/files", owner, repo, prNumber)
	return func(yield func(model.PullRequestFile, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghFile](ctx, c.http, path) {
			if err != nil {
				yield(model.PullRequestFile{}, err)
				return
			}
			if !yield(model.PullRequestFile{
				Path:      raw.Filename,
				Additions: raw.Additions,
				Deletions: raw.Deletions,
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) FetchPRMeta(ctx context.Context, owner, repo string, prNumber int) (head, base *model.PullRequestMeta, err error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, prNumber)
	var raw ghPullRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, nil, err
	}
	head = &model.PullRequestMeta{
		HeadOrBase: "head",
		Label:      raw.Head.Label,
		Ref:        raw.Head.Ref,
		SHA:        raw.Head.SHA,
	}
	base = &model.PullRequestMeta{
		HeadOrBase: "base",
		Label:      raw.Base.Label,
		Ref:        raw.Base.Ref,
		SHA:        raw.Base.SHA,
	}
	return head, base, nil
}

// --- EventCollector ---

// fetchRepoEvents returns all issue/timeline events from the repo-wide events endpoint.
// Both ListIssueEvents and ListPREvents call this shared iterator to avoid
// fetching the same endpoint twice when both are collected sequentially.
// The isPR callback determines whether an event belongs to a PR or an issue.
func (c *Client) fetchRepoEvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[ghEvent, error] {
	path := fmt.Sprintf("/repos/%s/%s/issues/events", owner, repo)
	return func(yield func(ghEvent, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghEvent](ctx, c.http, path) {
			if err != nil {
				yield(ghEvent{}, err)
				return
			}
			if !since.IsZero() && raw.CreatedAt.Before(since) {
				continue
			}
			if !yield(raw, nil) {
				return
			}
		}
	}
}

func (c *Client) ListIssueEvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.IssueEvent, error] {
	return func(yield func(model.IssueEvent, error) bool) {
		for raw, err := range c.fetchRepoEvents(ctx, owner, repo, since) {
			if err != nil {
				yield(model.IssueEvent{}, err)
				return
			}
			// Skip events on PRs (they have a pull_request field on the issue).
			if raw.Issue != nil && raw.Issue.PullRequest != nil {
				continue
			}
			platIssueID := int64(0)
			if raw.Issue != nil {
				platIssueID = int64(raw.Issue.Number)
			}
			if !yield(model.IssueEvent{
				PlatformEventID:  raw.ID,
				PlatformID:       model.PlatformGitHub,
				PlatformIssueID:  platIssueID,
				NodeID:           raw.NodeID,
				Action:           raw.Event,
				ActionCommitHash: raw.CommitID,
				CreatedAt:        raw.CreatedAt,
				ActorRef:         ghUserToRef(raw.Actor),
			}, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPREvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[model.PullRequestEvent, error] {
	return func(yield func(model.PullRequestEvent, error) bool) {
		for raw, err := range c.fetchRepoEvents(ctx, owner, repo, since) {
			if err != nil {
				yield(model.PullRequestEvent{}, err)
				return
			}
			// Only include events on PRs.
			if raw.Issue == nil || raw.Issue.PullRequest == nil {
				continue
			}
			if !yield(model.PullRequestEvent{
				PlatformEventID:  raw.ID,
				PlatformID:       model.PlatformGitHub,
				PlatformPRID:     int64(raw.Issue.Number),
				NodeID:           raw.NodeID,
				Action:           raw.Event,
				ActionCommitHash: raw.CommitID,
				CreatedAt:        raw.CreatedAt,
				ActorRef:         ghUserToRef(raw.Actor),
			}, nil) {
				return
			}
		}
	}
}

// --- MessageCollector ---

func (c *Client) ListIssueComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.MessageWithRef, error] {
	path := fmt.Sprintf("/repos/%s/%s/issues/comments?sort=updated&direction=desc", owner, repo)
	if !since.IsZero() {
		path += "&since=" + since.Format(time.RFC3339)
	}

	return func(yield func(platform.MessageWithRef, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghComment](ctx, c.http, path) {
			if err != nil {
				yield(platform.MessageWithRef{}, err)
				return
			}
			// Determine if this is an issue or PR comment by checking issue_url.
			isPR := strings.Contains(raw.IssueURL, "/pull/") || strings.Contains(raw.HTMLURL, "/pull/")

			msg := model.Message{
				PlatformMsgID: raw.ID,
				PlatformID:    model.PlatformGitHub,
				NodeID:        raw.NodeID,
				Text:          raw.Body,
				Timestamp:     raw.CreatedAt,
				AuthorRef:     ghUserToRef(raw.User),
			}

			// Parse issue/PR number from the issue_url or html_url.
			// issue_url looks like: https://api.github.com/repos/owner/repo/issues/42
			// html_url looks like: https://github.com/owner/repo/issues/42 or /pull/42
			parentNumber := parseTrailingNumber(raw.IssueURL)
			if parentNumber == 0 {
				parentNumber = parseTrailingNumber(raw.HTMLURL)
			}

			ref := platform.MessageWithRef{Message: msg}
			if isPR {
				ref.PRRef = &model.PullRequestMessageRef{
					PlatformSrcID:    raw.ID,
					PlatformNodeID:   raw.NodeID,
					PlatformPRNumber: parentNumber,
				}
			} else {
				ref.IssueRef = &model.IssueMessageRef{
					PlatformSrcID:       raw.ID,
					PlatformNodeID:      raw.NodeID,
					PlatformIssueNumber: parentNumber,
				}
			}

			if !yield(ref, nil) {
				return
			}
		}
	}
}

func (c *Client) ListPRComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.MessageWithRef, error] {
	// PR comments in GitHub are the same as issue comments.
	// The ListIssueComments method already classifies them.
	// This method exists for the interface; delegate to ListIssueComments
	// and filter to PR-only.
	return func(yield func(platform.MessageWithRef, error) bool) {
		for ref, err := range c.ListIssueComments(ctx, owner, repo, since) {
			if err != nil {
				yield(platform.MessageWithRef{}, err)
				return
			}
			if ref.PRRef != nil {
				if !yield(ref, nil) {
					return
				}
			}
		}
	}
}

func (c *Client) ListReviewComments(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[platform.ReviewCommentWithRef, error] {
	path := fmt.Sprintf("/repos/%s/%s/pulls/comments?sort=updated&direction=desc", owner, repo)
	if !since.IsZero() {
		path += "&since=" + since.Format(time.RFC3339)
	}

	return func(yield func(platform.ReviewCommentWithRef, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghReviewComment](ctx, c.http, path) {
			if err != nil {
				yield(platform.ReviewCommentWithRef{}, err)
				return
			}
			msg := model.Message{
				PlatformMsgID: raw.ID,
				PlatformID:    model.PlatformGitHub,
				NodeID:        raw.NodeID,
				Text:          raw.Body,
				Timestamp:     raw.CreatedAt,
				AuthorRef:     ghUserToRef(raw.User),
			}
			comment := model.ReviewComment{
				PlatformSrcID:     raw.ID,
				PlatformReviewID:  raw.PullRequestReviewID,
				NodeID:            raw.NodeID,
				DiffHunk:          raw.DiffHunk,
				Path:              raw.Path,
				Position:          raw.Position,
				OriginalPosition:  raw.OriginalPosition,
				CommitID:          raw.CommitID,
				OriginalCommitID:  raw.OriginalCommitID,
				Line:              raw.Line,
				OriginalLine:      raw.OriginalLine,
				Side:              raw.Side,
				StartLine:         raw.StartLine,
				OriginalStartLine: raw.OriginalStartLine,
				StartSide:         raw.StartSide,
				AuthorAssociation: raw.AuthorAssociation,
				HTMLURL:           raw.HTMLURL,
				UpdatedAt:         raw.UpdatedAt,
			}
			if !yield(platform.ReviewCommentWithRef{Message: msg, Comment: comment}, nil) {
				return
			}
		}
	}
}

// --- ReleaseCollector ---

func (c *Client) ListReleases(ctx context.Context, owner, repo string) iter.Seq2[model.Release, error] {
	path := fmt.Sprintf("/repos/%s/%s/releases", owner, repo)
	return func(yield func(model.Release, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghRelease](ctx, c.http, path) {
			if err != nil {
				yield(model.Release{}, err)
				return
			}
			if !yield(model.Release{
				ID:           fmt.Sprintf("%d", raw.ID),
				Name:         raw.Name,
				Description:  raw.Body,
				Author:       raw.Author.Login,
				TagName:      raw.TagName,
				URL:          raw.HTMLURL,
				CreatedAt:    raw.CreatedAt,
				PublishedAt:  raw.PublishedAt,
				IsDraft:      raw.Draft,
				IsPrerelease: raw.Prerelease,
				TagOnly:      false,
			}, nil) {
				return
			}
		}
	}
}

// --- ContributorCollector ---

func (c *Client) ListContributors(ctx context.Context, owner, repo string) iter.Seq2[model.Contributor, error] {
	path := fmt.Sprintf("/repos/%s/%s/contributors", owner, repo)
	return func(yield func(model.Contributor, error) bool) {
		for raw, err := range platform.PaginateGitHub[ghContributor](ctx, c.http, path) {
			if err != nil {
				yield(model.Contributor{}, err)
				return
			}
			contrib := model.Contributor{
				Login: raw.Login,
				Identities: []model.ContributorIdentity{{
					Platform:          model.PlatformGitHub,
					UserID:            raw.ID,
					Login:             raw.Login,
					AvatarURL:         raw.AvatarURL,
					URL:               raw.HTMLURL,
					NodeID:            raw.NodeID,
					Type:              raw.Type,
					IsAdmin:           raw.SiteAdmin,
					GravatarID:        raw.GravatarID,
					FollowersURL:      raw.FollowersURL,
					FollowingURL:      raw.FollowingURL,
					GistsURL:          raw.GistsURL,
					StarredURL:        raw.StarredURL,
					SubscriptionsURL:  raw.SubscriptionsURL,
					OrganizationsURL:  raw.OrganizationsURL,
					ReposURL:          raw.ReposURL,
					EventsURL:         raw.EventsURL,
					ReceivedEventsURL: raw.ReceivedEventsURL,
				}},
			}
			if !yield(contrib, nil) {
				return
			}
		}
	}
}

func (c *Client) EnrichContributor(ctx context.Context, login string) (*model.Contributor, error) {
	path := fmt.Sprintf("/users/%s", login)
	var raw ghUser
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	var createdAt time.Time
	if raw.CreatedAt != "" {
		createdAt, _ = time.Parse(time.RFC3339, raw.CreatedAt)
	}
	return &model.Contributor{
		Login:     raw.Login,
		Email:     raw.Email,
		FullName:  raw.Name,
		Company:   raw.Company,
		Location:  raw.Location,
		CreatedAt: createdAt,
		Identities: []model.ContributorIdentity{{
			Platform:          model.PlatformGitHub,
			UserID:            raw.ID,
			Login:             raw.Login,
			Name:              raw.Name,
			Email:             raw.Email,
			AvatarURL:         raw.AvatarURL,
			URL:               raw.HTMLURL,
			NodeID:            raw.NodeID,
			Type:              raw.Type,
			IsAdmin:           raw.SiteAdmin,
			GravatarID:        raw.GravatarID,
			FollowersURL:      raw.FollowersURL,
			FollowingURL:      raw.FollowingURL,
			GistsURL:          raw.GistsURL,
			StarredURL:        raw.StarredURL,
			SubscriptionsURL:  raw.SubscriptionsURL,
			OrganizationsURL:  raw.OrganizationsURL,
			ReposURL:          raw.ReposURL,
			EventsURL:         raw.EventsURL,
			ReceivedEventsURL: raw.ReceivedEventsURL,
		}},
	}, nil
}

// --- RepoCollector ---

func (c *Client) FetchRepoInfo(ctx context.Context, owner, repo string) (*model.RepoInfo, error) {
	// Use GraphQL to get complete repo metadata in one call.
	// The REST API doesn't return PR/issue/commit counts, community profile files,
	// or separate open/closed/merged PR counts. Matches Augur's GraphQL approach.
	query := repoInfoGraphQL(owner, repo)
	var result graphQLRepoInfoResponse
	err := c.graphqlRequest(ctx, query, &result)
	if err != nil {
		// Fall back to REST API if GraphQL fails.
		return c.fetchRepoInfoREST(ctx, owner, repo)
	}

	r := result.Data.Repository
	license := ""
	if r.LicenseInfo != nil {
		license = r.LicenseInfo.Name
	}
	defaultBranch := ""
	commitCount := 0
	if r.DefaultBranchRef != nil {
		defaultBranch = r.DefaultBranchRef.Name
		commitCount = r.DefaultBranchRef.Target.History.TotalCount
	}

	codeOfConduct := ""
	if r.CodeOfConduct != nil {
		codeOfConduct = r.CodeOfConduct.Name
	}

	return &model.RepoInfo{
		LastUpdated:       r.UpdatedAt,
		IssuesEnabled:     r.HasIssuesEnabled,
		WikiEnabled:       r.HasWikiEnabled,
		PagesEnabled:      false, // GraphQL doesn't expose this
		PRsEnabled:        true,
		ForkCount:         r.ForkCount,
		StarCount:         r.StargazerCount,
		WatcherCount:      r.Watchers.TotalCount,
		OpenIssues:        r.OpenIssues.TotalCount,
		CommitCount:       commitCount,
		IssuesCount:       r.TotalIssues.TotalCount,
		IssuesClosed:      r.ClosedIssues.TotalCount,
		PRCount:           r.TotalPRs.TotalCount,
		PRsOpen:           r.OpenPRs.TotalCount,
		PRsClosed:         r.ClosedPRs.TotalCount,
		PRsMerged:         r.MergedPRs.TotalCount,
		DefaultBranch:     defaultBranch,
		License:           license,
		ChangelogFile:     filePresent(r.Changelog),
		ContributingFile:  filePresent(r.Contributing),
		LicenseFile:       license,
		CodeOfConductFile: codeOfConduct,
		SecurityIssueFile: filePresent(r.SecurityPolicy),
		Status:            statusStr(r.IsArchived, r.IsDisabled),
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitHub GraphQL",
		},
	}, nil
}

// fetchRepoInfoREST is the fallback if GraphQL fails.
func (c *Client) fetchRepoInfoREST(ctx context.Context, owner, repo string) (*model.RepoInfo, error) {
	path := fmt.Sprintf("/repos/%s/%s", owner, repo)
	var raw ghRepoInfo
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	license := ""
	if raw.License != nil {
		license = raw.License.Name
	}
	return &model.RepoInfo{
		LastUpdated:   raw.UpdatedAt,
		IssuesEnabled: raw.HasIssues,
		WikiEnabled:   raw.HasWiki,
		PagesEnabled:  raw.HasPages,
		PRsEnabled:    true,
		ForkCount:     raw.ForksCount,
		StarCount:     raw.StargazersCount,
		WatcherCount:  raw.WatchersCount,
		OpenIssues:    raw.OpenIssuesCount,
		DefaultBranch: raw.DefaultBranch,
		License:       license,
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitHub REST",
		},
	}, nil
}

// repoInfoGraphQL builds the GraphQL query for complete repo metadata.
func repoInfoGraphQL(owner, repo string) string {
	return fmt.Sprintf(`{
  repository(owner: "%s", name: "%s") {
    updatedAt
    hasIssuesEnabled
    hasWikiEnabled
    isArchived
    isDisabled
    forkCount
    stargazerCount
    watchers { totalCount }
    openIssues: issues(states: OPEN) { totalCount }
    totalIssues: issues { totalCount }
    closedIssues: issues(states: CLOSED) { totalCount }
    openPRs: pullRequests(states: OPEN) { totalCount }
    totalPRs: pullRequests { totalCount }
    closedPRs: pullRequests(states: CLOSED) { totalCount }
    mergedPRs: pullRequests(states: MERGED) { totalCount }
    defaultBranchRef {
      name
      target {
        ... on Commit {
          history { totalCount }
        }
      }
    }
    licenseInfo { name spdxId }
    codeOfConduct { name }
    contributing: object(expression: "HEAD:CONTRIBUTING.md") { ... on Blob { text } }
    changelog: object(expression: "HEAD:CHANGELOG.md") { ... on Blob { text } }
    securityPolicy: object(expression: "HEAD:SECURITY.md") { ... on Blob { text } }
  }
}`, owner, repo)
}

// graphqlRequest sends a query to the GitHub GraphQL API.
func (c *Client) graphqlRequest(ctx context.Context, query string, result interface{}) error {
	body := fmt.Sprintf(`{"query": %q}`, query)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.github.com/graphql", strings.NewReader(body))
	if err != nil {
		return err
	}
	key, err := c.http.Keys().GetKey(ctx)
	if err != nil {
		return err
	}
	// GraphQL requires "bearer" token format.
	req.Header.Set("Authorization", "bearer "+key.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GraphQL request failed: status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

type graphQLRepoInfoResponse struct {
	Data struct {
		Repository graphQLRepo `json:"repository"`
	} `json:"data"`
}

type graphQLRepo struct {
	UpdatedAt        time.Time `json:"updatedAt"`
	HasIssuesEnabled bool      `json:"hasIssuesEnabled"`
	HasWikiEnabled   bool      `json:"hasWikiEnabled"`
	IsArchived       bool      `json:"isArchived"`
	IsDisabled       bool      `json:"isDisabled"`
	ForkCount        int       `json:"forkCount"`
	StargazerCount   int       `json:"stargazerCount"`
	Watchers         struct{ TotalCount int `json:"totalCount"` } `json:"watchers"`
	OpenIssues       struct{ TotalCount int `json:"totalCount"` } `json:"openIssues"`
	TotalIssues      struct{ TotalCount int `json:"totalCount"` } `json:"totalIssues"`
	ClosedIssues     struct{ TotalCount int `json:"totalCount"` } `json:"closedIssues"`
	OpenPRs          struct{ TotalCount int `json:"totalCount"` } `json:"openPRs"`
	TotalPRs         struct{ TotalCount int `json:"totalCount"` } `json:"totalPRs"`
	ClosedPRs        struct{ TotalCount int `json:"totalCount"` } `json:"closedPRs"`
	MergedPRs        struct{ TotalCount int `json:"totalCount"` } `json:"mergedPRs"`
	DefaultBranchRef *struct {
		Name   string `json:"name"`
		Target struct {
			History struct{ TotalCount int `json:"totalCount"` } `json:"history"`
		} `json:"target"`
	} `json:"defaultBranchRef"`
	LicenseInfo    *struct{ Name string `json:"name"`; SpdxId string `json:"spdxId"` } `json:"licenseInfo"`
	CodeOfConduct  *struct{ Name string `json:"name"` } `json:"codeOfConduct"`
	Contributing   *struct{ Text string `json:"text"` } `json:"contributing"`
	Changelog      *struct{ Text string `json:"text"` } `json:"changelog"`
	SecurityPolicy *struct{ Text string `json:"text"` } `json:"securityPolicy"`
}

func filePresent(obj *struct{ Text string `json:"text"` }) string {
	if obj != nil { return "present" }
	return ""
}

func statusStr(archived, disabled bool) string {
	if disabled { return "Disabled" }
	if archived { return "Archived" }
	return "Active"
}

func (c *Client) FetchCloneStats(ctx context.Context, owner, repo string) ([]model.RepoClone, error) {
	path := fmt.Sprintf("/repos/%s/%s/traffic/clones", owner, repo)
	var raw ghCloneTraffic
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}

	clones := make([]model.RepoClone, len(raw.Clones))
	for i, c := range raw.Clones {
		clones[i] = model.RepoClone{
			Timestamp:    c.Timestamp,
			TotalClones:  c.Count,
			UniqueClones: c.Uniques,
		}
	}
	return clones, nil
}

// FetchIssueByNumber fetches a single issue by number for targeted gap filling.
func (c *Client) FetchIssueByNumber(ctx context.Context, owner, repo string, number int) (*model.Issue, error) {
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", owner, repo, number)
	var raw ghIssue
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	// GitHub returns PRs via the issues endpoint — check and reject.
	if raw.PullRequest != nil && raw.PullRequest.URL != "" {
		return nil, fmt.Errorf("issue %d is a pull request", number)
	}
	issue := &model.Issue{
		PlatformID:   raw.ID,
		Number:       raw.Number,
		NodeID:       raw.NodeID,
		Title:        raw.Title,
		Body:         raw.Body,
		State:        raw.State,
		URL:          raw.URL,
		HTMLURL:      raw.HTMLURL,
		CreatedAt:    raw.CreatedAt,
		UpdatedAt:    raw.UpdatedAt,
		ClosedAt:     raw.ClosedAt,
		CommentCount: raw.Comments,
		ReporterRef:  ghUserToRef(raw.User),
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitHub API (gap fill)",
		},
	}
	return issue, nil
}

// FetchPRByNumber fetches a single pull request by number for targeted gap filling.
func (c *Client) FetchPRByNumber(ctx context.Context, owner, repo string, number int) (*model.PullRequest, error) {
	path := fmt.Sprintf("/repos/%s/%s/pulls/%d", owner, repo, number)
	var raw ghPullRequest
	if err := c.http.GetJSON(ctx, path, &raw); err != nil {
		return nil, err
	}
	state := raw.State
	if raw.MergedAt != nil {
		state = "merged"
	}
	pr := &model.PullRequest{
		PlatformSrcID:     raw.ID,
		Number:            raw.Number,
		NodeID:            raw.NodeID,
		Title:             raw.Title,
		Body:              raw.Body,
		State:             state,
		URL:               raw.URL,
		HTMLURL:           raw.HTMLURL,
		DiffURL:           raw.DiffURL,
		Locked:            raw.Locked,
		CreatedAt:         raw.CreatedAt,
		UpdatedAt:         raw.UpdatedAt,
		ClosedAt:          raw.ClosedAt,
		MergedAt:          raw.MergedAt,
		MergeCommitSHA:    raw.MergeCommitSHA,
		AuthorRef:         ghUserToRef(raw.User),
		AuthorAssociation: raw.AuthorAssociation,
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: "GitHub API (gap fill)",
		},
	}
	return pr, nil
}
