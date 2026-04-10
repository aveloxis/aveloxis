// Package github implements the platform.Client interface for GitHub.
package github

import "time"

// Raw GitHub API response types. These map directly to GitHub's REST API
// and are converted to platform-agnostic model types by the collector methods.

type ghUser struct {
	ID                 int64  `json:"id"`
	Login              string `json:"login"`
	NodeID             string `json:"node_id"`
	AvatarURL          string `json:"avatar_url"`
	GravatarID         string `json:"gravatar_id"`
	HTMLURL            string `json:"html_url"`
	FollowersURL       string `json:"followers_url"`
	FollowingURL       string `json:"following_url"`
	GistsURL           string `json:"gists_url"`
	StarredURL         string `json:"starred_url"`
	SubscriptionsURL   string `json:"subscriptions_url"`
	OrganizationsURL   string `json:"organizations_url"`
	ReposURL           string `json:"repos_url"`
	EventsURL          string `json:"events_url"`
	ReceivedEventsURL  string `json:"received_events_url"`
	Type               string `json:"type"`
	SiteAdmin          bool   `json:"site_admin"`
	Name               string `json:"name"`
	Email              string `json:"email"`
	Company            string `json:"company"`
	Location           string `json:"location"`
	CreatedAt          string `json:"created_at"`
}

type ghLabel struct {
	ID          int64  `json:"id"`
	NodeID      string `json:"node_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       string `json:"color"`
	Default     bool   `json:"default"`
}

type ghIssue struct {
	ID          int64      `json:"id"`
	Number      int        `json:"number"`
	NodeID      string     `json:"node_id"`
	Title       string     `json:"title"`
	Body        string     `json:"body"`
	State       string     `json:"state"`
	URL         string     `json:"url"`
	HTMLURL     string     `json:"html_url"`
	User        ghUser     `json:"user"`
	Labels      []ghLabel  `json:"labels"`
	Assignees   []ghUser   `json:"assignees"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ClosedAt    *time.Time `json:"closed_at"`
	Comments    int        `json:"comments"`
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request"`
}

type ghPullRequest struct {
	ID                 int64      `json:"id"`
	Number             int        `json:"number"`
	NodeID             string     `json:"node_id"`
	Title              string     `json:"title"`
	Body               string     `json:"body"`
	State              string     `json:"state"`
	Locked             bool       `json:"locked"`
	URL                string     `json:"url"`
	HTMLURL            string     `json:"html_url"`
	DiffURL            string     `json:"diff_url"`
	User               ghUser     `json:"user"`
	Labels             []ghLabel  `json:"labels"`
	Assignees          []ghUser   `json:"assignees"`
	RequestedReviewers []ghUser   `json:"requested_reviewers"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	ClosedAt           *time.Time `json:"closed_at"`
	MergedAt           *time.Time `json:"merged_at"`
	MergeCommitSHA     string     `json:"merge_commit_sha"`
	AuthorAssociation  string     `json:"author_association"`
	Head               ghPRBranch `json:"head"`
	Base               ghPRBranch `json:"base"`
}

type ghPRBranch struct {
	Label string `json:"label"`
	Ref   string `json:"ref"`
	SHA   string `json:"sha"`
	User  ghUser `json:"user"`
}

type ghReview struct {
	ID                int64     `json:"id"`
	NodeID            string    `json:"node_id"`
	User              ghUser    `json:"user"`
	Body              string    `json:"body"`
	State             string    `json:"state"`
	HTMLURL           string    `json:"html_url"`
	SubmittedAt       time.Time `json:"submitted_at"`
	CommitID          string    `json:"commit_id"`
	AuthorAssociation string    `json:"author_association"`
}

type ghComment struct {
	ID        int64     `json:"id"`
	NodeID    string    `json:"node_id"`
	User      ghUser    `json:"user"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	HTMLURL   string    `json:"html_url"`
	IssueURL  string    `json:"issue_url"`
}

type ghReviewComment struct {
	ID                    int64     `json:"id"`
	NodeID                string    `json:"node_id"`
	PullRequestReviewID   int64     `json:"pull_request_review_id"`
	User                  ghUser    `json:"user"`
	Body                  string    `json:"body"`
	CreatedAt             time.Time `json:"created_at"`
	UpdatedAt             time.Time `json:"updated_at"`
	HTMLURL               string    `json:"html_url"`
	DiffHunk              string    `json:"diff_hunk"`
	Path                  string    `json:"path"`
	Position              *int      `json:"position"`
	OriginalPosition      *int      `json:"original_position"`
	CommitID              string    `json:"commit_id"`
	OriginalCommitID      string    `json:"original_commit_id"`
	Line                  *int      `json:"line"`
	OriginalLine          *int      `json:"original_line"`
	Side                  string    `json:"side"`
	StartLine             *int      `json:"start_line"`
	OriginalStartLine     *int      `json:"original_start_line"`
	StartSide             string    `json:"start_side"`
	AuthorAssociation     string    `json:"author_association"`
}

type ghEvent struct {
	ID        int64     `json:"id"`
	NodeID    string    `json:"node_id"`
	URL       string    `json:"url"`
	Actor     ghUser    `json:"actor"`
	Event     string    `json:"event"`
	CommitID  string    `json:"commit_id"`
	CreatedAt time.Time `json:"created_at"`
	Issue     *struct {
		Number      int `json:"number"`
		PullRequest *struct {
			URL string `json:"url"`
		} `json:"pull_request"`
	} `json:"issue"`
}

type ghRelease struct {
	ID          int64      `json:"id"`
	TagName     string     `json:"tag_name"`
	Name        string     `json:"name"`
	Body        string     `json:"body"`
	Draft       bool       `json:"draft"`
	Prerelease  bool       `json:"prerelease"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at"`
	HTMLURL     string     `json:"html_url"`
	Author      ghUser     `json:"author"`
}

type ghCommit struct {
	SHA    string `json:"sha"`
	NodeID string `json:"node_id"`
	Commit struct {
		Message   string `json:"message"`
		Author    ghGitActor `json:"author"`
		Committer ghGitActor `json:"committer"`
	} `json:"commit"`
	Author    *ghUser `json:"author"`
	Committer *ghUser `json:"committer"`
}

type ghGitActor struct {
	Name  string    `json:"name"`
	Email string    `json:"email"`
	Date  time.Time `json:"date"`
}

type ghFile struct {
	Filename  string `json:"filename"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

type ghRepoInfo struct {
	ForksCount      int    `json:"forks_count"`
	StargazersCount int    `json:"stargazers_count"`
	WatchersCount   int    `json:"watchers_count"`
	OpenIssuesCount int    `json:"open_issues_count"`
	DefaultBranch   string `json:"default_branch"`
	HasIssues       bool   `json:"has_issues"`
	HasWiki         bool   `json:"has_wiki"`
	HasPages        bool   `json:"has_pages"`
	Archived        bool   `json:"archived"`
	Fork            bool   `json:"fork"`
	Parent          *struct {
		FullName string `json:"full_name"`
	} `json:"parent"`
	License *struct {
		Name string `json:"name"`
	} `json:"license"`
	UpdatedAt time.Time `json:"updated_at"`
}

type ghCloneTraffic struct {
	Count   int `json:"count"`
	Uniques int `json:"uniques"`
	Clones  []struct {
		Timestamp time.Time `json:"timestamp"`
		Count     int       `json:"count"`
		Uniques   int       `json:"uniques"`
	} `json:"clones"`
}

type ghContributor struct {
	ghUser
	Contributions int `json:"contributions"`
}
