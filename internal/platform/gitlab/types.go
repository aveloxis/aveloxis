// Package gitlab implements the platform.Client interface for GitLab.
package gitlab

import "time"

// Raw GitLab API v4 response types. Converted to platform-agnostic model types.

type glUser struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Name      string `json:"name"`
	State     string `json:"state"`
	AvatarURL string `json:"avatar_url"`
	WebURL    string `json:"web_url"`
	Email     string `json:"email"`       // only from /users/:id with admin
	PublicEmail string `json:"public_email"` // from /users/:id
	Company   string `json:"organization"`
	Location  string `json:"location"`
	CreatedAt string `json:"created_at"`
}

type glLabel struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Color       string `json:"color"`
}

type glIssue struct {
	ID          int64      `json:"id"`   // global ID
	IID         int        `json:"iid"`  // project-scoped number
	Title       string     `json:"title"`
	Description string     `json:"description"`
	State       string     `json:"state"` // "opened", "closed"
	WebURL      string     `json:"web_url"`
	Author      glUser     `json:"author"`
	Labels      []string   `json:"labels"` // label names only in list endpoint
	Assignees   []glUser   `json:"assignees"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ClosedAt    *time.Time `json:"closed_at"`
	ClosedBy    *glUser    `json:"closed_by"`
	UserNotesCount int    `json:"user_notes_count"`
}

type glMergeRequest struct {
	ID                 int64      `json:"id"`
	IID                int        `json:"iid"`
	Title              string     `json:"title"`
	Description        string     `json:"description"`
	State              string     `json:"state"` // "opened", "closed", "merged", "locked"
	WebURL             string     `json:"web_url"`
	DiffURL            string     // constructed from web_url
	Author             glUser     `json:"author"`
	Labels             []string   `json:"labels"`
	Assignees          []glUser   `json:"assignees"`
	Reviewers          []glUser   `json:"reviewers"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
	ClosedAt           *time.Time `json:"closed_at"`
	MergedAt           *time.Time `json:"merged_at"`
	MergeCommitSHA     string     `json:"merge_commit_sha"`
	SquashCommitSHA    string     `json:"squash_commit_sha"`
	SourceBranch       string     `json:"source_branch"`
	TargetBranch       string     `json:"target_branch"`
	SourceProjectID    int64      `json:"source_project_id"`
	TargetProjectID    int64      `json:"target_project_id"`
	SHA                string     `json:"sha"` // head commit SHA
	UserNotesCount     int        `json:"user_notes_count"`
	Draft              bool       `json:"draft"`
}

type glNote struct {
	ID        int64     `json:"id"`
	Body      string    `json:"body"`
	Author    glUser    `json:"author"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	System    bool      `json:"system"` // true for events masquerading as notes
	NoteableType string `json:"noteable_type"` // "Issue", "MergeRequest"
	NoteableIID  int    `json:"noteable_iid"`
}

// glResourceEvent represents a label, state, or milestone event from
// GitLab's resource events API.
type glResourceEvent struct {
	ID        int64     `json:"id"`
	User      glUser    `json:"user"`
	CreatedAt time.Time `json:"created_at"`
	// For state events:
	ResourceType string `json:"resource_type"` // "Issue", "MergeRequest"
	State        string `json:"state"`         // "opened", "closed", "merged", "reopened"
	// For label events:
	Label  *glLabel `json:"label"`
	Action string   `json:"action"` // "add", "remove"
}

type glRelease struct {
	TagName     string     `json:"tag_name"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	CreatedAt   time.Time  `json:"created_at"`
	ReleasedAt  *time.Time `json:"released_at"`
	Author      glUser     `json:"author"`
	Links       struct {
		Self string `json:"self"`
	} `json:"_links"`
}

type glCommit struct {
	ID             string    `json:"id"` // SHA
	ShortID        string    `json:"short_id"`
	Title          string    `json:"title"`
	Message        string    `json:"message"`
	AuthorName     string    `json:"author_name"`
	AuthorEmail    string    `json:"author_email"`
	AuthoredDate   time.Time `json:"authored_date"`
	CommitterName  string    `json:"committer_name"`
	CommitterEmail string    `json:"committer_email"`
	CommittedDate  time.Time `json:"committed_date"`
	WebURL         string    `json:"web_url"`
}

type glDiff struct {
	OldPath     string `json:"old_path"`
	NewPath     string `json:"new_path"`
	Diff        string `json:"diff"`
	NewFile     bool   `json:"new_file"`
	RenamedFile bool   `json:"renamed_file"`
	DeletedFile bool   `json:"deleted_file"`
}

type glMRApproval struct {
	ID   int64  `json:"id"`
	User glUser `json:"user"`
}

type glProject struct {
	ID                int64     `json:"id"`
	Description       string    `json:"description"`
	DefaultBranch     string    `json:"default_branch"`
	WebURL            string    `json:"web_url"`
	StarCount         int       `json:"star_count"`
	ForksCount        int       `json:"forks_count"`
	OpenIssuesCount   int       `json:"open_issues_count"`
	LastActivityAt    time.Time `json:"last_activity_at"`
	Archived          bool      `json:"archived"`
	Visibility        string    `json:"visibility"`
	IssuesEnabled     bool      `json:"issues_enabled"`
	MergeRequestsEnabled bool  `json:"merge_requests_enabled"`
	WikiEnabled       bool      `json:"wiki_enabled"`
	PagesAccessLevel  string    `json:"pages_access_level"`
	ForkedFromProject *struct {
		PathWithNamespace string `json:"path_with_namespace"`
	} `json:"forked_from_project"`
	License *struct {
		Name string `json:"name"`
	} `json:"license"`
	Statistics *struct {
		CommitCount int `json:"commit_count"`
	} `json:"statistics"`
}

type glContributor struct {
	Name      string `json:"name"`
	Email     string `json:"email"`
	Commits   int    `json:"commits"`
	Additions int    `json:"additions"`
	Deletions int    `json:"deletions"`
}

type glMember struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Name      string `json:"name"`
	State     string `json:"state"`
	AvatarURL string `json:"avatar_url"`
	WebURL    string `json:"web_url"`
	AccessLevel int  `json:"access_level"`
}

// glDiscussion represents a threaded discussion on a merge request.
// Discussions may be general notes or diff-positioned (review comments).
type glDiscussion struct {
	ID    string             `json:"id"`
	Notes []glDiscussionNote `json:"notes"`
}

// glDiscussionNote is a note within a discussion, optionally positioned on a diff.
type glDiscussionNote struct {
	ID        int64          `json:"id"`
	Body      string         `json:"body"`
	Author    glUser         `json:"author"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	System    bool           `json:"system"`
	Position  *glNotePosition `json:"position"`
}

// glNotePosition describes where a diff-positioned note sits in the code.
type glNotePosition struct {
	BaseSHA      string `json:"base_sha"`
	StartSHA     string `json:"start_sha"`
	HeadSHA      string `json:"head_sha"`
	OldPath      string `json:"old_path"`
	NewPath      string `json:"new_path"`
	PositionType string `json:"position_type"` // "text" or "image"
	OldLine      *int   `json:"old_line"`
	NewLine      *int   `json:"new_line"`
	LineRange    *struct {
		Start struct {
			LineCode string `json:"line_code"`
			Type     string `json:"type"`
			OldLine  *int   `json:"old_line"`
			NewLine  *int   `json:"new_line"`
		} `json:"start"`
		End struct {
			LineCode string `json:"line_code"`
			Type     string `json:"type"`
			OldLine  *int   `json:"old_line"`
			NewLine  *int   `json:"new_line"`
		} `json:"end"`
	} `json:"line_range"`
}
