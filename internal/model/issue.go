package model

import "time"

// Issue represents an issue on any platform.
type Issue struct {
	ID           int64
	RepoID       int64
	PlatformID   int64  // issue ID on the source platform
	Number       int    // human-readable number within the repo
	NodeID       string // GraphQL node ID (GitHub) or empty
	Title        string
	Body         string
	State        string // "open", "closed"
	URL          string // API URL
	HTMLURL      string // web URL
	ReporterID   *string // contributor UUID who opened
	ClosedByID   *string // contributor UUID who closed (nullable)
	ReporterRef  UserRef // raw platform user data for contributor resolution
	ClosedByRef  UserRef // raw platform user data for contributor resolution
	CreatedAt    time.Time
	UpdatedAt    time.Time
	ClosedAt     *time.Time
	CommentCount int
	Origin       DataOrigin
}

// IssueLabel is a label attached to an issue.
type IssueLabel struct {
	ID          int64
	IssueID     int64
	RepoID      int64
	PlatformID  int64
	NodeID      string
	Text        string
	Description string
	Color       string
	Origin      DataOrigin
}

// IssueAssignee links a contributor to an issue assignment.
type IssueAssignee struct {
	ID             int64
	IssueID        int64
	RepoID         int64
	ContributorID  int64
	PlatformSrcID  int64
	PlatformNodeID string
	Origin         DataOrigin
}

// IssueEvent records a state change or action on an issue.
type IssueEvent struct {
	ID               int64
	IssueID          int64
	RepoID           int64
	ContributorID    *string
	ActorRef         UserRef
	PlatformID       Platform
	PlatformEventID  int64
	PlatformIssueID  int64  // issue number on the platform, used to resolve IssueID during staged processing
	NodeID           string
	Action           string // "opened", "closed", "labeled", "assigned", etc.
	ActionCommitHash string
	CreatedAt        time.Time
	Origin           DataOrigin
}
