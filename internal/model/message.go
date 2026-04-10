package model

import "time"

// Message is a comment on an issue, PR/MR, or commit. Shared across platforms.
type Message struct {
	ID            int64
	RepoID        int64
	PlatformMsgID int64
	PlatformID    Platform
	NodeID        string
	Text          string
	Timestamp     time.Time
	ContributorID *string
	AuthorRef     UserRef
	Origin        DataOrigin
}

// IssueMessageRef links a message to an issue.
type IssueMessageRef struct {
	ID              int64
	IssueID         int64
	RepoID          int64
	MsgID           int64
	PlatformSrcID   int64
	PlatformNodeID  string
	PlatformIssueNumber int // issue number on the platform, for staged processing lookup
}

// PullRequestMessageRef links a message to a PR/MR.
type PullRequestMessageRef struct {
	ID              int64
	PRID            int64
	RepoID          int64
	MsgID           int64
	PlatformSrcID   int64
	PlatformNodeID  string
	PlatformPRNumber int // PR number on the platform, for staged processing lookup
}

// ReviewComment is a comment within a PR review, with code position context.
type ReviewComment struct {
	ID                int64
	ReviewID          int64  // DB pr_review_id (resolved during processing)
	RepoID            int64
	MsgID             int64
	PlatformSrcID     int64
	PlatformReviewID  int64  // Platform review ID for staged processing lookup
	NodeID            string
	DiffHunk          string
	Path              string
	Position          *int
	OriginalPosition  *int
	CommitID          string
	OriginalCommitID  string
	Line              *int
	OriginalLine      *int
	Side              string
	StartLine         *int
	OriginalStartLine *int
	StartSide         string
	AuthorAssociation string
	HTMLURL           string
	UpdatedAt         time.Time
}
