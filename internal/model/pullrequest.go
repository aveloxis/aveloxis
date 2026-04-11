package model

import "time"

// PullRequest represents a pull request (GitHub) or merge request (GitLab).
type PullRequest struct {
	ID                int64
	RepoID            int64
	PlatformSrcID     int64  // PR/MR ID on the platform
	NodeID            string // GraphQL node ID
	Number            int    // human-readable number within the repo
	URL               string
	HTMLURL           string
	DiffURL           string
	Title             string
	Body              string
	State             string // "open", "closed", "merged"
	Locked            bool
	CreatedAt         time.Time
	UpdatedAt         time.Time
	ClosedAt          *time.Time
	MergedAt          *time.Time
	MergeCommitSHA    string
	AuthorID          *string // contributor UUID
	AuthorRef         UserRef  // raw platform user data for contributor resolution
	AuthorAssociation string // "CONTRIBUTOR", "COLLABORATOR", "MEMBER", etc.
	MetaHeadID        *int64 // FK to PullRequestMeta
	MetaBaseID        *int64
	Origin            DataOrigin
}

// PullRequestLabel is a label on a PR/MR.
type PullRequestLabel struct {
	ID          int64
	PRID        int64
	RepoID      int64
	PlatformID  int64
	NodeID      string
	Name        string
	Description string
	Color       string
	IsDefault   bool
	Origin      DataOrigin
}

// PullRequestAssignee links a contributor to a PR/MR assignment.
type PullRequestAssignee struct {
	ID            int64
	PRID          int64
	RepoID        int64
	ContributorID int64
	PlatformSrcID int64
	Origin        DataOrigin
}

// PullRequestReviewer is a requested or completed reviewer.
type PullRequestReviewer struct {
	ID            int64
	PRID          int64
	RepoID        int64
	ContributorID int64
	PlatformSrcID int64
	Origin        DataOrigin
}

// PullRequestReview is a single review on a PR/MR.
type PullRequestReview struct {
	ID                int64
	PRID              int64
	RepoID            int64
	ContributorID     *string
	AuthorRef         UserRef
	PlatformID        Platform
	PlatformReviewID  int64
	NodeID            string
	State             string // "APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED"
	Body              string
	SubmittedAt       time.Time
	AuthorAssociation string
	CommitID          string // commit SHA being reviewed
	HTMLURL           string
	Origin            DataOrigin
}

// PullRequestMeta holds head or base branch metadata for a PR/MR.
type PullRequestMeta struct {
	ID         int64
	PRID       int64
	RepoID     int64
	AuthorID   *string
	HeadOrBase string // "head" or "base"
	Label      string // e.g. "owner:branch"
	Ref        string // branch name
	SHA        string // commit SHA
	Origin     DataOrigin
}

// PullRequestCommit is a commit within a PR/MR.
type PullRequestCommit struct {
	ID          int64
	PRID        int64
	RepoID      int64
	AuthorID    *string
	AuthorRef   UserRef
	SHA         string
	NodeID      string
	Message     string
	AuthorEmail string
	Timestamp   time.Time
	Origin      DataOrigin
}

// PullRequestFile is a file changed in a PR/MR.
type PullRequestFile struct {
	ID        int64
	PRID      int64
	RepoID    int64
	Path      string
	Additions int
	Deletions int
	Origin    DataOrigin
}

// PullRequestRepo stores information about the fork/upstream repository
// referenced in a pull request's head or base. This captures which repo a
// PR branch comes from (head = fork repo, base = upstream repo).
// Populated from GitHub's head.repo/base.repo and GitLab's source_project/target_project.
type PullRequestRepo struct {
	ID           int64
	MetaID       int64  // FK to PullRequestMeta.ID
	HeadOrBase   string // "head" or "base"
	SrcRepoID    int64  // repo ID on the platform (e.g., GitHub repo ID)
	SrcNodeID    string // GraphQL node ID
	RepoName     string // short name (e.g., "my-fork")
	RepoFullName string // full name (e.g., "contributor/my-fork")
	Private      bool
	ContribID    *string // contributor UUID of the repo owner
	Origin       DataOrigin
}

// PullRequestEvent records a state change on a PR/MR.
type PullRequestEvent struct {
	ID               int64
	PRID             int64
	RepoID           int64
	ContributorID    *string
	ActorRef         UserRef
	PlatformID       Platform
	PlatformEventID  int64
	PlatformPRID     int64  // PR number on the platform, used to resolve PRID during staged processing
	NodeID           string
	Action           string
	ActionCommitHash string
	CreatedAt        time.Time
	Origin           DataOrigin
}
