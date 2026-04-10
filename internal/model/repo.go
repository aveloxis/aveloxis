// Package model defines the platform-agnostic data model for Aveloxis.
// All types here are shared between GitHub and GitLab collectors.
package model

import (
	"time"

	"github.com/google/uuid"
)

// Platform identifies the source forge.
type Platform int

const (
	PlatformGitHub     Platform = 1
	PlatformGitLab     Platform = 2
	PlatformGenericGit Platform = 3 // Generic git host — git-only collection (facade, analysis, scorecard)
)

func (p Platform) String() string {
	switch p {
	case PlatformGitHub:
		return "GitHub"
	case PlatformGitLab:
		return "GitLab"
	case PlatformGenericGit:
		return "Git"
	default:
		return "Unknown"
	}
}

// IsGitOnly returns true if this platform only supports git-based collection
// (no forge API for issues/PRs/events/messages).
func (p Platform) IsGitOnly() bool {
	return p == PlatformGenericGit
}

// RepoGroup is a collection of repositories (e.g. an org or user-defined group).
type RepoGroup struct {
	ID          int64
	Name        string
	Description string
	Website     string
	Type        string // e.g. "GitHub Org", "GitLab Group", "User Created"
}

// Repo represents a single repository, independent of platform.
type Repo struct {
	ID              int64
	GroupID         int64
	Platform        Platform
	GitURL          string // canonical clone URL
	Name            string
	Owner           string // org/user or GitLab group path
	Path            string // local clone path
	Description     string
	PrimaryLanguage string
	ForkedFrom      string
	Archived        bool
	CreatedAt       time.Time
	UpdatedAt       time.Time
	PlatformID      string // platform-specific numeric/string ID
}

// DataOrigin tracks provenance of collected data.
type DataOrigin struct {
	ToolSource         string
	ToolVersion        string
	DataSource         string
	DataCollectionDate time.Time
}

// ContributorIdentity holds platform-specific identity fields.
// A single Contributor can have multiple identities across platforms.
type ContributorIdentity struct {
	Platform  Platform
	UserID    int64
	Login     string
	Name      string
	Email     string
	AvatarURL string
	URL       string // profile URL on the platform
	NodeID    string // GraphQL node ID (GitHub) or empty
	Type      string // "User", "Bot", "Organization"
	IsAdmin   bool

	// GitHub-specific URL fields for denormalized gh_* columns on contributors.
	GravatarID        string
	FollowersURL      string
	FollowingURL      string
	GistsURL          string
	StarredURL        string
	SubscriptionsURL  string
	OrganizationsURL  string
	ReposURL          string
	EventsURL         string
	ReceivedEventsURL string
}

// Contributor is a person or bot that interacts with repositories.
type Contributor struct {
	ID         uuid.UUID
	Login      string // primary login across platforms
	Email      string
	FullName   string
	Company    string
	Location   string
	Canonical  string // canonical email for dedup
	CreatedAt  time.Time
	Identities []ContributorIdentity
}
