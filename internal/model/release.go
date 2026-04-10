package model

import "time"

// Release represents a tagged release on any platform.
type Release struct {
	ID          string // platform release ID
	RepoID      int64
	Name        string
	Description string
	Author      string
	TagName     string
	URL         string
	CreatedAt   time.Time
	PublishedAt *time.Time
	UpdatedAt   time.Time
	IsDraft     bool
	IsPrerelease bool
	TagOnly     bool // true if this came from a git tag, not a formal release
	Origin      DataOrigin
}
