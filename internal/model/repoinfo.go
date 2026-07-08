// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// RepoInfo is a point-in-time snapshot of repository metadata.
// Populated via GitHub GraphQL API to get accurate PR/issue/commit counts
// and community profile files — the REST API doesn't expose these.
type RepoInfo struct {
	ID     int64
	RepoID int64
	// FullName is the forge's CANONICAL "owner/name" spelling exactly as
	// reported (GitHub nameWithOwner / REST full_name, GitLab
	// path_with_namespace — the latter carries nested groups in full).
	// Both forges accept case-variant lookups but always return the
	// canonical casing; the Phase 0 self-heal (HealRepoCaseDrift) uses
	// this to correct case-drifted repo_git values. Empty when the
	// transport didn't provide it.
	FullName       string
	LastUpdated    time.Time
	IssuesEnabled  bool
	PRsEnabled     bool
	WikiEnabled    bool
	PagesEnabled   bool
	ForkCount      int
	StarCount      int
	WatcherCount   int
	OpenIssues     int
	CommitterCount int
	CommitCount    int
	IssuesCount    int
	IssuesClosed   int
	PRCount        int
	PRsOpen        int
	PRsClosed      int
	PRsMerged      int
	DefaultBranch  string
	License        string
	// v0.23.0 — basic repo metadata captured at first observation
	// and refreshed each collection cycle. Written to repos.repo_description,
	// repos.primary_language, and repos.languages (JSONB) on Phase 0.
	Description     string
	PrimaryLanguage string
	Languages       map[string]int // language name → bytes (GitHub) or normalized weight (GitLab)
	// Community profile files — these fields store the filename if present,
	// empty string if not found.
	IssueContributorsCount string
	ChangelogFile          string
	ContributingFile       string
	LicenseFile            string
	CodeOfConductFile      string
	SecurityIssueFile      string
	SecurityAuditFile      string
	Status                 string
	Keywords               string
	Origin                 DataOrigin
}

// RepoClone holds clone/traffic statistics.
type RepoClone struct {
	ID           int64
	RepoID       int64
	Timestamp    time.Time
	TotalClones  int
	UniqueClones int
	Origin       DataOrigin
}
