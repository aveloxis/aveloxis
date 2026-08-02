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
	// v0.27.78 — fork signal, written to repos.forked_from on Phase 0.
	// IsFork is the forge's boolean (GitHub isFork / REST fork; GitLab
	// implies it via forked_from_project presence). ForkParent is the
	// upstream's full "owner/name" when the forge reports it — it can
	// be empty on a genuine fork whose upstream was deleted or is
	// inaccessible. Consumers read ForkedFrom(), never the raw fields.
	IsFork     bool
	ForkParent string
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

// UnknownForkParent is stored in repos.forked_from when the forge
// says a repository IS a fork but cannot name the upstream (deleted
// or inaccessible parent). Self-describing on purpose: fork-status
// consumers only test non-emptiness, and anything rendering the
// column reads honestly. Never fabricate an owner/name here.
const UnknownForkParent = "(unknown upstream)"

// ForkedFrom returns the value Phase 0 stores in repos.forked_from:
// the upstream full name when known, UnknownForkParent for a fork
// with no reported parent, and "" for a repository that is not a
// fork. This is the single source of the stored representation —
// both metadata writers (staged Phase 0 and the startup backfill)
// route through it.
func (ri RepoInfo) ForkedFrom() string {
	if ri.ForkParent != "" {
		return ri.ForkParent
	}
	if ri.IsFork {
		return UnknownForkParent
	}
	return ""
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
