// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"time"

	"github.com/jackc/pgx/v5"
)

// RepoStats holds gathered (actual row counts) and metadata (from repo_info API snapshot)
// counts for PRs, issues, and commits. Used by both the monitoring dashboard and the
// web GUI to show collection completeness at a glance.
type RepoStats struct {
	RepoID          int64 `json:"repo_id"`
	GatheredPRs     int   `json:"gathered_prs"`
	GatheredIssues  int   `json:"gathered_issues"`
	GatheredCommits int   `json:"gathered_commits"`
	MetadataPRs     int   `json:"metadata_prs"`     // pr_count from repo_info (GitHub API reported total)
	MetadataIssues  int   `json:"metadata_issues"`  // issues_count from repo_info
	MetadataCommits int   `json:"metadata_commits"` // commit_count from repo_info
	Vulnerabilities int   `json:"vulnerabilities"`  // current (unresolved, non-self) CVEs from OSV.dev scan
	CriticalVulns   int   `json:"critical_vulns"`   // current CVEs with severity CRITICAL or cvss_score >= 9.0
	// Archived (v0.27.50) is the forge's read-only status from the
	// latest repo_info snapshot (status='Archived') — the ACCURATE
	// signal, distinct from the repos.repo_archived boolean (which
	// prelim sets only for DEAD/404 repos). The GUI reads THIS.
	Archived bool `json:"archived"`
	// LastActivityAt (v0.27.50) is the most recent observed activity
	// (MAX commit/issue/PR timestamp), driving the chart last-active
	// ceiling + the dormant/archived chip. Nil = no activity yet.
	LastActivityAt *time.Time `json:"last_activity_at,omitempty"`
	// ForkedFrom (v0.27.79) is the upstream "owner/name" when the repo
	// is a fork (repos.forked_from — captured by Phase 0 since
	// v0.27.78; model.UnknownForkParent when the upstream was deleted).
	// Empty = not a fork. Drives the GUI's "Forked from X" chip.
	ForkedFrom string `json:"forked_from,omitempty"`
	// LastCollected (v0.27.84) is collection_queue.last_collected —
	// the GUI's ONLY way to distinguish "queued for first collection"
	// (nil → prominent queued banner) from "collected, zero activity"
	// (honest zeros). Absent when the repo has no queue row too.
	LastCollected *time.Time `json:"last_collected,omitempty"`
}

// SearchRepoResult is a minimal repo record for search results.
type SearchRepoResult struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	// Starred is annotated by the API layer when the caller presents a
	// Bearer identity (v0.27.4 — star toggles on search results).
	Starred bool `json:"starred"`
}

// SearchRepos searches repos by name or owner (case-insensitive). Used by the
// comparison page's repo search dropdown.
func (s *PostgresStore) SearchRepos(ctx context.Context, query string, limit int) ([]SearchRepoResult, error) {
	pattern := "%" + strings.ToLower(query) + "%"
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_owner, repo_name
		FROM aveloxis_data.repos
		WHERE LOWER(repo_name) LIKE $1 OR LOWER(repo_owner) LIKE $1 OR LOWER(repo_git) LIKE $1
		ORDER BY repo_owner, repo_name
		LIMIT $2`, pattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []SearchRepoResult
	for rows.Next() {
		var r SearchRepoResult
		if err := rows.Scan(&r.ID, &r.Owner, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetRepoStats returns gathered vs metadata counts for a single repo.
// Gathered counts come from actual rows in the data tables.
// Metadata counts come from the most recent repo_info snapshot (GitHub/GitLab API totals).
//
// v0.27.36: every query error propagates. The pre-fix structure
// discarded all Scan errors, so a DB failure served all-zero stats as
// if they were real (summary/18 Phase 0b). A missing repo_info
// snapshot (never-collected repo) is legitimate and yields zero
// metadata counts — that is the only tolerated no-row case.
func (s *PostgresStore) GetRepoStats(ctx context.Context, repoID int64) (*RepoStats, error) {
	st := &RepoStats{RepoID: repoID}

	// Gathered counts — actual rows in data tables. COUNT(*) always
	// returns a row, so any error here is a real failure.
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = $1`, repoID).Scan(&st.GatheredPRs); err != nil {
		return nil, fmt.Errorf("gathered PR count: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&st.GatheredIssues); err != nil {
		return nil, fmt.Errorf("gathered issue count: %w", err)
	}
	// commits table has one row per file per commit, so count distinct hashes.
	if err := s.pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&st.GatheredCommits); err != nil {
		return nil, fmt.Errorf("gathered commit count: %w", err)
	}

	// Metadata counts — from the most recent repo_info snapshot.
	// ErrNoRows = never collected; zeros are the honest answer.
	var status string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(pr_count, 0), COALESCE(issues_count, 0), COALESCE(commit_count, 0),
		       COALESCE(status, '')
		FROM aveloxis_data.repo_info
		WHERE repo_id = $1
		ORDER BY data_collection_date DESC
		LIMIT 1`, repoID).Scan(&st.MetadataPRs, &st.MetadataIssues, &st.MetadataCommits, &status)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("metadata counts: %w", err)
	}
	st.Archived = status == "Archived"

	// v0.27.79: fork lineage for the GUI chip. ErrNoRows cannot happen
	// for a repo the caller already resolved, but degrade the same way
	// the metadata block does rather than 500 the whole stats payload.
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE(forked_from, '') FROM aveloxis_data.repos WHERE repo_id = $1`,
		repoID).Scan(&st.ForkedFrom); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("fork lineage: %w", err)
	}

	// v0.27.50: last observed activity — drives the chart last-active
	// ceiling and the dormant/archived chip. Non-fatal: an error here
	// leaves LastActivityAt nil (the chip/ceiling degrade to off).
	if la, ok, laErr := s.LastActivityAt(ctx, []int64{repoID}); laErr == nil && ok {
		st.LastActivityAt = &la
	}

	// Vulnerability counts from OSV.dev scan.
	st.Vulnerabilities, st.CriticalVulns, err = s.CountRepoVulnerabilities(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("vulnerability counts: %w", err)
	}

	// v0.27.84: queue freshness — nil for never-collected repos (and
	// repos with no queue row), driving the GUI's queued banner.
	// ErrNoRows degrades to nil like the metadata block.
	if err := s.pool.QueryRow(ctx,
		`SELECT last_collected FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
		repoID).Scan(&st.LastCollected); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("queue last_collected: %w", err)
	}

	return st, nil
}

// GetRepoStatsBatch returns stats for multiple repos in fewer queries.
// Used by the web GUI group detail page and the monitor dashboard.
//
// v0.18.30 rewrite: instead of issuing 5 separate aggregate queries
// against the heavy child tables (pull_requests / issues / commits /
// repo_info / repo_deps_vulnerabilities), this reads gathered counts
// directly from collection_queue.last_* — those columns are populated
// at CompleteJob time, so they're always in sync with the actual rows
// without requiring a COUNT(*) over millions of rows on every
// dashboard render. Metadata counts come from repo_info via a single
// LATERAL-style DISTINCT ON join, and vulnerability counts use a
// scoped subquery filtered by `WHERE repo_id = ANY($1)` so the GROUP
// BY only touches relevant rows. Total: two queries instead of five,
// no million-row scans.
//
// v0.27.36: (a) query/scan errors propagate instead of silently
// serving zeros; (b) the vulnerability counts apply the SAME
// predicates as CountRepoVulnerabilities — `resolved_at IS NULL AND
// COALESCE(dependency_kind, ”) <> 'self'` — so the batch endpoint can
// never disagree with the single-repo endpoint again (summary/18
// Phase 1e; the pre-fix batch counted resolved + self rows and
// reported systematically higher, ever-growing totals).
func (s *PostgresStore) GetRepoStatsBatch(ctx context.Context, repoIDs []int64) (map[int64]*RepoStats, error) {
	result := make(map[int64]*RepoStats, len(repoIDs))
	if len(repoIDs) == 0 {
		return result, nil
	}

	// Initialize all entries.
	for _, id := range repoIDs {
		result[id] = &RepoStats{RepoID: id}
	}

	// Gathered counts (last_issues / last_prs / last_commits) come from
	// collection_queue's pre-computed cache. Metadata counts come from
	// the latest repo_info snapshot. Single query, single scan of the
	// queue index, single index lookup per repo into repo_info.
	rows, err := s.pool.Query(ctx, `
		SELECT q.repo_id,
		       COALESCE(q.last_issues, 0),
		       COALESCE(q.last_prs, 0),
		       COALESCE(q.last_commits, 0),
		       q.last_collected,
		       COALESCE(ri.pr_count, 0),
		       COALESCE(ri.issues_count, 0),
		       COALESCE(ri.commit_count, 0)
		FROM aveloxis_ops.collection_queue q
		LEFT JOIN LATERAL (
		    SELECT pr_count, issues_count, commit_count
		    FROM aveloxis_data.repo_info
		    WHERE repo_id = q.repo_id
		    ORDER BY data_collection_date DESC
		    LIMIT 1
		) ri ON TRUE
		WHERE q.repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("batch gathered/metadata counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var gIssues, gPRs, gCommits, mPRs, mIssues, mCommits int
		var lastCollected *time.Time
		if err := rows.Scan(&id, &gIssues, &gPRs, &gCommits, &lastCollected, &mPRs, &mIssues, &mCommits); err != nil {
			return nil, fmt.Errorf("batch stats scan: %w", err)
		}
		if st, ok := result[id]; ok {
			st.GatheredIssues = gIssues
			st.GatheredPRs = gPRs
			st.GatheredCommits = gCommits
			st.LastCollected = lastCollected
			st.MetadataPRs = mPRs
			st.MetadataIssues = mIssues
			st.MetadataCommits = mCommits
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("batch stats iteration: %w", err)
	}

	// Vulnerability counts. Scoped subquery: only scans rows whose
	// repo_id is in the requested set, so this stays cheap. Predicates
	// mirror CountRepoVulnerabilities exactly (see doc comment above).
	rows5, err := s.pool.Query(ctx, `
		SELECT repo_id, COUNT(*), COUNT(*) FILTER (WHERE severity = 'CRITICAL' OR cvss_score >= 9.0)
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = ANY($1)
		  AND resolved_at IS NULL
		  AND COALESCE(dependency_kind, '') <> 'self'
		GROUP BY repo_id`, repoIDs)
	if err != nil {
		return nil, fmt.Errorf("batch vulnerability counts: %w", err)
	}
	defer rows5.Close()
	for rows5.Next() {
		var id int64
		var total, critical int
		if err := rows5.Scan(&id, &total, &critical); err != nil {
			return nil, fmt.Errorf("batch vulnerability scan: %w", err)
		}
		if st, ok := result[id]; ok {
			st.Vulnerabilities = total
			st.CriticalVulns = critical
		}
	}
	if err := rows5.Err(); err != nil {
		return nil, fmt.Errorf("batch vulnerability iteration: %w", err)
	}

	return result, nil
}
