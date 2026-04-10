package db

import (
	"context"
	"strings"
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
	MetadataIssues  int   `json:"metadata_issues"`   // issues_count from repo_info
	MetadataCommits int   `json:"metadata_commits"`  // commit_count from repo_info
	Vulnerabilities int   `json:"vulnerabilities"`    // total known CVEs from OSV.dev scan
	CriticalVulns   int   `json:"critical_vulns"`     // critical/high severity CVEs
}

// SearchRepoResult is a minimal repo record for search results.
type SearchRepoResult struct {
	ID    int64  `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
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
		if rows.Scan(&r.ID, &r.Owner, &r.Name) == nil {
			result = append(result, r)
		}
	}
	return result, rows.Err()
}

// GetRepoStats returns gathered vs metadata counts for a single repo.
// Gathered counts come from actual rows in the data tables.
// Metadata counts come from the most recent repo_info snapshot (GitHub/GitLab API totals).
func (s *PostgresStore) GetRepoStats(ctx context.Context, repoID int64) (*RepoStats, error) {
	st := &RepoStats{RepoID: repoID}

	// Gathered counts — actual rows in data tables.
	s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = $1`, repoID).Scan(&st.GatheredPRs)
	s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1`, repoID).Scan(&st.GatheredIssues)
	// commits table has one row per file per commit, so count distinct hashes.
	s.pool.QueryRow(ctx,
		`SELECT COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = $1`, repoID).Scan(&st.GatheredCommits)

	// Metadata counts — from the most recent repo_info snapshot.
	s.pool.QueryRow(ctx, `
		SELECT COALESCE(pr_count, 0), COALESCE(issues_count, 0), COALESCE(commit_count, 0)
		FROM aveloxis_data.repo_info
		WHERE repo_id = $1
		ORDER BY data_collection_date DESC
		LIMIT 1`, repoID).Scan(&st.MetadataPRs, &st.MetadataIssues, &st.MetadataCommits)

	// Vulnerability counts from OSV.dev scan.
	st.Vulnerabilities, st.CriticalVulns, _ = s.CountRepoVulnerabilities(ctx, repoID)

	return st, nil
}

// GetRepoStatsBatch returns stats for multiple repos in fewer queries.
// Used by the web GUI group detail page to avoid N+1 queries.
func (s *PostgresStore) GetRepoStatsBatch(ctx context.Context, repoIDs []int64) (map[int64]*RepoStats, error) {
	result := make(map[int64]*RepoStats, len(repoIDs))
	if len(repoIDs) == 0 {
		return result, nil
	}

	// Initialize all entries.
	for _, id := range repoIDs {
		result[id] = &RepoStats{RepoID: id}
	}

	// Gathered PRs.
	rows, err := s.pool.Query(ctx,
		`SELECT repo_id, COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = ANY($1) GROUP BY repo_id`, repoIDs)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id int64
			var cnt int
			rows.Scan(&id, &cnt)
			if st, ok := result[id]; ok {
				st.GatheredPRs = cnt
			}
		}
	}

	// Gathered issues.
	rows2, err := s.pool.Query(ctx,
		`SELECT repo_id, COUNT(*) FROM aveloxis_data.issues WHERE repo_id = ANY($1) GROUP BY repo_id`, repoIDs)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var id int64
			var cnt int
			rows2.Scan(&id, &cnt)
			if st, ok := result[id]; ok {
				st.GatheredIssues = cnt
			}
		}
	}

	// Gathered commits — count distinct hashes since the commits table
	// has one row per file touched per commit.
	rows3, err := s.pool.Query(ctx,
		`SELECT repo_id, COUNT(DISTINCT cmt_commit_hash) FROM aveloxis_data.commits WHERE repo_id = ANY($1) GROUP BY repo_id`, repoIDs)
	if err == nil {
		defer rows3.Close()
		for rows3.Next() {
			var id int64
			var cnt int
			rows3.Scan(&id, &cnt)
			if st, ok := result[id]; ok {
				st.GatheredCommits = cnt
			}
		}
	}

	// Metadata counts — latest repo_info per repo.
	rows4, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (repo_id)
			repo_id, COALESCE(pr_count, 0), COALESCE(issues_count, 0), COALESCE(commit_count, 0)
		FROM aveloxis_data.repo_info
		WHERE repo_id = ANY($1)
		ORDER BY repo_id, data_collection_date DESC`, repoIDs)
	if err == nil {
		defer rows4.Close()
		for rows4.Next() {
			var id int64
			var prs, issues, commits int
			rows4.Scan(&id, &prs, &issues, &commits)
			if st, ok := result[id]; ok {
				st.MetadataPRs = prs
				st.MetadataIssues = issues
				st.MetadataCommits = commits
			}
		}
	}

	// Vulnerability counts.
	rows5, err := s.pool.Query(ctx, `
		SELECT repo_id, COUNT(*), COUNT(*) FILTER (WHERE severity = 'CRITICAL' OR cvss_score >= 9.0)
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = ANY($1)
		GROUP BY repo_id`, repoIDs)
	if err == nil {
		defer rows5.Close()
		for rows5.Next() {
			var id int64
			var total, critical int
			rows5.Scan(&id, &total, &critical)
			if st, ok := result[id]; ok {
				st.Vulnerabilities = total
				st.CriticalVulns = critical
			}
		}
	}

	return result, nil
}
