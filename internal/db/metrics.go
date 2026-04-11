// Package db — metrics.go provides database queries for the Augur-compatible
// REST API metrics. These queries are ported from Augur's Python API
// (augur/api/metrics/*.py) and adapted for the Aveloxis schema.
//
// Schema mapping notes:
//   - Augur's augur_data schema = Aveloxis's aveloxis_data schema
//   - Augur's repo_groups = Aveloxis's repo_groups
//   - issue_state column = Augur gh_issue_state_id mapped to string
//   - pull_request state = pr_src_state in Augur, issue_state in Aveloxis
//   - dm_repo_annual/monthly/weekly tables match 1:1
//   - message table: Augur uses msg_timestamp, Aveloxis uses msg_timestamp
//   - contributors: cntrb_id is UUID in both
package db

import (
	"context"
	"fmt"
	"time"
)

// ============================================================
// Utility / lookup types and queries
// ============================================================

// RepoGroupResult represents a repo group for API responses.
type RepoGroupResult struct {
	ID          int64  `json:"repo_group_id"`
	Name        string `json:"rg_name"`
	Description string `json:"description,omitempty"`
	Type        string `json:"rg_type,omitempty"`
}

// RepoResult represents a repo for API responses.
type RepoResult struct {
	ID        int64  `json:"repo_id"`
	GroupID   int64  `json:"repo_group_id"`
	GitURL    string `json:"repo_git"`
	Name      string `json:"repo_name"`
	Owner     string `json:"repo_owner"`
	Platform  int    `json:"platform_id"`
	Archived  bool   `json:"repo_archived"`
}

// GetAllRepoGroups returns all repo groups.
func (s *PostgresStore) GetAllRepoGroups(ctx context.Context) ([]RepoGroupResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_group_id, rg_name, COALESCE(rg_description, ''), COALESCE(rg_type, '')
		FROM aveloxis_data.repo_groups
		ORDER BY rg_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RepoGroupResult
	for rows.Next() {
		var r RepoGroupResult
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Type); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetAllRepos returns all repos with basic info.
func (s *PostgresStore) GetAllRepos(ctx context.Context) ([]RepoResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_group_id, repo_git, repo_name, repo_owner, platform_id, COALESCE(repo_archived, false)
		FROM aveloxis_data.repos
		ORDER BY repo_owner, repo_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RepoResult
	for rows.Next() {
		var r RepoResult
		if err := rows.Scan(&r.ID, &r.GroupID, &r.GitURL, &r.Name, &r.Owner, &r.Platform, &r.Archived); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetReposByGroup returns repos belonging to a specific repo group.
func (s *PostgresStore) GetReposByGroup(ctx context.Context, groupID int64) ([]RepoResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, repo_group_id, repo_git, repo_name, repo_owner, platform_id, COALESCE(repo_archived, false)
		FROM aveloxis_data.repos
		WHERE repo_group_id = $1
		ORDER BY repo_owner, repo_name`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RepoResult
	for rows.Next() {
		var r RepoResult
		if err := rows.Scan(&r.ID, &r.GroupID, &r.GitURL, &r.Name, &r.Owner, &r.Platform, &r.Archived); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// GetRepoByOwnerName finds a repo by owner and name (case-insensitive).
func (s *PostgresStore) GetRepoByOwnerName(ctx context.Context, owner, name string) (*RepoResult, error) {
	var r RepoResult
	err := s.pool.QueryRow(ctx, `
		SELECT repo_id, repo_group_id, repo_git, repo_name, repo_owner, platform_id, COALESCE(repo_archived, false)
		FROM aveloxis_data.repos
		WHERE LOWER(repo_owner) = LOWER($1) AND LOWER(repo_name) = LOWER($2)
		LIMIT 1`, owner, name).Scan(&r.ID, &r.GroupID, &r.GitURL, &r.Name, &r.Owner, &r.Platform, &r.Archived)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// GetRepoGroupByName finds a repo group by name (case-insensitive).
func (s *PostgresStore) GetRepoGroupByName(ctx context.Context, name string) (*RepoGroupResult, error) {
	var r RepoGroupResult
	err := s.pool.QueryRow(ctx, `
		SELECT repo_group_id, rg_name, COALESCE(rg_description, ''), COALESCE(rg_type, '')
		FROM aveloxis_data.repo_groups
		WHERE LOWER(rg_name) = LOWER($1)
		LIMIT 1`, name).Scan(&r.ID, &r.Name, &r.Description, &r.Type)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// ============================================================
// Issue metrics
// ============================================================

// MetricRow is a generic time-series metric result.
type MetricRow struct {
	Date  time.Time `json:"date"`
	Value int       `json:"value"`
	Name  string    `json:"repo_name,omitempty"`
}

// IssuesNew returns count of new issues opened per period.
func (s *PostgresStore) IssuesNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, i.created_at::DATE) AS date, COUNT(i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND i.created_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// IssuesClosed returns count of closed issues per period.
func (s *PostgresStore) IssuesClosed(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, i.closed_at::DATE) AS date, COUNT(i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND i.closed_at IS NOT NULL
		AND i.closed_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// IssuesActive returns count of issues with events per period.
func (s *PostgresStore) IssuesActive(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, ie.created_at::DATE) AS date, COUNT(DISTINCT i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.issue_events ie ON i.issue_id = ie.issue_id
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND ie.created_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// IssueBacklog returns the count of currently open issues.
func (s *PostgresStore) IssueBacklog(ctx context.Context, repoID int64) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(issue_id)
		FROM aveloxis_data.issues
		WHERE repo_id = $1 AND issue_state = 'open' AND pull_request IS NULL`, repoID).Scan(&count)
	return count, err
}

// IssueThroughput returns the ratio of closed to total issues.
func (s *PostgresStore) IssueThroughput(ctx context.Context, repoID int64) (float64, error) {
	var throughput float64
	err := s.pool.QueryRow(ctx, `
		SELECT CASE WHEN total = 0 THEN 0.0
		ELSE CAST(closed AS FLOAT) / CAST(total AS FLOAT) END
		FROM (
			SELECT COUNT(issue_id) AS total,
			       COUNT(issue_id) FILTER (WHERE issue_state = 'closed') AS closed
			FROM aveloxis_data.issues
			WHERE repo_id = $1 AND pull_request IS NULL
		) sub`, repoID).Scan(&throughput)
	return throughput, err
}

// IssueDurationRow represents the duration of a single issue.
type IssueDurationRow struct {
	IssueID   int64     `json:"issue_id"`
	CreatedAt time.Time `json:"created_at"`
	ClosedAt  time.Time `json:"closed_at"`
	Days      float64   `json:"duration_days"`
	Name      string    `json:"repo_name,omitempty"`
}

// IssueDuration returns the duration from creation to closure for each closed issue.
func (s *PostgresStore) IssueDuration(ctx context.Context, repoID int64, begin, end time.Time) ([]IssueDurationRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT i.issue_id, i.created_at, i.closed_at,
		       EXTRACT(EPOCH FROM i.closed_at - i.created_at) / 86400.0 AS days,
		       r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $1
		AND i.pull_request IS NULL
		AND i.closed_at IS NOT NULL
		AND i.created_at BETWEEN $2 AND $3
		ORDER BY i.issue_id`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []IssueDurationRow
	for rows.Next() {
		var r IssueDurationRow
		if err := rows.Scan(&r.IssueID, &r.CreatedAt, &r.ClosedAt, &r.Days, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// AverageIssueResolutionTime returns the average days from issue creation to closure.
func (s *PostgresStore) AverageIssueResolutionTime(ctx context.Context, repoID int64) (float64, error) {
	var avg float64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(AVG(EXTRACT(EPOCH FROM closed_at - created_at) / 86400.0), 0)
		FROM aveloxis_data.issues
		WHERE repo_id = $1
		AND closed_at IS NOT NULL
		AND pull_request IS NULL`, repoID).Scan(&avg)
	return avg, err
}

// AbandonedIssues returns open issues not updated for 1+ year.
func (s *PostgresStore) AbandonedIssues(ctx context.Context, repoID int64) ([]map[string]interface{}, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT issue_id, updated_at
		FROM aveloxis_data.issues
		WHERE repo_id = $1
		AND issue_state = 'open'
		AND pull_request IS NULL
		AND EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM updated_at) >= 1
		ORDER BY updated_at`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []map[string]interface{}
	for rows.Next() {
		var issueID int64
		var updatedAt time.Time
		if err := rows.Scan(&issueID, &updatedAt); err != nil {
			return nil, err
		}
		result = append(result, map[string]interface{}{
			"issue_id":   issueID,
			"updated_at": updatedAt,
		})
	}
	return result, rows.Err()
}

// ============================================================
// Pull request metrics
// ============================================================

// PRsNew returns count of new pull requests per period.
func (s *PostgresStore) PRsNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, pr_created_at::DATE) AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND pr_created_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ReviewsAccepted returns merged PRs per period.
func (s *PostgresStore) ReviewsAccepted(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, pr_merged_at::DATE) AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND pr_merged_at IS NOT NULL
		AND pr_merged_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ReviewsDeclined returns closed (not merged) PRs per period.
func (s *PostgresStore) ReviewsDeclined(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, pr_closed_at::DATE) AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND pr_closed_at IS NOT NULL
		AND pr_merged_at IS NULL
		AND pr_closed_at BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ReviewDurationRow is the duration from creation to merge for a PR.
type ReviewDurationRow struct {
	PRID      int64     `json:"pull_request_id"`
	CreatedAt time.Time `json:"created_at"`
	MergedAt  time.Time `json:"merged_at"`
	Days      float64   `json:"duration_days"`
	Name      string    `json:"repo_name,omitempty"`
}

// ReviewDuration returns creation-to-merge duration for each merged PR.
func (s *PostgresStore) ReviewDuration(ctx context.Context, repoID int64, begin, end time.Time) ([]ReviewDurationRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT p.pull_request_id, p.pr_created_at, p.pr_merged_at,
		       EXTRACT(EPOCH FROM p.pr_merged_at - p.pr_created_at) / 86400.0 AS days,
		       r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $1
		AND p.pr_merged_at IS NOT NULL
		AND p.pr_created_at BETWEEN $2 AND $3
		ORDER BY p.pull_request_id`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ReviewDurationRow
	for rows.Next() {
		var r ReviewDurationRow
		if err := rows.Scan(&r.PRID, &r.CreatedAt, &r.MergedAt, &r.Days, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ============================================================
// Commit metrics
// ============================================================

// Committers returns count of unique committers per period.
func (s *PostgresStore) Committers(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, cmt_author_date::DATE) AS date,
		       COUNT(DISTINCT cmt_author_email) AS value,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_date BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// CodeChanges returns commit counts from the aggregate tables by period.
func (s *PostgresStore) CodeChanges(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, cmt_author_date::DATE) AS date,
		       COUNT(DISTINCT cmt_commit_hash) AS value,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_date BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// CodeChangesLinesRow holds added/removed lines per period.
type CodeChangesLinesRow struct {
	Date    time.Time `json:"date"`
	Added   int64     `json:"added"`
	Removed int64     `json:"removed"`
	Name    string    `json:"repo_name,omitempty"`
}

// CodeChangesLines returns lines added and removed per period.
func (s *PostgresStore) CodeChangesLines(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]CodeChangesLinesRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT date_trunc($1, cmt_author_date::DATE) AS date,
		       SUM(cmt_added) AS added,
		       SUM(cmt_removed) AS removed,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_date BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []CodeChangesLinesRow
	for rows.Next() {
		var r CodeChangesLinesRow
		if err := rows.Scan(&r.Date, &r.Added, &r.Removed, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ============================================================
// Contributor metrics
// ============================================================

// ContributorRow holds a contributor's activity summary.
type ContributorRow struct {
	UserID       string `json:"user_id"`
	Commits      int    `json:"commits"`
	Issues       int    `json:"issues"`
	PRs          int    `json:"pull_requests"`
	IssueComments int   `json:"issue_comments"`
	PRComments   int    `json:"pull_request_comments"`
	Total        int    `json:"total"`
	Name         string `json:"repo_name,omitempty"`
}

// Contributors returns contributor activity summaries for a repo.
func (s *PostgresStore) Contributors(ctx context.Context, repoID int64, begin, end time.Time) ([]ContributorRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(cntrb_id::TEXT, 'unknown') AS user_id,
		       SUM(commits) AS commits, SUM(issues) AS issues,
		       SUM(prs) AS prs, SUM(issue_comments) AS issue_comments,
		       SUM(pr_comments) AS pr_comments,
		       SUM(commits + issues + prs + issue_comments + pr_comments) AS total,
		       r.repo_name
		FROM (
			-- Commits
			SELECT cmt_ght_author_id AS cntrb_id, COUNT(DISTINCT cmt_commit_hash) AS commits,
			       0 AS issues, 0 AS prs, 0 AS issue_comments, 0 AS pr_comments, repo_id
			FROM aveloxis_data.commits
			WHERE repo_id = $1 AND cmt_ght_author_id IS NOT NULL
			AND cmt_author_date BETWEEN $2 AND $3
			GROUP BY cmt_ght_author_id, repo_id
			UNION ALL
			-- Issues opened
			SELECT reporter_id AS cntrb_id, 0, COUNT(*), 0, 0, 0, repo_id
			FROM aveloxis_data.issues
			WHERE repo_id = $1 AND reporter_id IS NOT NULL AND pull_request IS NULL
			AND created_at BETWEEN $2 AND $3
			GROUP BY reporter_id, repo_id
			UNION ALL
			-- PRs opened
			SELECT pr_augur_contributor_id AS cntrb_id, 0, 0, COUNT(*), 0, 0, repo_id
			FROM aveloxis_data.pull_requests
			WHERE repo_id = $1 AND pr_augur_contributor_id IS NOT NULL
			AND pr_created_at BETWEEN $2 AND $3
			GROUP BY pr_augur_contributor_id, repo_id
		) a
		JOIN aveloxis_data.repos r ON a.repo_id = r.repo_id
		GROUP BY cntrb_id, r.repo_name
		ORDER BY total DESC`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ContributorRow
	for rows.Next() {
		var r ContributorRow
		if err := rows.Scan(&r.UserID, &r.Commits, &r.Issues, &r.PRs, &r.IssueComments, &r.PRComments, &r.Total, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ContributorsNew returns count of new contributors per period.
func (s *PostgresStore) ContributorsNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, first_seen::DATE) AS date, COUNT(*) AS value, r.repo_name
		FROM (
			SELECT cmt_ght_author_id AS cntrb_id, MIN(cmt_author_date) AS first_seen, repo_id
			FROM aveloxis_data.commits
			WHERE repo_id = $2 AND cmt_ght_author_id IS NOT NULL
			GROUP BY cmt_ght_author_id, repo_id
			UNION ALL
			SELECT reporter_id, MIN(created_at), repo_id
			FROM aveloxis_data.issues
			WHERE repo_id = $2 AND reporter_id IS NOT NULL AND pull_request IS NULL
			GROUP BY reporter_id, repo_id
		) first_appearances
		JOIN aveloxis_data.repos r ON first_appearances.repo_id = r.repo_id
		WHERE first_seen BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ============================================================
// Repo metadata metrics
// ============================================================

// StarsTimeSeries returns star count over time from repo_info snapshots.
func (s *PostgresStore) StarsTimeSeries(ctx context.Context, repoID int64) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT data_collection_date AS date, stars_count AS value, r.repo_name
		FROM aveloxis_data.repo_info ri
		JOIN aveloxis_data.repos r ON ri.repo_id = r.repo_id
		WHERE ri.repo_id = $1
		ORDER BY data_collection_date`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MetricRow
	for rows.Next() {
		var r MetricRow
		if err := rows.Scan(&r.Date, &r.Value, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ForksTimeSeries returns fork count over time from repo_info snapshots.
func (s *PostgresStore) ForksTimeSeries(ctx context.Context, repoID int64) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT data_collection_date AS date, fork_count AS value, r.repo_name
		FROM aveloxis_data.repo_info ri
		JOIN aveloxis_data.repos r ON ri.repo_id = r.repo_id
		WHERE ri.repo_id = $1
		ORDER BY data_collection_date`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MetricRow
	for rows.Next() {
		var r MetricRow
		if err := rows.Scan(&r.Date, &r.Value, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// LatestCount returns the most recent value for a repo_info integer column.
func (s *PostgresStore) LatestCount(ctx context.Context, repoID int64, column string) (int, string, error) {
	// Validate column name to prevent SQL injection.
	allowed := map[string]bool{
		"stars_count": true, "fork_count": true, "watchers_count": true,
		"commit_count": true, "issues_count": true, "pr_count": true,
	}
	if !allowed[column] {
		return 0, "", fmt.Errorf("invalid column: %s", column)
	}
	var count int
	var name string
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT ri.%s, r.repo_name
		FROM aveloxis_data.repo_info ri
		JOIN aveloxis_data.repos r ON ri.repo_id = r.repo_id
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC
		LIMIT 1`, column), repoID).Scan(&count, &name)
	return count, name, err
}

// Languages returns the primary language for a repo.
func (s *PostgresStore) Languages(ctx context.Context, repoID int64) (string, error) {
	var lang string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(repo_language, '')
		FROM aveloxis_data.repos
		WHERE repo_id = $1`, repoID).Scan(&lang)
	return lang, err
}

// ============================================================
// Release metrics
// ============================================================

// ReleaseRow represents a release for API responses.
type ReleaseRow struct {
	ID          string     `json:"release_id"`
	Name        string     `json:"release_name"`
	Description string     `json:"release_description,omitempty"`
	Author      string     `json:"release_author"`
	TagName     string     `json:"release_tag_name"`
	URL         string     `json:"release_url,omitempty"`
	CreatedAt   time.Time  `json:"release_created_at"`
	PublishedAt *time.Time `json:"release_published_at,omitempty"`
	RepoName    string     `json:"repo_name,omitempty"`
}

// Releases returns all releases for a repo.
func (s *PostgresStore) Releases(ctx context.Context, repoID int64) ([]ReleaseRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT rl.release_id, rl.release_name, COALESCE(rl.release_description, ''),
		       rl.release_author, rl.release_tag_name, COALESCE(rl.release_url, ''),
		       rl.release_created_at, rl.release_published_at,
		       r.repo_name
		FROM aveloxis_data.releases rl
		JOIN aveloxis_data.repos r ON rl.repo_id = r.repo_id
		WHERE rl.repo_id = $1
		ORDER BY rl.release_published_at DESC NULLS LAST`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ReleaseRow
	for rows.Next() {
		var r ReleaseRow
		if err := rows.Scan(&r.ID, &r.Name, &r.Description, &r.Author, &r.TagName, &r.URL,
			&r.CreatedAt, &r.PublishedAt, &r.RepoName); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ============================================================
// Dependency metrics
// ============================================================

// DepRow represents a dependency for API responses.
type DepRow struct {
	Name           string  `json:"name"`
	Requirement    string  `json:"requirement"`
	Type           string  `json:"dep_type"`
	PackageManager string  `json:"package_manager"`
	CurrentVersion string  `json:"current_version"`
	LatestVersion  string  `json:"latest_version"`
	Libyear        float64 `json:"libyear"`
	License        string  `json:"license,omitempty"`
	Purl           string  `json:"purl,omitempty"`
}

// Deps returns all dependencies for a repo (latest collection).
func (s *PostgresStore) Deps(ctx context.Context, repoID int64) ([]DepRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, COALESCE(requirement, ''), COALESCE(dep_type, ''),
		       COALESCE(package_manager, ''), COALESCE(current_version, ''),
		       COALESCE(latest_version, ''), COALESCE(libyear, 0),
		       COALESCE(license, ''), COALESCE(purl, '')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1
		ORDER BY name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DepRow
	for rows.Next() {
		var r DepRow
		if err := rows.Scan(&r.Name, &r.Requirement, &r.Type, &r.PackageManager,
			&r.CurrentVersion, &r.LatestVersion, &r.Libyear, &r.License, &r.Purl); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ============================================================
// Message metrics
// ============================================================

// RepoMessages returns message counts per period.
func (s *PostgresStore) RepoMessages(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT date_trunc($1, msg_timestamp::DATE) AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.message m
		JOIN aveloxis_data.repos r ON m.repo_id = r.repo_id
		WHERE m.repo_id = $2
		AND msg_timestamp BETWEEN $3 AND $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ============================================================
// Collection status
// ============================================================

// CollectionStatusRow holds collection progress for a repo.
type CollectionStatusRow struct {
	RepoID          int64  `json:"repo_id"`
	RepoName        string `json:"repo_name"`
	CoreStatus      string `json:"core_status"`
	FacadeStatus    string `json:"facade_status"`
	GatheredIssues  int    `json:"gathered_issues"`
	GatheredPRs     int    `json:"gathered_prs"`
	GatheredCommits int    `json:"gathered_commits"`
	MetadataIssues  int    `json:"metadata_issues"`
	MetadataPRs     int    `json:"metadata_prs"`
	MetadataCommits int    `json:"metadata_commits"`
}

// ============================================================
// Complexity metrics (from SCC data in repo_labor)
// ============================================================

// ComplexityRow holds code complexity metrics per language.
type ComplexityRow struct {
	Language string `json:"language"`
	Files    int    `json:"files"`
	Lines    int    `json:"lines"`
	Blanks   int    `json:"blanks"`
	Comments int    `json:"comments"`
	Code     int    `json:"code"`
	Complexity int  `json:"complexity"`
}

// ProjectLanguages returns language breakdown from SCC data.
func (s *PostgresStore) ProjectLanguages(ctx context.Context, repoID int64) ([]ComplexityRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT rl_language, COUNT(*) AS files,
		       COALESCE(SUM(rl_code + rl_blank + rl_comment), 0) AS lines,
		       COALESCE(SUM(rl_blank), 0) AS blanks,
		       COALESCE(SUM(rl_comment), 0) AS comments,
		       COALESCE(SUM(rl_code), 0) AS code,
		       COALESCE(SUM(rl_complexity), 0) AS complexity
		FROM aveloxis_data.repo_labor
		WHERE repo_id = $1
		GROUP BY rl_language
		ORDER BY code DESC`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []ComplexityRow
	for rows.Next() {
		var r ComplexityRow
		if err := rows.Scan(&r.Language, &r.Files, &r.Lines, &r.Blanks, &r.Comments, &r.Code, &r.Complexity); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ============================================================
// Helper: generic time-series metric query
// ============================================================

func (s *PostgresStore) timeSeriesMetric(ctx context.Context, query string, args ...interface{}) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []MetricRow
	for rows.Next() {
		var r MetricRow
		if err := rows.Scan(&r.Date, &r.Value, &r.Name); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
