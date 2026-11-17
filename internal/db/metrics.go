// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
//
// Windows (stage 1b, eval-4 item 4): every windowed reader takes a
// half-open [lo, hi) — `>= lo AND < hi`, never BETWEEN — and the commit
// readers count by the UTC day of cmt_author_timestamp (N19: the same day
// as /timeseries and the daily commit table), never by casting the TEXT
// cmt_author_date, whose one empty-string row failed the whole query (22007). A
// bucket is the UTC midnight that starts its period, as an instant:
// `date_trunc($1, ts AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'` truncates the
// UTC wall time and reads it back as an instant (then .UTC() in Go for one
// serialization). The earlier `date_trunc($1, x::DATE)` cast the date to
// timestamptz in the SESSION's zone, so the bucket's midnight — and the
// serialized `date` — depended on the database host's zone. An empty
// result is [] (D6: pgx.CollectRows).
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
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
	ID       int64  `json:"repo_id"`
	GroupID  int64  `json:"repo_group_id"`
	GitURL   string `json:"repo_git"`
	Name     string `json:"repo_name"`
	Owner    string `json:"repo_owner"`
	Platform int    `json:"platform_id"`
	Archived bool   `json:"repo_archived"`
}

// RepoScope is what a catalog reader may show (stage 1b, D1): everything
// (an administrator's session, a caller without an identity), or exactly
// the repositories in IDs — an API token's groups. The zero value shows
// NOTHING: an empty scope is never read as "all" (eval-4 item 5).
type RepoScope struct {
	All bool
	IDs []int64
}

// repoScopeSQL is the predicate every catalog reader applies, with the
// scope bound as ($all boolean, $ids bigint[]); col is the repository id
// column. One spelling for every reader (SR-17).
func repoScopeSQL(col, all, ids string) string {
	return "(" + all + "::boolean OR " + col + " = ANY(" + ids + "::bigint[]))"
}

// ids never binds a nil slice: pgx sends a nil []int64 as SQL NULL, and
// `x = ANY(NULL)` is NULL — which reads as false here, but an explicit empty
// array says what is meant.
func (sc RepoScope) ids() []int64 {
	if sc.IDs == nil {
		return []int64{}
	}
	return sc.IDs
}

// RepoOutOfScopeError is a catalog lookup's answer when the name exists
// only outside the caller's scope: the repository's id, so the caller can
// refuse with the structured 403 that names it (N21; D5: known but out of
// scope is 403, absent is 404).
type RepoOutOfScopeError struct{ RepoID int64 }

func (e *RepoOutOfScopeError) Error() string {
	return fmt.Sprintf("repository %d is outside the caller's scope", e.RepoID)
}

// repoResultColumns is the one projection every catalog reader scans into
// RepoResult (pgx.RowToStructByPos: the order is the struct's).
const repoResultColumns = `repo_id, COALESCE(repo_group_id, 0), repo_git, repo_name, repo_owner, platform_id, COALESCE(repo_archived, false)`

// ListRepoGroups returns the legacy repository groups the scope may see:
// every group when unscoped, otherwise the groups holding at least one
// in-scope repository (N20). Ordered by name. Empty is [] (D6).
func (s *PostgresStore) ListRepoGroups(ctx context.Context, scope RepoScope) ([]RepoGroupResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_group_id, rg_name, COALESCE(rg_description, ''), COALESCE(rg_type, '')
		FROM aveloxis_data.repo_groups rg
		WHERE $1::boolean OR EXISTS (
			SELECT 1 FROM aveloxis_data.repos r
			WHERE r.repo_group_id = rg.repo_group_id AND r.repo_id = ANY($2::bigint[]))
		ORDER BY rg_name`, scope.All, scope.ids())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[RepoGroupResult])
}

// ListRepos returns the repositories the scope may see, ordered by owner
// and name. Empty is [] (D6).
func (s *PostgresStore) ListRepos(ctx context.Context, scope RepoScope) ([]RepoResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+repoResultColumns+`
		FROM aveloxis_data.repos
		WHERE `+repoScopeSQL("repo_id", "$1", "$2")+`
		ORDER BY repo_owner, repo_name`, scope.All, scope.ids())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[RepoResult])
}

// GetReposByGroup returns the repositories of one legacy group the scope
// may see, ordered by owner and name. A group id no group has, or a group
// with nothing in scope, is an empty list (D6: [], not null).
func (s *PostgresStore) GetReposByGroup(ctx context.Context, groupID int64, scope RepoScope) ([]RepoResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+repoResultColumns+`
		FROM aveloxis_data.repos
		WHERE repo_group_id = $1 AND `+repoScopeSQL("repo_id", "$2", "$3")+`
		ORDER BY repo_owner, repo_name`, groupID, scope.All, scope.ids())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[RepoResult])
}

// GetRepoByOwnerName finds the repository with this owner and name (without
// regard to letter case) inside the scope; with the name on two forges the
// pick is by platform then id (N21). When the name exists only outside the
// scope the answer is RepoOutOfScopeError with that repository's id; no
// such repository at all is ErrRepoNotFound (SR-5: every other failure is
// the store's).
func (s *PostgresStore) GetRepoByOwnerName(ctx context.Context, owner, name string, scope RepoScope) (*RepoResult, error) {
	return s.catalogPick(ctx, `LOWER(repo_owner) = LOWER($1) AND LOWER(repo_name) = LOWER($2)`, []any{owner, name}, scope,
		fmt.Sprintf("repo %s/%s", owner, name))
}

// GetRepoInGroupByName finds the repository named name (without regard to
// letter case) in the legacy group groupID, with GetRepoByOwnerName's
// scope rule (stage 1b: the by-group lookup compared the name
// case-sensitively while the owner/name one did not).
func (s *PostgresStore) GetRepoInGroupByName(ctx context.Context, groupID int64, name string, scope RepoScope) (*RepoResult, error) {
	return s.catalogPick(ctx, `repo_group_id = $1 AND LOWER(repo_name) = LOWER($2)`, []any{groupID, name}, scope,
		fmt.Sprintf("repo %s in group %d", name, groupID))
}

// catalogPick is the one lookup behind the two by-name readers: the first
// match of where (its arguments first) inside the scope by platform then
// id; otherwise an unscoped probe tells RepoOutOfScopeError (the twin's
// id) from ErrRepoNotFound.
func (s *PostgresStore) catalogPick(ctx context.Context, where string, args []any, scope RepoScope, what string) (*RepoResult, error) {
	n := len(args)
	all, ids := fmt.Sprintf("$%d", n+1), fmt.Sprintf("$%d", n+2)
	scoped := append(append([]any{}, args...), scope.All, scope.ids())
	rows, err := s.pool.Query(ctx, `
		SELECT `+repoResultColumns+`
		FROM aveloxis_data.repos
		WHERE `+where+` AND `+repoScopeSQL("repo_id", all, ids)+`
		ORDER BY platform_id, repo_id
		LIMIT 1`, scoped...)
	if err != nil {
		return nil, err
	}
	r, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[RepoResult])
	if err == nil {
		return &r, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if scope.All {
		return nil, fmt.Errorf("%s: %w", what, ErrRepoNotFound)
	}
	var twin int64
	err = s.pool.QueryRow(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE `+where+`
		ORDER BY platform_id, repo_id
		LIMIT 1`, args...).Scan(&twin)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%s: %w", what, ErrRepoNotFound)
	}
	if err != nil {
		return nil, err
	}
	return nil, &RepoOutOfScopeError{RepoID: twin}
}

// ErrRepoGroupNotFound is GetRepoGroupByName's answer for a name no repo
// group has: the typed not-found (SR-5), so a handler can tell it from a
// store failure.
var ErrRepoGroupNotFound = errors.New("repo group not found")

// GetRepoGroupByName finds a repo group by name (case-insensitive) that the
// scope may see (N20: a group holding no in-scope repository is not
// found). A name no such group has is ErrRepoGroupNotFound; every other
// failure is the store's.
func (s *PostgresStore) GetRepoGroupByName(ctx context.Context, name string, scope RepoScope) (*RepoGroupResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_group_id, rg_name, COALESCE(rg_description, ''), COALESCE(rg_type, '')
		FROM aveloxis_data.repo_groups rg
		WHERE LOWER(rg_name) = LOWER($1)
		  AND ($2::boolean OR EXISTS (
			SELECT 1 FROM aveloxis_data.repos r
			WHERE r.repo_group_id = rg.repo_group_id AND r.repo_id = ANY($3::bigint[])))
		ORDER BY repo_group_id
		LIMIT 1`, name, scope.All, scope.ids())
	if err != nil {
		return nil, err
	}
	g, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[RepoGroupResult])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("repo group %q: %w", name, ErrRepoGroupNotFound)
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}

// ============================================================
// Issue metrics
// ============================================================

// bucketSQL is the one spelling of a metric bucket (SR-17): the period $1
// of expr's UTC wall time, read back as an instant — the file comment says
// why the earlier cast through the session's zone was wrong. expr MUST be a
// timestamptz: `timestamp AT TIME ZONE 'UTC'` is a timestamptz, so a
// timestamp handed in would be truncated in the session's zone (review
// round 2 caught the DATE site). A DATE is handed in as
// `(col::timestamp AT TIME ZONE 'UTC')`, its midnight as a UTC instant;
// TestEveryBucketSQLSiteIsATimestamptz types every site.
func bucketSQL(expr string) string {
	return "(date_trunc($1, " + expr + " AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')"
}

// MetricRow is a generic time-series metric result.
type MetricRow struct {
	Date  time.Time `json:"date"`
	Value int       `json:"value"`
	Name  string    `json:"repo_name,omitempty"`
}

// IssuesNew returns count of new issues opened per period.
func (s *PostgresStore) IssuesNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("i.created_at")+` AS date, COUNT(i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND i.created_at >= $3 AND i.created_at < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// IssuesClosed returns count of closed issues per period.
func (s *PostgresStore) IssuesClosed(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("i.closed_at")+` AS date, COUNT(i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND i.closed_at IS NOT NULL
		AND i.closed_at >= $3 AND i.closed_at < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// IssuesActive returns count of issues with events per period.
func (s *PostgresStore) IssuesActive(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("ie.created_at")+` AS date, COUNT(DISTINCT i.issue_id) AS value, r.repo_name
		FROM aveloxis_data.issues i
		JOIN aveloxis_data.issue_events ie ON i.issue_id = ie.issue_id
		JOIN aveloxis_data.repos r ON i.repo_id = r.repo_id
		WHERE i.repo_id = $2
		AND i.pull_request IS NULL
		AND ie.created_at >= $3 AND ie.created_at < $4
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
		AND i.created_at >= $2 AND i.created_at < $3
		ORDER BY i.issue_id`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[IssueDurationRow])
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
func (s *PostgresStore) AbandonedIssues(ctx context.Context, repoID int64) ([]map[string]any, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT issue_id, updated_at
		FROM aveloxis_data.issues
		WHERE repo_id = $1
		AND issue_state = 'open'
		AND pull_request IS NULL
		AND updated_at < NOW() - INTERVAL '1 year'
		ORDER BY updated_at`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{} // D6: empty is []
	for rows.Next() {
		var issueID int64
		var updatedAt time.Time
		if err := rows.Scan(&issueID, &updatedAt); err != nil {
			return nil, err
		}
		result = append(result, map[string]any{
			"issue_id":   issueID,
			"updated_at": updatedAt,
		})
	}
	return result, rows.Err()
}

// ClosedIssueCount is the count of closed issues (pull requests excluded):
// /closed-issues-count's body, in the store since stage 1b (eval-4 item 3:
// the handler ran this SQL itself).
func (s *PostgresStore) ClosedIssueCount(ctx context.Context, repoID int64) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(issue_id)
		FROM aveloxis_data.issues
		WHERE repo_id = $1 AND issue_state = 'closed' AND pull_request IS NULL`, repoID).Scan(&count)
	return count, err
}

// ============================================================
// Pull request metrics
// ============================================================

// PRsNew returns count of new pull requests per period.
func (s *PostgresStore) PRsNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("p.created_at")+` AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND p.created_at >= $3 AND p.created_at < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ReviewsAccepted returns merged PRs per period.
func (s *PostgresStore) ReviewsAccepted(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("p.merged_at")+` AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND p.merged_at IS NOT NULL
		AND p.merged_at >= $3 AND p.merged_at < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ReviewsDeclined returns closed (not merged) PRs per period.
func (s *PostgresStore) ReviewsDeclined(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("p.closed_at")+` AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $2
		AND p.closed_at IS NOT NULL
		AND p.merged_at IS NULL
		AND p.closed_at >= $3 AND p.closed_at < $4
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
		SELECT p.pull_request_id, p.created_at, p.merged_at,
		       EXTRACT(EPOCH FROM p.merged_at - p.created_at) / 86400.0 AS days,
		       r.repo_name
		FROM aveloxis_data.pull_requests p
		JOIN aveloxis_data.repos r ON p.repo_id = r.repo_id
		WHERE p.repo_id = $1
		AND p.merged_at IS NOT NULL
		AND p.created_at >= $2 AND p.created_at < $3
		ORDER BY p.pull_request_id`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ReviewDurationRow])
}

// ============================================================
// Commit metrics
// ============================================================

// Committers returns count of unique committers per period.
func (s *PostgresStore) Committers(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	// v0.27.37 (summary/18 Phase 1f + the Phase 2 committers merge):
	// count IDENTITIES, not raw emails. Raw-email counting collapsed
	// the ~5.6M empty-email commit rows into ONE phantom committer and
	// counted one human with N emails N times — and disagreed with the
	// identity-based /compare committers metric for the same repo.
	// cmt_ght_author_id (~92% fleet coverage) leads; unresolved
	// commits fall back to their non-empty email so nobody vanishes.
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("cmt_author_timestamp")+` AS date,
		       COUNT(DISTINCT COALESCE(cmt_ght_author_id::text, NULLIF(cmt_author_email, ''))) AS value,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_timestamp >= $3 AND cmt_author_timestamp < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// CodeChanges returns commit counts from the aggregate tables by period.
func (s *PostgresStore) CodeChanges(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("cmt_author_timestamp")+` AS date,
		       COUNT(DISTINCT cmt_commit_hash) AS value,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_timestamp >= $3 AND cmt_author_timestamp < $4
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
		SELECT `+bucketSQL("cmt_author_timestamp")+` AS date,
		       SUM(cmt_added) AS added,
		       SUM(cmt_removed) AS removed,
		       r.repo_name
		FROM aveloxis_data.commits c
		JOIN aveloxis_data.repos r ON c.repo_id = r.repo_id
		WHERE c.repo_id = $2
		AND cmt_author_timestamp >= $3 AND cmt_author_timestamp < $4
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[CodeChangesLinesRow])
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Date = out[i].Date.UTC()
	}
	return out, nil
}

// ============================================================
// Contributor metrics
// ============================================================

// ContributorRow holds a contributor's activity summary.
type ContributorRow struct {
	UserID        string `json:"user_id"`
	Commits       int    `json:"commits"`
	Issues        int    `json:"issues"`
	PRs           int    `json:"pull_requests"`
	IssueComments int    `json:"issue_comments"`
	PRComments    int    `json:"pull_request_comments"`
	Total         int    `json:"total"`
	Name          string `json:"repo_name,omitempty"`
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
			AND cmt_author_timestamp >= $2 AND cmt_author_timestamp < $3
			GROUP BY cmt_ght_author_id, repo_id
			UNION ALL
			-- Issues opened
			SELECT reporter_id AS cntrb_id, 0, COUNT(*), 0, 0, 0, repo_id
			FROM aveloxis_data.issues
			WHERE repo_id = $1 AND reporter_id IS NOT NULL AND pull_request IS NULL
			AND created_at >= $2 AND created_at < $3
			GROUP BY reporter_id, repo_id
			UNION ALL
			-- PRs opened
			SELECT author_id AS cntrb_id, 0, 0, COUNT(*), 0, 0, repo_id
			FROM aveloxis_data.pull_requests
			WHERE repo_id = $1 AND author_id IS NOT NULL
			AND created_at >= $2 AND created_at < $3
			GROUP BY author_id, repo_id
			UNION ALL
			-- Issue conversation comments (v0.27.37 Phase 1f: these
			-- fields were hardcoded 0 while the JSON exposed them —
			-- every consumer saw "0 comments" for every contributor)
			SELECT m.cntrb_id, 0, 0, 0, COUNT(*), 0, imr.repo_id
			FROM aveloxis_data.issue_message_ref imr
			JOIN aveloxis_data.messages m ON m.msg_id = imr.msg_id
			WHERE imr.repo_id = $1 AND m.cntrb_id IS NOT NULL
			AND m.msg_timestamp >= $2 AND m.msg_timestamp < $3
			GROUP BY m.cntrb_id, imr.repo_id
			UNION ALL
			-- PR conversation comments
			SELECT m.cntrb_id, 0, 0, 0, 0, COUNT(*), pmr.repo_id
			FROM aveloxis_data.pull_request_message_ref pmr
			JOIN aveloxis_data.messages m ON m.msg_id = pmr.msg_id
			WHERE pmr.repo_id = $1 AND m.cntrb_id IS NOT NULL
			AND m.msg_timestamp >= $2 AND m.msg_timestamp < $3
			GROUP BY m.cntrb_id, pmr.repo_id
		) a
		JOIN aveloxis_data.repos r ON a.repo_id = r.repo_id
		GROUP BY cntrb_id, r.repo_name
		ORDER BY total DESC`, repoID, begin, end)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ContributorRow])
}

// ContributorsNew returns count of new contributors per period.
func (s *PostgresStore) ContributorsNew(ctx context.Context, repoID int64, period string, begin, end time.Time) ([]MetricRow, error) {
	// v0.27.37 (Phase 1f): one row per PERSON — the inner UNION yields
	// per-source first appearances, and the middle GROUP BY cntrb_id
	// takes the earliest across sources. Pre-fix the union fed the
	// bucket count directly, so a contributor whose first commit AND
	// first issue both fell in the window counted as TWO new people.
	return s.timeSeriesMetric(ctx, `
		SELECT `+bucketSQL("(first_seen::timestamp AT TIME ZONE 'UTC')")+` AS date, COUNT(*) AS value, r.repo_name
		FROM (
			SELECT cntrb_id, MIN(first_seen) AS first_seen, repo_id
			FROM (
				SELECT cmt_ght_author_id AS cntrb_id, MIN((cmt_author_timestamp AT TIME ZONE 'UTC')::DATE) AS first_seen, repo_id
				FROM aveloxis_data.commits
				WHERE repo_id = $2 AND cmt_ght_author_id IS NOT NULL AND cmt_author_timestamp IS NOT NULL
				GROUP BY cmt_ght_author_id, repo_id
				UNION ALL
				SELECT reporter_id, MIN((created_at AT TIME ZONE 'UTC')::DATE), repo_id
				FROM aveloxis_data.issues
				WHERE repo_id = $2 AND reporter_id IS NOT NULL AND pull_request IS NULL
				GROUP BY reporter_id, repo_id
			) per_source
			GROUP BY cntrb_id, repo_id
		) first_appearances
		JOIN aveloxis_data.repos r ON first_appearances.repo_id = r.repo_id
		WHERE first_seen >= ($3::timestamptz AT TIME ZONE 'UTC')::DATE AND first_seen < ($4::timestamptz AT TIME ZONE 'UTC')::DATE
		GROUP BY date, r.repo_name
		ORDER BY date`, period, repoID, begin, end)
}

// ============================================================
// Repo metadata metrics
// ============================================================

// snapshotColumns are the repo_info counts a caller may ask for by name
// (LatestCount, SnapshotSeries): the one allowlist that keeps the column
// name out of the SQL text's reach.
var snapshotColumns = map[string]bool{
	"star_count": true, "fork_count": true, "watcher_count": true,
	"commit_count": true, "issues_count": true, "pr_count": true,
}

// SnapshotSeries returns one repo_info count over time: the history rows
// and the current snapshot (the history pattern keeps only the latest in
// repo_info — v0.27.37: without the union a "series" had one point),
// oldest first. The one reader behind /stars, /forks and /watchers (stage
// 1b, eval-4 item 7). A NULL-dated row (legacy snapshots exist) is left
// out, and so is a NULL count (0.29.145): the count of that snapshot is
// unknown, and drawing it as 0 put a false value in the chart — the
// migrate's ClearStarCountWatchers makes such rows on purpose. Both failed
// the scan before stage 1b. Empty is [].
func (s *PostgresStore) SnapshotSeries(ctx context.Context, repoID int64, column string) ([]MetricRow, error) {
	if !snapshotColumns[column] {
		return nil, fmt.Errorf("invalid column: %s", column)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT snap.date, snap.value, r.repo_name
		FROM (
			SELECT data_collection_date AS date, %[1]s AS value, repo_id
			FROM aveloxis_data.repo_info WHERE repo_id = $1
			UNION ALL
			SELECT data_collection_date, %[1]s, repo_id
			FROM aveloxis_data.repo_info_history WHERE repo_id = $1
		) snap
		JOIN aveloxis_data.repos r ON snap.repo_id = r.repo_id
		WHERE snap.date IS NOT NULL AND snap.value IS NOT NULL
		ORDER BY snap.date`, column), repoID)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[MetricRow])
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Date = out[i].Date.UTC() // one serialization with the windowed series (review round 1)
	}
	return out, nil
}

// LatestCount returns the most recent value for a repo_info integer column.
func (s *PostgresStore) LatestCount(ctx context.Context, repoID int64, column string) (int, string, error) {
	if !snapshotColumns[column] {
		return 0, "", fmt.Errorf("invalid column: %s", column)
	}
	var count int
	var name string
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT COALESCE(ri.%s, 0), r.repo_name
		FROM aveloxis_data.repo_info ri
		JOIN aveloxis_data.repos r ON ri.repo_id = r.repo_id
		WHERE ri.repo_id = $1
		ORDER BY ri.data_collection_date DESC NULLS LAST, ri.repo_info_id DESC
		LIMIT 1`, column), repoID).Scan(&count, &name)
	if errors.Is(err, pgx.ErrNoRows) {
		// No repo_info snapshot yet (repo not collected) — zero, not 500.
		return 0, "", nil
	}
	return count, name, err
}

// Languages returns the primary language for a repo.
func (s *PostgresStore) Languages(ctx context.Context, repoID int64) (string, error) {
	var lang string
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(primary_language, '')
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
		SELECT rl.release_id::TEXT, rl.release_name, COALESCE(rl.release_description, ''),
		       rl.release_author, rl.release_tag_name, COALESCE(rl.release_url, ''),
		       rl.created_at, rl.published_at,
		       r.repo_name
		FROM aveloxis_data.releases rl
		JOIN aveloxis_data.repos r ON rl.repo_id = r.repo_id
		WHERE rl.repo_id = $1
		ORDER BY rl.published_at DESC NULLS LAST`, repoID)
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
		SELECT name, COALESCE(requirement, ''), COALESCE(type, ''),
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
	result := []DepRow{} // D6: empty is []
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

// DepsFiltered is the license drill-down behind the licenses table
// (v0.28.1, item 7a): it restricts the dependency rows to the SAME
// row universe GetRepoLicensesScoped aggregates, so clicking a
// license bucket of N lists exactly N rows. The githubactions
// exclusion applies unconditionally (those rows carry no license
// data and the aggregate excludes them in BOTH scopes); runtimeOnly
// applies the aggregate's non-runtime-type exclusion; a non-empty
// license keeps only rows whose NORMALIZED license equals it — the
// aggregate's buckets are post-normalization (synonyms merged via
// normalizeLicense in Go), so matching must happen post-normalization
// too. Raw SQL string matching would orphan synonym-form rows
// ("MIT License" never matching the "MIT" bucket) — the documented
// client-side trap this method exists to avoid.
func (s *PostgresStore) DepsFiltered(ctx context.Context, repoID int64, license string, runtimeOnly bool) ([]DepRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, COALESCE(requirement, ''), COALESCE(type, ''),
		       COALESCE(package_manager, ''), COALESCE(current_version, ''),
		       COALESCE(latest_version, ''), COALESCE(libyear, 0),
		       COALESCE(license, ''), COALESCE(purl, '')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1`+licenseRowUniverseSQL+`
		ORDER BY name`, repoID, runtimeOnly)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []DepRow{} // D6: empty is []
	for rows.Next() {
		var r DepRow
		if err := rows.Scan(&r.Name, &r.Requirement, &r.Type, &r.PackageManager,
			&r.CurrentVersion, &r.LatestVersion, &r.Libyear, &r.License, &r.Purl); err != nil {
			return nil, err
		}
		if license != "" && normalizeLicense(r.License) != license {
			continue
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
		SELECT `+bucketSQL("msg_timestamp")+` AS date, COUNT(*) AS value, r.repo_name
		FROM aveloxis_data.messages m
		JOIN aveloxis_data.repos r ON m.repo_id = r.repo_id
		WHERE m.repo_id = $2
		AND msg_timestamp >= $3 AND msg_timestamp < $4
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
	Language   string `json:"language"`
	Files      int    `json:"files"`
	Lines      int    `json:"lines"`
	Blanks     int    `json:"blanks"`
	Comments   int    `json:"comments"`
	Code       int    `json:"code"`
	Complexity int    `json:"complexity"`
}

// ProjectLanguages returns language breakdown from SCC data.
func (s *PostgresStore) ProjectLanguages(ctx context.Context, repoID int64) ([]ComplexityRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT programming_language, COUNT(*) AS files,
		       COALESCE(SUM(total_lines), 0) AS lines,
		       COALESCE(SUM(blank_lines), 0) AS blanks,
		       COALESCE(SUM(comment_lines), 0) AS comments,
		       COALESCE(SUM(code_lines), 0) AS code,
		       COALESCE(SUM(code_complexity), 0) AS complexity
		FROM aveloxis_data.repo_labor
		WHERE repo_id = $1
		GROUP BY programming_language
		ORDER BY code DESC`, repoID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[ComplexityRow])
}

// ============================================================
// Helper: generic time-series metric query
// ============================================================

func (s *PostgresStore) timeSeriesMetric(ctx context.Context, query string, args ...any) ([]MetricRow, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[MetricRow])
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Date = out[i].Date.UTC() // a bucket is a UTC midnight instant (bucketSQL), serialized as one
	}
	return out, nil
}
