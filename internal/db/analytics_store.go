// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.2 — comparison-analytics queries (plan §4/§5,
// summary/api-analytics-plan-2026-07-10.md). All temporal metrics are
// bucketed server-side; entity = one repo or an org (union of its
// tracked repos, DISTINCT applied across the union for
// people-counting metrics — the queries take repo-id SETS so a
// 7-entity comparison is 7 bounded queries, cacheable upstream).

import (
	"context"
	"fmt"
	"math"
	"time"
)

// WeeklyPoint is one bucketed value in a metric time series.
type WeeklyPoint struct {
	Bucket time.Time `json:"bucket"`
	Value  float64   `json:"value"`
}

// OrgRepoCap bounds how many repos an org entity may expand to.
const OrgRepoCap = 500

// ResolveOrgRepos expands an org entity (host + owner login) into its
// tracked repo ids, capped at OrgRepoCap.
func (s *PostgresStore) ResolveOrgRepos(ctx context.Context, host, login string) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT repo_id FROM aveloxis_data.repos
		WHERE LOWER(repo_owner) = LOWER($1)
		  AND repo_git ILIKE '%://' || $2 || '/%'
		ORDER BY repo_id LIMIT $3`, login, host, OrgRepoCap)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// metricSeriesSQL maps each base temporal metric to its bucketed
// query. $1 = repo ids, $2 = since, $3 = until, %s = bucket
// (validated week|month upstream).
var metricSeriesSQL = map[string]string{
	// DISTINCT people with ANY contribution event in the bucket —
	// same source set as the v0.23.10 contributions CTE.
	"contributors": `
		SELECT date_trunc('%s', ts AT TIME ZONE 'UTC') AS bucket, COUNT(DISTINCT cntrb)::float
		FROM (
			SELECT created_at AS ts, reporter_id AS cntrb FROM aveloxis_data.issues WHERE repo_id = ANY($1) AND reporter_id IS NOT NULL
			UNION ALL
			SELECT created_at, author_id FROM aveloxis_data.pull_requests WHERE repo_id = ANY($1) AND author_id IS NOT NULL
			UNION ALL
			SELECT cmt_author_timestamp, cmt_ght_author_id FROM aveloxis_data.commits WHERE repo_id = ANY($1) AND cmt_ght_author_id IS NOT NULL
			UNION ALL
			SELECT msg_timestamp, cntrb_id FROM aveloxis_data.messages WHERE repo_id = ANY($1) AND cntrb_id IS NOT NULL
			UNION ALL
			SELECT created_at, cntrb_id FROM aveloxis_data.issue_events WHERE repo_id = ANY($1) AND cntrb_id IS NOT NULL
			UNION ALL
			SELECT created_at, cntrb_id FROM aveloxis_data.pull_request_events WHERE repo_id = ANY($1) AND cntrb_id IS NOT NULL
		) u
		WHERE ts >= $2 AND ts < $3
		GROUP BY 1 ORDER BY 1`,
	"change_requests": `
		SELECT date_trunc('%s', created_at AT TIME ZONE 'UTC'), COUNT(*)::float
		FROM aveloxis_data.pull_requests
		WHERE repo_id = ANY($1) AND created_at >= $2 AND created_at < $3
		GROUP BY 1 ORDER BY 1`,
	"change_requests_merged": `
		SELECT date_trunc('%s', merged_at AT TIME ZONE 'UTC'), COUNT(*)::float
		FROM aveloxis_data.pull_requests
		WHERE repo_id = ANY($1) AND merged_at IS NOT NULL AND merged_at >= $2 AND merged_at < $3
		GROUP BY 1 ORDER BY 1`,
	"issues": `
		SELECT date_trunc('%s', created_at AT TIME ZONE 'UTC'), COUNT(*)::float
		FROM aveloxis_data.issues
		WHERE repo_id = ANY($1) AND created_at >= $2 AND created_at < $3
		GROUP BY 1 ORDER BY 1`,
	"issues_closed": `
		SELECT date_trunc('%s', closed_at AT TIME ZONE 'UTC'), COUNT(*)::float
		FROM aveloxis_data.issues
		WHERE repo_id = ANY($1) AND closed_at IS NOT NULL AND closed_at >= $2 AND closed_at < $3
		GROUP BY 1 ORDER BY 1`,
	"code_change_commits": `
		SELECT date_trunc('%s', cmt_author_timestamp AT TIME ZONE 'UTC'), COUNT(DISTINCT cmt_commit_hash)::float
		FROM aveloxis_data.commits
		WHERE repo_id = ANY($1) AND cmt_author_timestamp >= $2 AND cmt_author_timestamp < $3
		GROUP BY 1 ORDER BY 1`,
	"committers": `
		SELECT date_trunc('%s', cmt_author_timestamp AT TIME ZONE 'UTC'), COUNT(DISTINCT cmt_ght_author_id)::float
		FROM aveloxis_data.commits
		WHERE repo_id = ANY($1) AND cmt_ght_author_id IS NOT NULL AND cmt_author_timestamp >= $2 AND cmt_author_timestamp < $3
		GROUP BY 1 ORDER BY 1`,
}

// MetricWeeklySeries returns the bucketed series for one base
// temporal metric over a repo-id set. bucket ∈ week|month (validated
// here as defense in depth — it is interpolated into SQL).
func (s *PostgresStore) MetricWeeklySeries(ctx context.Context, repoIDs []int64, metric, bucket string, since, until time.Time) ([]WeeklyPoint, error) {
	if bucket != "week" && bucket != "month" {
		return nil, fmt.Errorf("invalid bucket %q", bucket)
	}
	q, ok := metricSeriesSQL[metric]
	if !ok {
		return nil, fmt.Errorf("unknown temporal metric %q", metric)
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(q, bucket), repoIDs, since, until)
	if err != nil {
		return nil, fmt.Errorf("metric %s: %w", metric, err)
	}
	defer rows.Close()
	var out []WeeklyPoint
	for rows.Next() {
		var p WeeklyPoint
		if err := rows.Scan(&p.Bucket, &p.Value); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SnapshotValue is one snapshot-metric result for an entity.
type SnapshotValue struct {
	Value  float64        `json:"value"`
	AsOf   *time.Time     `json:"as_of,omitempty"`
	Detail map[string]any `json:"detail,omitempty"`
}

// LaborInvestmentSnapshot: COCOMO-II basic over the latest SCC scan.
// person-months = 2.94 · KLOC^1.0997 (documented in
// docs/guide/metrics.md with the $-conversion left to the caller).
func (s *PostgresStore) LaborInvestmentSnapshot(ctx context.Context, repoIDs []int64) (SnapshotValue, error) {
	var kloc float64
	var asOf *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(rl.code_lines), 0) / 1000.0, MAX(rl.rl_analysis_date)
		FROM aveloxis_data.repo_labor rl
		JOIN (SELECT repo_id, MAX(rl_analysis_date) AS d
		      FROM aveloxis_data.repo_labor WHERE repo_id = ANY($1) GROUP BY repo_id) latest
		  ON latest.repo_id = rl.repo_id AND latest.d = rl.rl_analysis_date`, repoIDs).Scan(&kloc, &asOf)
	if err != nil {
		return SnapshotValue{}, err
	}
	pm := 0.0
	if kloc > 0 {
		pm = 2.94 * math.Pow(kloc, 1.0997)
	}
	return SnapshotValue{Value: pm, AsOf: asOf, Detail: map[string]any{
		"kloc": kloc, "model": "COCOMO-II basic: 2.94*KLOC^1.0997 person-months",
	}}, nil
}

// UpstreamDependenciesSnapshot: direct-dependency count + median
// libyear staleness from the latest libyear pass.
func (s *PostgresStore) UpstreamDependenciesSnapshot(ctx context.Context, repoIDs []int64) (SnapshotValue, error) {
	var count float64
	var median *float64
	var asOf *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)::float,
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY libyear),
		       MAX(data_collection_date)
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = ANY($1) AND libyear IS NOT NULL`, repoIDs).Scan(&count, &median, &asOf)
	if err != nil {
		return SnapshotValue{}, err
	}
	d := map[string]any{}
	if median != nil {
		d["median_libyear"] = *median
	}
	return SnapshotValue{Value: count, AsOf: asOf, Detail: d}, nil
}

// LicenseCoverageSnapshot: % of scanned source files carrying a
// detected SPDX license + distinct license count (scancode).
func (s *PostgresStore) LicenseCoverageSnapshot(ctx context.Context, repoIDs []int64) (SnapshotValue, error) {
	var total, licensed, distinct float64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)::float,
		       COUNT(*) FILTER (WHERE COALESCE(detected_license_expression_spdx,'') <> '')::float,
		       COUNT(DISTINCT NULLIF(detected_license_expression_spdx,''))::float
		FROM aveloxis_scan.scancode_file_results
		WHERE repo_id = ANY($1)`, repoIDs).Scan(&total, &licensed, &distinct)
	if err != nil {
		return SnapshotValue{}, err
	}
	pct := 0.0
	if total > 0 {
		pct = 100 * licensed / total
	}
	return SnapshotValue{Value: pct, Detail: map[string]any{
		"files_scanned": total, "files_with_license": licensed, "distinct_spdx": distinct,
	}}, nil
}

// OrgSearchResult is one owner-grouping for the entity picker.
type OrgSearchResult struct {
	Host      string `json:"host"`
	Login     string `json:"login"`
	RepoCount int    `json:"repo_count"`
}

// SearchOrgs groups tracked repos by owner for the compare picker.
func (s *PostgresStore) SearchOrgs(ctx context.Context, query string, limit int) ([]OrgSearchResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT LOWER(repo_owner),
		       COALESCE(split_part(split_part(MIN(repo_git), '://', 2), '/', 1), ''),
		       COUNT(*)
		FROM aveloxis_data.repos
		WHERE repo_owner ILIKE '%' || $1 || '%'
		GROUP BY 1 ORDER BY 3 DESC LIMIT $2`, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrgSearchResult
	for rows.Next() {
		var o OrgSearchResult
		if err := rows.Scan(&o.Login, &o.Host, &o.RepoCount); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
