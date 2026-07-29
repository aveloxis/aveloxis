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
		  -- v0.27.37 (Phase 1f): the catalog definition promises
		  -- soft-deleted merge-loser identities are excluded; the SQL
		  -- now actually does it (child FKs still point at loser
		  -- cntrb_ids, so without this a merged person counted twice).
		  AND NOT EXISTS (
			SELECT 1 FROM aveloxis_data.contributors c
			WHERE c.cntrb_id = u.cntrb AND COALESCE(c.cntrb_deleted, 0) <> 0)
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
		FROM aveloxis_data.commits co
		WHERE repo_id = ANY($1) AND cmt_ght_author_id IS NOT NULL AND cmt_author_timestamp >= $2 AND cmt_author_timestamp < $3
		  -- v0.27.37 (Phase 1f): exclude soft-deleted merge losers,
		  -- matching the contributors metric and the catalog text.
		  AND NOT EXISTS (
			SELECT 1 FROM aveloxis_data.contributors c
			WHERE c.cntrb_id = co.cmt_ght_author_id AND COALESCE(c.cntrb_deleted, 0) <> 0)
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
	// v0.27.46 (summary/19 P3, decision #4 — regulatory alignment
	// over 8Knot compatibility): the HEADLINE count + median cover
	// runtime-scope deps only; dev/test/build/optional/peer split out
	// into detail companions. Staleness of pinned dev tooling is a
	// weaker signal than staleness of shipped deps, and the P2 Python
	// expansion would otherwise discontinuously jump every Python
	// repo's headline the week the knob flips on. DOCUMENTED SEMANTIC
	// BUMP: pre-v0.27.46 the headline counted all deps.
	var runtimeCount, devCount float64
	var runtimeMedian, devMedian *float64
	var asOf *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE COALESCE(type, '') NOT IN ('dev','test','build','optional','peer'))::float,
		       COUNT(*) FILTER (WHERE COALESCE(type, '') IN ('dev','test','build','optional','peer'))::float,
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY libyear)
		           FILTER (WHERE COALESCE(type, '') NOT IN ('dev','test','build','optional','peer')),
		       percentile_cont(0.5) WITHIN GROUP (ORDER BY libyear)
		           FILTER (WHERE COALESCE(type, '') IN ('dev','test','build','optional','peer')),
		       MAX(data_collection_date)
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = ANY($1) AND libyear IS NOT NULL`, repoIDs).
		Scan(&runtimeCount, &devCount, &runtimeMedian, &devMedian, &asOf)
	if err != nil {
		return SnapshotValue{}, err
	}
	d := map[string]any{
		"total_count": runtimeCount + devCount,
		"dev_count":   devCount,
	}
	if runtimeMedian != nil {
		d["median_libyear"] = *runtimeMedian
	}
	if devMedian != nil {
		d["dev_median_libyear"] = *devMedian
	}
	return SnapshotValue{Value: runtimeCount, AsOf: asOf, Detail: d}, nil
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

// FirstActivityAt returns the earliest known activity across a repo
// set (v0.27.24): the LEAST of first issue, first PR, first commit
// (author timestamp), and the forge's repo creation date. It is the
// per-entity floor the compare endpoint clamps chart windows to, so a
// young repo's series starts when its data starts instead of at the
// requested window's beginning.
//
// ok=false means the set has no dateable activity at all (nothing
// collected yet) — callers fall back to the unclamped window.
//
// Fail-safe by construction: a repo with imported ancient history (or
// a bogus 1970 git timestamp) yields a floor BEFORE the window start,
// making the clamp a no-op — never hidden data inside the window.
// Postgres LEAST ignores NULL operands.
//
// Cost note: the issues/PR MINs ride idx_issues_repo_created /
// idx_pull_requests_repo_created; the commits MIN has no serving
// index and scans the repo's per-file rows. The API layer memoizes
// per entity for the process lifetime (first activity is immutable —
// history does not grow backward), so the cost is paid once per
// entity per process. Deliberately NO index was added for this: a
// once-per-process aggregate does not justify permanent write
// amplification on the fleet's largest table.
// LastActivityAt returns the most recent observed activity across the
// repo set — the MAX of last issue, last PR, and last commit (author
// timestamp). Unlike FirstActivityAt it deliberately omits
// repos.created_at (creation is not activity) and is NOT cached: last
// activity advances as a repo collects, so a process-lifetime cache
// would go stale (the v0.27.24 first-activity cache is safe only
// because first activity is immutable). ok=false when the repo set
// has no activity yet. (v0.27.50: the chart "last-active ceiling" for
// archived/dormant repos, mirror of the first-activity floor.)
func (s *PostgresStore) LastActivityAt(ctx context.Context, repoIDs []int64) (time.Time, bool, error) {
	if len(repoIDs) == 0 {
		return time.Time{}, false, nil
	}
	var la *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT GREATEST(
			(SELECT MAX(created_at) FROM aveloxis_data.issues WHERE repo_id = ANY($1)),
			(SELECT MAX(created_at) FROM aveloxis_data.pull_requests WHERE repo_id = ANY($1)),
			(SELECT MAX(cmt_author_timestamp) FROM aveloxis_data.commits WHERE repo_id = ANY($1))
		)`, repoIDs).Scan(&la)
	if err != nil {
		return time.Time{}, false, err
	}
	if la == nil {
		return time.Time{}, false, nil
	}
	return la.UTC(), true, nil
}

func (s *PostgresStore) FirstActivityAt(ctx context.Context, repoIDs []int64) (time.Time, bool, error) {
	if len(repoIDs) == 0 {
		return time.Time{}, false, nil
	}
	var fa *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT LEAST(
			(SELECT MIN(created_at) FROM aveloxis_data.issues WHERE repo_id = ANY($1)),
			(SELECT MIN(created_at) FROM aveloxis_data.pull_requests WHERE repo_id = ANY($1)),
			(SELECT MIN(cmt_author_timestamp) FROM aveloxis_data.commits WHERE repo_id = ANY($1)),
			(SELECT MIN(created_at) FROM aveloxis_data.repos WHERE repo_id = ANY($1))
		)`, repoIDs).Scan(&fa)
	if err != nil {
		return time.Time{}, false, err
	}
	if fa == nil {
		return time.Time{}, false, nil
	}
	return fa.UTC(), true, nil
}
