// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.16 — contributor_retention (drive-by vs repeat contributors,
// an 8Knot port; operator ask 2026-07-15).
//
// ARCHITECTURE (operator-decided): computed LIVE from BASE TABLES
// scoped to the entity repo set. The explorer_contributor_actions
// matview is OFF-LIMITS — 171M rows / 51 GB on production with no
// repo-leading index; a negative tripwire pins that this file never
// references it. The action sources below mirror that matview's
// DEFINITION instead:
//
//   - commits            — one action per DISTINCT commit hash
//                          (the table is per-file), via the resolved
//                          cmt_ght_author_id identity
//   - issues opened      — reporter_id
//   - change requests    — pull_requests.author_id (opened)
//   - PR reviews         — pull_request_reviews.cntrb_id
//   - conversation text  — messages.cntrb_id joined through BOTH
//                          bridge tables (issue_message_ref +
//                          pull_request_message_ref; the unified
//                          message architecture stores all comment
//                          bodies in messages)
//
// Every branch is a straightforward repo_id-scoped scan (all sources
// are repo_id-indexed). Coordinator-measured on production: the
// 3-branch core of this query on augurlabs/augur = 325ms — the live
// /compare route is viable without any materialization.
//
// SEMANTICS: each contributor is classified by their TOTAL action
// count over ALL collected history for the repo set — drive-by when
// below the threshold, repeat when AT or ABOVE it (8Knot's ">="
// comparison). Contributors bucket by their FIRST contribution; the
// [since, until) window filters WHICH first-contribution buckets are
// displayed, never the classification itself (a contributor's class
// must not change with the chart's zoom level). Bots (gh_type='Bot'
// or a login ending in "[bot]") and v0.20.2 soft-deleted merge-loser
// identities are excluded.

import (
	"context"
	"fmt"
	"time"
)

// retentionSeriesSQL: $1 = repo ids, $2 = since, $3 = until,
// $4 = threshold, %s = bucket (validated week|month upstream — same
// defense-in-depth contract as metricSeriesSQL). Literal % characters
// are doubled for fmt.Sprintf.
//
// The commits arm reads the daily table for a repository whose picture is
// complete (0.29.140, operator decision 2026-11-05; measured on kate: the
// per-file arm alone ran past the API's 3-minute bound for nixpkgs and
// zephyr, the last series on those rows after 0.29.137 moved the
// contributors, committers and code_change_commits arms). $5 = the
// repositories read from the commits table, $6 = those read from the daily
// table; the split is on the completeness stamp alone, since the
// classification is over ALL history (no window rule applies). Two
// consequences, decided with the move: a daily action is dated the UTC
// midnight of its day, which is the same week or month bucket as the exact
// timestamp; and the author is dailyAuthorIdentitySQL's (the stored id, else
// the forge's numeric id, else the login, else the email rule — merge losers
// resolved to their winners), where the per-file arm counts the stored
// cmt_ght_author_id, so a contributor whose commits the resolver never
// stamped now counts through her alias, as in every other series.
const retentionSeriesSQL = `
WITH actions (cntrb, ts) AS (
    -- commits: ONE action per distinct commit hash (per-file table), for a
    -- repository whose daily picture is not complete.
    SELECT cmt_ght_author_id, MIN(cmt_author_timestamp)
      FROM aveloxis_data.commits
     WHERE repo_id = ANY($5) AND cmt_ght_author_id IS NOT NULL
       AND cmt_author_timestamp IS NOT NULL
     GROUP BY cmt_ght_author_id, cmt_commit_hash
    UNION ALL
    -- commits from the daily table: one action per commit, dated the UTC
    -- midnight of its day (0.29.140).
    SELECT i.cntrb_id, i.day::timestamp AT TIME ZONE 'UTC'
      FROM (SELECT ` + dailyAuthorIdentitySQL + ` AS cntrb_id, d.day, d.commits
              FROM aveloxis_data.repo_commit_daily d
             WHERE d.repo_id = ANY($6)) i,
           generate_series(1, i.commits)
     WHERE i.cntrb_id IS NOT NULL
    UNION ALL
    SELECT reporter_id, created_at
      FROM aveloxis_data.issues
     WHERE repo_id = ANY($1) AND reporter_id IS NOT NULL AND created_at IS NOT NULL
    UNION ALL
    SELECT author_id, created_at
      FROM aveloxis_data.pull_requests
     WHERE repo_id = ANY($1) AND author_id IS NOT NULL AND created_at IS NOT NULL
    UNION ALL
    SELECT cntrb_id, submitted_at
      FROM aveloxis_data.pull_request_reviews
     WHERE repo_id = ANY($1) AND cntrb_id IS NOT NULL AND submitted_at IS NOT NULL
    UNION ALL
    SELECT m.cntrb_id, m.msg_timestamp
      FROM aveloxis_data.messages m
      JOIN aveloxis_data.issue_message_ref br ON br.msg_id = m.msg_id
     WHERE m.repo_id = ANY($1) AND m.cntrb_id IS NOT NULL AND m.msg_timestamp IS NOT NULL
    UNION ALL
    SELECT m.cntrb_id, m.msg_timestamp
      FROM aveloxis_data.messages m
      JOIN aveloxis_data.pull_request_message_ref br ON br.msg_id = m.msg_id
     WHERE m.repo_id = ANY($1) AND m.cntrb_id IS NOT NULL AND m.msg_timestamp IS NOT NULL
),
per_contrib AS (
    -- Classification over ALL collected history (not the window).
    SELECT a.cntrb, COUNT(*) AS total, MIN(a.ts) AS first_ts
      FROM actions a
      JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb
     WHERE COALESCE(c.cntrb_deleted, 0) = 0
       AND COALESCE(c.gh_type, '') <> 'Bot'
       AND c.cntrb_login NOT ILIKE '%%[bot]'
       AND COALESCE(c.gh_login, '') NOT ILIKE '%%[bot]'
     GROUP BY a.cntrb
)
SELECT date_trunc('%s', first_ts AT TIME ZONE 'UTC') AS bucket,
       COUNT(*) FILTER (WHERE total <  $4)::float AS drive_by,
       COUNT(*) FILTER (WHERE total >= $4)::float AS repeat_cnt
  FROM per_contrib
 WHERE first_ts >= $2 AND first_ts < $3
 GROUP BY 1 ORDER BY 1`

// ContributorRetentionSeries returns the drive-by and repeat
// new-contributor series for a repo-id set, bucketed by the FIRST
// contribution of each contributor. threshold is the minimum total
// action count that makes a contributor "repeat" (8Knot default: 4).
func (s *PostgresStore) ContributorRetentionSeries(ctx context.Context, repoIDs []int64, bucket string, since, until time.Time, threshold int) (driveBy, repeat []WeeklyPoint, err error) {
	if bucket != "week" && bucket != "month" {
		return nil, nil, fmt.Errorf("invalid bucket %q", bucket)
	}
	if threshold < 1 {
		return nil, nil, fmt.Errorf("retention threshold must be >= 1, got %d", threshold)
	}
	daily, err := s.reposWithCompleteCommitDaily(ctx, repoIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("contributor retention: %w", err)
	}
	live := make([]int64, 0, len(repoIDs))
	isDaily := make(map[int64]bool, len(daily))
	for _, id := range daily {
		isDaily[id] = true
	}
	for _, id := range repoIDs {
		if !isDaily[id] {
			live = append(live, id)
		}
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(retentionSeriesSQL, bucket), repoIDs, since, until, threshold, live, daily)
	if err != nil {
		return nil, nil, fmt.Errorf("contributor retention: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var b time.Time
		var d, rp float64
		if err := rows.Scan(&b, &d, &rp); err != nil {
			return nil, nil, err
		}
		driveBy = append(driveBy, WeeklyPoint{Bucket: b, Value: d})
		repeat = append(repeat, WeeklyPoint{Bucket: b, Value: rp})
	}
	return driveBy, repeat, rows.Err()
}
