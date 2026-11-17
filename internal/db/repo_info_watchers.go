// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// watcherFixVersion is the release whose GitHub REST fallback first read
// the watcher count from subscribers_count. Before it the fallback stored
// REST's watchers_count, a legacy alias of the star count.
const watcherFixVersion = "0.29.143"

// isBeforeWatcherFix reports whether a snapshot's tool_version proves it
// was written before the fix. An empty version is a row from before
// snapshots recorded one, which is before the fix. Otherwise the version
// must be a release number, dotted digits with an optional leading v, and
// the order is CompareVersionish's (0.29.99 is before 0.29.143). Anything
// else ("dev", "0.29.143-rc1") proves nothing and the row is left alone:
// CompareVersionish orders such words too, as pre-releases, which is an
// order and not evidence of which code wrote the row.
func isBeforeWatcherFix(toolVersion string) bool {
	if toolVersion == "" {
		return true
	}
	for _, segment := range strings.Split(strings.TrimPrefix(toolVersion, "v"), ".") {
		if segment == "" {
			return false
		}
		for _, r := range segment {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return CompareVersionish(toolVersion, watcherFixVersion) < 0
}

// clearStarCountWatchersSQL clears one table's rows and, in the same
// statement, stamps the cache state of exactly the repositories it
// changed (their /watchers answer changed). %s is the table.
const clearStarCountWatchersSQL = `
	WITH cleared AS (
		UPDATE aveloxis_data.%s
		   SET watcher_count = NULL
		 WHERE data_source = $1
		   AND watcher_count IS NOT NULL
		   AND COALESCE(tool_version, '') = ANY($2)
		RETURNING repo_id),
	stamped AS (
		` + stampRepoCacheStateSQL + ` WHERE repo_id IN (SELECT repo_id FROM cleared))
	SELECT COUNT(*) FROM cleared`

// ClearStarCountWatchers sets watcher_count to NULL on every repo_info and
// repo_info_history snapshot the GitHub REST fallback wrote before 0.29.143
// (data_source = model.DataSourceGitHubREST, tool_version before the fix):
// those rows hold the repository's star count. It returns the rows cleared
// in the history and in the current snapshots.
//
// NULL, not a repaired number: the watcher count of that day cannot be
// recovered, and SnapshotSeries leaves a NULL point out. The current
// snapshots are included because the next collection rotates the current
// row into the history as it is. Rows the fallback wrote since the fix
// are correct and are not matched; neither is any other source's row.
// Idempotent: a second run matches nothing.
//
// A snapshot's tool_version is the column default, which each migrate sets
// to its own release, and a process refuses to start on a schema of
// another release: the version on a row is that of the process that
// INSERTED it. Two known gaps, both left as they are because nothing on
// the row tells them apart (review round 1):
//   - a fallback answer staged before 0.29.143 and drained from staging by
//     a later release is inserted with the later version and the star
//     count; it is not matched (a stop between a job's staging and its
//     drain, across that one upgrade);
//   - setToolVersionDefaults only warns when its ALTER fails, so a fleet
//     whose 0.29.143 migrate logged `tool_version default sweep: alter
//     failed` for repo_info kept stamping an older version, and its
//     correct fallback counts since then are cleared with the wrong ones.
func (s *PostgresStore) ClearStarCountWatchers(ctx context.Context) (history, current int64, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(tool_version, '') FROM aveloxis_data.repo_info
		 WHERE data_source = $1 AND watcher_count IS NOT NULL
		UNION
		SELECT COALESCE(tool_version, '') FROM aveloxis_data.repo_info_history
		 WHERE data_source = $1 AND watcher_count IS NOT NULL`, model.DataSourceGitHubREST)
	if err != nil {
		return 0, 0, fmt.Errorf("reading the GitHub REST fallback snapshots' versions: %w", err)
	}
	defer rows.Close()
	before := []string{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return 0, 0, fmt.Errorf("reading the GitHub REST fallback snapshots' versions: %w", err)
		}
		if isBeforeWatcherFix(v) {
			before = append(before, v)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("reading the GitHub REST fallback snapshots' versions: %w", err)
	}
	if len(before) == 0 {
		return 0, 0, nil
	}
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(clearStarCountWatchersSQL, "repo_info_history"),
		model.DataSourceGitHubREST, before).Scan(&history); err != nil {
		return 0, 0, fmt.Errorf("clearing repo_info_history: %w", err)
	}
	if err := s.pool.QueryRow(ctx, fmt.Sprintf(clearStarCountWatchersSQL, "repo_info"),
		model.DataSourceGitHubREST, before).Scan(&current); err != nil {
		return history, 0, fmt.Errorf("clearing repo_info (the history is done: %d rows): %w", history, err)
	}
	return history, current, nil
}
