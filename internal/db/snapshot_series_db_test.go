// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Stage 1b item 7: one snapshot series for stars, forks and watchers over
// the current snapshot and its history; a NULL-dated row is skipped (before
// 1b a NULL failed the scan, a 500) and so is a NULL count (0.29.145: an
// unknown count was drawn as 0 until then).
func TestSnapshotSeriesReadsHistoryAndTolerateNulls(t *testing.T) {
	store, ctx := openSR5Store(t)
	suffix := time.Now().UnixNano()
	owner := fmt.Sprintf("avx-snap-%d", suffix)
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, 's', 1) RETURNING repo_id`, "https://github.com/"+owner+"/s", owner).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_info_history WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	d1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.AddDate(0, 1, 0)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info (repo_id, data_collection_date, star_count, fork_count, watcher_count) VALUES ($1, $2, 30, NULL, 3)`, repoID, d2)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info_history (repo_id, data_collection_date, star_count, fork_count, watcher_count) VALUES ($1, $2, 10, 1, 1)`, repoID, d1)
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_info_history (repo_id, data_collection_date, star_count, fork_count, watcher_count) VALUES ($1, NULL, 99, 99, 99)`, repoID)
	for column, want := range map[string][]int{"star_count": {10, 30}, "watcher_count": {1, 3}} {
		rows, err := store.SnapshotSeries(ctx, repoID, column)
		if err != nil {
			t.Fatalf("%s: %v", column, err)
		}
		if len(rows) != 2 || rows[0].Value != want[0] || rows[1].Value != want[1] || !rows[0].Date.Equal(d1) || !rows[1].Date.Equal(d2) {
			t.Errorf("%s = %+v; want the dated history then the current snapshot %v (the NULL-dated row skipped)", column, rows, want)
		}
	}
	// The current snapshot's fork count is NULL: unknown, so no point.
	forks, err := store.SnapshotSeries(ctx, repoID, "fork_count")
	if err != nil {
		t.Fatalf("fork_count: %v", err)
	}
	if len(forks) != 1 || forks[0].Value != 1 || !forks[0].Date.Equal(d1) {
		t.Errorf("fork_count = %+v; want the one snapshot with a count (a NULL count is unknown, not 0)", forks)
	}
	if _, err := store.SnapshotSeries(ctx, repoID, "repo_id"); err == nil {
		t.Error("a column outside the allowlist must be refused")
	}
	if rows, err := store.SnapshotSeries(ctx, repoID+1000000, "star_count"); err != nil || rows == nil || len(rows) != 0 {
		t.Errorf("no snapshots = %#v, %v; want an empty, non-nil list", rows, err)
	}
	if n, err := store.ClosedIssueCount(ctx, repoID); err != nil || n != 0 {
		t.Errorf("ClosedIssueCount of a repository without issues = %d, %v; want 0", n, err)
	}
}
