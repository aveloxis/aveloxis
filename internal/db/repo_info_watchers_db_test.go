// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Before 0.29.143 the GitHub REST fallback stored the star count as
// watcher_count. Those values are known wrong and exactly identifiable (the
// fallback's data_source, a tool_version below the fix), so the migrate
// clears them once: the count becomes unknown (NULL). Nothing else moves —
// not a fallback row written since the fix, not another source's row, not
// another column.
func TestClearStarCountWatchers(t *testing.T) {
	store, ctx := openSR5Store(t)
	suffix := time.Now().UnixNano()
	owner := fmt.Sprintf("avx-watch-%d", suffix)
	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, 'w', 1) RETURNING repo_id`, "https://github.com/"+owner+"/w", owner).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_info_history WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, repoID)
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
	})
	null := sql.NullString{}
	ver := func(v string) sql.NullString { return sql.NullString{String: v, Valid: true} }
	type snap struct {
		name       string
		table      string
		dataSource string
		version    sql.NullString
		cleared    bool
	}
	snaps := []snap{
		{"a fallback snapshot from before the fix", "repo_info_history", model.DataSourceGitHubREST, ver("0.29.142"), true},
		// 0.29.99 < 0.29.143 as numbers and > as text: the order is the version's.
		{"a fallback snapshot whose version sorts after the fix as text", "repo_info_history", model.DataSourceGitHubREST, ver("0.29.99"), true},
		{"a fallback snapshot from an earlier minor line", "repo_info_history", model.DataSourceGitHubREST, ver("0.27.103"), true},
		{"a fallback snapshot with a v-prefixed version", "repo_info_history", model.DataSourceGitHubREST, ver("v0.29.142"), true},
		{"a fallback snapshot from before rows recorded a version (NULL)", "repo_info_history", model.DataSourceGitHubREST, null, true},
		{"a fallback snapshot from before rows recorded a version (empty)", "repo_info_history", model.DataSourceGitHubREST, ver(""), true},
		{"the fix's own release: the boundary is kept", "repo_info_history", model.DataSourceGitHubREST, ver("0.29.143"), false},
		{"a fallback snapshot written since the fix", "repo_info_history", model.DataSourceGitHubREST, ver("0.29.145"), false},
		{"a fallback snapshot from the next minor line", "repo_info_history", model.DataSourceGitHubREST, ver("0.30.0"), false},
		{"a version that is not a release number proves nothing", "repo_info_history", model.DataSourceGitHubREST, ver("dev"), false},
		{"a GraphQL snapshot from before the fix", "repo_info_history", "GitHub API", ver("0.29.142"), false},
		{"a snapshot with no source", "repo_info_history", "", ver("0.29.142"), false},
		{"another spelling of the source is another source", "repo_info_history", "github rest", ver("0.29.142"), false},
		// The current snapshot too: its next rotation would carry the star
		// count into the history after the step had run.
		{"the current snapshot, written by the fallback before the fix", "repo_info", model.DataSourceGitHubREST, ver("0.29.142"), true},
	}
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ids := make([]int64, len(snaps))
	for i, s := range snaps {
		if err := store.pool.QueryRow(ctx, fmt.Sprintf(`
			INSERT INTO aveloxis_data.%s (repo_id, data_collection_date, star_count, fork_count, watcher_count, data_source, tool_version)
			VALUES ($1, $2, 5000, 70, 5000, $3, $4) RETURNING repo_info_id`, s.table),
			repoID, base.AddDate(0, 0, i), s.dataSource, s.version).Scan(&ids[i]); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, repoID)
	var wantHistory, wantCurrent int64
	for _, s := range snaps {
		switch {
		case s.cleared && s.table == "repo_info":
			wantCurrent++
		case s.cleared:
			wantHistory++
		}
	}

	history, current, err := store.ClearStarCountWatchers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The package's database is shared by its tests: another test's rows
	// can only add to the counts.
	if history < wantHistory || current < wantCurrent {
		t.Errorf("cleared %d history and %d current snapshots, want at least %d and %d", history, current, wantHistory, wantCurrent)
	}
	for i, s := range snaps {
		var watchers, stars, forks sql.NullInt64
		if err := store.pool.QueryRow(ctx, fmt.Sprintf(`
			SELECT watcher_count, star_count, fork_count FROM aveloxis_data.%s WHERE repo_info_id = $1`, s.table), ids[i]).
			Scan(&watchers, &stars, &forks); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if s.cleared && watchers.Valid {
			t.Errorf("%s: watcher_count = %d, want NULL (the star count the fallback stored)", s.name, watchers.Int64)
		}
		if !s.cleared && (!watchers.Valid || watchers.Int64 != 5000) {
			t.Errorf("%s: watcher_count = %v, want it left at 5000", s.name, watchers)
		}
		if stars.Int64 != 5000 || forks.Int64 != 70 {
			t.Errorf("%s: star_count, fork_count = %d, %d; want 5000, 70 (only the watcher count is cleared)", s.name, stars.Int64, forks.Int64)
		}
	}
	// The /watchers answer of the repository changed: its cache state moves.
	var stamped bool
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if !stamped {
		t.Error("the repository whose snapshots were cleared was not stamped (data_changed_at): its cached /watchers answer stays valid")
	}

	// The series leaves the cleared points out: drawn as 0 they would be a
	// second false value. Dates of the kept points, oldest first.
	rows, err := store.SnapshotSeries(ctx, repoID, "watcher_count")
	if err != nil {
		t.Fatal(err)
	}
	var wantDates []time.Time
	for i, s := range snaps {
		if !s.cleared {
			wantDates = append(wantDates, base.AddDate(0, 0, i))
		}
	}
	if len(rows) != len(wantDates) {
		t.Fatalf("the watcher series has %d points, want the %d snapshots that kept a count: %+v", len(rows), len(wantDates), rows)
	}
	for i, r := range rows {
		if !r.Date.Equal(wantDates[i]) || r.Value != 5000 {
			t.Errorf("point %d = %s %d, want %s 5000", i, r.Date, r.Value, wantDates[i])
		}
	}
	// The star series still has every snapshot.
	if stars, err := store.SnapshotSeries(ctx, repoID, "star_count"); err != nil || len(stars) != len(snaps) {
		t.Errorf("the star series has %d points (%v), want all %d", len(stars), err, len(snaps))
	}

	// A second run finds nothing of this repository's to clear and leaves
	// its stamp alone.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, repoID)
	if _, _, err := store.ClearStarCountWatchers(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT data_changed_at IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped {
		t.Error("a second run stamped a repository it changed nothing of")
	}
}

// A failed read is the step's failure, so the ledger does not record it.
func TestClearStarCountWatchersReturnsAFailedRead(t *testing.T) {
	store, ctx := openSR5Store(t)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := store.ClearStarCountWatchers(canceled); err == nil {
		t.Error("ClearStarCountWatchers on a canceled context reported success")
	}
}

func TestIsBeforeWatcherFix(t *testing.T) {
	for v, want := range map[string]bool{
		"": true, "0.29.142": true, "0.29.99": true, "0.1.0": true, "v0.29.142": true, "0.29": true,
		"0.29.143": false, "0.29.144": false, "0.30.0": false, "1.0.0": false, "dev": false, "unknown": false,
		// Not release numbers: no evidence of which code wrote the row,
		// whatever order a version comparison gives them.
		"0.29.142-rc1": false, "0.29.dev": false, "0.29.": false, ".": false, "v": false, " 0.29.142": false, "0.29.１４２": false,
	} {
		if got := isBeforeWatcherFix(v); got != want {
			t.Errorf("isBeforeWatcherFix(%q) = %v, want %v", v, got, want)
		}
	}
}

// The migrate runs the step under its ledger label and a failure reaches
// the migrate's errors (so the ledger does not record a step that failed).
func TestMigrateRunsTheWatcherClearOnce(t *testing.T) {
	src := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func migrateStage10RecentReleases("))
	const label = `"v0.29.145 clear the star counts the GitHub REST fallback stored as watcher_count"`
	i := strings.Index(src, label)
	if i < 0 {
		t.Fatal("migrateStage10RecentReleases no longer names the step's ledger label")
	}
	// The step's closure: from the label to the next ledgered or plain step.
	body := src[i:]
	if end := strings.Index(body[len(label):], "runOnce"); end >= 0 {
		body = body[:len(label)+end]
	}
	call, fail, ret := strings.Index(body, "pg.ClearStarCountWatchers(ctx)"), strings.Index(body, "*errs = append(*errs,"), strings.Index(body, "return")
	if call < 0 || fail < call || ret < fail {
		t.Errorf("the step calls ClearStarCountWatchers, appends its error to the step's errors and returns (offsets %d, %d, %d)", call, fail, ret)
	}
	if strings.Count(src, "ClearStarCountWatchers(") != 1 {
		t.Error("ClearStarCountWatchers is called once in the stage, by the ledgered step")
	}
}
