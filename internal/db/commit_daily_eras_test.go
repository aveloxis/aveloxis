// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
	"time"
)

// Two histories on one row (0.29.140, operator decision 2026-11-05 on
// microsoft/bobsql): a repository deleted and re-created upstream, adopted as
// a continuation, has an EARLIER era — the commits of the repository that
// stood at the URL before the forge's creation date of the new one. The
// facade's trimming walk sees only the new history, so trimming to it dropped
// 548 of bobsql's 567 commits from the daily table (the charts) while the
// commits table kept them (the gathered count). The trim now keeps every
// daily row older than the newest adopted change's boundary day: the earlier
// era is history the walk can never see again, not a row the walk missed.
func TestReplaceRepoCommitDailyKeepsAnEarlierEraOnAnAdoptedChange(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era01")
	plain := seedCommitDaily(t, store, ctx, "_avcd_era02")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	// The new upstream was created on fx.in at 16:38 UTC: its boundary day
	// is fx.in; a row dated the day before belongs to the earlier era.
	created := fx.in.Add(16*time.Hour + 38*time.Minute)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", ""); err != nil {
		t.Fatal(err)
	}
	old, boundary, next := fx.in.AddDate(0, 0, -1), fx.in, fx.in.AddDate(0, 0, 1)
	for _, id := range []int64{fx.repoID, plain.repoID} {
		if err := store.ReplaceRepoCommitDaily(ctx, id, []CommitDailyRow{
			{Day: old, AuthorEmail: "old@x", Commits: 3},
			{Day: boundary, AuthorEmail: "cur@x", Commits: 1},
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	// The next walk (the re-created repository's history) sees only a
	// commit on the day after the boundary.
	for _, id := range []int64{fx.repoID, plain.repoID} {
		if err := store.ReplaceRepoCommitDaily(ctx, id, []CommitDailyRow{
			{Day: next, AuthorEmail: "cur@x", Commits: 2},
		}, true); err != nil {
			t.Fatal(err)
		}
	}
	days := func(id int64) map[string]int {
		t.Helper()
		rows, err := store.pool.Query(ctx, `SELECT day::text, commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, id)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]int{}
		for rows.Next() {
			var d string
			var n int
			if err := rows.Scan(&d, &n); err != nil {
				t.Fatal(err)
			}
			out[d] = n
		}
		return out
	}
	got := days(fx.repoID)
	if got[old.Format("2006-01-02")] != 3 {
		t.Errorf("the earlier era's row (the day before the boundary) is kept: %v", got)
	}
	if _, present := got[boundary.Format("2006-01-02")]; present {
		t.Errorf("a current-era row the walk did not see is trimmed as before: %v", got)
	}
	if got[next.Format("2006-01-02")] != 2 {
		t.Errorf("the walk's own row is written: %v", got)
	}
	if ok, err := store.RepoCommitDailyComplete(ctx, fx.repoID); err != nil || !ok {
		t.Errorf("the trimming walk still stamps the picture complete: %v %v", ok, err)
	}
	if got := days(plain.repoID); len(got) != 1 || got[next.Format("2006-01-02")] != 2 {
		t.Errorf("without an adopted change the trim is whole, as before: %v", got)
	}
}

// The contributor_retention series reads the daily table for a repository
// whose picture is complete (operator decision 2026-11-05: the last series on
// the per-file rows; on kate the commits arm alone ran past the 3-minute
// bound for nixpkgs and zephyr). The fixture's commits table says ONE commit
// by alice (a drive-by under threshold 4); five daily commits make her a
// repeat contributor — an answer only the daily table gives.
func TestContributorRetentionSeriesReadsTheDailyTableWhenComplete(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_ret01")
	other := seedCommitDaily(t, store, ctx, "_avcd_ret02") // stays on the commits table
	since, until := utcDay(time.Now()).AddDate(0, 0, -60), utcDay(time.Now()).AddDate(0, 0, 1)
	sum := func(ids []int64) (driveBy, repeat float64) {
		t.Helper()
		d, r, err := store.ContributorRetentionSeries(ctx, ids, "week", since, until, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range d {
			driveBy += p.Value
		}
		for _, p := range r {
			repeat += p.Value
		}
		return driveBy, repeat
	}
	if d, r := sum([]int64{fx.repoID}); d != 1 || r != 0 {
		t.Fatalf("unfilled: the commits table says one drive-by, got drive_by=%v repeat=%v", d, r)
	}
	mon := fx.in.AddDate(0, 0, -int((fx.in.Weekday()+6)%7))
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: mon, AuthorEmail: "_avcd_ret01@x", Commits: 5}, // alice, through the alias
	}, true); err != nil {
		t.Fatal(err)
	}
	if d, r := sum([]int64{fx.repoID}); d != 0 || r != 1 {
		t.Fatalf("complete: five daily commits make alice a repeat contributor, got drive_by=%v repeat=%v", d, r)
	}
	// A set mixes the arms per repository: the other fixture's alice is
	// still the commits table's one drive-by.
	if d, r := sum([]int64{fx.repoID, other.repoID}); d != 1 || r != 1 {
		t.Fatalf("a set reads each repository from its own table, got drive_by=%v repeat=%v", d, r)
	}
}
