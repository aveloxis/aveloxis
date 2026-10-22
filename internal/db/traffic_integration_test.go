// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The traffic writers against a real Postgres: the latest read of a
// day, window or week wins, a day's path and referrer snapshot is
// replaced as a whole, and every write moves repos.data_changed_at,
// which the repository page's cached answers are checked against. The
// arbiters, the delete-then-insert order and the stamp are SQL
// semantics, so Postgres is the ground truth here (testing.md, "Where
// expected values come from").

// trafficTables are the tables a traffic read writes: the daily clones
// FetchCloneStats returns and the five tables of the rest of the read.
var trafficTables = []string{
	"repo_clones", "repo_views", "repo_traffic_paths",
	"repo_traffic_referrers", "repo_traffic_windows", "repo_traffic_weeks",
}

// trafficTestRepo seeds one GitHub repository for a traffic test and
// removes it and its traffic rows before and after the test.
func trafficTestRepo(ctx context.Context, t *testing.T, store *PostgresStore, slug string) int64 {
	t.Helper()
	like := "%" + slug + "%"
	cleanup := func() {
		bg := context.Background() // never t.Context() in a cleanup (TEST-4)
		for _, table := range trafficTables {
			cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.`+table+` WHERE repo_id IN
				(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`, like)
		}
		cleanupExecRetry(bg, store, `DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`, like)
	}
	cleanup()
	t.Cleanup(cleanup)
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "/Repo",
		Owner:    slug,
		Name:     "Repo",
	})
	if err != nil {
		t.Fatalf("seed repository: %v", err)
	}
	return repoID
}

// trafficRows returns the rows a query yields, one string per row. The
// query takes the repository id as $1 and selects one text column.
func trafficRows(ctx context.Context, t *testing.T, store *PostgresStore, query string, repoID int64) []string {
	t.Helper()
	rows, err := store.pool.Query(ctx, query, repoID)
	if err != nil {
		t.Fatalf("query traffic rows: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan traffic row: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate traffic rows: %v", err)
	}
	return out
}

func assertTrafficStored(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("%s:\n got  %q\n want %q", what, got, want)
	}
}

// ageTrafficRows moves every traffic row of the repository back to an
// old collection date and an old tool version, so a later write that
// refreshes them is visible.
func ageTrafficRows(ctx context.Context, t *testing.T, store *PostgresStore, repoID int64) {
	t.Helper()
	for _, table := range trafficTables {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.`+table+`
			SET data_collection_date = '2000-01-01T00:00:00Z', tool_version = 'aged'
			WHERE repo_id = $1`, repoID)
	}
}

// The provenance a refreshing write leaves on a row: the collection
// date of this write and the running binary's version.
const refreshedProvenance = `CASE WHEN data_collection_date > '2000-01-02T00:00:00Z' THEN 'fresh' ELSE 'aged' END
	|| ' ' || tool_version`

// octDay is midnight UTC of a day in October 2026.
func octDay(day int) time.Time {
	return time.Date(2026, time.October, day, 0, 0, 0, 0, time.UTC)
}

// TestTrafficLatestDailyReadWins: GitHub can still revise the newest
// day of a read, so a day read again ends with the figures of the
// later read, in one row. The scenario: a collection at 01:00 UTC on
// Monday 2026-10-19 reads 5 for Sunday 2026-10-18, and the collection
// on Tuesday reads 40 for that Sunday. Views go through
// UpsertRepoTraffic and clones through UpsertRepoClone, as the staged
// processor writes them; a day only the first read returned keeps its
// row.
func TestTrafficLatestDailyReadWins(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_daily")

	monday := &model.RepoTraffic{
		SnapshotDate: octDay(19),
		Views: []model.RepoView{
			{Timestamp: octDay(17), TotalViews: 3, UniqueViews: 1},
			{Timestamp: octDay(18), TotalViews: 5, UniqueViews: 2},
		},
	}
	mondayClones := []model.RepoClone{
		{RepoID: repoID, Timestamp: octDay(17), TotalClones: 2, UniqueClones: intPtr(1)},
		{RepoID: repoID, Timestamp: octDay(18), TotalClones: 5, UniqueClones: intPtr(3)},
	}
	tuesday := &model.RepoTraffic{
		SnapshotDate: octDay(20),
		Views: []model.RepoView{
			{Timestamp: octDay(18), TotalViews: 40, UniqueViews: 9},
			{Timestamp: octDay(19), TotalViews: 0, UniqueViews: 0},
		},
	}
	tuesdayClones := []model.RepoClone{
		{RepoID: repoID, Timestamp: octDay(18), TotalClones: 40, UniqueClones: intPtr(7)},
		{RepoID: repoID, Timestamp: octDay(19), TotalClones: 0, UniqueClones: intPtr(0)},
	}

	write := func(tr *model.RepoTraffic, clones []model.RepoClone) {
		t.Helper()
		if err := store.UpsertRepoTraffic(ctx, repoID, tr); err != nil {
			t.Fatalf("UpsertRepoTraffic: %v", err)
		}
		for i := range clones {
			if err := store.UpsertRepoClone(ctx, &clones[i]); err != nil {
				t.Fatalf("UpsertRepoClone: %v", err)
			}
		}
	}
	write(monday, mondayClones)
	ageTrafficRows(ctx, t, store, repoID)
	write(tuesday, tuesdayClones)

	fresh := "fresh " + ToolVersion
	assertTrafficStored(t, "daily views after the Monday and Tuesday reads",
		trafficRows(ctx, t, store, `
			SELECT to_char(view_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_views || '/' || unique_views
				|| ' ' || `+refreshedProvenance+`
			FROM aveloxis_data.repo_views WHERE repo_id = $1 ORDER BY view_timestamp`, repoID),
		[]string{"2026-10-17 3/1 aged aged", "2026-10-18 40/9 " + fresh, "2026-10-19 0/0 " + fresh})
	assertTrafficStored(t, "daily clones after the Monday and Tuesday reads",
		trafficRows(ctx, t, store, `
			SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_clones || '/' || unique_clones
				|| ' ' || `+refreshedProvenance+`
			FROM aveloxis_data.repo_clones WHERE repo_id = $1 ORDER BY clone_timestamp`, repoID),
		[]string{"2026-10-17 2/1 aged aged", "2026-10-18 40/7 " + fresh, "2026-10-19 0/0 " + fresh})
}

// TestTrafficCloneWithoutUniquesIsStoredAsNull: a forge that counts
// clones but not who made them (GitLab's project statistics) gives a
// day with no distinct count, which must be stored as NULL and never as
// 0, because 0 would claim that nobody cloned. A day with a count of 0
// stays 0. The latest read still wins in both directions: a read
// without uniques replaces a stored number with NULL, and a read with
// them fills a NULL, each keeping its own total, data source and
// provenance.
func TestTrafficCloneWithoutUniquesIsStoredAsNull(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_nulluniques")

	const (
		statsSource  = "/projects/:id/statistics"
		githubSource = "/repos/{o}/{r}/traffic/clones"
	)
	clone := func(day, total int, uniques *int, source string) *model.RepoClone {
		return &model.RepoClone{
			RepoID: repoID, Timestamp: octDay(day), TotalClones: total, UniqueClones: uniques,
			Origin: model.DataOrigin{DataSource: source},
		}
	}
	write := func(clones ...*model.RepoClone) {
		t.Helper()
		for _, c := range clones {
			if err := store.UpsertRepoClone(ctx, c); err != nil {
				t.Fatalf("UpsertRepoClone: %v", err)
			}
		}
	}
	stored := func(what string, want []string) {
		t.Helper()
		assertTrafficStored(t, what, trafficRows(ctx, t, store, `
			SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_clones
				|| '/' || COALESCE(unique_clones::text, 'NULL') || ' ' || data_source
				|| ' ' || `+refreshedProvenance+`
			FROM aveloxis_data.repo_clones WHERE repo_id = $1 ORDER BY clone_timestamp`, repoID), want)
	}

	// 10-16 had no uniques and no fetch; 10-17 had fetches; 10-18 is a
	// GitHub day with 0 distinct cloners (a number, not a missing one);
	// 10-19 is read with a number first and without one after.
	write(
		clone(16, 0, nil, statsSource),
		clone(17, 7, nil, statsSource),
		clone(18, 2, intPtr(0), githubSource),
		clone(19, 4, intPtr(2), statsSource),
	)
	stored("first read", []string{
		"2026-10-16 0/NULL " + statsSource + " fresh " + ToolVersion,
		"2026-10-17 7/NULL " + statsSource + " fresh " + ToolVersion,
		"2026-10-18 2/0 " + githubSource + " fresh " + ToolVersion,
		"2026-10-19 4/2 " + statsSource + " fresh " + ToolVersion,
	})

	ageTrafficRows(ctx, t, store, repoID)
	write(
		clone(17, 12, nil, statsSource),      // the counted day grew
		clone(19, 4, nil, statsSource),       // a number replaced by NULL
		clone(16, 3, intPtr(1), statsSource), // NULL replaced by a number
	)
	fresh := " " + statsSource + " fresh " + ToolVersion
	stored("second read", []string{
		"2026-10-16 3/1" + fresh,
		"2026-10-17 12/NULL" + fresh,
		"2026-10-18 2/0 " + githubSource + " aged aged",
		"2026-10-19 4/NULL" + fresh,
	})
}

// TestTrafficLatestWindowReadWins: a window read twice (two
// collections on the same UTC day see the same 14 daily buckets) keeps
// one row per kind with the later uniques; another window is its own
// row.
func TestTrafficLatestWindowReadWins(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_window")

	read := func(start, end time.Time, views, clones int) *model.RepoTraffic {
		return &model.RepoTraffic{
			SnapshotDate: octDay(19),
			Windows: []model.RepoTrafficWindow{
				{Kind: model.TrafficKindViews, Start: start, End: end, Uniques: views},
				{Kind: model.TrafficKindClones, Start: start, End: end, Uniques: clones},
			},
		}
	}
	for _, tr := range []*model.RepoTraffic{
		read(octDay(4), octDay(17), 30, 8),
		read(octDay(5), octDay(18), 21, 6),
	} {
		if err := store.UpsertRepoTraffic(ctx, repoID, tr); err != nil {
			t.Fatalf("UpsertRepoTraffic: %v", err)
		}
	}
	ageTrafficRows(ctx, t, store, repoID)
	if err := store.UpsertRepoTraffic(ctx, repoID, read(octDay(5), octDay(18), 25, 0)); err != nil {
		t.Fatalf("UpsertRepoTraffic, second read of the window: %v", err)
	}

	fresh := "fresh " + ToolVersion
	assertTrafficStored(t, "window rows",
		trafficRows(ctx, t, store, `
			SELECT (traffic_kind || ' ' || window_start::text || '..' || window_end::text || ' ' || uniques
				|| ' ' || `+refreshedProvenance+`) COLLATE "C"
			FROM aveloxis_data.repo_traffic_windows WHERE repo_id = $1 ORDER BY 1`, repoID),
		[]string{
			"clones 2026-10-04..2026-10-17 8 aged aged",
			"clones 2026-10-05..2026-10-18 0 " + fresh,
			"views 2026-10-04..2026-10-17 30 aged aged",
			"views 2026-10-05..2026-10-18 25 " + fresh,
		})
}

// TestTrafficWeekSeenTwiceKeepsTheLaterUniques: a complete week lies
// inside the window of several consecutive daily reads; it is one row
// per repository, kind and week start, holding the later read's
// uniques.
func TestTrafficWeekSeenTwiceKeepsTheLaterUniques(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_week")

	week := func(views, clones int) []model.RepoTrafficWeek {
		return []model.RepoTrafficWeek{
			{Kind: model.TrafficKindViews, WeekStart: octDay(5), Uniques: views},
			{Kind: model.TrafficKindClones, WeekStart: octDay(5), Uniques: clones},
		}
	}
	if err := store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{SnapshotDate: octDay(19), Weeks: week(11, 4)}); err != nil {
		t.Fatalf("UpsertRepoTraffic, read of 2026-10-19: %v", err)
	}
	ageTrafficRows(ctx, t, store, repoID)
	if err := store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{SnapshotDate: octDay(20), Weeks: week(12, 0)}); err != nil {
		t.Fatalf("UpsertRepoTraffic, read of 2026-10-20: %v", err)
	}

	fresh := "fresh " + ToolVersion
	assertTrafficStored(t, "weekly rows",
		trafficRows(ctx, t, store, `
			SELECT (traffic_kind || ' ' || week_start::text || ' ' || uniques || ' ' || `+refreshedProvenance+`) COLLATE "C"
			FROM aveloxis_data.repo_traffic_weeks WHERE repo_id = $1 ORDER BY 1`, repoID),
		[]string{"clones 2026-10-05 0 " + fresh, "views 2026-10-05 12 " + fresh})
}

// TestTrafficSnapshotIsReplacedPerDate: a second collection on the
// same UTC date replaces that date's path and referrer snapshot
// entirely (an entry that dropped out of the top 10 does not survive),
// and the snapshots of other dates are kept.
func TestTrafficSnapshotIsReplacedPerDate(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_snapshot")

	reads := []*model.RepoTraffic{
		{
			SnapshotDate: octDay(18),
			Paths: []model.RepoTrafficPath{
				{Path: "/o/r", Title: "o/r", TotalViews: 50, UniqueViews: 10},
				{Path: "/o/r/issues", Title: "Issues", TotalViews: 7, UniqueViews: 3},
			},
			Referrers: []model.RepoTrafficReferrer{{Referrer: "github.com", TotalViews: 30, UniqueViews: 9}},
		},
		{
			SnapshotDate: octDay(19),
			Paths: []model.RepoTrafficPath{
				{Path: "/o/r", Title: "o/r", TotalViews: 52, UniqueViews: 11},
				{Path: "/o/r/issues", Title: "Issues", TotalViews: 8, UniqueViews: 3},
				{Path: "/o/r/wiki", Title: "Wiki", TotalViews: 2, UniqueViews: 1},
			},
			Referrers: []model.RepoTrafficReferrer{
				{Referrer: "github.com", TotalViews: 31, UniqueViews: 9},
				{Referrer: "Google", TotalViews: 4, UniqueViews: 2},
			},
		},
		{
			SnapshotDate: octDay(19),
			Paths: []model.RepoTrafficPath{
				{Path: "/o/r", Title: "o/r: a renamed title", TotalViews: 60, UniqueViews: 12},
				{Path: "/o/r/pulls", Title: "Pull requests", TotalViews: 5, UniqueViews: 2},
			},
			Referrers: []model.RepoTrafficReferrer{{Referrer: "news.example", TotalViews: 6, UniqueViews: 5}},
		},
	}
	for i, tr := range reads {
		if err := store.UpsertRepoTraffic(ctx, repoID, tr); err != nil {
			t.Fatalf("UpsertRepoTraffic, read %d: %v", i+1, err)
		}
	}

	assertTrafficStored(t, "path snapshots",
		trafficRows(ctx, t, store, `
			SELECT (snapshot_date::text || ' ' || path || ' [' || title || '] ' || total_views || '/' || unique_views) COLLATE "C"
			FROM aveloxis_data.repo_traffic_paths WHERE repo_id = $1 ORDER BY 1`, repoID),
		[]string{
			"2026-10-18 /o/r [o/r] 50/10",
			"2026-10-18 /o/r/issues [Issues] 7/3",
			"2026-10-19 /o/r [o/r: a renamed title] 60/12",
			"2026-10-19 /o/r/pulls [Pull requests] 5/2",
		})
	assertTrafficStored(t, "referrer snapshots",
		trafficRows(ctx, t, store, `
			SELECT (snapshot_date::text || ' ' || referrer || ' ' || total_views || '/' || unique_views) COLLATE "C"
			FROM aveloxis_data.repo_traffic_referrers WHERE repo_id = $1 ORDER BY 1`, repoID),
		[]string{"2026-10-18 github.com 30/9", "2026-10-19 news.example 6/5"})
}

// TestTrafficWritersStampDataChangedAt: the staged processor writes
// traffic outside collection jobs too (heal-collection-gaps, the
// leftover drain), so each write of either writer moves
// repos.data_changed_at, which the repository page's cached answers
// are checked against.
func TestTrafficWritersStampDataChangedAt(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_stamp")

	writes := []struct {
		name  string
		write func() error
	}{
		{"UpsertRepoTraffic, first read", func() error {
			return store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{
				SnapshotDate: octDay(19),
				Views:        []model.RepoView{{Timestamp: octDay(18), TotalViews: 1, UniqueViews: 1}},
			})
		}},
		{"UpsertRepoTraffic, same read again", func() error {
			return store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{
				SnapshotDate: octDay(19),
				Views:        []model.RepoView{{Timestamp: octDay(18), TotalViews: 1, UniqueViews: 1}},
			})
		}},
		{"UpsertRepoClone, new day", func() error {
			return store.UpsertRepoClone(ctx, &model.RepoClone{RepoID: repoID, Timestamp: octDay(18), TotalClones: 2, UniqueClones: intPtr(1)})
		}},
		{"UpsertRepoClone, same day again", func() error {
			return store.UpsertRepoClone(ctx, &model.RepoClone{RepoID: repoID, Timestamp: octDay(18), TotalClones: 2, UniqueClones: intPtr(1)})
		}},
		{"UpsertRepoClone, day without uniques", func() error {
			return store.UpsertRepoClone(ctx, &model.RepoClone{RepoID: repoID, Timestamp: octDay(17), TotalClones: 2})
		}},
		{"UpsertRepoClone, same day without uniques again", func() error {
			return store.UpsertRepoClone(ctx, &model.RepoClone{RepoID: repoID, Timestamp: octDay(17), TotalClones: 3})
		}},
	}
	for _, w := range writes {
		mustExecRetry(ctx, t, store,
			`UPDATE aveloxis_data.repos SET data_changed_at = '2000-01-01T00:00:00Z' WHERE repo_id = $1`, repoID)
		if err := w.write(); err != nil {
			t.Fatalf("%s: %v", w.name, err)
		}
		var moved bool
		if err := store.pool.QueryRow(ctx, `
			SELECT COALESCE(data_changed_at > '2000-01-02T00:00:00Z', FALSE)
			FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&moved); err != nil {
			t.Fatal(err)
		}
		if !moved {
			t.Errorf("%s left repos.data_changed_at at its old value: the repository page would keep serving its cached traffic", w.name)
		}
	}
}

// TestTrafficReadIsWrittenInOneTransaction: the window rows mark a
// successful read, so they must never exist without the rest of that
// read, nor the rest without them. A read whose last row Postgres
// refuses (a kind the traffic_kind CHECK rejects) writes nothing: no
// daily row, the earlier snapshot of that date not deleted, and no
// stamp.
func TestTrafficReadIsWrittenInOneTransaction(t *testing.T) {
	ctx, store := caseConnect(t)
	repoID := trafficTestRepo(ctx, t, store, "_avtraffic_atomic")

	if err := store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{
		SnapshotDate: octDay(19),
		Paths:        []model.RepoTrafficPath{{Path: "/o/r", Title: "o/r", TotalViews: 5, UniqueViews: 2}},
	}); err != nil {
		t.Fatalf("UpsertRepoTraffic, first read: %v", err)
	}
	mustExecRetry(ctx, t, store,
		`UPDATE aveloxis_data.repos SET data_changed_at = '2000-01-01T00:00:00Z' WHERE repo_id = $1`, repoID)

	err := store.UpsertRepoTraffic(ctx, repoID, &model.RepoTraffic{
		SnapshotDate: octDay(19),
		Views:        []model.RepoView{{Timestamp: octDay(18), TotalViews: 4, UniqueViews: 2}},
		Paths:        []model.RepoTrafficPath{{Path: "/o/r/wiki", Title: "Wiki", TotalViews: 1, UniqueViews: 1}},
		Windows: []model.RepoTrafficWindow{
			{Kind: model.TrafficKindViews, Start: octDay(5), End: octDay(18), Uniques: 2},
			{Kind: model.TrafficKind("stars"), Start: octDay(5), End: octDay(18), Uniques: 1},
		},
	})
	if err == nil {
		t.Fatal("UpsertRepoTraffic accepted a window of kind \"stars\"; the traffic_kind CHECK must refuse it")
	}

	for _, table := range []string{"repo_views", "repo_traffic_windows"} {
		var n int
		if err := store.pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM aveloxis_data.`+table+` WHERE repo_id = $1`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s holds %d rows of a read that failed: the read must be written in one transaction", table, n)
		}
	}
	assertTrafficStored(t, "path snapshot after a failed read of the same date",
		trafficRows(ctx, t, store, `
			SELECT (snapshot_date::text || ' ' || path) COLLATE "C" FROM aveloxis_data.repo_traffic_paths
			WHERE repo_id = $1 ORDER BY 1`, repoID),
		[]string{"2026-10-19 /o/r"})
	var stamped bool
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(data_changed_at > '2000-01-02T00:00:00Z', FALSE)
		FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped {
		t.Error("a failed traffic read moved repos.data_changed_at: the stamp must commit with the data, not without it")
	}
}
