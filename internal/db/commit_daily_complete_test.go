// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// PR #226 review 5448678338: "has daily rows" is not "the daily picture is
// complete". A facade walk that swallowed commit writes on a repository
// that was never filled upserts the buckets it saw; with an existence
// probe, one such row made the readers prefer that sparse picture over the
// fuller commits table. Completeness is a stamp (repos.
// commit_daily_complete_at) that only a trimming replace or the heal
// command's fill sets; a partial replace preserves whatever state it found.
func TestCommitDailyCompletenessIsStampedOnlyByACleanReplacement(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_cmp01")
	since := utcDay(time.Now()).AddDate(0, 0, -30)
	weekly := func() int {
		t.Helper()
		ts, err := store.GetRepoTimeSeries(ctx, fx.repoID, since, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		sum := 0
		for _, p := range ts.Commits {
			sum += p.Count
		}
		return sum
	}
	complete := func() bool {
		t.Helper()
		c, err := store.RepoCommitDailyComplete(ctx, fx.repoID)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	pending := func() bool {
		t.Helper()
		ids, err := store.ListReposNeedingCommitDaily(ctx, 1000)
		if err != nil {
			t.Fatal(err)
		}
		return containsID(ids, fx.repoID)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_commits) VALUES ($1, 100, 'queued', NOW(), 1)`, fx.repoID); err != nil {
		t.Fatal(err)
	}

	// 1. Never filled: not complete, pending, the commits table answers.
	if complete() || !pending() {
		t.Fatal("a never-filled repository is not complete and is pending")
	}
	// 2. A walk that swallowed writes (trim=false) on it: its rows land,
	// but the picture is still not complete — the readers keep the commits
	// table (1 commit), not the sparse rows (5 would show otherwise), and
	// the heal command still lists it.
	partial := []CommitDailyRow{{Day: fx.in, AuthorEmail: "_avcd_cmp01@x", Commits: 5}}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, partial, false); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("the partial walk's row is recorded, got %d rows", n)
	}
	if complete() {
		t.Fatal("rows from a walk that swallowed writes must not make the picture complete")
	}
	if !pending() {
		t.Fatal("a repository with only partial rows is still listed for the heal command")
	}
	if got := weekly(); got != 1 {
		t.Fatalf("not complete: the readers take the commits table (1), got %d", got)
	}
	// 3. A clean walk (trim=true) stamps it: the daily rows answer now.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: fx.in, AuthorEmail: "_avcd_cmp01@x", Commits: 3}}, true); err != nil {
		t.Fatal(err)
	}
	if !complete() || pending() {
		t.Fatal("a trimming replace stamps the picture complete and the repository leaves the heal list")
	}
	if got := weekly(); got != 3 {
		t.Fatalf("complete: the daily table answers (3), got %d", got)
	}
	var stampedAt time.Time
	if err := store.pool.QueryRow(ctx, `SELECT commit_daily_complete_at FROM aveloxis_data.repos WHERE repo_id = $1`, fx.repoID).Scan(&stampedAt); err != nil {
		t.Fatal(err)
	}
	// 4. A later partial walk preserves the stamp (its rows still cover the
	// earlier clean walk; counts never go down) — and does not re-stamp.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: fx.in, AuthorEmail: "_avcd_cmp01@x", Commits: 2}}, false); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err := store.pool.QueryRow(ctx, `SELECT commit_daily_complete_at FROM aveloxis_data.repos WHERE repo_id = $1`, fx.repoID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !complete() || !after.Equal(stampedAt) {
		t.Fatalf("a partial walk after a clean one keeps the stamp as it was: complete=%v stamp %v → %v", complete(), stampedAt, after)
	}
	if got := weekly(); got != 3 {
		t.Fatalf("the fuller count stands until a clean walk, got %d", got)
	}
	// 5. A clean walk that saw nothing: the rows go, the picture is a
	// complete EMPTY one — the readers answer zero from it, never the
	// commits table's stale rows (the reverse half of the finding).
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, nil, true); err != nil {
		t.Fatal(err)
	}
	if !complete() || pending() {
		t.Fatal("an empty trimmed replace is a complete picture")
	}
	if got := weekly(); got != 0 {
		t.Fatalf("complete and empty: zero commits, got %d (the commits table still holds a row)", got)
	}
	// 6. The heal command's fill is complete by construction.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if complete() || !pending() {
		t.Fatal("precondition: unstamped again")
	}
	if _, err := store.FillRepoCommitDailyFromCommits(ctx, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if !complete() || pending() {
		t.Fatal("the fill stamps the picture complete")
	}
	if got := weekly(); got != 1 {
		t.Fatalf("filled from the commits table: 1, got %d", got)
	}
}

// The column's migration stamps the repositories that already have rows
// ONCE — on the run that adds the column — because those rows were written
// when a trimming walk and the heal fill were the only writers the readers
// could use. A later run must not stamp: by then an unstamped repository
// with rows is exactly the partial case the column exists to expose.
// The add+stamp path itself (the one an upgraded fleet takes) is driven
// below on a scratch table through the same generic helper, including the
// atomicity the first shape lacked (L10 round 1 of 0.29.75: a stamp that
// failed after a committed ALTER was lost for good).
func TestColumnWithOneTimeStampIsAtomicAndRunsOnce(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	const tbl = "_avcd_stamp_probe"
	_, _ = store.pool.Exec(ctx, `DROP TABLE IF EXISTS aveloxis_data.`+tbl)
	if _, err := store.pool.Exec(ctx, `CREATE TABLE aveloxis_data.`+tbl+` (id bigint PRIMARY KEY, has_rows boolean NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = store.pool.Exec(context.Background(), `DROP TABLE IF EXISTS aveloxis_data.`+tbl) })
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.`+tbl+` VALUES (1, true), (2, false)`); err != nil {
		t.Fatal(err)
	}
	hasColumn := func() bool {
		t.Helper()
		var ok bool
		if err := store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'aveloxis_data' AND table_name = $1 AND column_name = 'stamped_at')`, tbl).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	stampOf := func(id int) *time.Time {
		t.Helper()
		var at *time.Time
		if err := store.pool.QueryRow(ctx, `SELECT stamped_at FROM aveloxis_data.`+tbl+` WHERE id = $1`, id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	stamp := `UPDATE aveloxis_data.` + tbl + ` SET stamped_at = NOW() WHERE stamped_at IS NULL AND has_rows`
	// 1. A stamp that fails rolls the column back with it: the rerun will
	// redo both. (The first shape committed the ALTER, lost the stamp, and
	// the rerun saw "exists" and did nothing.)
	var errs []error
	addColumnWithOneTimeStamp(ctx, store, slog.Default(), &errs, "aveloxis_data", tbl, "stamped_at", "TIMESTAMPTZ", `UPDATE aveloxis_data.`+tbl+` SET stamped_at = NOW() WHERE no_such_column`)
	if len(errs) != 1 {
		t.Fatalf("a failed stamp is an error: %v", errs)
	}
	if hasColumn() {
		t.Fatal("a failed stamp must roll the column back, or the rerun stamps nothing")
	}
	// 2. The upgrade run: adds the column and stamps the rows that qualify.
	errs = nil
	addColumnWithOneTimeStamp(ctx, store, slog.Default(), &errs, "aveloxis_data", tbl, "stamped_at", "TIMESTAMPTZ", stamp)
	if len(errs) != 0 || !hasColumn() {
		t.Fatalf("the upgrade run adds the column: errs=%v", errs)
	}
	if stampOf(1) == nil || stampOf(2) != nil {
		t.Fatalf("stamped exactly the qualifying row: %v / %v", stampOf(1), stampOf(2))
	}
	// 3. Once: NULL again while qualifying (the partial case) stays NULL on
	// the next run.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.`+tbl+` SET stamped_at = NULL`); err != nil {
		t.Fatal(err)
	}
	errs = nil
	addColumnWithOneTimeStamp(ctx, store, slog.Default(), &errs, "aveloxis_data", tbl, "stamped_at", "TIMESTAMPTZ", stamp)
	if len(errs) != 0 || stampOf(1) != nil {
		t.Fatalf("a later run must not stamp: errs=%v stamp=%v", errs, stampOf(1))
	}
}

func TestCommitDailyCompleteColumnMigrationStampsOnce(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	withRows := seedCommitDaily(t, store, ctx, "_avcd_mig01")
	without := seedCommitDaily(t, store, ctx, "_avcd_mig02")
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits) VALUES ($1, $2::date, '_avcd_mig01@x', 1)`, withRows.repoID, withRows.in); err != nil {
		t.Fatal(err)
	}
	stamp := func(id int64) *time.Time {
		t.Helper()
		var at *time.Time
		if err := store.pool.QueryRow(ctx, `SELECT commit_daily_complete_at FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	run := func() {
		t.Helper()
		var errs []error
		addCommitDailyCompleteColumn(ctx, store, slog.Default(), &errs)
		if len(errs) != 0 {
			t.Fatalf("migration errors: %v", errs)
		}
	}
	// The column exists (this test DB was built from schema.sql; a view
	// depends on it, so the test cannot drop it to walk the upgrade path):
	// a run changes nothing — the unstamped repository with rows stays
	// unstamped. The stamp the upgrade run makes is its own function.
	run()
	if stamp(withRows.repoID) != nil || stamp(without.repoID) != nil {
		t.Fatal("with the column present the migration stamps nothing")
	}
	if _, err := store.pool.Exec(ctx, stampCommitDailyCompleteWhereFilledSQL); err != nil {
		t.Fatal(err)
	}
	if stamp(withRows.repoID) == nil {
		t.Fatal("the upgrade stamp marks a repository that has daily rows")
	}
	if stamp(without.repoID) != nil {
		t.Fatal("a repository without rows is not stamped")
	}
	// Once: NULL again with rows present (the partial case) stays NULL on
	// the next run of the migration.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, withRows.repoID); err != nil {
		t.Fatal(err)
	}
	run()
	if stamp(withRows.repoID) != nil {
		t.Fatal("a later run must not stamp an unstamped repository that has rows")
	}
}

// kate 2026-10-07, after the fleet-wide heal: the warm run's /stats answered
// 503 (nginx's 120 s) on the nine largest repositories. /stats reads
// LastActivityAt, whose commits arm falls back from the stored bound
// (NULL until the repository's next collection) to a live scan of the
// commits table — the scattered-page scan the daily table retired for
// the other two readers. A complete daily picture answers the bound from
// its primary key instead: the stored bound first (exact), then the daily
// table (a UTC day: midnight of the last/first day — a lower bound of the
// true last instant, which every consumer rounds to a day anyway), then
// the live scan only when neither exists. A bogus day (the daily fold
// keeps every dated commit, 2080 included) is never a bound.
func TestActivityBoundsReadTheDailyTableWhenComplete(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_act01") // one commit at fx.in + 10h; repos bounds NULL
	last := func() time.Time {
		t.Helper()
		la, ok, err := store.LastActivityAt(ctx, []int64{fx.repoID})
		if err != nil || !ok {
			t.Fatalf("LastActivityAt: %v %v", ok, err)
		}
		return la
	}
	first := func() time.Time {
		t.Helper()
		fa, ok, err := store.FirstActivityAt(ctx, []int64{fx.repoID})
		if err != nil || !ok {
			t.Fatalf("FirstActivityAt: %v %v", ok, err)
		}
		return fa
	}
	// Not complete, no stored bound: the live scan answers the exact instant.
	if got := last(); !got.Equal(fx.in.Add(10 * time.Hour)) {
		t.Fatalf("not complete: live last = %v", got)
	}
	// Complete (the heal's fill): the daily table answers — midnight of the day.
	if _, err := store.FillRepoCommitDailyFromCommits(ctx, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if got := last(); !got.Equal(fx.in) {
		t.Fatalf("complete: the daily table's last day answers, got %v want %v", got, fx.in)
	}
	if got := first(); !got.Equal(fx.in) {
		t.Fatalf("complete: the daily table's first day answers, got %v want %v", got, fx.in)
	}
	// A bogus day in the daily rows (a 2080 commit folded by the heal) is
	// not a bound: the plausible days answer.
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits) VALUES ($1, '2080-01-01', 'z@x', 1), ($1, '1970-01-01', 'z@x', 1)`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if got := last(); !got.Equal(fx.in) {
		t.Fatalf("a bogus future day is not the last activity, got %v", got)
	}
	if got := first(); !got.Equal(fx.in) {
		t.Fatalf("the epoch day is not the first activity, got %v", got)
	}
	// Rows present but the picture NOT complete (a walk that swallowed
	// writes on a never-filled repository): the daily rows are not read —
	// the live instant answers (the gate's refusal, pinned at runtime).
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if got := last(); !got.Equal(fx.in.Add(10 * time.Hour)) {
		t.Fatalf("unstamped rows are not read: the live instant answers, got %v", got)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NOW() WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	// The stored bound, when plausible, wins (it is exact).
	exact := fx.in.Add(10 * time.Hour)
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET last_commit_at = $2, first_commit_at = $2 WHERE repo_id = $1`, fx.repoID, exact); err != nil {
		t.Fatal(err)
	}
	if got := last(); !got.Equal(exact) {
		t.Fatalf("the stored bound wins, got %v", got)
	}
}
