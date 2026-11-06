// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
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
	// A SET stays on the hash path for every repository (Copilot PR #252):
	// the per-file arm counts a commit shared by two repositories (a fork
	// and its upstream, the same hash) ONCE per author across the set, which
	// daily counts cannot. The other fixture holds fx's alice's commit under
	// the same hash: on the hash path she has ONE action across the set (a
	// drive-by), while fx's daily table alone would make her a repeat.
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'c0ffee1', 'b.go', 'a', '_avcd_ret01@x', '2026-09-29', $2, $3::uuid)`, other.repoID, fx.in.Add(10*time.Hour), fx.alice)
	if d, r := sum([]int64{fx.repoID, other.repoID}); d != 2 || r != 0 {
		t.Fatalf("a set keeps every repository on the hash path (fx's alice once across the set, other's alice once), got drive_by=%v repeat=%v", d, r)
	}
}

// Copilot PR #252: the trim keeps earlier-era buckets that still EXIST, but a
// repository adopted before 0.29.140 (or trimmed by a walk between the
// re-creation and the adoption — bobsql's 18:07 collection) had already lost
// them. The adoption restores the earlier era's buckets from the commits
// table (append-only: it still holds every old commit), for days before the
// boundary only, never touching a current-era bucket; the ledgered migrate
// step does the same once for every repository already adopted.
func TestAdoptForgeIDRestoresTheEarlierErasDailyBuckets(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era03")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	old := fx.in.AddDate(0, 0, -30)
	// The earlier repository's commit (two files, one hash) and a current-era
	// bucket the walk reported with a different count than the commits
	// table would (the walk is the truth for the current era: it must stand).
	for _, f := range []string{"x.go", "y.go"} {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
			VALUES ($1, 'o1dc0ffee', $2, 'o', 'old@x', '2026-08-01', $3)`, fx.repoID, f, old.Add(9*time.Hour))
	}
	// The trimmed state: the new history's walk left only the current era.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: fx.in, AuthorEmail: "_avcd_era03@x", Commits: 3}}, true); err != nil {
		t.Fatal(err)
	}
	created := fx.in.Add(16 * time.Hour)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", ""); err != nil {
		t.Fatal(err)
	}
	rows := func() map[string]int {
		t.Helper()
		r, err := store.pool.Query(ctx, `SELECT day::text || ' ' || author_email, commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		out := map[string]int{}
		for r.Next() {
			var k string
			var n int
			if err := r.Scan(&k, &n); err != nil {
				t.Fatal(err)
			}
			out[k] = n
		}
		return out
	}
	got := rows()
	if got[old.Format("2006-01-02")+" old@x"] != 1 {
		t.Errorf("the earlier era's bucket is restored from the commits table (one hash, two files): %v", got)
	}
	if got[fx.in.Format("2006-01-02")+" _avcd_era03@x"] != 3 {
		t.Errorf("the current era's bucket is the walk's, untouched (3, not the commits table's 1): %v", got)
	}
	if ok, err := store.RepoCommitDailyComplete(ctx, fx.repoID); err != nil || !ok {
		t.Errorf("the picture stays complete: %v %v", ok, err)
	}
	if at := repoDataChangedAt(ctx, t, store, fx.repoID); at == nil {
		t.Error("the restore changes the charts: the cache state is stamped")
	}
	// The repair step for repositories adopted before 0.29.140: idempotent,
	// the same rows.
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day < $2`, fx.repoID, fx.in)
	repair := migrateStepSQL(t, restoreEarlierErasLabel)
	for i := 0; i < 2; i++ {
		if _, err := store.pool.Exec(ctx, repair); err != nil {
			t.Fatal(err)
		}
	}
	if got := rows(); got[old.Format("2006-01-02")+" old@x"] != 1 || got[fx.in.Format("2006-01-02")+" _avcd_era03@x"] != 3 || len(got) != 2 {
		t.Errorf("the migrate repair restores the earlier era once, current era untouched: %v", got)
	}
}

// Every taker of the daily table's per-repository lock takes it BEFORE any
// write of the repository's repos row (the fresh review of the PR #252 round):
// the facade's trimming walk, the heal fill and the completeness clear all go
// lock → repos row, and an adoption that went repos row → lock deadlocked
// with a walk of the same repository (reproduced with two sessions), with no
// retry on its side. One order for every taker, and the four known takers
// are REQUIRED (a floor count let a phantom taker — the const's own
// package-level chunk under a cut-at-`func` split — absorb the loss of a real
// one: the fresh review's mutant that dropped the adoption's lock passed).
func TestDailyLockPrecedesEveryReposWriteInItsTaker(t *testing.T) {
	takers := map[string]bool{}
	for _, file := range srctest.PackageFiles(t, "internal/db", 30) {
		for name, body := range srctest.FuncParts(t, srctest.StripGoComments(file)) {
			if name == srctest.PackageLevel {
				continue // the declaration lives here
			}
			lock := strings.Index(body, "repoCommitDailyLockSQL")
			if lock < 0 {
				continue
			}
			takers[name] = true
			for _, write := range []string{"UPDATE aveloxis_data.repos", "stampCommitDailyCompleteSQL", "stampRepoCacheStateSQL", "resurrectInTx(", "resurrectSQL("} {
				if at := strings.Index(body, write); at >= 0 && at < lock {
					t.Errorf("%s writes the repos row (%s at %d) before taking repoCommitDailyLockSQL (%d): every other taker locks first, and the reverse order deadlocks with a trimming walk", name, write, at, lock)
				}
			}
		}
	}
	for _, want := range []string{"ReplaceRepoCommitDaily", "InvalidateCommitDailyComplete", "FillRepoCommitDailyFromCommits", "AdoptForgeID"} {
		if !takers[want] {
			t.Errorf("%s must take repoCommitDailyLockSQL (it writes repo_commit_daily or the completeness stamp); takers seen: %v", want, takers)
		}
	}
}

// The runtime half: while a walk holds the repository's daily lock, the
// adoption WAITS (and a deadline ends it cleanly, nothing committed) instead
// of deadlocking; once the lock is free it goes through.
func TestAdoptForgeIDWaitsForTheDailyLock(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era04")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	holder, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx) //nolint:errcheck // released below or at exit
	if _, err := holder.Exec(ctx, repoCommitDailyLockSQL, fx.repoID); err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	err = store.AdoptForgeID(short, fx.repoID, "95336057", "1438985131", nil, "operator", "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("with the daily lock held the adoption waits and the deadline ends it, got %v", err)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT platform_repo_id FROM aveloxis_data.repos WHERE repo_id = $1`, fx.repoID).Scan(&stored); err != nil || stored != "95336057" {
		t.Fatalf("nothing committed while waiting: %q %v", stored, err)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", nil, "operator", ""); err != nil {
		t.Fatalf("with the lock free the adoption goes through: %v", err)
	}
}
