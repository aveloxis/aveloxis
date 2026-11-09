// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/jackc/pgx/v5"
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
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", "", publicListing()); err != nil {
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
	// The boundary day, per author (Copilot PR #252, twelfth review): the
	// walk-unseen current-era row is trimmed as before, while the fixture
	// author's commit at 10:00 — before the 16:38 re-creation, so the
	// earlier repository's — is kept (refolded from the commits table).
	onBoundary := func(email string) (int, bool) {
		t.Helper()
		var n int
		err := store.pool.QueryRow(ctx, `SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day = $2 AND author_email = $3`, fx.repoID, boundary, email).Scan(&n)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false
		}
		if err != nil {
			t.Fatal(err)
		}
		return n, true
	}
	if _, present := onBoundary("cur@x"); present {
		t.Errorf("a current-era row the walk did not see is trimmed as before: %v", got)
	}
	if n, present := onBoundary("_avcd_era01@x"); !present || n != 1 {
		t.Errorf("the earlier repository's commit of the boundary morning is kept: %d %v", n, present)
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
	sumIn := func(ids []int64, since, until time.Time) (driveBy, repeat float64) {
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
	sum := func(ids []int64) (driveBy, repeat float64) { return sumIn(ids, since, until) }
	// Zero bounds mean "unbounded" here as in every other series (Copilot PR
	// #252, eighth review: a zero until answered nothing).
	if d0, r0 := sumIn([]int64{fx.repoID}, time.Time{}, time.Time{}); d0+r0 == 0 {
		t.Error("an unbounded window answers the repository's contributors, not nothing")
	} else if d, r := sum([]int64{fx.repoID}); d0+r0 != d+r {
		t.Errorf("unbounded %v vs the 60-day window %v: the fixture lies inside the window", d0+r0, d+r)
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
	isPrivate := false
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", "", &isPrivate); err != nil {
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
	// Copilot PR #252, seventh review: the repair changes the charts the
	// same way the adoption does, so it stamps the cache state of every
	// repository it restored a bucket for, in the same statement — and only
	// those (the idempotent second run inserts nothing and stamps nothing).
	repair := migrateStepSQL(t, restoreEarlierErasLabel)
	for i := 0; i < 2; i++ {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET data_changed_at = NULL WHERE repo_id = $1`, fx.repoID)
		if _, err := store.pool.Exec(ctx, repair); err != nil {
			t.Fatal(err)
		}
		if at := repoDataChangedAt(ctx, t, store, fx.repoID); (at != nil) != (i == 0) {
			t.Errorf("run %d: the repair stamps exactly the repositories it restored buckets for (stamped: %v)", i+1, at != nil)
		}
	}
	if got := rows(); got[old.Format("2006-01-02")+" old@x"] != 1 || got[fx.in.Format("2006-01-02")+" _avcd_era03@x"] != 3 || len(got) != 2 {
		t.Errorf("the migrate repair restores the earlier era once, current era untouched: %v", got)
	}
}

// The re-creation DAY holds both eras (Copilot PR #252, twelfth review): the
// boundary is a UTC day, so the earlier repository's commits authored that
// day before the re-creation moment fell between the trim (day >= boundary)
// and the restore (day < boundary) and were lost. Now, for every author with
// a commit authored before the exact era start on that day, the bucket is
// the commits table's fold of the day — both eras' distinct commits —
// after the walk's trim, in the adoption's restore and in the migrate repair.
func TestTheEraBoundaryDayKeepsTheEarlierRepositorysCommits(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era04")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	day := fx.in.AddDate(0, 0, -10)
	created := day.Add(16 * time.Hour) // the forge re-created the repository at 16:00 UTC
	commit := func(hash, email string, at time.Time) {
		t.Helper()
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
			VALUES ($1, $2, 'f.go', 'n', $3, $4, $5)`, fx.repoID, hash, email, at.Format("2006-01-02"), at)
	}
	commit("e4a0000001", "a@old", day.Add(9*time.Hour))   // the earlier repository only
	commit("e4c0000001", "c@both", day.Add(10*time.Hour)) // the earlier repository...
	commit("e4c0000002", "c@both", day.Add(17*time.Hour)) // ...and the new one, the same day
	commit("e4b0000001", "b@new", day.Add(18*time.Hour))  // the new repository only
	// A new-history author with a commit a force-push since removed: the
	// commits table holds two of the day's rows, the walk's history one. The
	// boundary day refolds only the authors with a commit before the era start,
	// so this bucket stays the walk's (the evaluation of 2026-11-08: dropping
	// that condition passed every test).
	commit("e4d0000001", "d@new", day.Add(19*time.Hour))
	commit("e4d0000002", "d@new", day.Add(20*time.Hour))
	bucket := func(email string) int {
		t.Helper()
		var n int
		err := store.pool.QueryRow(ctx, `SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day = $2 AND author_email = $3`, fx.repoID, day, email).Scan(&n)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		return n
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", "", publicListing()); err != nil {
		t.Fatal(err)
	}
	if bucket("a@old") != 1 || bucket("c@both") != 2 || bucket("d@new") != 0 {
		t.Errorf("the adoption's restore folds the re-creation day for the earlier era's authors only: a=%d c=%d d=%d, want 1, 2 and none", bucket("a@old"), bucket("c@both"), bucket("d@new"))
	}
	// The new repository's walk sees only its own commits that day.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: day, AuthorEmail: "c@both", Commits: 1}, {Day: day, AuthorEmail: "b@new", Commits: 1},
		{Day: day, AuthorEmail: "d@new", Commits: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	if a, c, b, d := bucket("a@old"), bucket("c@both"), bucket("b@new"), bucket("d@new"); a != 1 || c != 2 || b != 1 || d != 1 {
		t.Errorf("after the walk: a=%d c=%d b=%d d=%d, want 1, 2, 1, 1 (the earlier commits of the day kept, the new authors the walk's)", a, c, b, d)
	}
	// The migrate repair for a repository adopted before 0.29.140.
	mustExecRetry(ctx, t, store, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day = $2 AND author_email = 'a@old'`, fx.repoID, day)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_commit_daily SET commits = 1 WHERE repo_id = $1 AND day = $2 AND author_email = 'c@both'`, fx.repoID, day)
	if _, err := store.pool.Exec(ctx, migrateStepSQL(t, restoreEarlierErasLabel)); err != nil {
		t.Fatal(err)
	}
	if a, c, b, d := bucket("a@old"), bucket("c@both"), bucket("b@new"), bucket("d@new"); a != 1 || c != 2 || b != 1 || d != 1 {
		t.Errorf("after the migrate repair: a=%d c=%d b=%d d=%d, want 1, 2, 1, 1", a, c, b, d)
	}
}

// An earlier-era bucket the new history re-states only in part is not lowered
// (Copilot PR #252, fifteenth review): a re-created repository that imports
// commit A of an old (day, author) bucket holding A and B keeps A's old author
// date, so its trimming walk upserted that bucket to 1 — the trim keeps rows
// before the boundary, and the restores skipped an existing bucket (DO
// NOTHING), so B was gone from the charts while still in commits. Every
// earlier-era bucket is folded from the commits table, raise-only: after the
// walk, in the adoption's restore and in the migrate repair.
func TestAPartlyRestatedEarlierEraBucketKeepsItsCommits(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era07")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	old := fx.in.AddDate(0, 0, -40)
	for _, h := range []string{"e7a0000001", "e7b0000001"} {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
			VALUES ($1, $2, 'f.go', 'n', 'x@old', $3, $4)`, fx.repoID, h, old.Format("2006-01-02"), old.Add(9*time.Hour))
	}
	bucket := func() int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE((SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day = $2 AND author_email = 'x@old'), 0)`, fx.repoID, old).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// The bucket as the earlier repository's walks left it, then partly
	// overwritten before the adoption: the adoption's restore raises it.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits) VALUES ($1, $2, 'x@old', 1)`, fx.repoID, old)
	created := fx.in.Add(16 * time.Hour)
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET platform_repo_id = '95336057' WHERE repo_id = $1`, fx.repoID)
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", &created, "operator", "", publicListing()); err != nil {
		t.Fatal(err)
	}
	if got := bucket(); got != 2 {
		t.Errorf("the adoption's restore raises a partly overwritten earlier-era bucket: %d, want 2", got)
	}
	// The new history re-states only commit A of that day.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: old, AuthorEmail: "x@old", Commits: 1}}, true); err != nil {
		t.Fatal(err)
	}
	if got := bucket(); got != 2 {
		t.Errorf("a walk re-stating one of the bucket's two earlier commits leaves it at 2, got %d", got)
	}
	// The migrate repair raises a bucket a walk lowered before 0.29.140.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repo_commit_daily SET commits = 1 WHERE repo_id = $1 AND day = $2 AND author_email = 'x@old'`, fx.repoID, old)
	if _, err := store.pool.Exec(ctx, migrateStepSQL(t, restoreEarlierErasLabel)); err != nil {
		t.Fatal(err)
	}
	if got := bucket(); got != 2 {
		t.Errorf("the migrate repair raises a partly overwritten earlier-era bucket: %d, want 2", got)
	}
}

// The refold costs nothing for a repository with no adopted change (the L10
// pass on the twelfth-review fix: it runs on every trimming walk, and a NULL
// era start inside a date comparison against a column did not short-circuit
// — a Parallel Seq Scan of commits for a 479 K-row repository, 0 rows). The
// era start's IS NOT NULL is a pseudoconstant the planner turns into a
// One-Time Filter that skips the whole scan.
func TestTheRefoldIsAOneTimeFilterWithoutAnAdoptedChange(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era05")
	rows, err := store.pool.Query(ctx, "EXPLAIN "+refoldEarlierEraSQL, fx.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "One-Time Filter") {
		t.Errorf("the refold must gate on a pseudoconstant (a One-Time Filter), not filter the repository's commit rows:\n%s", plan.String())
	}
	// No SubPlan: the era start inside the boundary day's HAVING bool_or is an
	// uncorrelated scalar subquery, an InitPlan computed once. A correlated
	// subquery over commits here was a hashed SubPlan from a scan of the WHOLE
	// table (the fifteenth review's fix, inside an OR) and then a per-row
	// probe of every repository's rows (the evaluation of 2026-11-08); the
	// scan count is pinned by TestTheEraFoldsReadCommitsOncePerArm.
	if strings.Contains(plan.String(), "SubPlan") {
		t.Errorf("the refold's subqueries must be InitPlans, never a (hashed or correlated) SubPlan:\n%s", plan.String())
	}
}

// Each era fold reads the commits table once per arm and never again per row
// (the evaluation of 2026-11-08, Track 0a): the boundary day's "this author has
// a commit that day before the era start" was a correlated EXISTS that the
// planner served from idx_commits_author_email, filtering repo_id afterwards,
// so every boundary-day row walked that author's commits in EVERY repository —
// 58,497 probes and 82 million buffer hits, 88 s, on a 2-million-row copy;
// kate's table has 1.47 billion. All of a boundary-day author's rows are one
// group, so the condition is a HAVING bool_or over the group, read from the
// rows the arm already has (0.3 s, the same rows). The count of commits scan
// nodes is cost-independent: a correlated read adds a node whatever the plan.
func TestTheEraFoldsReadCommitsOncePerArm(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era_scans")
	commitsScan := regexp.MustCompile(`(?m)Scan(?: Backward)?(?: using \S+)? on commits\b`)
	explain := func(sql string, args ...any) string {
		t.Helper()
		rows, err := store.pool.Query(ctx, "EXPLAIN "+sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var plan strings.Builder
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(line)
			plan.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return plan.String()
	}
	for name, plan := range map[string]string{
		"the per-walk refold":    explain(refoldEarlierEraSQL, fx.repoID),
		"the migrate era repair": explain(migrateStepSQL(t, restoreEarlierErasLabel)),
	} {
		if n := len(commitsScan.FindAllString(plan, -1)); n != 2 {
			t.Errorf("%s reads commits %d times, want 2 (one per arm): a correlated per-row read of commits is back\n%s", name, n, plan)
		}
	}
}

// The boundary day is 24 UTC hours whatever the session's time zone (the L10
// pass on the refold's range): `+ INTERVAL '1 day'` on a timestamptz is
// calendar arithmetic in the session zone, so on a daylight-saving day it was
// 23 or 25 hours — on the spring-forward day under America/Chicago (this
// server's and CI's zone) the range ended at 23:00Z, and an earlier-era
// commit at 23:30Z was lost. Run on a connection pinned to that zone.
func TestTheEraBoundaryDayIsTwentyFourUTCHours(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_era06")
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1`, fx.repoID)
	})
	start := time.Date(2026, 3, 8, 23, 50, 0, 0, time.UTC) // US clocks sprang forward that morning
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_forge_id_changes (repo_id, old_forge_id, new_forge_id, forge_created_at, adopted_at, observed_private)
		VALUES ($1, '1', '2', $2, NOW(), FALSE)`, fx.repoID, start)
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'e6a0000001', 'f.go', 'n', 'late@old', '2026-03-08', $2)`, fx.repoID, start.Add(-20*time.Minute))
	conn, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET TimeZone = 'America/Chicago'`); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(context.Background(), `RESET TimeZone`) //nolint:errcheck // the connection goes back to the pool
	if _, err := conn.Exec(ctx, refoldEarlierEraSQL, fx.repoID); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT COALESCE((SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND day = DATE '2026-03-08' AND author_email = 'late@old'), 0)`, fx.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("an earlier-era commit at 23:30Z on the spring-forward day is folded into its bucket: got %d, want 1", n)
	}
}

// Every taker of the daily table's per-repository lock takes it as the FIRST
// use of its transaction (the fresh review of the PR #252 round): the
// facade's trimming walk, the heal fill and the completeness clear all go
// lock → repos row, and an adoption that went repos row → lock deadlocked
// with a walk of the same repository (reproduced with two sessions), with no
// retry on its side. The rule is structural, not a list of write spellings
// (a list let a write hidden in a helper call pass — the review's mutant
// hoisted Commit into the caller and reordered): between `.Begin(ctx)` and
// the lock statement, the transaction is not used at all — no `tx` token in
// any spelling (a method, an argument, an alias, a struct field) — except its
// deferred Rollback. The four known takers
// are REQUIRED (a floor count let a phantom taker absorb the loss of a real
// one).
func TestDailyLockPrecedesEveryReposWriteInItsTaker(t *testing.T) {
	takers := map[string]bool{}
	for _, file := range srctest.PackageFiles(t, "internal/db", 30) {
		for name, body := range srctest.FuncParts(t, srctest.StripGoComments(file)) {
			if name == srctest.PackageLevel || !strings.Contains(body, "repoCommitDailyLockSQL") {
				continue
			}
			takers[name] = true
			lock := strings.Index(body, "tx.Exec(ctx, repoCommitDailyLockSQL")
			begin := strings.Index(body, ".Begin(ctx)")
			if lock < 0 || begin < 0 || begin > lock {
				t.Errorf("%s: the lock must be taken as `tx.Exec(ctx, repoCommitDailyLockSQL …)` on a transaction begun before it (begin %d, lock %d)", name, begin, lock)
				continue
			}
			// Any `tx` token at all (the fresh review's third round: a list of
			// spellings let an alias, a wrapped call with tx on its own line
			// and a struct-carried tx all pass); the declaration sits before
			// the segment, and the deferred Rollback is stripped first.
			between := strings.ReplaceAll(body[begin:lock], "tx.Rollback(ctx)", "")
			if regexp.MustCompile(`\btx\b`).MatchString(between) {
				t.Errorf("%s uses its transaction before taking repoCommitDailyLockSQL — every taker locks FIRST (a repos-row write first, inline or through a helper, deadlocks with a trimming walk):\n%s", name, between)
			}
		}
	}
	for _, want := range []string{"ReplaceRepoCommitDaily", "InvalidateCommitDailyComplete", "FillRepoCommitDailyFromCommits", "adoptForgeIDUnderLock"} {
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
	isPrivate := false
	err = store.AdoptForgeID(short, fx.repoID, "95336057", "1438985131", nil, "operator", "", &isPrivate)
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
	if err := store.AdoptForgeID(ctx, fx.repoID, "95336057", "1438985131", nil, "operator", "", &isPrivate); err != nil {
		t.Fatalf("with the lock free the adoption goes through: %v", err)
	}
}
