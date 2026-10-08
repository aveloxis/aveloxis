// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// summary/49 (2026-10-06): the repository page's two commit answers — the
// weekly commit series and the commits arm of top contributors — timed out
// on kernel forks because the commits table is one row per FILE per commit
// and one repository's rows lie about one per page across the whole table
// (NVIDIA/nova: 118 s, 1.48M heap pages for 337K rows). The facade already
// walks the whole default branch every run, so it folds what it walks into
// aveloxis_data.repo_commit_daily — one row per repository, UTC day and
// author email — and the two readers sum that table when the repository's
// picture is complete (repos.commit_daily_complete_at: a trimming walk or
// the heal fill, never a walk that swallowed writes) and the window is
// UTC-day aligned, falling back to the commits table otherwise. Author identity at read time is the contract's own rule R5:
// contributors_aliases maps every commit email of a resolved contributor to
// its cntrb_id.

func TestUTCDayAlignment(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")
	midnightUTC := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	cases := map[string]struct {
		t       time.Time
		aligned bool
		day     time.Time
	}{
		"UTC midnight":              {midnightUTC, true, midnightUTC},
		"UTC midnight in a zone":    {midnightUTC.In(ny), true, midnightUTC},
		"an hour in":                {midnightUTC.Add(time.Hour), false, midnightUTC},
		"a second in":               {midnightUTC.Add(time.Second), false, midnightUTC},
		"local midnight, not UTC's": {time.Date(2026, 10, 6, 0, 0, 0, 0, ny), false, time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)},
		"zero":                      {time.Time{}, true, time.Time{}},
	}
	for name, tc := range cases {
		if got := alignedToUTCDay(tc.t); got != tc.aligned {
			t.Errorf("%s: aligned=%v want %v", name, got, tc.aligned)
		}
		if got := utcDay(tc.t); !got.Equal(tc.day) {
			t.Errorf("%s: utcDay=%v want %v", name, got, tc.day)
		}
	}
}

// commitDailyFixture seeds a repository, a resolved contributor with an
// alias for her commit email, and ONE commit row in the window, so the
// commits table says "1 commit" — any other answer came from the daily
// table.
type commitDailyFixture struct {
	repoID int64
	alice  string
	in     time.Time // a UTC day inside the window
}

func seedCommitDaily(t *testing.T, store *PostgresStore, ctx context.Context, owner string) commitDailyFixture {
	t.Helper()
	login := owner + "_alice"
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, owner)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, owner)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, owner+"@x")
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)

	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, $2, 'r', 1) RETURNING repo_id`, "https://github.com/"+owner+"/r", owner).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	alice := fmt.Sprintf("0100abcd-0000-0000-0000-%012x", crc32.ChecksumIEEE([]byte(owner))) // distinct per fixture owner
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_full_name, gh_activity_class)
		VALUES ($1::uuid, $2, 'Alice', 'active')
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, alice, login); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, $2, $2) ON CONFLICT (alias_email) DO UPDATE SET cntrb_id = EXCLUDED.cntrb_id`, alice, owner+"@x"); err != nil {
		t.Fatal(err)
	}
	in := utcDay(time.Now()).AddDate(0, 0, -7)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'c0ffee1', 'a.go', 'a', $2, '2026-09-29', $3, $4::uuid)`, repoID, owner+"@x", in.Add(10*time.Hour), alice); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = $1`, owner+"@x")
		_, _ = store.pool.Exec(bg, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	return commitDailyFixture{repoID: repoID, alice: alice, in: in}
}

func TestReplaceRepoCommitDailyReplacesAndClears(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_rep01")

	filled, err := store.RepoCommitDailyComplete(ctx, fx.repoID)
	if err != nil || filled {
		t.Fatalf("a repository with no daily rows is not complete: complete=%v err=%v", filled, err)
	}
	day2 := fx.in.AddDate(0, 0, 1)
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "_avcd_rep01@x", Commits: 5},
		{Day: day2, AuthorEmail: "other@x", Commits: 2},
	}, true); err != nil {
		t.Fatal(err)
	}
	if filled, err = store.RepoCommitDailyComplete(ctx, fx.repoID); err != nil || !filled {
		t.Fatalf("complete after a trimming replace: complete=%v err=%v", filled, err)
	}
	// A second replace carries only what the new walk saw: the old rows go.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: day2, AuthorEmail: "other@x", Commits: 3}}, true); err != nil {
		t.Fatal(err)
	}
	var n, sum int
	if err := store.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(commits), 0) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != 1 || sum != 3 {
		t.Fatalf("a replace is a replace: rows=%d sum=%d want 1/3", n, sum)
	}
	// Round 2 F2: a walk that swallowed commit writes upserts what it saw
	// and trims nothing — a sparser picture never replaces a fuller one,
	// and the rows stay fresh for what was walked.
	// Round 3 F3: and a bucket the partial walk saw SMALLER (a commit whose
	// rows failed this run still sits in the commits table) keeps its fuller
	// count — GREATEST — while a bucket that grew takes the new count.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: day2, AuthorEmail: "other@x", Commits: 2}, // was 3: kept
		{Day: fx.in, AuthorEmail: "late@x", Commits: 1}, // new: added
	}, false); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(commits), 0) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != 2 || sum != 4 {
		t.Fatalf("untrimmed replace keeps the earlier rows, keeps a fuller count and adds the new: rows=%d sum=%d want 2/4", n, sum)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: day2, AuthorEmail: "other@x", Commits: 2}}, true); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(commits), 0) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n, &sum); err != nil {
		t.Fatal(err)
	}
	if n != 1 || sum != 2 {
		t.Fatalf("a trimmed (clean) walk is the truth, smaller counts included: rows=%d sum=%d want 1/2", n, sum)
	}
	// Round 4 F1: a walk that changes nothing writes nothing — no new row
	// version, no dead tuple, no WAL (kate's storage stalls on writes; a
	// kernel fork has ~10^6 buckets). computed_at moves only on a change.
	var before, after time.Time
	if err := store.pool.QueryRow(ctx, `SELECT computed_at FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	// Round 5 F1: not merely un-updated — untouched. ON CONFLICT DO UPDATE
	// locks every conflicting row BEFORE it evaluates the WHERE, and a
	// tuple lock stamps the row's xmax with the locking transaction (and
	// dirties the page, and logs a record); so identical rows are left out
	// of the INSERT itself. The oracle: xmax does not change across the
	// identical walk (a lock would stamp a NEW transaction id; every
	// writing transaction has its own).
	var xmaxBefore, xmaxAfter int64
	if err := store.pool.QueryRow(ctx, `SELECT xmax::text::bigint FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&xmaxBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: day2, AuthorEmail: "other@x", Commits: 2}}, true); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT computed_at FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) {
		t.Fatalf("an identical walk must not rewrite the row: computed_at %v -> %v", before, after)
	}
	if err := store.pool.QueryRow(ctx, `SELECT xmax::text::bigint FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&xmaxAfter); err != nil {
		t.Fatal(err)
	}
	if xmaxAfter != xmaxBefore {
		t.Fatalf("an identical walk must not even lock the row (xmax %d -> %d): identical rows stay out of the INSERT", xmaxBefore, xmaxAfter)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: day2, AuthorEmail: "other@x", Commits: 2, AuthorLogin: "other"}}, true); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT computed_at FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.After(before) {
		t.Fatal("a walk that learns a login rewrites the row")
	}
	// A walk that saw nothing (an empty default branch) clears the table —
	// and that empty picture is complete (PR #226 review 5448678338): the
	// readers answer zero from it, never the commits table's stale rows.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("no rows after an empty replace, got %d", n)
	}
	if filled, err = store.RepoCommitDailyComplete(ctx, fx.repoID); err != nil || !filled {
		t.Fatalf("an empty trimmed replace is a complete picture: complete=%v err=%v", filled, err)
	}
}

func TestTopContributorsReadsTheDailyTableWhenFilledAndAligned(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_top02")
	since := utcDay(time.Now()).AddDate(0, 0, -30)

	commitsOf := func(since time.Time) int {
		t.Helper()
		rows, err := store.TopContributors(ctx, fx.repoID, since, time.Time{}, 20, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			if r.CntrbID == fx.alice {
				return r.Commits
			}
		}
		return -1
	}

	if got := commitsOf(since); got != 1 {
		t.Fatalf("unfilled: the commits table says 1, got %d", got)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: fx.in, AuthorEmail: "_avcd_top02@x", Commits: 5}}, true); err != nil {
		t.Fatal(err)
	}
	if got := commitsOf(since); got != 5 {
		t.Fatalf("filled + aligned window: the daily table says 5 (through the alias), got %d", got)
	}
	if got := commitsOf(since.Add(time.Hour)); got != 1 {
		t.Fatalf("filled but an unaligned window must fall back to the commits table (a day bucket cannot split): got %d", got)
	}
	// A day outside the window does not count.
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: since.AddDate(0, 0, -1), AuthorEmail: "_avcd_top02@x", Commits: 9}}, true); err != nil {
		t.Fatal(err)
	}
	if got := commitsOf(since); got != -1 {
		t.Fatalf("a day before the window counts nothing: got %d (want alice absent)", got)
	}
}

func TestRepoTimeSeriesReadsTheDailyTableWhenFilledAndAligned(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_ts003")
	since := utcDay(time.Now()).AddDate(0, 0, -30)

	weekly := func(since time.Time) int {
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
	if got := weekly(since); got != 1 {
		t.Fatalf("unfilled: the commits table says 1, got %d", got)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "_avcd_ts003@x", Commits: 3},
		{Day: fx.in.AddDate(0, 0, 1), AuthorEmail: "nobody@x", Commits: 2}, // unresolved authors still count as commits
	}, true); err != nil {
		t.Fatal(err)
	}
	if got := weekly(since); got != 5 {
		t.Fatalf("filled + aligned: the daily table says 5, got %d", got)
	}
	if got := weekly(since.Add(time.Minute)); got != 1 {
		t.Fatalf("unaligned window falls back: got %d", got)
	}
}

// The time series ends at the plausible bound on both paths: a commit (and
// a daily row) dated 2080 adds no week beyond it.
func TestRepoTimeSeriesEndsAtThePlausibleBound(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_fut10")
	far := time.Date(2080, 6, 1, 12, 0, 0, 0, time.UTC)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'fu7u4e1', 'z.go', 'a', '_avcd_fut10@x', '2080-06-01', $2)`, fx.repoID, far); err != nil {
		t.Fatal(err)
	}
	bound := LatestPlausibleCommitTime(time.Now())
	since := utcDay(time.Now()).AddDate(0, 0, -30)
	check := func(path string) {
		t.Helper()
		ts, err := store.GetRepoTimeSeries(ctx, fx.repoID, since, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		sum := 0
		for _, p := range ts.Commits {
			sum += p.Count
			if !p.WeekStart.Before(bound) {
				t.Errorf("%s: a week at %v lies beyond the plausible bound %v", path, p.WeekStart, bound)
			}
		}
		if sum != 1 {
			t.Errorf("%s: the one plausible commit counts, the 2080 one does not: got %d", path, sum)
		}
	}
	check("commits table")
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "_avcd_fut10@x", Commits: 1},
		{Day: utcDay(far), AuthorEmail: "_avcd_fut10@x", Commits: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	check("daily table")
	// An explicit until beyond the bound is clamped too.
	ts, err := store.GetRepoTimeSeries(ctx, fx.repoID, since, far.AddDate(1, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	farSum := 0
	for _, p := range ts.Commits {
		farSum += p.Count
		if !p.WeekStart.Before(bound) {
			t.Errorf("explicit far until: a week at %v beyond the bound", p.WeekStart)
		}
	}
	if farSum != 1 {
		t.Errorf("explicit far until: the one plausible commit still counts, got %d", farSum)
	}
}

// The series starts no earlier than the plausible floor on both paths: an
// epoch-dated commit (an unset clock) and a year-0001 daily row add no week.
func TestRepoTimeSeriesStartsAtThePlausibleFloor(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_pst11")
	epoch := time.Unix(0, 0).UTC()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'ep0ch001', 'y.go', 'a', '_avcd_pst11@x', '1970-01-01', $2)`, fx.repoID, epoch); err != nil {
		t.Fatal(err)
	}
	floor := EarliestPlausibleCommitTime()
	check := func(path string, since time.Time) {
		t.Helper()
		ts, err := store.GetRepoTimeSeries(ctx, fx.repoID, since, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		sum := 0
		for _, p := range ts.Commits {
			sum += p.Count
			// The floor (a Friday) sits inside the week that starts on the
			// Monday before it; no week may start before THAT Monday.
			if floorWeek := floor.AddDate(0, 0, -int((floor.Weekday()+6)%7)); p.WeekStart.Before(floorWeek) {
				t.Errorf("%s: a week at %v lies before the plausible floor's week %v", path, p.WeekStart, floorWeek)
			}
		}
		if sum != 1 {
			t.Errorf("%s: the one plausible commit counts, the epoch one does not: got %d", path, sum)
		}
	}
	check("commits table, open since", time.Time{})
	check("commits table, year-1 since", time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "_avcd_pst11@x", Commits: 1},
		{Day: time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), AuthorEmail: "_avcd_pst11@x", Commits: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	check("daily table, open since", time.Time{})
}

func TestFillRepoCommitDailyFromCommitsBackfillsOnce(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_fil04")
	// Two more file rows: a second commit on the same day by the same
	// author (two files — must count once), and one the next day by someone
	// unresolved.
	for _, f := range []string{"b.go", "c.go"} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
			VALUES ($1, 'c0ffee2', $2, 'a', '_avcd_fil04@x', '2026-09-29', $3)`, fx.repoID, f, fx.in.Add(20*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'c0ffee3', 'd.go', 'n', 'nobody@x', '2026-09-30', $2)`, fx.repoID, fx.in.AddDate(0, 0, 1)); err != nil {
		t.Fatal(err)
	}
	// A NULL-timestamp row buckets nowhere (the readers exclude it too).
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date)
		VALUES ($1, 'c0ffee4', 'e.go', 'n', 'nobody@x', '')`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_commits) VALUES ($1, 100, 'queued', NOW(), 3)`, fx.repoID); err != nil {
		t.Fatal(err)
	}

	pending, err := store.ListReposNeedingCommitDaily(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !containsID(pending, fx.repoID) {
		t.Fatal("a repository with commits and no daily rows is pending")
	}
	rows, err := store.FillRepoCommitDailyFromCommits(ctx, fx.repoID)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("two (day, author) rows expected, got %d", rows)
	}
	var day1, day2 int
	if err := store.pool.QueryRow(ctx, `SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = '_avcd_fil04@x' AND day = $2::date`, fx.repoID, fx.in).Scan(&day1); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT commits FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = 'nobody@x' AND day = $2::date`, fx.repoID, fx.in.AddDate(0, 0, 1)).Scan(&day2); err != nil {
		t.Fatal(err)
	}
	if day1 != 2 || day2 != 1 {
		t.Fatalf("distinct commits per (day, author): %d/%d want 2/1", day1, day2)
	}
	// F2: the backfill carries the commits table's stored identity.
	var storedID *string
	if err := store.pool.QueryRow(ctx, `SELECT cntrb_id::text FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = '_avcd_fil04@x'`, fx.repoID).Scan(&storedID); err != nil {
		t.Fatal(err)
	}
	if storedID == nil || *storedID != fx.alice {
		t.Fatalf("the backfill must carry cmt_ght_author_id into cntrb_id, got %v", storedID)
	}
	// Round 2 F5: a bucket whose commits carry two different ids is
	// ambiguous and carries none (SR-6), not the textually larger one.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'c0ffee5', 'f.go', 'a', '_avcd_fil04@x', '2026-09-29', $2, $3::uuid)`, fx.repoID, fx.in.Add(21*time.Hour), PlatformUUID(1, 4040).String()); err != nil {
		t.Fatal(err)
	}
	// The picture is complete; a rebuild means clearing the stamp first
	// (the fill skips a complete repository, 0.29.77).
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if _, err = store.FillRepoCommitDailyFromCommits(ctx, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT cntrb_id::text FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = '_avcd_fil04@x'`, fx.repoID).Scan(&storedID); err != nil {
		t.Fatal(err)
	}
	if storedID != nil {
		t.Fatalf("an ambiguous bucket carries no identity, got %v", *storedID)
	}
	pending, err = store.ListReposNeedingCommitDaily(ctx, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if containsID(pending, fx.repoID) {
		t.Fatal("a filled repository is no longer pending")
	}
	// Idempotent: a second fill (stamp cleared) replaces with the same rows.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET commit_daily_complete_at = NULL WHERE repo_id = $1`, fx.repoID); err != nil {
		t.Fatal(err)
	}
	if rows, err = store.FillRepoCommitDailyFromCommits(ctx, fx.repoID); err != nil || rows != 2 {
		t.Fatalf("second fill: rows=%d err=%v", rows, err)
	}
}

func containsID(ids []int64, id int64) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// ─── Review round 1 (summary/49) ────────────────────────────────

// F2: the old commits arm counted by cmt_ght_author_id, which the resolver's
// strategy 1 derives from a GitHub noreply address WITHOUT writing an alias
// row — so an alias-only join dropped every web-UI and squash-merge commit.
// A daily row carries a nullable cntrb_id: the facade stores the noreply
// identity (the same deterministic rule, PlatformUUID), the backfill
// carries the commits table's stored id, and the reader falls back to the
// alias rule and the contributor's own profile emails.
func TestTopContributorsDailyArmCountsStoredAndNoreplyIdentities(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_idn05")
	since := utcDay(time.Now()).AddDate(0, 0, -30)

	// The web committer's row is a LEGACY one: its cntrb_id is not the
	// deterministic PlatformUUID(1, 777) (round 2 F1 — the resolver keeps
	// the existing row's id when the login already exists), so only the
	// login rule finds it. Also a canonical-email contributor and an alias
	// owned by a soft-deleted contributor (round 2 F4/F8).
	legacyID := "0100abcd-0000-0000-0000-00000000c0de"
	canonID := PlatformUUID(1, 5051).String()
	deadID := PlatformUUID(1, 5052).String()
	for _, c := range []struct {
		id, login, canonical string
		deleted              int
	}{
		{legacyID, "_avcd_idn05_Web", "", 0},
		{canonID, "_avcd_idn05_canon", "canon@idn05.x", 0},
		{deadID, "_avcd_idn05_dead", "", 1},
	} {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, c.login)
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, cntrb_canonical, cntrb_deleted, gh_activity_class)
			VALUES ($1::uuid, $2, $2, $3, $4, 'active') ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, c.id, c.login, c.canonical, c.deleted); err != nil {
			t.Fatal(err)
		}
		login := c.login
		t.Cleanup(func() {
			_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
		})
	}
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = 'deadalias@idn05.x'`)
	if _, err := store.pool.Exec(ctx, `INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email) VALUES ($1::uuid, 'deadalias@idn05.x', 'deadalias@idn05.x')`, deadID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email = 'deadalias@idn05.x'`)
	})
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "777+_avcd_idn05_web@users.noreply.github.com", Commits: 4, AuthorLogin: "_avcd_idn05_web"}, // the facade stored the noreply login (lower-cased here: the match is LOWER(gh_login)); the row's id is legacy
		{Day: fx.in, AuthorEmail: "stored@x", Commits: 2, CntrbID: fx.alice},                                                  // the backfill carried the commits table's id (no alias)
		{Day: fx.in, AuthorEmail: "_avcd_idn05@x", Commits: 3},                                                                // identity through the alias rule
		{Day: fx.in, AuthorEmail: "canon@idn05.x", Commits: 6},                                                                // identity through the contributor's canonical email
		{Day: fx.in, AuthorEmail: "deadalias@idn05.x", Commits: 8},                                                            // an alias owned by a soft-deleted contributor: no identity
		{Day: fx.in, AuthorEmail: "nobody@x", Commits: 9},                                                                     // no identity: not a top contributor
	}, true); err != nil {
		t.Fatal(err)
	}
	rows, err := store.TopContributors(ctx, fx.repoID, since, time.Time{}, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, r := range rows {
		got[r.CntrbID] = r.Commits
	}
	if got[legacyID] != 4 {
		t.Errorf("a noreply author counts through the login rule, at the row the resolver KEEPS: got %d want 4 (%v)", got[legacyID], got)
	}
	if got[fx.alice] != 5 {
		t.Errorf("alice = 2 (stored id) + 3 (alias): got %d (%v)", got[fx.alice], got)
	}
	if got[canonID] != 6 {
		t.Errorf("a canonical-email match counts: got %d (%v)", got[canonID], got)
	}
	if len(got) != 3 {
		t.Errorf("an email with no live identity (none, or a soft-deleted alias owner) is not a top contributor: %v", got)
	}
}

// F4 / replace semantics: an identity the table already knows survives a
// facade replace that does not know it (the facade knows only noreply
// identities; the backfill and the commits table know more), and a
// (day, author) the new walk did not see goes away.
func TestReplaceRepoCommitDailyKeepsKnownIdentities(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_kep06")
	day2 := fx.in.AddDate(0, 0, 1)
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "a@x", Commits: 1, CntrbID: fx.alice},
		{Day: day2, AuthorEmail: "gone@x", Commits: 1},
	}, true); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "a@x", Commits: 7}, // no identity known to this walk
	}, true); err != nil {
		t.Fatal(err)
	}
	var n int
	var id *string
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT cntrb_id::text FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1 AND author_email = 'a@x'`, fx.repoID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if n != 1 || id == nil || *id != fx.alice {
		t.Fatalf("rows=%d (want 1: gone@x removed), id=%v (want alice kept)", n, id)
	}
}

// F3: an author email with invalid UTF-8 (kernel-style Latin-1 in git log)
// must neither fail the replace nor split the alias join: the fold key and
// the stored value are the scrubbed string, the same one the commits rows
// carry (the tracer scrubs every string argument — and now string slices).
func TestReplaceRepoCommitDailyScrubsInvalidUTF8(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_utf07")
	bad := "j\xf6rg@x" // Latin-1 ö
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{{Day: fx.in, AuthorEmail: bad, Commits: 1}}, true); err != nil {
		t.Fatalf("a replace with an invalid-UTF-8 email must succeed: %v", err)
	}
	var stored string
	if err := store.pool.QueryRow(ctx, `SELECT author_email FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, fx.repoID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != SafeUTF8(bad) {
		t.Fatalf("stored %q want the scrubbed %q", stored, SafeUTF8(bad))
	}
}

func TestUTF8ScrubCoversStringSlices(t *testing.T) {
	mine := []string{"fine", "b\xffad"}
	args := []any{"ok", mine}
	scrubArgsInPlace(args)
	got, ok := args[1].([]string)
	if !ok || got[1] != SafeUTF8("b\xffad") || got[0] != "fine" {
		t.Fatalf("a []string argument is scrubbed element by element, got %#v", args[1])
	}
	if mine[1] != "b\xffad" {
		t.Fatal("the caller's slice is not written through (round 2 F9): the scrubbed copy goes into args")
	}
}

// F4: the two writers serialise per repository with an advisory
// transaction lock, so the heal's minutes-long scan and the facade's replace
// cannot interleave (a 23505 that is not retried, or a union of two walks).
func TestRepoCommitDailyWritersTakeThePerRepositoryLock(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/commit_daily.go"))
	if !strings.Contains(src, "repoCommitDailyLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('repo_commit_daily:' || ($1::bigint)::text, 0))`") {
		t.Fatal("the per-repository advisory lock is one named statement")
	}
	for _, fn := range []string{"func (s *PostgresStore) ReplaceRepoCommitDaily(", "func (s *PostgresStore) FillRepoCommitDailyFromCommits("} {
		body := srctest.FuncBody(t, src, fn)
		lock := strings.Index(body, "tx.Exec(ctx, repoCommitDailyLockSQL, repoID)")
		if lock < 0 {
			t.Errorf("%s must take the per-repository advisory lock in its transaction", fn)
			continue
		}
		for _, write := range []string{"DELETE FROM", "INSERT INTO"} {
			if w := strings.Index(body, write); w >= 0 && w < lock {
				t.Errorf("%s: the lock comes before %s", fn, write)
			}
		}
	}
}

// Round 2 (pre-empted): the commits arm's identity fallbacks join
// contributors on cntrb_email / cntrb_canonical, which are NOT unique. A
// LEFT JOIN that matches two contributors fans one daily row out into two
// and SUM counts it twice; and attributing an ambiguous email to either is
// fabricated identity (SR-6). The fallbacks are scalar subqueries that
// attribute only an unambiguous match, so an email two contributors claim
// counts for neither, and a single claimant gets it once.
func TestTopContributorsDailyArmNeverFansOutOnSharedProfileEmails(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_fan08")
	since := utcDay(time.Now()).AddDate(0, 0, -30)
	ids := []string{PlatformUUID(1, 8801).String(), PlatformUUID(1, 8802).String(), PlatformUUID(1, 8803).String()}
	for i, id := range ids {
		login := fmt.Sprintf("_avcd_fan08_u%d", i)
		email := "shared@fan08.x"
		if i == 2 {
			email = "single@fan08.x"
		}
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, gh_activity_class)
			VALUES ($1::uuid, $2, $3, 'active') ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, id, login, email); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
		})
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "shared@fan08.x", Commits: 6}, // two contributors claim it
		{Day: fx.in, AuthorEmail: "single@fan08.x", Commits: 2}, // one does
	}, true); err != nil {
		t.Fatal(err)
	}
	rows, err := store.TopContributors(ctx, fx.repoID, since, time.Time{}, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	total := 0
	for _, r := range rows {
		got[r.CntrbID] = r.Commits
		total += r.Commits
	}
	if got[ids[2]] != 2 {
		t.Errorf("a single claimant gets its commits once: got %d (%v)", got[ids[2]], got)
	}
	if got[ids[0]] != 0 || got[ids[1]] != 0 {
		t.Errorf("an email two contributors claim attributes to neither (SR-6): %v", got)
	}
	if total != 2 {
		t.Errorf("no fan-out: the window's attributed commits total 2, got %d (%v)", total, got)
	}
}

// Round 2 F4 (SR-17): the arm's email rule IS the house rule
// (ResolveContributorIDByEmail: the unambiguous direct match over
// cntrb_email OR cntrb_canonical among live contributors first, then the
// alias joined to a live contributor). The two live in different shapes (a
// Go function running two statements; a scalar expression inside one arm),
// so their predicates are pinned equal here, with the arm's precedence.
func TestDailyArmEmailRuleMatchesTheHouseResolver(t *testing.T) {
	house := srctest.FuncBody(t, srctest.StripGoComments(srctest.Read(t, "internal/db/email_message_store.go")), "func (s *PostgresStore) ResolveContributorIDByEmail(")
	arm := srctest.StripGoComments(srctest.Read(t, "internal/db/commit_daily.go"))
	for _, predicate := range []string{
		"(cntrb_email = $1 OR cntrb_canonical = $1)",
		"COALESCE(cntrb_deleted, 0) = 0",
		"count(DISTINCT cntrb_id) AS n",
		"WHERE t.n = 1",
		"JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id",
		"COALESCE(c.cntrb_deleted, 0) = 0",
	} {
		if !strings.Contains(house, predicate) {
			t.Errorf("the house resolver no longer carries %q — update this pin and the arm together", predicate)
		}
		want := strings.ReplaceAll(predicate, "$1", "d.author_email")
		if !strings.Contains(arm, want) {
			t.Errorf("the daily arm's email rule must carry %q (the house rule, SR-17)", want)
		}
	}
	// Round 3 F4: the direct lookup is pinned WHOLE (an added predicate
	// changes its semantics while every fragment still appears), and the
	// precedence names every arm in order.
	directWhole := `(SELECT t.cntrb_id FROM (
                        SELECT (array_agg(DISTINCT cntrb_id::text))[1]::uuid AS cntrb_id,
                               count(DISTINCT cntrb_id) AS n
                        FROM aveloxis_data.contributors
                        WHERE (cntrb_email = d.author_email OR cntrb_canonical = d.author_email)
                          AND COALESCE(cntrb_deleted, 0) = 0
                    ) t WHERE t.n = 1)`
	if !strings.Contains(arm, directWhole) {
		t.Error("the direct email lookup must be the house statement verbatim with $1 → d.author_email, nothing added")
	}
	stored := strings.Index(arm, "COALESCE(\n                   d.cntrb_id,")
	forge := strings.Index(arm, "c.gh_user_id = d.author_gh_user_id")
	login := strings.Index(arm, "LOWER(c.gh_login) = LOWER(d.author_login)")
	direct := strings.Index(arm, directWhole)
	alias := strings.Index(arm, "a.alias_email = d.author_email")
	if !(stored >= 0 && forge > stored && login > forge && direct > login && alias > direct) {
		t.Errorf("precedence: the stored id, GitHub's numeric user id, the noreply login (the backfill's own rule), the direct unambiguous email match, the live alias — got stored=%d forge=%d login=%d direct=%d alias=%d", stored, forge, login, direct, alias)
	}
	// Round 3 F1: both gh_login indexes are PARTIAL (WHERE gh_login != ''),
	// and the planner cannot prove that from LOWER(c.gh_login) = …; the
	// literal guard is what makes them usable (the backfill's own v0.27.25
	// lesson), as is the forge id's partial index's.
	if !strings.Contains(arm, "LOWER(c.gh_login) = LOWER(d.author_login) AND c.gh_login <> ''") {
		t.Error("the login lookup must carry the literal c.gh_login <> '' guard, or no index serves it")
	}
	if !strings.Contains(arm, "c.gh_user_id = d.author_gh_user_id AND c.gh_user_id <> 0") {
		t.Error("the forge-id lookup must carry the literal c.gh_user_id <> 0 guard, the partial index's predicate")
	}
	for _, lookup := range []string{"LOWER(c.gh_login) = LOWER(d.author_login)", "c.gh_user_id = d.author_gh_user_id"} {
		at := strings.Index(arm, lookup)
		window := arm[at : at+400]
		if !strings.Contains(window, "HAVING COUNT(DISTINCT c.cntrb_id) = 1") {
			t.Errorf("the %s lookup must attribute only an unambiguous match (SR-6)", lookup)
		}
		// Round 4 F2: a merge loser still holding the id or login must not
		// make the live winner ambiguous.
		if !strings.Contains(window, "COALESCE(c.cntrb_deleted, 0) = 0") {
			t.Errorf("the %s lookup must exclude soft-deleted contributors", lookup)
		}
	}
}

// Round 3 F2/F4: a renamed GitHub account keeps its numeric id while the
// noreply address keeps the OLD login — the forge-id rule finds it; two
// live contributors sharing one gh_login are ambiguous and credit neither;
// a stored id outranks a conflicting alias.
func TestTopContributorsDailyArmResolvesByForgeIDAndRefusesAmbiguity(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	fx := seedCommitDaily(t, store, ctx, "_avcd_fid09")
	since := utcDay(time.Now()).AddDate(0, 0, -30)
	renamedID := "0100abcd-0000-0000-0000-0000000f1d09"
	dupA := PlatformUUID(1, 9091).String()
	dupB := PlatformUUID(1, 9092).String()
	for _, c := range []struct {
		id, login, ghLogin string
		uid                int64
		deleted            int
	}{
		{renamedID, "_avcd_fid09_new", "_avcd_fid09_new", 9090, 0},                        // renamed: the address still says the old login
		{PlatformUUID(1, 9099).String(), "_avcd_fid09_loser", "_avcd_fid09_old", 9090, 1}, // a merge loser still holding the id AND the old login: soft-deleted, so not ambiguous (round 4 F2)
		{dupA, "_avcd_fid09_dupa", "_avcd_fid09_dup", 0, 0},
		{dupB, "_avcd_fid09_dupb", "_avcd_fid09_dup", 0, 0},
	} {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, c.login)
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login, gh_user_id, cntrb_deleted, gh_activity_class)
			VALUES ($1::uuid, $2, $3, NULLIF($4, 0), $5, 'active') ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, c.id, c.login, c.ghLogin, c.uid, c.deleted); err != nil {
			t.Fatal(err)
		}
		login := c.login
		t.Cleanup(func() {
			_, _ = store.pool.Exec(context.Background(), `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
		})
	}
	if err := store.ReplaceRepoCommitDaily(ctx, fx.repoID, []CommitDailyRow{
		{Day: fx.in, AuthorEmail: "9090+_avcd_fid09_old@users.noreply.github.com", Commits: 4, AuthorLogin: "_avcd_fid09_old", AuthorGhUserID: 9090},
		{Day: fx.in, AuthorEmail: "_avcd_fid09_dup@users.noreply.github.com", Commits: 6, AuthorLogin: "_avcd_fid09_dup"},
		{Day: fx.in, AuthorEmail: "_avcd_fid09@x", Commits: 2, CntrbID: dupA}, // the alias says alice; the stored id wins
	}, true); err != nil {
		t.Fatal(err)
	}
	rows, err := store.TopContributors(ctx, fx.repoID, since, time.Time{}, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, r := range rows {
		got[r.CntrbID] = r.Commits
	}
	if got[renamedID] != 4 {
		t.Errorf("a renamed account's noreply commits count through GitHub's numeric id: got %d (%v)", got[renamedID], got)
	}
	if got[dupA] != 2 || got[dupB] != 0 {
		t.Errorf("two live contributors sharing one gh_login credit neither for it; the stored id outranks the alias: %v", got)
	}
	if got[fx.alice] != 0 {
		t.Errorf("the alias must not override a stored id: %v", got)
	}
}
