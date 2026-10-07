// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// O11 option 2 (operator decision, 2026-09-29, after the zephyr/pytorch 503s):
// the last and first commit times are stored per repository. Production has
// no (repo_id, cmt_author_timestamp) index — only a partial timestamp index
// from 2024 on — so MAX/MIN(cmt_author_timestamp) for one repository read
// every one of its per-file commit rows, and the giant repositories' /stats
// ran past nginx's 60 s. The facade maintains the two columns; the readers
// use them and fall back to the live scan only while a repository is unfilled.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func commitBoundsFixture(t *testing.T, slug string) (*PostgresStore, func(string) int64, func(int64, string, *time.Time), func(int64) (*time.Time, *time.Time)) {
	t.Helper()
	store, ctx := v0251Connect(t)
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`,
		} {
			cleanupExecRetry(ctx, store, sql, "%"+slug+"%")
		}
	}
	cleanup()
	t.Cleanup(cleanup)
	repo := func(name string) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, 'org', $2, 1) RETURNING repo_id`,
			"https://github.com/org/"+slug+name, slug+name).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	commit := func(repoID int64, hash string, at *time.Time) {
		t.Helper()
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
			VALUES ($1, $2, 'f.go', $3)`, repoID, hash, at)
	}
	bounds := func(repoID int64) (first, last *time.Time) {
		t.Helper()
		if err := store.pool.QueryRow(ctx,
			`SELECT first_commit_at, last_commit_at FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).
			Scan(&first, &last); err != nil {
			t.Fatal(err)
		}
		return first, last
	}
	return store, repo, commit, bounds
}

func TestRecordCommitBounds(t *testing.T) {
	store, repo, commit, bounds := commitBoundsFixture(t, "_avcmtbounds")
	ctx := t.Context()
	d := func(y int) time.Time { return time.Date(y, 3, 1, 0, 0, 0, 0, time.UTC) }
	p := func(t time.Time) *time.Time { return &t }
	eq := func(got *time.Time, want time.Time) bool { return got != nil && got.Equal(want) }

	// An unfilled repository is filled from the table in full — including
	// commits no longer on the default branch, which the run's walk does not
	// see — and NULL-dated rows are ignored.
	a := repo("a")
	commit(a, strings.Repeat("1", 40), p(d(2015)))
	commit(a, strings.Repeat("2", 40), p(d(2080))) // a bogus future-dated commit (2026-10-07: NVIDIA/nova carries 2080s): never a bound
	commit(a, strings.Repeat("3", 40), nil)
	if err := store.RecordCommitBounds(ctx, a, d(2020), d(2021)); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(a); !eq(f, d(2015)) || !eq(l, d(2015)) {
		t.Errorf("unfilled: first=%v last=%v; want the table's plausible 2015 and 2015 (the 2030 row is implausible)", f, l)
	}
	// Filled: the run's bounds widen the stored ones, never narrow them.
	if err := store.RecordCommitBounds(ctx, a, d(2016), d(2014)); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(a); !eq(f, d(2015)) || !eq(l, d(2015)) {
		t.Errorf("narrower run: first=%v last=%v; want unchanged", f, l)
	}
	if err := store.RecordCommitBounds(ctx, a, d(2010), d(2081)); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(a); !eq(f, d(2010)) || !eq(l, d(2015)) {
		t.Errorf("wider run with an implausible last: first=%v last=%v; want 2010 and 2015 (2081 ignored)", f, l)
	}
	// A stored bogus bound (written before the rule) is repaired on the next
	// run from the table's plausible maximum.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET last_commit_at = $2 WHERE repo_id = $1`, a, d(2080))
	if err := store.RecordCommitBounds(ctx, a, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(a); !eq(f, d(2010)) || !eq(l, d(2015)) {
		t.Errorf("repair: first=%v last=%v; want 2010 and the table's plausible 2015", f, l)
	}
	// The same for a stored bogus first (a repository whose only dated
	// commits were bogus when filled), and a repository whose only dated
	// commits are implausible stays NULL rather than filling from them.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET first_commit_at = $2 WHERE repo_id = $1`, a, d(2080))
	if err := store.RecordCommitBounds(ctx, a, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(a); !eq(f, d(2015)) || !eq(l, d(2015)) {
		t.Errorf("first repair: first=%v last=%v; want the table's plausible 2015 both", f, l)
	}
	// The past end (operator, the same day): an epoch-dated row never
	// becomes a first bound, and a stored one is repaired.
	y := repo("y")
	epoch := time.Unix(0, 0).UTC()
	commit(y, strings.Repeat("a", 40), p(epoch))
	commit(y, strings.Repeat("b", 40), p(d(2015)))
	if err := store.RecordCommitBounds(ctx, y, epoch, d(2015)); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(y); !eq(f, d(2015)) || !eq(l, d(2015)) {
		t.Errorf("epoch row: first=%v last=%v; want 2015 both", f, l)
	}
	// On a FILLED row the Go-side drop is what keeps a run's epoch first
	// out of LEAST (review round 3 F4): without it the stored first would
	// alternate between the epoch and its repair every other run.
	if err := store.RecordCommitBounds(ctx, y, epoch, d(2015)); err != nil {
		t.Fatal(err)
	}
	if f, _ := bounds(y); !eq(f, d(2015)) {
		t.Errorf("filled row, run first = epoch: first=%v; want 2015 kept", f)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET first_commit_at = $2 WHERE repo_id = $1`, y, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := store.RecordCommitBounds(ctx, y, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, _ := bounds(y); !eq(f, d(2015)) {
		t.Errorf("stored year-1 first repaired: first=%v; want 2015", f)
	}
	z := repo("z")
	commit(z, strings.Repeat("9", 40), p(d(2080)))
	if err := store.RecordCommitBounds(ctx, z, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(z); f != nil || l != nil {
		t.Errorf("only implausible commits: first=%v last=%v; want NULL both (never filled from them)", f, l)
	}
	// A run that wrote nothing (zero bounds) still fills an unfilled row.
	b := repo("b")
	commit(b, strings.Repeat("4", 40), p(d(2019)))
	if err := store.RecordCommitBounds(ctx, b, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(b); !eq(f, d(2019)) || !eq(l, d(2019)) {
		t.Errorf("zero-bounds run on an unfilled row: first=%v last=%v; want 2019 both", f, l)
	}
	// ...and changes nothing on a filled one.
	if err := store.RecordCommitBounds(ctx, b, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(b); !eq(f, d(2019)) || !eq(l, d(2019)) {
		t.Errorf("zero-bounds run on a filled row: first=%v last=%v", f, l)
	}
	// No dated commits at all: stays NULL (the readers' live fallback is
	// then a scan of a repository with nothing to find).
	c := repo("c")
	commit(c, strings.Repeat("5", 40), nil)
	if err := store.RecordCommitBounds(ctx, c, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(c); f != nil || l != nil {
		t.Errorf("no dated commits: first=%v last=%v; want NULL", f, l)
	}
}

// TestActivityBoundsReadTheStoredCommitTimes: the readers take the stored
// column and scan commits only while it is NULL.
func TestActivityBoundsReadTheStoredCommitTimes(t *testing.T) {
	store, repo, commit, _ := commitBoundsFixture(t, "_avcmtread")
	ctx := t.Context()
	d := func(y int) time.Time { return time.Date(y, 3, 1, 0, 0, 0, 0, time.UTC) }
	p := func(t time.Time) *time.Time { return &t }

	stored := repo("stored")
	commit(stored, strings.Repeat("6", 40), p(d(2020)))
	commit(stored, strings.Repeat("8", 40), p(d(2080))) // bogus: the live arms must skip it
	// A stored value that differs from the table proves which one is read.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET last_commit_at = $2, first_commit_at = $3 WHERE repo_id = $1`,
		stored, d(2025), d(2005))
	if la, ok, err := store.LastActivityAt(ctx, []int64{stored}); err != nil || !ok || !la.Equal(d(2025)) {
		t.Errorf("LastActivityAt = %v, %v, %v; want the stored 2025", la, ok, err)
	}
	// A stored bogus bound reads as unfilled: the live plausible maximum.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET last_commit_at = $2 WHERE repo_id = $1`, stored, d(2080))
	if la, ok, err := store.LastActivityAt(ctx, []int64{stored}); err != nil || !ok || !la.Equal(d(2020)) {
		t.Errorf("LastActivityAt with a bogus stored bound = %v, %v, %v; want the table's plausible 2020 (not the 2080 row)", la, ok, err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET last_commit_at = $2, first_commit_at = $3 WHERE repo_id = $1`, stored, d(2025), d(2080))
	if fa, ok, err := store.FirstActivityAt(ctx, []int64{stored}); err != nil || !ok || !fa.Equal(d(2020)) {
		t.Errorf("FirstActivityAt with a bogus stored first = %v, %v, %v; want the table's plausible 2020", fa, ok, err)
	}
	commit(stored, strings.Repeat("c", 40), p(time.Unix(0, 0).UTC())) // an unset clock: the live arm must skip it
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET first_commit_at = $2 WHERE repo_id = $1`, stored, time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))
	if fa, ok, err := store.FirstActivityAt(ctx, []int64{stored}); err != nil || !ok || !fa.Equal(d(2020)) {
		t.Errorf("FirstActivityAt with a stored year-1 first = %v, %v, %v; want the table's plausible 2020 (not the epoch row)", fa, ok, err)
	}
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET last_commit_at = $2, first_commit_at = $3 WHERE repo_id = $1`, stored, d(2025), d(2005))
	if fa, ok, err := store.FirstActivityAt(ctx, []int64{stored}); err != nil || !ok || !fa.Equal(d(2005)) {
		t.Errorf("FirstActivityAt = %v, %v, %v; want the stored 2005", fa, ok, err)
	}
	unfilled := repo("unfilled")
	commit(unfilled, strings.Repeat("7", 40), p(d(2021)))
	if la, ok, err := store.LastActivityAt(ctx, []int64{unfilled}); err != nil || !ok || !la.Equal(d(2021)) {
		t.Errorf("unfilled: LastActivityAt = %v, %v, %v; want the live 2021", la, ok, err)
	}
}

// TestActivityBoundsPreferTheStoredColumn pins the lazy form: the stored
// column first, the live scan only as COALESCE's fallback.
func TestActivityBoundsPreferTheStoredColumn(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/analytics_store.go"))
	// 2026-10-07: both reads take the stored column only when it is
	// plausible (a bogus bound, future or epoch, reads as unfilled) and
	// scan only plausible rows.
	for fn, needle := range map[string]string{
		"LastActivityAt":  "COALESCE( (SELECT last_commit_at FROM aveloxis_data.repos WHERE repo_id = r.id AND last_commit_at >= $3::timestamptz AND last_commit_at < $2::timestamptz), (SELECT cmt_author_timestamp FROM aveloxis_data.commits WHERE repo_id = r.id AND cmt_author_timestamp IS NOT NULL AND cmt_author_timestamp >= $3::timestamptz AND cmt_author_timestamp < $2::timestamptz",
		"FirstActivityAt": "COALESCE( (SELECT first_commit_at FROM aveloxis_data.repos WHERE repo_id = r.id AND first_commit_at >= $3::timestamptz AND first_commit_at < $2::timestamptz), (SELECT cmt_author_timestamp FROM aveloxis_data.commits WHERE repo_id = r.id AND cmt_author_timestamp IS NOT NULL AND cmt_author_timestamp >= $3::timestamptz AND cmt_author_timestamp < $2::timestamptz",
	} {
		body := srctest.NormalizeWS(srctest.FuncBody(t, src, "func (s *PostgresStore) "+fn+"("))
		if !strings.Contains(body, needle) {
			t.Errorf("%s: the commits arm must read the stored column first and scan commits only when it is NULL (or, for last, implausible)", fn)
		}
	}
}

// TestRecordCommitBoundsWritesNothingWhenUnchanged — round 3 side note: the
// gone recheck calls this for every still-gone repository every cadence; an
// unchanged row must not get a new tuple each time.
func TestRecordCommitBoundsWritesNothingWhenUnchanged(t *testing.T) {
	store, repo, commit, _ := commitBoundsFixture(t, "_avcmtnochange")
	ctx := t.Context()
	at := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	id := repo("x")
	commit(id, strings.Repeat("8", 40), &at)
	if err := store.RecordCommitBounds(ctx, id, time.Time{}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	var before, after string
	if err := store.pool.QueryRow(ctx, `SELECT ctid::text FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCommitBounds(ctx, id, at, at); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT ctid::text FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Errorf("an unchanged record rewrote the row (ctid %s → %s)", before, after)
	}
}

// TestRecordCommitBoundsNeverShrinksUnderConcurrency — review round 4 F1:
// with the bounds computed in a CTE joined to the UPDATE, a statement that
// waited on another writer's row lock applied values computed from the row
// it saw BEFORE the wait (READ COMMITTED re-checks only the WHERE), so a
// stored last_commit_at went backwards. The SET expressions must read the
// target row, which the post-wait recheck re-evaluates.
func TestRecordCommitBoundsNeverShrinksUnderConcurrency(t *testing.T) {
	store, repo, commit, bounds := commitBoundsFixture(t, "_avcmtrace")
	ctx := t.Context()
	d := func(y int) time.Time { return time.Date(y, 3, 1, 0, 0, 0, 0, time.UTC) }
	id := repo("x")
	early := d(2020)
	commit(id, strings.Repeat("a", 40), &early)

	// Writer A holds the row lock with a newer commit and wider bounds.
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	late := d(2025)
	if _, err := tx.Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
		VALUES ($1, $2, 'f.go', $3)`, id, strings.Repeat("b", 40), late); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE aveloxis_data.repos SET first_commit_at = $2, last_commit_at = $3 WHERE repo_id = $1`, id, early, late); err != nil {
		t.Fatal(err)
	}
	// Writer B starts while A holds the lock: it sees the row unfilled and
	// the table without A's commit, then waits.
	done := make(chan error, 1)
	go func() { done <- store.RecordCommitBounds(context.Background(), id, time.Time{}, time.Time{}) }()
	waiting := false
	for i := 0; i < 200 && !waiting; i++ {
		if err := store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%first_commit_at%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if !waiting {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if !waiting {
		t.Fatal("writer B never waited on A's row lock — the race was not set up")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f, l := bounds(id); f == nil || l == nil || !f.Equal(early) || !l.Equal(late) {
		t.Errorf("after the race: first=%v last=%v; want %v and %v — a filled column must never shrink", f, l, early, late)
	}
}
