// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// O11 option 1 (2026-09-29): /repos/{id}/stats timed out at nginx's 60 s on
// the largest repositories (zephyr, pytorch; nginx's maintenance handler
// turned the 504 into a 503 page). LastActivityAt aggregated MAX(...) over
// `repo_id = ANY($1)`, which defeats PostgreSQL's one-row MIN/MAX shortcut:
// measured on a synthetic 12M-row copy, the issue and PR arms scanned every
// one of a giant repository's index entries. The rewrite asks each arm for
// ONE row per repository (ORDER BY … LIMIT 1 on the (repo_id, ts) index).
// These pin that the answer is unchanged — including the trap the rewrite
// introduces: DESC order puts NULLs FIRST, so every arm must skip them.

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestActivityBoundsAcrossArmsAndRepos(t *testing.T) {
	store, ctx := v0251Connect(t)
	const slug = "_avactbounds"
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE $1)`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE $1`,
		} {
			cleanupExecRetry(ctx, store, sql, "%"+slug+"%")
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	d := func(y int, m time.Month, day int) time.Time { return time.Date(y, m, day, 12, 0, 0, 0, time.UTC) }
	repo := func(name string, created time.Time) int64 {
		t.Helper()
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, created_at)
			VALUES ($1, 'org', $2, 1, $3) RETURNING repo_id`,
			"https://github.com/org/"+slug+name, slug+name, created).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	issue := func(repoID int64, n int, at *time.Time) {
		t.Helper()
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, created_at)
			VALUES ($1, $2, $3, $4)`, repoID, repoID*1000+int64(n), n, at)
	}
	pr := func(repoID int64, n int, at *time.Time) {
		t.Helper()
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, created_at)
			VALUES ($1, $2, $3, $4)`, repoID, repoID*1000+int64(n), n, at)
	}
	commit := func(repoID int64, hash string, at *time.Time) {
		t.Helper()
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
			VALUES ($1, $2, 'f.go', $3)`, repoID, hash, at)
	}
	p := func(t time.Time) *time.Time { return &t }

	// A: each arm has a NULL-dated row (DESC puts it first) and real rows;
	// the latest is the PR, the earliest the commit; repos.created_at is
	// later than all of it (a floor candidate FirstActivityAt must ignore
	// here because an earlier activity exists).
	a := repo("a", d(2021, 1, 1))
	issue(a, 1, nil)
	issue(a, 2, p(d(2018, 3, 1)))
	issue(a, 3, p(d(2019, 3, 1)))
	pr(a, 1, nil)
	pr(a, 2, p(d(2020, 5, 5)))
	commit(a, strings.Repeat("a", 40), nil)
	commit(a, strings.Repeat("b", 40), p(d(2016, 7, 7)))
	commit(a, strings.Repeat("c", 40), p(d(2017, 7, 7)))
	// B: commits only, later than anything in A.
	b := repo("b", d(2022, 1, 1))
	commit(b, strings.Repeat("d", 40), p(d(2023, 2, 2)))
	// C: only NULL-dated activity — no last activity at all.
	c := repo("c", d(2015, 1, 1))
	issue(c, 1, nil)
	commit(c, strings.Repeat("e", 40), nil)

	// D, E, F: each arm in turn holds the maximum next to a NULL-dated row
	// (the DESC NULLS FIRST trap per arm), with an older row in another arm.
	dIss := repo("d", d(2010, 1, 1))
	issue(dIss, 1, nil)
	issue(dIss, 2, p(d(2024, 4, 4)))
	commit(dIss, strings.Repeat("f", 40), p(d(2011, 1, 1)))
	ePR := repo("e", d(2010, 1, 1))
	pr(ePR, 1, nil)
	pr(ePR, 2, p(d(2024, 5, 5)))
	issue(ePR, 1, p(d(2011, 1, 1)))
	fCmt := repo("f", d(2010, 1, 1))
	commit(fCmt, strings.Repeat("1", 40), nil)
	commit(fCmt, strings.Repeat("2", 40), p(d(2024, 6, 6)))
	pr(fCmt, 1, p(d(2011, 1, 1)))
	for _, tc := range []struct {
		id   int64
		want time.Time
		arm  string
	}{{dIss, d(2024, 4, 4), "issues"}, {ePR, d(2024, 5, 5), "pull requests"}, {fCmt, d(2024, 6, 6), "commits"}} {
		if la, ok, err := store.LastActivityAt(ctx, []int64{tc.id}); err != nil || !ok || !la.Equal(tc.want) {
			t.Errorf("LastActivityAt with the max in the %s arm beside a NULL row = %v, %v, %v; want %v", tc.arm, la, ok, err, tc.want)
		}
	}
	// The mirror for FirstActivityAt: each arm holds the minimum beside a
	// NULL row (ASC sorts NULLs last, but the skip must hold either way).
	gIss := repo("g", d(2030, 1, 1))
	issue(gIss, 1, nil)
	issue(gIss, 2, p(d(2001, 1, 1)))
	pr(gIss, 1, p(d(2005, 1, 1)))
	if fa, ok, err := store.FirstActivityAt(ctx, []int64{gIss}); err != nil || !ok || !fa.Equal(d(2001, 1, 1)) {
		t.Errorf("FirstActivityAt with the min in the issues arm = %v, %v, %v; want 2001-01-01", fa, ok, err)
	}

	cases := []struct {
		ids        []int64
		last       time.Time
		lastOK     bool
		first      time.Time
		firstOK    bool
		whatItTest string
	}{
		{[]int64{a}, d(2020, 5, 5), true, d(2016, 7, 7), true, "NULLs skipped in every arm; the max is the PR, the min the commit"},
		{[]int64{a, b}, d(2023, 2, 2), true, d(2016, 7, 7), true, "several repositories: the max and min across them"},
		{[]int64{c}, time.Time{}, false, d(2015, 1, 1), true, "only NULL activity: no last activity; creation is the first-activity floor"},
		{[]int64{c, a}, d(2020, 5, 5), true, d(2015, 1, 1), true, "a repository with nothing contributes nothing to the max"},
	}
	for _, tc := range cases {
		la, ok, err := store.LastActivityAt(ctx, tc.ids)
		if err != nil || ok != tc.lastOK || (ok && !la.Equal(tc.last)) {
			t.Errorf("LastActivityAt(%v) = %v, %v, %v; want %v, %v (%s)", tc.ids, la, ok, err, tc.last, tc.lastOK, tc.whatItTest)
		}
		fa, ok, err := store.FirstActivityAt(ctx, tc.ids)
		if err != nil || ok != tc.firstOK || (ok && !fa.Equal(tc.first)) {
			t.Errorf("FirstActivityAt(%v) = %v, %v, %v; want %v, %v (%s)", tc.ids, fa, ok, err, tc.first, tc.firstOK, tc.whatItTest)
		}
	}
}

// TestActivityBoundsAskOneRowPerRepository pins the plan-shaping form: no
// aggregate over `repo_id = ANY(...)` (the measured full-index scan), and
// each arm is a per-repository ORDER BY … LIMIT 1 over the unnested ids.
func TestActivityBoundsAskOneRowPerRepository(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/analytics_store.go"))
	for _, fn := range []string{"LastActivityAt", "FirstActivityAt"} {
		body := srctest.FuncBody(t, src, "func (s *PostgresStore) "+fn+"(")
		if strings.Contains(body, "= ANY(") {
			t.Errorf("%s: an aggregate over repo_id = ANY(...) cannot take the one-row index path", fn)
		}
		if !strings.Contains(body, "FROM unnest($1::bigint[]) AS r(id)") || strings.Count(body, "LIMIT 1") < 3 {
			t.Errorf("%s: each arm must ask one row per repository (unnest the ids, ORDER BY … LIMIT 1 per arm)", fn)
		}
	}
}
