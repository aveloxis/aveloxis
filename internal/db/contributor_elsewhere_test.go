// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"
)

// v0.27.64 — the read surface over the v0.27.58 daily-history tables:
// ContributorsElsewhere ("where else are this repo's top contributors
// active?") and ContributorActivity (one person's cross-repo monthly
// view). Source pins on the honesty-critical decisions, then an
// integration test.

func elsewhereSQL(t *testing.T) string {
	t.Helper()
	return readSourceFile(t, "contributor_elsewhere_store.go")
}

// Every contributor entry must carry gh_history_backfilled_at: an
// un-backfilled contributor has NO history rows, and without the
// stamp the frontend would render that absence as "active nowhere
// else" — a lie. "History pending" and "zero elsewhere activity" must
// be distinguishable (the v0.27.58 honesty rule).
func TestElsewhereCarriesBackfilledAt(t *testing.T) {
	src := elsewhereSQL(t)
	if !strings.Contains(src, "gh_history_backfilled_at") {
		t.Error("elsewhere/activity responses must carry contributors.gh_history_backfilled_at")
	}
}

// The current repo is excluded from "elsewhere" case-insensitively:
// repo_full_name is GitHub-canonical TEXT and our stored owner/name
// casing can drift from it (the v0.25.32 case lesson).
func TestElsewhereExcludesCurrentRepoCaseInsensitively(t *testing.T) {
	src := elsewhereSQL(t)
	if !strings.Contains(src, "LOWER(ad.repo_full_name)") {
		t.Error("current-repo exclusion must compare LOWER(repo_full_name) — GitHub canonical casing can differ from ours")
	}
}

// The repo-link annotation join is restricted to platform 1: the
// history data comes from GitHub's contributionsCollection, so a
// same-path GitLab repo must not be linked (and the platform filter
// is what keeps the join at most 1:1 under uq_repos_repo_git_ci).
func TestElsewhereRepoLinkIsGitHubOnly(t *testing.T) {
	src := elsewhereSQL(t)
	if !strings.Contains(src, "platform_id = 1") {
		t.Error("repo_id link annotation must join repos with platform_id = 1 only")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestContributorElsewhereEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.Close()

	clean := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_days WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avelse%')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_day_totals WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avelse%')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avelse')`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avelse'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avelse%'`)
	}
	clean()
	t.Cleanup(clean)

	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/_avelse/home', '_avelse', 'home', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	// A tracked repo the contributor is ALSO active in — proves the
	// repo_id link annotation.
	var otherID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/_avelse/other', '_avelse', 'other', 1) RETURNING repo_id`).Scan(&otherID); err != nil {
		t.Fatal(err)
	}

	cid := "0100beef-0000-0000-0000-00000000e15e"
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_history_backfilled_at)
		VALUES ($1::uuid, '_avelse_carol', NOW())
		ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, cid); err != nil {
		t.Fatal(err)
	}
	// Make carol the repo's top contributor: one commit in-window.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'e15e111', 'a.go', 'c', 'c@x', '2026-07-01', NOW() - interval '5 days', $2::uuid)`, repoID, cid); err != nil {
		t.Fatal(err)
	}

	day := func(offset int, repoFullName string, commits int) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributor_activity_days (cntrb_id, day, repo_full_name, commit_count)
			VALUES ($1::uuid, CURRENT_DATE - $2::int, $3, $4)`, cid, offset, repoFullName, commits); err != nil {
			t.Fatal(err)
		}
	}
	// History: the home repo under GITHUB'S canonical casing (case
	// differs from ours — must still be excluded), a tracked other
	// repo, and an untracked one.
	day(3, "_AVelse/Home", 2)
	day(3, "_avelse/other", 5)
	day(4, "_avelse/other", 1)
	day(5, "torvalds/linux", 7)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_activity_day_totals (cntrb_id, day, total_contributions)
		VALUES ($1::uuid, CURRENT_DATE - 3, 15)`, cid); err != nil {
		t.Fatal(err)
	}

	since := time.Now().AddDate(0, 0, -180)
	rows, err := store.ContributorsElsewhere(ctx, repoID, since, 10, 10, false)
	if err != nil {
		t.Fatalf("ContributorsElsewhere: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 contributor, got %d: %+v", len(rows), rows)
	}
	c := rows[0]
	if c.Login != "_avelse_carol" {
		t.Fatalf("unexpected contributor: %+v", c)
	}
	if c.BackfilledAt == nil {
		t.Error("backfilled_at must be carried on every contributor entry")
	}
	names := map[string]ElsewhereRepo{}
	for _, er := range c.Elsewhere {
		names[er.RepoFullName] = er
	}
	if _, ok := names["_AVelse/Home"]; ok {
		t.Error("the current repo must be excluded from elsewhere (case-insensitively)")
	}
	other, ok := names["_avelse/other"]
	if !ok {
		t.Fatalf("tracked other repo missing from elsewhere: %+v", c.Elsewhere)
	}
	if other.RepoID == nil || *other.RepoID != otherID {
		t.Errorf("tracked repo must carry its repo_id link, got %+v", other)
	}
	if other.ActiveDays != 2 || other.Commits != 6 {
		t.Errorf("other repo aggregation wrong (want 2 active days, 6 commits): %+v", other)
	}
	linux, ok := names["torvalds/linux"]
	if !ok {
		t.Fatalf("untracked repo missing from elsewhere: %+v", c.Elsewhere)
	}
	if linux.RepoID != nil {
		t.Error("untracked repo must carry a nil repo_id")
	}

	// The person-level view: monthly buckets + day totals.
	act, err := store.ContributorActivity(ctx, cid, 24)
	if err != nil {
		t.Fatalf("ContributorActivity: %v", err)
	}
	if act.BackfilledAt == nil {
		t.Error("activity view must carry backfilled_at")
	}
	if len(act.DayTotals) != 1 || act.DayTotals[0].Total != 15 {
		t.Errorf("day_totals wrong: %+v", act.DayTotals)
	}
	var sawLinux bool
	for _, r := range act.Repos {
		if r.RepoFullName == "torvalds/linux" && len(r.Months) == 1 && r.Months[0].Commits == 7 {
			sawLinux = true
		}
	}
	if !sawLinux {
		t.Errorf("monthly per-repo buckets missing torvalds/linux: %+v", act.Repos)
	}
}
