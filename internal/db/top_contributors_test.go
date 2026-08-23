// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
	"time"
)

// v0.27.61 — TopContributors: the ranked per-contributor activity
// breakdown behind GET /repos/{id}/contributors/top. Source-contract
// pins first (the load-bearing SQL decisions), then an
// AVELOXIS_TEST_DB integration test that seeds one of every activity
// kind and checks the per-kind counts, ranking, window filter, and
// limit behave.

func topContributorsSQL(t *testing.T) string {
	t.Helper()
	src := readSourceFile(t, "contributions.go")
	if !strings.Contains(src, "func (s *PostgresStore) TopContributors(") {
		t.Fatal("TopContributors method missing from contributions.go")
	}
	return src
}

// The commits arm must range-filter on cmt_author_timestamp
// (TIMESTAMPTZ) — cmt_author_date is TEXT and the ::DATE cast pattern
// was one of the v0.27.5 dead-endpoint bugs. And commits are stored
// one row per FILE per commit, so the count must collapse on
// DISTINCT cmt_commit_hash or a 30-file commit counts 30×.
func TestTopContributorsCommitArmIsSafe(t *testing.T) {
	src := topContributorsSQL(t)
	if !strings.Contains(src, "COUNT(DISTINCT c.cmt_commit_hash)") &&
		!strings.Contains(src, "COUNT(DISTINCT cmt_commit_hash)") {
		t.Error("commits arm must COUNT(DISTINCT cmt_commit_hash) — the table is one row per file per commit")
	}
	if strings.Contains(src, "cmt_author_date::DATE") {
		t.Error("cmt_author_date is TEXT — range-filter on cmt_author_timestamp (TIMESTAMPTZ) instead")
	}
}

// Reviews are a first-class arm (the plan's explicit ask — review
// load is invisible in commit/PR counts alone), windowed on
// submitted_at like the contributorsInWindowCTE reviews arm.
func TestTopContributorsHasReviewsArm(t *testing.T) {
	src := topContributorsSQL(t)
	if !strings.Contains(src, "pull_request_reviews") {
		t.Error("TopContributors must count pull_request_reviews (reviews arm)")
	}
	if !strings.Contains(src, "submitted_at") {
		t.Error("reviews arm must window on submitted_at")
	}
}

// Identity join contract: same COALESCE login chain + soft-delete
// filter as GetRepoContributors, so the two contributor surfaces can
// never disagree about who a cntrb_id is.
func TestTopContributorsIdentityJoin(t *testing.T) {
	src := topContributorsSQL(t)
	if !strings.Contains(src, "COALESCE(NULLIF(c.cntrb_login, ''), c.gh_login, c.gl_username, '')") {
		t.Error("TopContributors must use the shared COALESCE login chain")
	}
	if !strings.Contains(src, "COALESCE(c.cntrb_deleted, 0) = 0") {
		t.Error("TopContributors must exclude soft-deleted (merge-tombstone) contributors")
	}
	if !strings.Contains(src, "gh_activity_class") {
		t.Error("TopContributors rows must carry the v0.27.57 activity_class")
	}
}

// v0.28.1 (A1): the hide-bots filter covers every non-human account
// TYPE, not just 'Bot'. Production 2026-08-23: codecov's contributor
// row is gh_type='Organization' — enrichment-by-login resolved the
// legacy login-only Bot actor "codecov" to Codecov's ORG account
// (id 8226205) — so the narrow = 'Bot' test let it through the
// hide-bots toggle. Organizations and fine-grained-PAT bot actors
// appearing as "contributors" are definitionally automation.
// Mannequin deliberately survives: import placeholders standing in
// for unmatched humans, not automation. Machine accounts GitHub
// types as plain 'User' (codecov-io, codecov-commenter) are caught
// by the shared githubSystemAccounts curated list instead.
func TestTopContributorsBotFilterCoversNonHumanTypes(t *testing.T) {
	src := topContributorsSQL(t)
	if !strings.Contains(src, `COALESCE(c.gh_type, '') IN ('Bot', 'ProgrammaticAccessBot', 'Organization')`) {
		t.Error("excludeBots must test gh_type IN ('Bot','ProgrammaticAccessBot','Organization') — codecov is typed Organization")
	}
	if strings.Contains(src, `COALESCE(c.gh_type, '') = 'Bot'`) {
		t.Error("the narrow = 'Bot' form must be gone from TopContributors (superseded by the IN list)")
	}
	if !strings.Contains(src, "githubSystemAccounts") ||
		!strings.Contains(src, `LOWER(c.cntrb_login) = ANY(`) {
		t.Error("excludeBots must also exclude the curated githubSystemAccounts list by login (machine accounts typed 'User')")
	}
	if strings.Contains(src, `'Mannequin'`) {
		t.Error("Mannequin must NOT appear in the filter — mannequins stand in for unmatched humans")
	}
}

// ─── Integration (AVELOXIS_TEST_DB) ─────────────────────────────

func TestTopContributorsEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

	// Scoped fixture: everything under repo_owner '_avtopc' so
	// parallel test packages can't interfere.
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avtopc')`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avtopc'`)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login IN ('_avtopc_alice', '_avtopc_bob')`)

	var repoID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ('https://github.com/_avtopc/r', '_avtopc', 'r', 1) RETURNING repo_id`).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.pull_request_reviews WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, repoID)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login IN ('_avtopc_alice', '_avtopc_bob')`)
	})

	seed := func(login, id string) string {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_full_name, gh_activity_class)
			VALUES ($1::uuid, $2, $3, 'active')
			ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, id, login, login+" Full"); err != nil {
			t.Fatal(err)
		}
		return id
	}
	alice := seed("_avtopc_alice", "0100abcd-0000-0000-0000-00000000a1ce")
	bob := seed("_avtopc_bob", "0100abcd-0000-0000-0000-000000000b0b")

	in := time.Now().AddDate(0, -1, 0) // inside the window
	out := time.Now().AddDate(-3, 0, 0)

	// Alice: 1 commit (TWO file rows, same hash — must count once),
	// 1 issue, 1 PR, 1 review, 1 comment. Bob: 1 commit only.
	for _, f := range []string{"a.go", "b.go"} {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
			VALUES ($1, 'aaaa111', $2, 'a', 'a@x', '2026-07-01', $3, $4::uuid)`, repoID, f, in, alice); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'bbbb222', 'c.go', 'b', 'b@x', '2026-07-01', $2, $3::uuid)`, repoID, in, bob); err != nil {
		t.Fatal(err)
	}
	// Out-of-window commit for alice must NOT count.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
		VALUES ($1, 'old0333', 'd.go', 'a', 'a@x', '2023-07-01', $2, $3::uuid)`, repoID, out, alice); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id, created_at, issue_title)
		VALUES ($1, 991, 991, $2::uuid, $3, 't')`, repoID, alice, in); err != nil {
		t.Fatal(err)
	}
	var prID int64
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, author_id, created_at)
		VALUES ($1, 881, 881, $2::uuid, $3) RETURNING pull_request_id`, repoID, alice, in).Scan(&prID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.pull_request_reviews (repo_id, pull_request_id, platform_review_id, platform_id, cntrb_id, submitted_at)
		VALUES ($1, $2, 771, 1, $3::uuid, $4)`, repoID, prID, alice, in); err != nil {
		t.Fatal(err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, cntrb_id, msg_text, msg_timestamp)
		VALUES ($1, 661, 1, $2::uuid, 'hi', $3)`, repoID, alice, in); err != nil {
		t.Fatal(err)
	}

	rows, err := store.TopContributors(ctx, repoID, time.Time{}, time.Time{}, 20, false)
	if err != nil {
		t.Fatalf("TopContributors: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("want 2 contributors, got %d: %+v", len(rows), rows)
	}
	// Alice ranks first (total 5 vs bob 1). Her commit count is 1
	// (two file rows, one hash) + the out-of-window default window
	// includes the old commit — wait, no: zero since/until = all time,
	// so alice has 2 commits all-time. Use an explicit window below
	// for the window assertion; here all-time totals:
	a := rows[0]
	if a.Login != "_avtopc_alice" {
		t.Fatalf("expected alice ranked first, got %+v", a)
	}
	if a.Commits != 2 || a.Issues != 1 || a.PRs != 1 || a.Reviews != 1 || a.Comments != 1 {
		t.Errorf("alice all-time counts wrong: %+v", a)
	}
	if a.Total != 6 {
		t.Errorf("alice total = %d, want 6", a.Total)
	}
	if a.ActivityClass != "active" {
		t.Errorf("activity_class not carried: %+v", a)
	}
	if rows[1].Login != "_avtopc_bob" || rows[1].Commits != 1 || rows[1].Total != 1 {
		t.Errorf("bob row wrong: %+v", rows[1])
	}

	// Windowed: the 3-year-old commit falls out — alice commits = 1
	// (deduped from two file rows), total 5.
	since := time.Now().AddDate(0, -2, 0)
	rows, err = store.TopContributors(ctx, repoID, since, time.Time{}, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].Commits != 1 {
		t.Errorf("windowed alice commits = %d, want 1 (DISTINCT hash + window filter)", rows[0].Commits)
	}
	if rows[0].Total != 5 {
		t.Errorf("windowed alice total = %d, want 5", rows[0].Total)
	}

	// Limit applies AFTER ranking: limit 1 returns only alice.
	rows, err = store.TopContributors(ctx, repoID, time.Time{}, time.Time{}, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Login != "_avtopc_alice" {
		t.Errorf("limit 1 must return exactly the top-ranked row, got %+v", rows)
	}

	// v0.27.69 — the bot filter. Three marker classes, seeded to
	// mirror the live k8s finding (k8s-ci-robot is gh_type='User' —
	// only the -robot suffix catches it).
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avtopc_x%'`)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avtopc_x%'`)
	})
	seedBot := func(login, id, ghType string) {
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_type)
			VALUES ($1::uuid, $2, $3)
			ON CONFLICT (cntrb_login) WHERE cntrb_login != '' DO NOTHING`, id, login, ghType); err != nil {
			t.Fatal(err)
		}
		if _, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name, cmt_author_email, cmt_author_date, cmt_author_timestamp, cmt_ght_author_id)
			VALUES ($1, $2, 'z.go', 'x', 'x@x', '2026-07-01', $3, $4::uuid)`,
			repoID, "h"+id[len(id)-6:], in, id); err != nil {
			t.Fatal(err)
		}
	}
	seedBot("_avtopc_x[bot]", "0100abcd-0000-0000-0000-0000000000b1", "Bot")      // App bot
	seedBot("_avtopc_x-ci-robot", "0100abcd-0000-0000-0000-0000000000b2", "User") // k8s-class machine user
	seedBot("_avtopc_x-bot", "0100abcd-0000-0000-0000-0000000000b3", "User")      // hyphen-bot machine user
	seedBot("_avtopc_xtalbot", "0100abcd-0000-0000-0000-0000000000b4", "User")    // HUMAN surname — must survive

	// v0.28.1 (A1): non-human account TYPES. Organization is the
	// codecov production shape; ProgrammaticAccessBot is the
	// fine-grained-PAT actor type; Mannequin is an import
	// placeholder for an unmatched HUMAN and must survive.
	seedBot("_avtopc_xorg", "0100abcd-0000-0000-0000-0000000000b5", "Organization")
	seedBot("_avtopc_xpab", "0100abcd-0000-0000-0000-0000000000b6", "ProgrammaticAccessBot")
	seedBot("_avtopc_xmann", "0100abcd-0000-0000-0000-0000000000b7", "Mannequin")

	all, err := store.TopContributors(ctx, repoID, time.Time{}, time.Time{}, 20, false)
	if err != nil {
		t.Fatal(err)
	}
	filtered, err := store.TopContributors(ctx, repoID, time.Time{}, time.Time{}, 20, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != len(filtered)+5 {
		t.Errorf("excludeBots must drop exactly the 5 automation rows: all=%d filtered=%d", len(all), len(filtered))
	}
	for _, r := range filtered {
		switch r.Login {
		case "_avtopc_x[bot]", "_avtopc_x-ci-robot", "_avtopc_x-bot", "_avtopc_xorg", "_avtopc_xpab":
			t.Errorf("automation account %q survived the filter", r.Login)
		}
	}
	survivors := map[string]bool{}
	for _, r := range filtered {
		survivors[r.Login] = true
	}
	if !survivors["_avtopc_xtalbot"] {
		t.Error("human login ending in 'bot' without a separator (talbot) must NOT be filtered")
	}
	if !survivors["_avtopc_xmann"] {
		t.Error("Mannequin rows must NOT be filtered — they stand in for unmatched humans")
	}
}
