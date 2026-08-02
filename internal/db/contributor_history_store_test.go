// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// contributor_history_store_test.go — TDD suite for the v0.27.58 daily
// contributor-history store. Contracts:
//   - two new tables (contributor_activity_days keyed (cntrb, day,
//     repo_full_name); contributor_activity_day_totals keyed (cntrb,
//     day)) with the house cntrb_id FK clause — repo_full_name is
//     deliberately TEXT, not a repos FK: these are mostly repositories
//     Aveloxis does not track;
//   - claim: never-backfilled first, active-classified contributors
//     prioritized, jittered quarterly cooldown;
//   - StoreContributorActivityHistory writes rows AND the
//     gh_history_backfilled_at stamp in ONE transaction (data and
//     freshness can't drift); MarkHistoryBackfilled is the dataless
//     path (deleted/renamed users).

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestSchemaDeclaresActivityHistoryTables(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	flat := strings.Join(strings.Fields(schema), " ")
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_activity_days",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.contributor_activity_day_totals",
		"UNIQUE (cntrb_id, day, repo_full_name)",
		"UNIQUE (cntrb_id, day)",
	} {
		if !strings.Contains(flat, needle) {
			t.Errorf("schema.sql must contain %q", needle)
		}
	}
	if !strings.Contains(flat, "idx_contributors_history_backfilled ON aveloxis_data.contributors (gh_history_backfilled_at ASC NULLS FIRST)") {
		t.Error("the history claim needs idx_contributors_history_backfilled declared ASC NULLS FIRST (v0.27.8 lesson)")
	}
}

func TestMigrateAddsActivityHistoryPieces(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	for _, needle := range []string{
		"contributor_activity_days",
		"contributor_activity_day_totals",
		`"gh_history_backfilled_at"`,
		"idx_contributors_history_backfilled",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must handle %q for existing fleets", needle)
		}
	}
}

func TestHistoryClaimContract(t *testing.T) {
	src := readSourceFile(t, "contributor_history_store.go")
	idx := strings.Index(src, "func (s *PostgresStore) GetContributorsForHistoryBackfill(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsForHistoryBackfill")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	flat := strings.Join(strings.Fields(body), " ")
	for _, needle := range []string{
		"gh_history_backfilled_at IS NULL",
		"ORDER BY gh_history_backfilled_at ASC NULLS FIRST",
		"BreadthCooldownJitterFrac",
		// Active-first bootstrap: contributors whose trailing-year class
		// shows activity get their history first — they're the rows the
		// frontend renders.
		"'public-active'",
		"'private-active'",
	} {
		if !strings.Contains(flat, needle) {
			t.Errorf("history claim must contain %q", needle)
		}
	}
}

func TestActivityHistoryEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	cooldown := 90 * 24 * time.Hour

	id := seedHistoryContributor(t, store, "_avhist_active", "public-active", "")
	quiet := seedHistoryContributor(t, store, "_avhist_quiet", "", "")
	done := seedHistoryContributor(t, store, "_avhist_done", "public-active", "NOW()")

	var poolN int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL AND gh_login != '' AND COALESCE(cntrb_deleted, 0) = 0`).Scan(&poolN); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.GetContributorsForHistoryBackfill(ctx, poolN+10, cooldown)
	if err != nil {
		t.Fatalf("GetContributorsForHistoryBackfill: %v", err)
	}
	pos := map[string]int{}
	for i, c := range claimed {
		pos[c.ID] = i + 1 // 1-based; 0 = absent
	}
	if pos[id] == 0 || pos[quiet] == 0 {
		t.Fatal("never-backfilled contributors must be claimable")
	}
	if pos[done] != 0 {
		t.Fatal("recently-backfilled contributor must be excluded by the cooldown")
	}
	if pos[id] > pos[quiet] {
		t.Errorf("active-classified contributor must be claimed before unclassified quiet one (active pos %d, quiet pos %d)", pos[id], pos[quiet])
	}

	// Store history: rows + stamp in one call.
	days := []model.ContributorDayActivity{
		{Day: "2026-03-05", RepoFullName: "aveloxis/aveloxis", Commits: 4, Issues: 1},
		{Day: "2026-03-06", RepoFullName: "chaoss/augur", PRs: 2, Reviews: 1},
	}
	totals := []model.ContributorDayTotal{{Day: "2026-03-05", Total: 9}, {Day: "2026-03-06", Total: 3}}
	if err := store.StoreContributorActivityHistory(ctx, id, days, totals); err != nil {
		t.Fatalf("StoreContributorActivityHistory: %v", err)
	}
	var commits, issues int
	if err := store.pool.QueryRow(ctx, `
		SELECT commit_count, issue_count FROM aveloxis_data.contributor_activity_days
		WHERE cntrb_id = $1::uuid AND day = '2026-03-05' AND repo_full_name = 'aveloxis/aveloxis'`,
		id).Scan(&commits, &issues); err != nil {
		t.Fatal(err)
	}
	if commits != 4 || issues != 1 {
		t.Errorf("day row wrong: commits=%d issues=%d", commits, issues)
	}
	var total int
	if err := store.pool.QueryRow(ctx, `
		SELECT total_contributions FROM aveloxis_data.contributor_activity_day_totals
		WHERE cntrb_id = $1::uuid AND day = '2026-03-06'`, id).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Errorf("day total wrong: %d", total)
	}

	// Idempotent re-store (the quarterly re-audit path): same keys, new
	// values overwrite, no duplicate-key error.
	days[0].Commits = 7
	if err := store.StoreContributorActivityHistory(ctx, id, days, totals); err != nil {
		t.Fatalf("re-store must upsert cleanly: %v", err)
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT commit_count FROM aveloxis_data.contributor_activity_days
		WHERE cntrb_id = $1::uuid AND day = '2026-03-05' AND repo_full_name = 'aveloxis/aveloxis'`,
		id).Scan(&commits); err != nil {
		t.Fatal(err)
	}
	if commits != 7 {
		t.Errorf("re-store must overwrite: commits=%d want 7", commits)
	}

	// Dataless path + both leave the claim pool.
	if err := store.MarkHistoryBackfilled(ctx, quiet); err != nil {
		t.Fatalf("MarkHistoryBackfilled: %v", err)
	}
	claimed, err = store.GetContributorsForHistoryBackfill(ctx, poolN+10, cooldown)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claimed {
		if c.ID == id || c.ID == quiet {
			t.Errorf("contributor %s still claimable after store/mark", c.ID)
		}
	}
}

func seedHistoryContributor(t *testing.T, store *PostgresStore, login, class, backfilledSQL string) string {
	t.Helper()
	ctx := t.Context()
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_days WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1)`, login)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_day_totals WHERE cntrb_id IN (SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1)`, login)
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	stamp := "NULL"
	if backfilledSQL != "" {
		stamp = backfilledSQL
	}
	var id string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, gh_login, gh_activity_class, gh_history_backfilled_at, data_collection_date)
		VALUES (gen_random_uuid(), $1, $1, $2, `+stamp+`, NOW())
		RETURNING cntrb_id::text`, login, class).Scan(&id); err != nil {
		t.Fatalf("seed %s: %v", login, err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_days WHERE cntrb_id = $1::uuid`, id)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_activity_day_totals WHERE cntrb_id = $1::uuid`, id)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, id)
	})
	return id
}
