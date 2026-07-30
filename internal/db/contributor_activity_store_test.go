// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// contributor_activity_store_test.go — TDD suite for the v0.27.57
// contributor-activity store layer (GitHub contributionsCollection
// classification). Contracts pinned:
//   - the five gh_activity columns exist in schema.sql AND migrate.go
//     (fresh installs + existing fleets);
//   - the claim query mirrors the breadth claim: NULLS-first ordering,
//     jittered cooldown (same BreadthCooldownJitterFrac — the same
//     feast/famine echo dynamics apply), and a serving index declared
//     ASC NULLS FIRST (the v0.27.8 lesson: without it the claim
//     full-sorts 2.4M contributor rows every tick);
//   - marking is decoupled from data: contributors ABSENT from the API
//     result still get gh_activity_checked_at stamped (the v0.20.17
//     lesson — unmarked dead-enders would pin the queue head forever).

import (
	"strings"
	"testing"
	"time"
)

var activityColumns = []string{
	"gh_public_contribs_year",
	"gh_restricted_contribs_year",
	"gh_last_contribution_year",
	"gh_activity_class",
	"gh_activity_checked_at",
}

func TestSchemaDeclaresActivityColumns(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	for _, col := range activityColumns {
		if !strings.Contains(schema, col) {
			t.Errorf("schema.sql must declare contributors.%s", col)
		}
	}
	flat := strings.Join(strings.Fields(schema), " ")
	if !strings.Contains(flat, "idx_contributors_activity_checked ON aveloxis_data.contributors (gh_activity_checked_at ASC NULLS FIRST)") {
		t.Error("schema.sql must declare idx_contributors_activity_checked with explicit ASC NULLS FIRST — the v0.27.8 breadth lesson: Postgres's ASC default is NULLS LAST and neither scan direction serves the claim's ORDER BY without it")
	}
}

func TestMigrateAddsActivityColumns(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	for _, col := range activityColumns {
		if !strings.Contains(src, `"`+col+`"`) {
			t.Errorf("migrate.go must addColumnIfMissing %s for existing fleets", col)
		}
	}
	if !strings.Contains(src, "idx_contributors_activity_checked") {
		t.Error("migrate.go must build idx_contributors_activity_checked (CONCURRENTLY) for existing fleets")
	}
}

func TestActivityClaimQueryContract(t *testing.T) {
	src := readSourceFile(t, "contributor_activity_store.go")
	idx := strings.Index(src, "func (s *PostgresStore) GetContributorsForActivityCheck(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsForActivityCheck")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	flat := strings.Join(strings.Fields(body), " ")
	for _, needle := range []string{
		"gh_activity_checked_at IS NULL",
		"ORDER BY gh_activity_checked_at ASC NULLS FIRST",
		"BreadthCooldownJitterFrac",
		`gh_login != ''`,
		"COALESCE(cntrb_deleted, 0) = 0",
	} {
		if !strings.Contains(flat, needle) {
			t.Errorf("activity claim query must contain %q (mirrors the breadth claim contract)", needle)
		}
	}
}

// MarkActivityCheckedBatch must stamp checked_at WITHOUT touching the
// activity fields — it's the "attempted, no data returned" path for
// deleted/renamed users, and clobbering a previously-stored class with
// empties would erase good data on a later failed check.
func TestMarkActivityCheckedBatchContract(t *testing.T) {
	src := readSourceFile(t, "contributor_activity_store.go")
	idx := strings.Index(src, "func (s *PostgresStore) MarkActivityCheckedBatch(")
	if idx < 0 {
		t.Fatal("cannot find MarkActivityCheckedBatch")
	}
	body := src[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	if !strings.Contains(body, "ANY($1::uuid[])") {
		t.Error("mark must be a chunked ANY(uuid[]) update (the MarkBreadthAttemptedBatch pattern), not per-row round trips")
	}
	if strings.Contains(body, "gh_activity_class") {
		t.Error("MarkActivityCheckedBatch must NOT touch gh_activity_class — a mark-only pass may not clobber previously-stored classification")
	}
}

// End-to-end against the scratch DB: claim honors cooldown + NULLS
// first; the update writes all fields; marked rows leave the claim
// pool.
func TestContributorActivityStoreEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.Close()
	cooldown := 240 * time.Hour

	never := seedActivityContributor(t, store, "_avact_never", "")
	recent := seedActivityContributor(t, store, "_avact_recent", "NOW()")
	old := seedActivityContributor(t, store, "_avact_old", "NOW() - '480:00:00'::interval")

	var poolN int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL AND gh_login != '' AND COALESCE(cntrb_deleted, 0) = 0`).Scan(&poolN); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.GetContributorsForActivityCheck(ctx, poolN+10, cooldown)
	if err != nil {
		t.Fatalf("GetContributorsForActivityCheck: %v", err)
	}
	ids := map[string]bool{}
	for _, c := range claimed {
		ids[c.ID] = true
	}
	if !ids[never] || !ids[old] {
		t.Fatal("never-checked and past-cooldown contributors must be claimable")
	}
	if ids[recent] {
		t.Fatal("recently-checked contributor must be excluded by the cooldown")
	}

	// Store activity for one; mark-only for another.
	if err := store.UpdateContributorActivityBatch(ctx, []ContributorActivityUpdate{
		{CntrbID: never, PublicContribs: 1742, RestrictedContribs: 1895, LastContributionYear: 2026, ActivityClass: "public-active"},
	}); err != nil {
		t.Fatalf("UpdateContributorActivityBatch: %v", err)
	}
	if err := store.MarkActivityCheckedBatch(ctx, []string{old}); err != nil {
		t.Fatalf("MarkActivityCheckedBatch: %v", err)
	}

	var pub, restr, lastYr int
	var class string
	if err := store.pool.QueryRow(ctx, `
		SELECT gh_public_contribs_year, gh_restricted_contribs_year, gh_last_contribution_year, gh_activity_class
		FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, never).Scan(&pub, &restr, &lastYr, &class); err != nil {
		t.Fatal(err)
	}
	if pub != 1742 || restr != 1895 || lastYr != 2026 || class != "public-active" {
		t.Errorf("stored activity wrong: pub=%d restr=%d yr=%d class=%q", pub, restr, lastYr, class)
	}

	// Both leave the claim pool.
	claimed, err = store.GetContributorsForActivityCheck(ctx, poolN+10, cooldown)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range claimed {
		if c.ID == never || c.ID == old {
			t.Errorf("contributor %s still claimable after update/mark", c.ID)
		}
	}
}

// seedActivityContributor mirrors seedJitterContributor for the
// gh_activity_checked_at column. checkedAtSQL "" means NULL.
func seedActivityContributor(t *testing.T, store *PostgresStore, login, checkedAtSQL string) string {
	t.Helper()
	ctx := t.Context()
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	stamp := "NULL"
	if checkedAtSQL != "" {
		stamp = checkedAtSQL
	}
	var id string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_id, cntrb_login, gh_login, gh_activity_checked_at, data_collection_date)
		VALUES (gen_random_uuid(), $1, $1, `+stamp+`, NOW())
		RETURNING cntrb_id::text`, login).Scan(&id); err != nil {
		t.Fatalf("seed %s: %v", login, err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login = $1`, login)
	})
	return id
}
