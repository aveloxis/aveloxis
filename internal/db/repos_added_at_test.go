// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// repos_added_at_test.go — TDD suite for v0.27.60: a STABLE
// "when did this repo enter the fleet" timestamp, feeding the
// new-repositories feeds (v0.27.62). Contracts:
//   - migration ORDER pins the NOW()-default trap: ADD COLUMN with a
//     STABLE default would stamp every legacy row with the migration
//     timestamp via attmissingval, destroying the backfill's
//     distinction — so: (1) add bare column, (2) backfill from
//     COALESCE(data_collection_date, created_at, NOW()) — an honest
//     last-touch APPROXIMATION for legacy rows (no queue created-at
//     exists), (3) only then SET DEFAULT NOW();
//   - added_at is INSERT-ONLY: absent from UpsertRepo's DO UPDATE SET,
//     so re-collections never move it;
//   - schema.sql carries the v0.27.58-lesson ALTER guard before its
//     index (existing fleets no-op the CREATE TABLE).

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

func TestAddedAtMigrationOrderPinsDefaultTrap(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	addPos := strings.Index(src, `"added_at", "TIMESTAMPTZ"`)
	if addPos < 0 {
		t.Fatal(`migrate.go must addColumnIfMissing("added_at", "TIMESTAMPTZ") — BARE type, no DEFAULT: a STABLE default at add-time stamps every legacy row with the migration timestamp`)
	}
	backfillPos := strings.Index(src, "SET added_at = COALESCE(data_collection_date, created_at, NOW())")
	if backfillPos < 0 {
		t.Fatal("migrate.go must backfill added_at from COALESCE(data_collection_date, created_at, NOW()) WHERE added_at IS NULL")
	}
	defaultPos := strings.Index(src, "ALTER COLUMN added_at SET DEFAULT NOW()")
	if defaultPos < 0 {
		t.Fatal("migrate.go must SET DEFAULT NOW() on added_at AFTER the backfill")
	}
	if !(addPos < backfillPos && backfillPos < defaultPos) {
		t.Errorf("order must be add(%d) < backfill(%d) < set-default(%d) — reordering reintroduces the attmissingval trap", addPos, backfillPos, defaultPos)
	}
	if !strings.Contains(src, "WHERE added_at IS NULL") {
		t.Error("the backfill must be idempotent by predicate (WHERE added_at IS NULL)")
	}
}

func TestAddedAtSchemaDeclarations(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	flat := strings.Join(strings.Fields(schema), " ")
	if !strings.Contains(flat, "added_at TIMESTAMPTZ DEFAULT NOW()") {
		t.Error("schema.sql repos CREATE TABLE must declare added_at TIMESTAMPTZ DEFAULT NOW() (fresh installs)")
	}
	// The v0.27.58 existing-fleet lesson: an index on a same-release
	// new column needs an ALTER guard ahead of it in schema.sql.
	guard := strings.Index(flat, "ALTER TABLE aveloxis_data.repos ADD COLUMN IF NOT EXISTS added_at")
	idx := strings.Index(flat, "CREATE INDEX IF NOT EXISTS idx_repos_added_at")
	if guard < 0 {
		t.Fatal("schema.sql must guard added_at with ALTER TABLE ... ADD COLUMN IF NOT EXISTS before its index (existing fleets no-op the CREATE TABLE)")
	}
	if idx >= 0 && guard > idx {
		t.Error("the added_at guard must precede idx_repos_added_at")
	}
}

func TestUpsertRepoNeverTouchesAddedAt(t *testing.T) {
	src := readSourceFile(t, "postgres.go")
	i := strings.Index(src, "func (s *PostgresStore) UpsertRepo(")
	if i < 0 {
		t.Fatal("cannot find UpsertRepo")
	}
	body := src[i:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	if strings.Contains(body, "added_at") {
		t.Error("UpsertRepo must not reference added_at at all — the DEFAULT stamps it at INSERT and the DO UPDATE must never move it (insert-only contract)")
	}
}

func TestAddedAtEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.Close()

	// Fresh insert gets a NOW()-ish stamp via the default.
	login := "_avaddedat/probe"
	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git = $1`, "https://github.com/"+login)
	var id int64
	var first time.Time
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, '_avaddedat', 'probe', 1) RETURNING repo_id, added_at`,
		"https://github.com/"+login).Scan(&id, &first); err != nil {
		t.Fatalf("insert with default: %v", err)
	}
	t.Cleanup(func() { _, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id) })
	if time.Since(first) > time.Minute {
		t.Errorf("fresh insert must stamp added_at ≈ now, got %s", first)
	}

	// A second UpsertRepo touch must NOT move added_at.
	if _, err := store.UpsertRepo(ctx, &model.Repo{
		GitURL: "https://github.com/" + login, Owner: "_avaddedat", Name: "probe",
		Platform: model.PlatformGitHub,
	}); err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	var second time.Time
	if err := store.pool.QueryRow(ctx, `SELECT added_at FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if !second.Equal(first) {
		t.Errorf("added_at moved on re-upsert: %s → %s (must be insert-only)", first, second)
	}

	// Backfill semantics: NULL the stamp, run the migration statement's
	// exact predicate path via Migrate, assert it heals from
	// data_collection_date.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET added_at = NULL, data_collection_date = '2026-01-15T00:00:00Z' WHERE repo_id = $1`, id); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	var healed time.Time
	if err := store.pool.QueryRow(ctx, `SELECT added_at FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&healed); err != nil {
		t.Fatal(err)
	}
	if healed.UTC().Format("2006-01-02") != "2026-01-15" {
		t.Errorf("backfill must use data_collection_date (2026-01-15), got %s", healed)
	}
}
