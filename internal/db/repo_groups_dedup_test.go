// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// TestRepoGroupsConsolidationMigrationShape pins the v0.27.17 steps:
// per-rg_name canonical repoint of every FK table, dm hygiene deletes,
// loser deletion, and the unique created AFTER the dedup.
func TestRepoGroupsConsolidationMigrationShape(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"v0.27.17 repoint repos.repo_group_id to canonical group per rg_name",
		"aveloxis_data.repo_groups_list_serve",
		"aveloxis_data.email_message",
		"aveloxis_data.email_message_ref",
		"aveloxis_data.repo_group_insights",
		"v0.27.17 delete consolidated loser repo_groups rows",
		"uq_repo_groups_rg_name",
		`MIN(repo_group_id) AS canon`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("migrate.go missing %q", needle)
		}
	}
	// Ordering: loser deletion before the unique-index creation.
	del := strings.Index(s, "v0.27.17 delete consolidated loser repo_groups rows")
	uq := strings.Index(s, `"uq_repo_groups_rg_name"`)
	if del < 0 || uq < 0 || del > uq {
		t.Error("loser deletion must run BEFORE uq_repo_groups_rg_name is created")
	}
}

// TestRepoGroupsConsolidatedEndToEnd runs against a migrated scratch DB
// (whose own long test history accumulated the duplicate-'Default'
// shape organically) and asserts the post-migration invariants: exactly
// one group per name, the unique arbiter in place, and the lazy
// default-group path returning the SAME group on repeated use.
func TestRepoGroupsConsolidatedEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t) // migrates (runs the consolidation)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool

	var dupNames int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
		  SELECT rg_name FROM aveloxis_data.repo_groups
		  GROUP BY rg_name HAVING COUNT(*) > 1) d`).Scan(&dupNames); err != nil {
		t.Fatal(err)
	}
	if dupNames != 0 {
		t.Errorf("%d duplicated rg_name values remain after consolidation", dupNames)
	}

	var uq int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname='aveloxis_data' AND indexname='uq_repo_groups_rg_name'`).Scan(&uq); err != nil {
		t.Fatal(err)
	}
	if uq != 1 {
		t.Fatal("uq_repo_groups_rg_name missing after migration")
	}

	// The lazy path must be stable now: same name → same id, twice.
	id1, err := store.UpsertRepoGroup(ctx, "consolidation-e2e-group", "test", "https://example.com/x")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := store.UpsertRepoGroup(ctx, "consolidation-e2e-group", "test", "https://example.com/x")
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("UpsertRepoGroup created a second group for the same name: %d then %d", id1, id2)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name='consolidation-e2e-group'`)
	})
}
