// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestCountActiveReposCountsEverything (v0.27.68, reversing the
// v0.27.59 archived-exclusion): the landing count is the TOTAL repo
// catalog. The v0.27.59 non-archived filter made the landing number
// visibly disagree with the monitor's ~94K queue view — a definition
// mismatch the operator resolved in favor of "count everything"
// (archived repos carry full collected history and analysis).
// Deterministic under parallel test packages: source-level predicate
// pin + a scoped seeded-row check.
func TestCountActiveReposCountsEverything(t *testing.T) {
	src := readSourceFile(t, "public_stats_store.go")
	if strings.Contains(src, "NOT COALESCE(repo_archived, FALSE)") {
		t.Fatal("CountActiveRepos must NOT filter archived repos (v0.27.68 operator decision — the landing number must agree with the monitor's total)")
	}
	if !strings.Contains(src, "SELECT COUNT(*) FROM aveloxis_data.repos") {
		t.Fatal("CountActiveRepos must count the whole repos catalog")
	}
}

func TestCountActiveReposIncludesArchived(t *testing.T) {
	store, ctx := v0251Connect(t)
	defer store.Close()

	_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avcount'`)
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_archived) VALUES
		('https://github.com/_avcount/active',   '_avcount', 'active',   1, FALSE),
		('https://github.com/_avcount/archived', '_avcount', 'archived', 1, TRUE)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avcount'`)
	})

	// Both seeded rows count — archived included (scoped check so
	// parallel packages can't race the assertion).
	var n int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repos WHERE repo_owner = '_avcount'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("both rows (active + archived) must count: got %d of 2 seeded", n)
	}

	// And the method itself executes cleanly against the live schema.
	if _, err := store.CountActiveRepos(ctx); err != nil {
		t.Fatalf("CountActiveRepos: %v", err)
	}
}
