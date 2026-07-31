// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestCountActiveReposPredicate (v0.27.59): the landing-page count must
// exclude archived repositories. Deterministic under parallel test
// packages (a global before/after count-delta races concurrent
// inserts into the shared scratch DB — the original form of this test
// flaked exactly that way): pin the method's predicate at the source
// level, then prove that predicate's behavior on seeded rows with a
// scoped query.
func TestCountActiveReposPredicate(t *testing.T) {
	src := readSourceFile(t, "public_stats_store.go")
	if !strings.Contains(src, "NOT COALESCE(repo_archived, FALSE)") {
		t.Fatal("CountActiveRepos must filter NOT COALESCE(repo_archived, FALSE) — archived repos are read-only and don't belong in 'repositories under analysis'")
	}
}

func TestCountActiveReposExcludesArchived(t *testing.T) {
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

	// The exact predicate the method is pinned to, scoped to the seeded
	// cohort: exactly the active row survives.
	var n int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.repos
		WHERE NOT COALESCE(repo_archived, FALSE) AND repo_owner = '_avcount'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("predicate must keep the active row and exclude the archived one: got %d of 2 seeded", n)
	}

	// And the method itself executes cleanly against the live schema.
	if _, err := store.CountActiveRepos(ctx); err != nil {
		t.Fatalf("CountActiveRepos: %v", err)
	}
}
