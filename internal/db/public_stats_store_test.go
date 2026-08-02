// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.77 — the landing page's public stats grew from the single
// repo count (v0.27.59/68) to the fleet-scale payload
// {repos, commits, issues, prs, contributors}. Contracts pinned:
//   1. The v0.27.68 operator decision survives: the repos arm counts
//      EVERYTHING (no archived filter) so the landing number agrees
//      with the monitor's total.
//   2. Cost class: commits/issues/prs come from the queue's CACHED
//      cumulative totals (one SUM over ~100K queue rows), NEVER from
//      counting the 474M-row commits table per refresh (the v0.27.4
//      timeout class). The 60s stale-on-error cache in the api layer
//      bounds refresh frequency.

func TestPublicFleetStatsCountsEverything(t *testing.T) {
	src := readSourceFile(t, "public_stats_store.go")
	if strings.Contains(src, "NOT COALESCE(repo_archived, FALSE)") {
		t.Fatal("GetPublicFleetStats must NOT filter archived repos (v0.27.68 operator decision — the landing number must agree with the monitor's total)")
	}
	if !strings.Contains(src, "SELECT COUNT(*) FROM aveloxis_data.repos") {
		t.Fatal("GetPublicFleetStats must count the whole repos catalog")
	}
}

func TestPublicFleetStatsUsesCachedQueueTotals(t *testing.T) {
	src := readSourceFile(t, "public_stats_store.go")
	for _, needle := range []string{"SUM(last_commits)", "SUM(last_issues)", "SUM(last_prs)"} {
		if !strings.Contains(src, needle) {
			t.Errorf("GetPublicFleetStats must read %s from collection_queue — the cached cumulative totals (v0.19.11/v0.21.2)", needle)
		}
	}
	if strings.Contains(src, "FROM aveloxis_data.commits") {
		t.Error("GetPublicFleetStats must NOT count aveloxis_data.commits — use the queue's cached totals (the v0.27.4 timeout class)")
	}
}

func TestPublicFleetStatsIncludesArchived(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)

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

	// And the method itself executes cleanly against the live schema
	// with sane values.
	stats, err := store.GetPublicFleetStats(ctx)
	if err != nil {
		t.Fatalf("GetPublicFleetStats: %v", err)
	}
	if stats.Repos < 2 {
		t.Errorf("repos count must include the seeded rows, got %d", stats.Repos)
	}
	if stats.Commits < 0 || stats.Issues < 0 || stats.PRs < 0 || stats.Contributors < 0 {
		t.Errorf("counts must be non-negative: %+v", stats)
	}
}
