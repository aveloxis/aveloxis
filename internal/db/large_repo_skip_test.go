// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.35 large-repo skip tests (collection.skip_largest_percent).

package db

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestDequeueNextExclusionShape pins the claim-side SQL: the exclusion
// must use the NOT (repo_id = ANY(...)) form with a guaranteed
// non-nil slice — `= ANY(NULL)` yields NULL and would silently make
// the WHERE clause never match (the v0.27.4 lesson).
func TestDequeueNextExclusionShape(t *testing.T) {
	body := extractFunctionBody(t, "queue.go", "DequeueNext")
	if !strings.Contains(body, "NOT (repo_id = ANY($2::bigint[]))") {
		t.Error("DequeueNext must exclude via NOT (repo_id = ANY($2::bigint[]))")
	}
	if !strings.Contains(body, "excludeRepoIDs = []int64{}") {
		t.Error("DequeueNext must coerce a nil exclusion slice to empty — = ANY(NULL) poisons the predicate")
	}
}

// TestLargestRepoIDsFractionValidation pins the guard rails.
func TestLargestRepoIDsFractionValidation(t *testing.T) {
	src, err := os.ReadFile("large_repo_skip.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"fraction <= 0 || fraction >= 1",
		"percentile_cont(1 - $1::float8)",
		"commit_count >= th.c OR ri.pr_count >= th.p",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("large_repo_skip.go missing %q", needle)
		}
	}
}

// TestLargestRepoIDsEndToEnd (AVELOXIS_TEST_DB): a repo with the
// fleet-maximum commit count is in the top set for ANY fraction; a
// repo whose counts are NULL (never measured) is NEVER in it — an
// unknown repo must collect so it can be measured.
func TestLargestRepoIDsEndToEnd(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	mkRepo := func(name string) int64 {
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
			VALUES ($1, '_avlarge', $2, 1, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avlarge/%s%d", name, suffix),
			fmt.Sprintf("%s%d", name, suffix)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	monster := mkRepo("monster")
	unknown := mkRepo("unknown")
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = ANY($1::bigint[])`, []int64{monster, unknown})
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1::bigint[])`, []int64{monster, unknown})
	})

	// Monster: strictly above the current fleet max, so it sits at or
	// above ANY percentile threshold regardless of the scratch DB's
	// existing distribution.
	var maxCommits int64
	pool.QueryRow(ctx, `SELECT COALESCE(MAX(commit_count), 0) FROM aveloxis_data.repo_info`).Scan(&maxCommits)
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, commit_count, pr_count)
		VALUES ($1, $2, 0)`, monster, maxCommits+100000); err != nil {
		t.Fatal(err)
	}
	// Unknown: repo_info row with NULL counts (metadata never came back).
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_info (repo_id, commit_count, pr_count)
		VALUES ($1, NULL, NULL)`, unknown); err != nil {
		t.Fatal(err)
	}

	ids, commitTh, _, err := store.LargestRepoIDs(ctx, 0.005)
	if err != nil {
		t.Fatal(err)
	}
	inSet := func(id int64) bool {
		for _, x := range ids {
			if x == id {
				return true
			}
		}
		return false
	}
	if !inSet(monster) {
		t.Errorf("fleet-max repo must be in the top 0.5%% set (commit threshold %v, %d ids)", commitTh, len(ids))
	}
	if inSet(unknown) {
		t.Error("NULL-count repos must never be classified as large — unknown repos must collect")
	}
	// Guard rails.
	if _, _, _, err := store.LargestRepoIDs(ctx, 0); err == nil {
		t.Error("fraction 0 must error")
	}
	if _, _, _, err := store.LargestRepoIDs(ctx, 1); err == nil {
		t.Error("fraction 1 must error")
	}
}

// TestDequeueNextHonorsExclusion (AVELOXIS_TEST_DB): with two due
// repos at fleet-winning priority, excluding the front-runner claims
// the other; an empty exclusion claims the front-runner.
func TestDequeueNextHonorsExclusion(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_DB") == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	store, ctx := v0251Connect(t)
	t.Cleanup(func() { store.pool.Close() })
	pool := store.pool
	suffix := time.Now().UnixNano()

	mk := func(name string, prio int) int64 {
		var id int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
			VALUES ($1, '_avdq', $2, 1, 1) RETURNING repo_id`,
			fmt.Sprintf("https://github.com/_avdq/%s%d", name, suffix),
			fmt.Sprintf("%s%d", name, suffix)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
			VALUES ($1, 'queued', $2, NOW() - INTERVAL '1 hour')`, id, prio); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// Priorities far below anything the scratch DB uses, so ordering
	// is deterministic on a shared database.
	front := mk("front", -100000)
	second := mk("second", -99999)
	t.Cleanup(func() {
		pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = ANY($1::bigint[])`, []int64{front, second})
		pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1::bigint[])`, []int64{front, second})
	})

	job, err := store.DequeueNext(ctx, "_avdq-test", []int64{front})
	if err != nil || job == nil {
		t.Fatalf("claim with exclusion: job=%v err=%v", job, err)
	}
	if job.RepoID == front {
		t.Fatal("excluded repo must never be claimed")
	}
	if job.RepoID != second {
		t.Fatalf("expected the runner-up (%d), got %d", second, job.RepoID)
	}

	// Empty exclusion (and nil, via coercion) claims the front-runner.
	job2, err := store.DequeueNext(ctx, "_avdq-test", nil)
	if err != nil || job2 == nil {
		t.Fatalf("claim without exclusion: job=%v err=%v", job2, err)
	}
	if job2.RepoID != front {
		t.Fatalf("without exclusion the front-runner (%d) must win, got %d", front, job2.RepoID)
	}
}
