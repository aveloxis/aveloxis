// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.0 (operator, 2026-08-31): the monitor's queue columns become
// sortable. Server-side sort through an ALLOWLIST — the house listing
// grammar (collectionRepoSorts, v0.27.74) — because only one 100-row
// page is client-side and sorting it alone would lie across pages.

// TestQueueSortsAllowlist pins the allowlist mechanism: every key the
// GUI offers resolves, and a hostile key falls back to the default
// composite ordering (injection-proof by construction). The v0.18.7
// repo_id tiebreaker is pinned by
// TestListQueuePageOrderByIncludesRepoIDTiebreaker (both the default
// composite and the allowlist append) and behaviorally below by the
// equal-sort-key case.
func TestQueueSortsAllowlist(t *testing.T) {
	src := srctest.Read(t, "internal/db/queue.go")
	if !strings.Contains(src, "var queueSorts = map[string]string{") {
		t.Fatal("queue.go must declare the queueSorts allowlist (the collectionRepoSorts pattern)")
	}
	for _, key := range []string{
		`"repo"`, `"status"`, `"priority"`, `"due"`, `"last_run"`,
		`"issues"`, `"prs"`, `"commits"`,
		`"meta_issues"`, `"meta_prs"`, `"meta_commits"`,
	} {
		if !strings.Contains(src, key+":") {
			t.Errorf("queueSorts must offer %s", key)
		}
	}
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) ListQueuePage("))
	if !strings.Contains(body, "queueSorts[") {
		t.Fatal("ListQueuePage must resolve the sort through the allowlist")
	}
	// The meta_* sorts read the LATEST repo_info snapshot per repo —
	// the v0.28.9 latest-pick rule (DESC NULLS LAST + repo_info_id
	// tiebreak), via a set-based DISTINCT ON (never a per-row LATERAL,
	// which would probe repo_info once per queue row fleet-wide).
	if !srctest.ContainsNormalized(body, "DISTINCT ON (repo_id)") ||
		!srctest.ContainsNormalized(body, "data_collection_date DESC NULLS LAST, repo_info_id DESC") {
		t.Error("meta sorts must join the latest repo_info snapshot set-based with the house latest-pick ordering")
	}
}

// queueSortSeed creates a repo via UpsertRepo (sequence-assigned id —
// the v0.27.152 rule: a fixed literal PK can silently hijack and then
// DELETE a colliding pre-existing row) and returns the id.
func queueSortSeed(t *testing.T, ctx context.Context, store *PostgresStore, owner, name string, issues, priority int, metaIssues int) int64 {
	t.Helper()
	id, err := store.UpsertRepo(ctx, &model.Repo{
		GitURL: fmt.Sprintf("https://example.com/%s/%s", owner, name),
		Owner:  owner, Name: name, Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustExecRetry(ctx, t, store, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at, last_issues)
		VALUES ($1, $2, 'queued', NOW(), $3)
		ON CONFLICT (repo_id) DO UPDATE SET priority = EXCLUDED.priority, last_issues = EXCLUDED.last_issues, status = 'queued'`,
		id, priority, issues)
	if metaIssues >= 0 {
		mustExecRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.repo_info (repo_id, issues_count, data_collection_date)
			VALUES ($1, $2, NOW())`, id, metaIssues)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repo_info WHERE repo_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	return id
}

// TestListQueuePageSortEndToEnd — seeded rows order by the requested
// key, a hostile key falls back to the default composite, and the
// meta sort reads the repo_info snapshot.
func TestListQueuePageSortEndToEnd(t *testing.T) {
	store, ctx := homeActivityConnect(t)
	// Search-scoped to our owner so fleet residue never perturbs.
	alpha := queueSortSeed(t, ctx, store, "_avqsort", "alpha", 5, 300, 50)
	bravo := queueSortSeed(t, ctx, store, "_avqsort", "bravo", 20, 200, 10)
	charlie := queueSortSeed(t, ctx, store, "_avqsort", "charlie", 1, 100, 99)
	// delta EQUALS bravo on the issues key — the behavioral half of the
	// v0.18.7 tiebreaker pin: equal sort keys must order by repo_id, or
	// Prev/Next pagination overlaps/skips under active collection.
	delta := queueSortSeed(t, ctx, store, "_avqsort", "delta", 20, 400, -1)

	get := func(sortKey, dir string) []int64 {
		t.Helper()
		jobs, total, err := store.ListQueuePage(ctx, 10, 0, "_avqsort/", sortKey, dir)
		if err != nil {
			t.Fatal(err)
		}
		if total != 4 {
			t.Fatalf("total = %d, want 4", total)
		}
		out := make([]int64, 0, len(jobs))
		for _, j := range jobs {
			out = append(out, j.RepoID)
		}
		return out
	}
	want := func(label string, got []int64, order ...int64) {
		t.Helper()
		if len(got) < len(order) {
			t.Fatalf("%s: only %d rows returned, want %d: %v", label, len(got), len(order), got)
		}
		for i := range order {
			if got[i] != order[i] {
				t.Fatalf("%s: order = %v, want %v", label, got, order)
			}
		}
	}
	// bravo and delta tie on issues=20: repo_id breaks the tie (bravo
	// was seeded first, so its sequence-assigned id is lower).
	want("issues desc + tiebreaker", get("issues", "desc"), bravo, delta, alpha, charlie)
	want("repo asc", get("repo", "asc"), alpha, bravo, charlie, delta)
	want("meta_issues desc", get("meta_issues", "desc"), charlie, alpha, bravo)
	// Hostile key: falls back to the default composite (status equal →
	// priority asc), never an error, never interpolated.
	want("hostile key falls back", get("evil; DROP TABLE x--", ""), charlie, bravo, alpha, delta)
	want("priority asc", get("priority", "asc"), charlie, bravo, alpha, delta)
}

// TestAdminMonitorQueueHandlerPassesSort pins the API wiring: the
// handler forwards sort/dir and echoes the EFFECTIVE values in the
// envelope (the v0.27.75 group-page contract).
func TestAdminMonitorQueueHandlerPassesSort(t *testing.T) {
	src := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/api/portal.go"),
		"func (s *Server) handleAdminMonitorQueue("))
	for _, needle := range []string{
		`Get("sort")`, `Get("dir")`, `"sort":`, `"dir":`,
		// EFFECTIVE echo (review 2026-08-31 #2): an unknown key means
		// the store used the default composite; the envelope must say
		// so, never parrot the caller's input (the handleGroupRepos /
		// v0.27.65 log-the-effective-value contract).
		"db.QueueSortValid(sortKey)",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("handleAdminMonitorQueue must carry %s (accept + echo the EFFECTIVE sort contract)", needle)
		}
	}
}
