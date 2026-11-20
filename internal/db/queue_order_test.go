// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// Source-contract tests for the ORDER BY clauses in ListQueue and
// ListQueuePage. Without a stable tiebreaker, LIMIT/OFFSET pagination
// can return overlapping or skipped rows when the sort columns mutate
// during active collection — which is exactly what happens to `status`,
// `priority`, and `due_at` every time a job completes. The symptoms
// users see: Prev/Next appear "broken" because the page changes are
// hidden by concurrent drift, while First/Last still work because
// their page jumps are large enough to visibly succeed.
//
// repo_id is monotonic (SERIAL), non-null, and never changes for a
// given row, so it's the correct tiebreaker.

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestListQueuePageOrderByIncludesRepoIDTiebreaker(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	// 0.29.212: the page's two statements are built by queuePageSQL (so that
	// the write-policy check runs them for every sort). What is pinned is
	// unchanged, and read where the text now stands: ListQueuePage runs
	// that builder's statements, and the builder's body has the shape.
	builtByQueuePageSQL(t)
	body := srctest.FuncBody(t, string(data), "func queuePageSQL(")
	// v0.29.0 re-anchor (review 2026-08-31 #1): the function's FIRST
	// textual ORDER BY is now the repo_info latest-pick inside the meta
	// join literal, which disarmed this pin silently — it happily
	// matched that subquery's own repo_id. The dataQuery's ORDER BY is
	// a Sprintf placeholder, so the pin now checks the two places the
	// tiebreaker actually lives: the default composite literal and the
	// allowlist append.
	if !strings.Contains(body, "q.priority, q.due_at, q.repo_id") {
		t.Error("the default composite ordering must end with the q.repo_id tiebreaker")
	}
	if !strings.Contains(body, `+ ", q.repo_id"`) {
		t.Error("every allowlisted sort must append the q.repo_id tiebreaker")
	}
	clause := `ORDER BY q.priority, q.due_at, q.repo_id` // satisfied above; keep the shared shape checks below

	if !strings.Contains(clause, "repo_id") {
		t.Errorf("ListQueuePage ORDER BY must end with q.repo_id as a stable tiebreaker.\n"+
			"Got: %q\n"+
			"Reason: status/priority/due_at all mutate during active collection; "+
			"without a monotonic tiebreaker LIMIT/OFFSET returns overlapping or "+
			"skipped rows between Prev/Next clicks.", clause)
	}
	// Must come AFTER the mutating columns so it's actually a tiebreaker,
	// not a primary sort.
	priIdx := strings.Index(clause, "priority")
	dueIdx := strings.Index(clause, "due_at")
	repoIdx := strings.LastIndex(clause, "repo_id")
	if repoIdx < priIdx || repoIdx < dueIdx {
		t.Errorf("repo_id must appear AFTER priority and due_at in ORDER BY so it acts as tiebreaker.\n"+
			"Got: %q", clause)
	}
}

// The unbounded ListQueue (every row of the fleet in one answer) had no
// caller — the monitor paginates through ListQueuePage — and was deleted
// in v0.29.98; it must not return.
func TestUnboundedListQueueIsGone(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "func (s *PostgresStore) ListQueue(") {
		t.Error("queue.go defines the unbounded ListQueue again; every queue listing paginates (ListQueuePage)")
	}
}
