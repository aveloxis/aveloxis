// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.21.2 — collection_queue.last_issues and last_prs are now
// cumulative counts (matching v0.19.11's treatment of
// last_commits), not per-cycle deltas. The pre-v0.21.2 behavior
// wrote whatever count the staged collector incremented as it
// staged rows — for incremental cycles with since-filtered API
// calls, that's "rows updated since the previous cycle", typically
// 0–single-digit for stable repos.
//
// The user-visible symptom on v0.21.0/v0.21.1: cycles finished in
// seconds (no scancode bottleneck anymore) and the dashboard
// repeatedly showed "Gathered: 0 / Meta: 53" on repos that had all
// 53 issues sitting in the DB from prior cycles. The data wasn't
// missing — the cache just reported the per-cycle delta, which is
// nearly always 0 when collection is keeping up.
//
// The fix matches what v0.19.11 did for commits: have CompleteJob
// write the cumulative count via subquery against the actual data
// table. SELECT COUNT(*) WHERE repo_id = $1 is indexed (the schema
// declares idx_issues_repo_id and idx_pull_requests_repo_id) and
// runs in a millisecond on typical repos, well within CompleteJob's
// existing transaction budget.

func TestCompleteJobWritesCumulativeIssuesCount(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Locate CompleteJob's body.
	idx := strings.Index(src, "func (s *PostgresStore) CompleteJob(")
	if idx < 0 {
		t.Fatal("cannot find CompleteJob")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of CompleteJob")
	}
	body := tail[:1+endRel]

	// The fix: last_issues comes from a SELECT COUNT(*) FROM
	// aveloxis_data.issues subquery, not the parameter directly.
	// A regression that drops the subquery and goes back to the
	// per-cycle parameter fires this test.
	if !strings.Contains(body, "SELECT COUNT(*) FROM aveloxis_data.issues") {
		t.Error("CompleteJob must write last_issues as SELECT COUNT(*) FROM aveloxis_data.issues WHERE repo_id = $1 — pre-v0.21.2 this column was per-cycle delta, which produced misleading 'Gathered: 0' dashboard reads on incremental cycles. See CLAUDE.md v0.21.2 entry.")
	}
	if !strings.Contains(body, "SELECT COUNT(*) FROM aveloxis_data.pull_requests") {
		t.Error("CompleteJob must write last_prs as SELECT COUNT(*) FROM aveloxis_data.pull_requests WHERE repo_id = $1. Same rationale as last_issues — match v0.19.11's cumulative-commits pattern.")
	}
}

func TestMigrationBackfillsCumulativeIssuesAndPRs(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The v0.21.2 backfill: one-shot UPDATE that sets every
	// queue row's last_issues and last_prs to the cumulative
	// COUNT from the data tables. Idempotent via
	// `IS DISTINCT FROM` so re-running migrate is a no-op once
	// counts catch up.
	for _, needle := range []string{
		"v0.21.2 backfill collection_queue.last_issues with cumulative counts",
		"v0.21.2 backfill collection_queue.last_prs with cumulative counts",
		"COUNT(*) AS cnt",
		"FROM aveloxis_data.issues",
		"FROM aveloxis_data.pull_requests",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migrate.go must backfill cumulative last_issues and last_prs from aveloxis_data.issues / pull_requests. Missing needle: %q. Without this, operators upgrading from v0.21.x or earlier still see the misleading 'Gathered: 0 / Meta: N' dashboard reads until each repo's next collection cycle.", needle)
		}
	}
}
