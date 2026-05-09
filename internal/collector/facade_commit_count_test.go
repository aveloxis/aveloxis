package collector

import (
	"os"
	"strings"
	"testing"
)

// TestFacadeIncrementsCommitsOncePerCommitNotPerFile pins the v0.19.11
// fix to the over-counting bug in `insertCommitBatch`. Pre-v0.19.11
// the line `result.Commits++` lived inside the inner
// `for _, file := range files` loop, so for every commit with N files
// modified, result.Commits incremented N times — making
// FacadeResult.Commits the row count, not the distinct-commit count.
//
// That value flowed: FacadeResult.Commits → buildOutcome →
// CompleteJob → collection_queue.last_commits → GetRepoStatsBatch →
// dashboards. Result: every dashboard showed inflated commit counts
// (typically 5–50× actual, depending on average files-per-commit).
//
// The fix moves the increment OUTSIDE the file loop so each commit
// contributes 1 to the count regardless of file count.
func TestFacadeIncrementsCommitsOncePerCommitNotPerFile(t *testing.T) {
	data, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (f *FacadeCollector) insertCommitBatch(")
	if fnIdx < 0 {
		t.Fatal("cannot find insertCommitBatch in facade.go")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	if end < 0 {
		t.Fatal("cannot find end of insertCommitBatch")
	}
	body := rest[:end+1]

	// Find the inner per-file loop bounds and assert the increment
	// is NOT inside them. The loop opens with `for _, file := range files {`
	// and closes with the matching `}`. We approximate by looking for the
	// open-brace line and the next "}" at the same indent depth — but
	// the simpler heuristic is: there must be a `result.Commits++` line
	// that lives AFTER the closing `}` of the file-loop, OR the body
	// must not contain the increment inside the for-files block at all.

	innerOpen := strings.Index(body, "for _, file := range files {")
	if innerOpen < 0 {
		t.Fatal("cannot find inner per-file loop in insertCommitBatch")
	}

	// Find the matching close brace of that for loop. A simple
	// brace-counting walker covers the typical Go formatting.
	depth := 0
	innerClose := -1
walk:
	for i := innerOpen; i < len(body); i++ {
		switch body[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				innerClose = i
				break walk
			}
		}
	}
	if innerClose == -1 {
		t.Fatal("cannot find close brace of inner per-file loop")
	}

	innerBlock := body[innerOpen:innerClose]
	if strings.Contains(innerBlock, "result.Commits++") {
		t.Error("insertCommitBatch increments result.Commits inside the " +
			"per-file `for _, file := range files` loop. That makes " +
			"FacadeResult.Commits a row count (one per file per commit) " +
			"rather than a distinct-commit count. The increment must " +
			"live OUTSIDE the file loop so dashboards display real " +
			"commit counts. See facade.go:494 in the v0.19.10 baseline " +
			"for the bug shape.")
	}

	// Sanity check: the increment must still exist somewhere in the
	// outer per-commit loop body. Otherwise commits are never counted.
	outerBlock := body[innerClose:]
	if !strings.Contains(outerBlock, "result.Commits++") &&
		!strings.Contains(body[:innerOpen], "result.Commits++") {
		t.Error("insertCommitBatch must still increment result.Commits — " +
			"just outside the per-file loop. Current body has no increment, " +
			"so FacadeResult.Commits will always be 0.")
	}
}

// TestMigrationBackfillsLastCommits pins the one-time backfill that
// corrects existing inflated values in collection_queue.last_commits.
// Without this, every row already in the queue keeps its inflated
// count until the repo's next collection cycle (which could be 30
// days away under default cooldown).
func TestMigrationBackfillsLastCommits(t *testing.T) {
	data, err := os.ReadFile("../db/migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "last_commits") || !strings.Contains(src, "COUNT(DISTINCT cmt_commit_hash)") {
		t.Error("migrate.go must include a backfill step that updates " +
			"collection_queue.last_commits using COUNT(DISTINCT " +
			"cmt_commit_hash). Without it, existing rows keep the " +
			"v0.18.30–v0.19.10 inflated values until natural " +
			"re-collection. See `summary/04-refactoring-plan.md` " +
			"for the v0.19.11 plan.")
	}
	if !strings.Contains(src, "IS DISTINCT FROM") {
		t.Error("the backfill UPDATE should filter `WHERE last_commits " +
			"IS DISTINCT FROM sub.cnt` so subsequent migrate runs are " +
			"effectively no-ops once the values are corrected.")
	}
}
