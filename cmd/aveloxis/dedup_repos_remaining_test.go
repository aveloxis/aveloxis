// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.28.18: mid-collection pairs sit outside the batch window, so the
// run's "merged == 0" exit no longer means "nothing left" — the summary
// must re-count the remaining groups (SR-19: the rerun-until-0 contract
// is honest only with the remaining count, not the per-round skips).
func TestDedupReposReportsRemainingGroups(t *testing.T) {
	src, err := os.ReadFile("dedup_repos.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if strings.Count(s, "db.CountCaseVariantRepoDups(ctx, store)") < 2 {
		t.Error("runDedupRepos must count the duplicate groups again AFTER the batch loop, not only before it")
	}
	if !strings.Contains(s, `"remaining_groups"`) {
		t.Error("the end-of-run summary must report the remaining duplicate groups")
	}
	// A --limit exit leaves groups for a reason that is NOT "mid-collection";
	// the race count is a run total, not the last round's snapshot; the
	// precondition refusal is reported as such, never as a mid-way error.
	for _, needle := range []string{"capped = true", "case capped:", "skipped += batchSkipped", "errors.Is(err, db.ErrEmailMessageIndexesNotReady)"} {
		if !strings.Contains(s, needle) {
			t.Errorf("runDedupRepos must contain %q", needle)
		}
	}
	for _, stale := range []string{"contributor_repo) repoint", "commit_comment_ref, contributor_repo, foundation_membership"} {
		if strings.Contains(s, stale) {
			t.Errorf("stale prose: contributor_repo is deliberately NOT repointed (v0.25.34): %q", stale)
		}
	}
}
