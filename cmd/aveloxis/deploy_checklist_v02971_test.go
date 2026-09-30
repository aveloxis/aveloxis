// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"
)

// TestV02971ChecklistNamesItsOperatorVisibleChanges pins what an operator
// must learn from `aveloxis deploy-checklist` for 0.29.71 (summary/43 §2),
// and that a fleet skipping 0.29.70 still gets its notes.
func TestV02971ChecklistNamesItsOperatorVisibleChanges(t *testing.T) {
	steps, ok := deployChecklistFor("0.29.71")
	if !ok || len(steps) == 0 {
		t.Fatal("0.29.71 has no deploy checklist")
	}
	if steps[0].cmd != "aveloxis stop all" || steps[len(steps)-1].cmd != "aveloxis start all" {
		t.Errorf("the ladder must start with stop all and end with start all")
	}
	var migrate, start string
	for _, s := range steps {
		switch {
		case strings.HasPrefix(s.cmd, "aveloxis migrate"):
			migrate = s.desc
		case s.cmd == "aveloxis start all":
			start = s.desc
		}
	}
	for _, idx := range []string{"idx_messages_repo_ts_cntrb", "idx_pr_reviews_repo_submitted_cntrb", "idx_pull_requests_repo_merged", "idx_issues_repo_closed", "CONCURRENTLY"} {
		if !strings.Contains(migrate, idx) {
			t.Errorf("the migrate step must name %q", idx)
		}
	}
	if !strings.Contains(migrate, "dependency_scope") {
		t.Error("the migrate step must say the dependency_scope backfill runs one last time")
	}
	// Whole-branch review D2: the list keeps the supply-chain view check
	// (v02967DeployChecklist[2]), so the migrate step must say it re-creates
	// them, and a fleet that skipped 0.29.69 must still learn its columns.
	for _, want := range []string{"supply-chain views", "0.29.69"} {
		if !strings.Contains(migrate, want) {
			t.Errorf("the migrate step must name %q", want)
		}
	}
	for _, want := range []string{"o***@", "approved add requests retried", "workspace", "token_hash", "X-Cache", "response size high-water mark"} {
		if !strings.Contains(start, want) {
			t.Errorf("the start step must name %q", want)
		}
	}
}
