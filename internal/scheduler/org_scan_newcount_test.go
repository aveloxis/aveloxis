// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// TestOrgScanCountsOnlyGenuinelyNewLinks pins the 2026-08-18 miscount fix.
//
// refreshUserOrgs' per-group loop used to increment newCounts[gid] whenever
// AddRepoToGroupByID returned a nil error — but that INSERT is ON CONFLICT
// DO NOTHING, which returns nil on the no-op path too. Result in the
// Aug 7–16 production run: "user org scan found new repos" fired 63,960
// times claiming 9.3 MILLION new repos, with the SAME org reporting the
// SAME count every 4-hour pass (kubernetes new_repos=79 for 8.8 days).
//
// The counter must gate on the inserted bool that AddRepoToGroupByID now
// returns from the command tag's RowsAffected. The LINKING behavior itself
// is unchanged — the call still runs for every repo × group every pass so
// new repos in a tracked org keep landing in every registered group.
func TestOrgScanCountsOnlyGenuinelyNewLinks(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	src := string(data)

	// Isolate refreshUserOrgs' body (up to the next top-level func).
	start := strings.Index(src, "func (s *Scheduler) refreshUserOrgs(")
	if start < 0 {
		t.Fatal("refreshUserOrgs not found in scheduler.go")
	}
	rest := src[start:]
	if end := strings.Index(rest[1:], "\nfunc "); end >= 0 {
		rest = rest[:end+1]
	}

	// The old bug shape: counting on a bare nil-error check (brace directly
	// after err == nil — the fixed form carries "&& inserted" there).
	if strings.Contains(rest, "AddRepoToGroupByID(ctx, gid, repoID); err == nil {") {
		t.Error("refreshUserOrgs counts newCounts on a bare err == nil from " +
			"AddRepoToGroupByID — ON CONFLICT DO NOTHING returns nil on no-op " +
			"links, so this counts every repo in the org as \"new\" on every " +
			"pass (the 9.3M bogus new-repos flood, 2026-08-18 diagnosis)")
	}
	// The fixed shape: an inserted bool gates the counter.
	if !strings.Contains(rest, "inserted") || !strings.Contains(rest, "newCounts[gid]++") {
		t.Error("refreshUserOrgs must gate newCounts[gid]++ on the inserted " +
			"bool returned by AddRepoToGroupByID")
	}
	// v0.27.92 (Copilot round on PR #178): link failures must be logged,
	// matching the sibling call sites in refreshGitHubOrg /
	// refreshGitLabGroup — a silently failed link means a repo never
	// appears in the user's group with zero operator signal.
	if !strings.Contains(rest, "failed to link discovered repo into user_repos") {
		t.Error("refreshUserOrgs must WARN when AddRepoToGroupByID fails, " +
			"using the same \"failed to link discovered repo into user_repos\" " +
			"message as refreshGitHubOrg/refreshGitLabGroup so one grep " +
			"covers all three link paths")
	}
}
