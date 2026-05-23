// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.24.0 — distribution-stats CLI tests.
//
// `aveloxis distribution-stats` is the operator-facing read-only
// command for inspecting DistributionWorker coverage. Three views:
//
//   - no flags: fleet rollup (total repos, scanned, with evidence,
//     manifest-without-registry count) plus a per-ecosystem breakdown.
//   - --orphans: list repos with manifest evidence but no registry
//     evidence (the headline analysis case).
//   - --repo OWNER/REPO: every distribution + manifest row for one
//     repo.

func TestDistributionStatsCmdFileExists(t *testing.T) {
	if _, err := os.Stat("distribution_stats_cmd.go"); err != nil {
		t.Fatalf("cmd/aveloxis/distribution_stats_cmd.go must exist (Phase H deliverable): %v", err)
	}
}

func TestDistributionStatsCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "distributionStatsCmd(") {
		t.Error("main.go must register distributionStatsCmd via root.AddCommand")
	}
}

func TestDistributionStatsCmdHasOrphansFlag(t *testing.T) {
	data, err := os.ReadFile("distribution_stats_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"orphans"`) {
		t.Error("distribution-stats must declare --orphans flag for the manifest-without-registry analysis case")
	}
}

func TestDistributionStatsCmdHasRepoFlag(t *testing.T) {
	data, err := os.ReadFile("distribution_stats_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `"repo"`) {
		t.Error("distribution-stats must declare --repo flag for per-repo drill-down")
	}
}

func TestDistributionStatsCmdIsReadOnly(t *testing.T) {
	data, err := os.ReadFile("distribution_stats_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	// Strip // comments so the v0.21.5 rationale block (which
	// MENTIONS store.Migrate explaining why we don't call it)
	// doesn't false-match on this contract test. Shared helper
	// from migrate_only_serve_and_migrate_test.go.
	src := stripLineComments(string(data))
	if strings.Contains(src, "store.Migrate(ctx)") {
		t.Error("distribution-stats must NOT call store.Migrate(ctx). v0.21.5 policy: only serve and migrate trigger migrations. Add a comment explaining why if a future contributor is tempted to revert.")
	}
}
