// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// v0.27.96 — the F1/F5/F7a index wave from the first pg_stat_statements
// snapshot (summary/21-perf-findings-2026-08.md, measured on aveloxis_large
// 2026-08-18 over an 11-day window):
//
//   F1  pull_requests(repo_id, pr_number)        — the staged processor's
//       PR-number→serial lookup was 284 h / 33.0M calls / 31 ms mean
//       (18% of ALL DB time); no index covered pr_number.
//   F5a pull_request_reviews(repo_id, platform_review_id) — 77 h / 4.7M
//       calls / 59.5 ms mean; only (pull_request_id, platform_review_id)
//       and single-column indexes existed.
//   F5b issues(repo_id, issue_number)            — 32 h / 8.5M calls /
//       13.5 ms mean; no index covered issue_number.
//   F7a staging purge partial index              — the hourly
//       PurgeStagedProcessed DELETE seq-scanned 33 GB at 61.6 s mean.

var perfWaveIndexes = []struct{ name, table, columns string }{
	{"idx_pull_requests_repo_number", "aveloxis_data.pull_requests", "(repo_id, pr_number)"},
	{"idx_pull_request_reviews_repo_platform_review_id", "aveloxis_data.pull_request_reviews", "(repo_id, platform_review_id)"},
	{"idx_issues_repo_number", "aveloxis_data.issues", "(repo_id, issue_number)"},
	{"idx_staging_processed_created", "aveloxis_ops.staging", "(created_at) WHERE processed"},
}

// TestSchemaDeclaresPerfWaveIndexes pins the fresh-install declarations.
func TestSchemaDeclaresPerfWaveIndexes(t *testing.T) {
	src := readFileForTest(t, "schema.sql")
	flat := strings.Join(strings.Fields(src), " ")
	for _, idx := range perfWaveIndexes {
		needle := "CREATE INDEX IF NOT EXISTS " + idx.name + " ON " + idx.table + " " + idx.columns
		if !strings.Contains(flat, needle) {
			t.Errorf("schema.sql missing %q — the v0.27.96 perf-wave index "+
				"(summary/21 F1/F5/F7a: measured 284h/77h/32h/3.6h of DB time "+
				"on the unindexed shapes)", needle)
		}
	}
}

// TestMigrationBuildsPerfWaveIndexesConcurrently pins the live-fleet path:
// all four build via execCreateIndexConcurrently (pull_requests is 34 GB,
// staging 33 GB — a blocking build would stall collection writers).
func TestMigrationBuildsPerfWaveIndexesConcurrently(t *testing.T) {
	combined := readFileForTest(t, "migrate.go") + readFileForTest(t, "perf_indexes.go")
	if !strings.Contains(combined, "execCreateIndexConcurrently") {
		t.Fatal("perf-wave indexes must build via execCreateIndexConcurrently")
	}
	for _, idx := range perfWaveIndexes {
		if !strings.Contains(combined, idx.name) {
			t.Errorf("migration source missing perf-wave index %q", idx.name)
		}
	}
}

// TestPerfWaveIndexNamesNotDropTargets guards against the v0.27.67 lesson:
// migrate.go's historical DROP INDEX steps run on every migrate, so reusing
// a dropped name would rebuild the index each run.
func TestPerfWaveIndexNamesNotDropTargets(t *testing.T) {
	src := readFileForTest(t, "migrate.go")
	for _, line := range strings.Split(src, "\n") {
		if !strings.Contains(line, "DROP INDEX") {
			continue
		}
		for _, idx := range perfWaveIndexes {
			if strings.Contains(line, idx.name) {
				t.Errorf("perf-wave index %q collides with a DROP INDEX step: %s", idx.name, strings.TrimSpace(line))
			}
		}
	}
}
