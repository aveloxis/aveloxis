// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"log/slog"
)

// v0.27.96 perf-wave indexes — the F1/F5/F7a findings from the first
// pg_stat_statements snapshot (summary/21-perf-findings-2026-08.md,
// measured on aveloxis_large 2026-08-18):
//
//   - pull_requests(repo_id, pr_number): the staged processor's
//     number→serial lookup ran 33.0M times at 31 ms mean (284 h — 18% of
//     ALL DB time in the 11-day window) filtering per-repo rows off the
//     single-column repo_id index.
//   - pull_request_reviews(repo_id, platform_review_id): same shape,
//     4.7M calls at 59.5 ms mean (77 h).
//   - issues(repo_id, issue_number): same shape, 8.5M calls at 13.5 ms
//     mean (32 h).
//   - staging (created_at) WHERE processed: the hourly
//     PurgeStagedProcessed DELETE seq-scanned the 33 GB staging table at
//     61.6 s mean. Partial: the purge predicate is
//     `processed AND created_at < …`, and unprocessed rows (the vast
//     majority by design) never qualify.
//
// This helper owns creation for BOTH fresh installs and live fleets
// (v0.27.98, PR #181 finding): the indexes are deliberately NOT declared
// in schema.sql because RunMigrations executes the base DDL first — a
// plain CREATE INDEX there would block-build on a live fleet's 34 GB
// pull_requests / 33 GB staging tables during the introducing release's
// first migrate, stalling every collection writer and turning the
// CONCURRENTLY statements below into no-ops. CONCURRENTLY on a fresh
// install's empty tables is instant, so nothing is lost. RULE: newly
// introduced indexes on fleet-scale tables are migration-only. Names
// verified against migrate.go's historical DROP INDEX steps
// (TestPerfWaveIndexNamesNotDropTargets).
var perfWaveIndexDDL = []struct{ name, sql string }{
	{"idx_pull_requests_repo_number",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pull_requests_repo_number
		 ON aveloxis_data.pull_requests (repo_id, pr_number)`},
	{"idx_pull_request_reviews_repo_platform_review_id",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pull_request_reviews_repo_platform_review_id
		 ON aveloxis_data.pull_request_reviews (repo_id, platform_review_id)`},
	{"idx_issues_repo_number",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_issues_repo_number
		 ON aveloxis_data.issues (repo_id, issue_number)`},
	{"idx_staging_processed_created",
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_processed_created
		 ON aveloxis_ops.staging (created_at) WHERE processed`},
}

func ensurePerfWaveIndexes(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, idx := range perfWaveIndexDDL {
		schema := "aveloxis_data"
		if idx.name == "idx_staging_processed_created" {
			schema = "aveloxis_ops"
		}
		execCreateIndexConcurrently(ctx, pg, logger, errs, schema, idx.name, idx.sql)
	}
}
