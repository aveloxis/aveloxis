// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// cntrbIDFKIndex enumerates the 15 child FK columns pointing at
// aveloxis_data.contributors(cntrb_id) that need btree indexes to
// make v0.22.1's ON UPDATE CASCADE tractable.
//
// Without these indexes every cntrb_id rewrite cascades into
// sequential scans of the child tables. On the production
// aveloxis_large DB this kept `aveloxis migrate-cntrb-ids` stuck
// on batch 1 for 17+ hours with zero committed progress: 5,000
// batch rows × 15 unindexed child tables (some 50M+ rows) =
// 75,000 sequential scans per batch attempt.
//
// contributor_identities.cntrb_id is NOT in this list — it has
// been indexed since the early schema days via
// idx_contributor_identities_cntrb (declared inline in schema.sql).
// Adding a redundant second index would be churn.
//
// The set MUST stay synchronized with cntrbIDChildFKs in
// cntrb_id_cascade.go (minus contributor_identities). Tests in
// cntrb_id_fk_indexes_test.go pin both lists against schema.sql
// and against this file.
type cntrbIDFKIndex struct {
	table     string // child table (relative to aveloxis_data schema)
	column    string // FK column
	indexName string // index name (idx_<table>_<column> convention)
}

var cntrbIDFKIndexes = []cntrbIDFKIndex{
	{"contributor_repo", "cntrb_id", "idx_contributor_repo_cntrb_id"},
	{"contributors_aliases", "cntrb_id", "idx_contributors_aliases_cntrb_id"},
	{"issue_assignees", "cntrb_id", "idx_issue_assignees_cntrb_id"},
	{"issue_events", "cntrb_id", "idx_issue_events_cntrb_id"},
	{"issues", "closed_by_id", "idx_issues_closed_by_id"},
	{"issues", "reporter_id", "idx_issues_reporter_id"},
	{"messages", "cntrb_id", "idx_messages_cntrb_id"},
	{"pull_request_assignees", "cntrb_id", "idx_pull_request_assignees_cntrb_id"},
	{"pull_request_commits", "author_cntrb_id", "idx_pull_request_commits_author_cntrb_id"},
	{"pull_request_events", "cntrb_id", "idx_pull_request_events_cntrb_id"},
	{"pull_request_meta", "cntrb_id", "idx_pull_request_meta_cntrb_id"},
	{"pull_request_repo", "pr_cntrb_id", "idx_pull_request_repo_pr_cntrb_id"},
	{"pull_request_reviewers", "cntrb_id", "idx_pull_request_reviewers_cntrb_id"},
	{"pull_request_reviews", "cntrb_id", "idx_pull_request_reviews_cntrb_id"},
	{"pull_requests", "author_id", "idx_pull_requests_author_id"},
}

// ensureCntrbIDFKIndexes is the v0.22.6 migration step that adds
// a btree index on every unindexed FK column pointing at
// aveloxis_data.contributors(cntrb_id). Uses
// execCreateIndexConcurrently so production keeps accepting writes
// during the (potentially long) build on multi-million-row tables.
//
// Idempotent via `CREATE INDEX CONCURRENTLY IF NOT EXISTS` plus the
// execCreateIndexConcurrently helper's INVALID-index detection
// inherited from v0.20.1 — interrupted CONCURRENT builds leave an
// INVALID index, the helper drops it before retrying.
//
// Errors are appended to the shared collector per the v0.19.4
// fail-closed contract — serve startup refuses to proceed if any
// index build fails.
//
// Build durations vary widely by table size. On the production
// fleet's aveloxis_large DB, the messages and pull_request_commits
// indexes are the long pole (each table holds tens of millions of
// rows). Operators can run `aveloxis migrate` and let it run; the
// CONCURRENTLY flag prevents the build from blocking writes.
func ensureCntrbIDFKIndexes(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, idx := range cntrbIDFKIndexes {
		sql := fmt.Sprintf(
			`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON aveloxis_data.%s (%s)`,
			idx.indexName, idx.table, idx.column,
		)
		execCreateIndexConcurrently(ctx, pg, logger, errs,
			"aveloxis_data", idx.indexName, sql)
	}
}
