// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// repoGroupFKIndex enumerates the repo_groups(repo_group_id) FK child
// columns that need a btree index (v0.28.15).
//
// The 2026-08-26 `aveloxis` DB upgrade (0.25.26 → 0.28.x) found the
// v0.27.17 consolidation's loser-group DELETE grinding for ~1.6 hours:
// every deleted group fires a DEFERRED NO ACTION FK check per child
// table (`SELECT 1 FROM child WHERE repo_group_id = $1 FOR KEY SHARE`),
// and with no index on email_message.repo_group_id that check was a
// 5.3 s sequential scan of a 12 GB table — 873 losers × two unindexed
// children. The v0.25.34 class recurring: an FK added after the
// v0.22.6/7 index audits (email_message, v0.25.7) shipped unindexed and
// stayed invisible until something bulk-deleted the referenced rows.
//
// Migration-owned via CONCURRENTLY (SR-2 — never in schema.sql: the
// base DDL runs before any migration step, so a plain declaration
// would block-build on a live fleet's 12 GB table). RunMigrations calls
// ensureRepoGroupFKIndexes BEFORE the v0.27.17 consolidation block so
// an un-consolidated fleet's loser DELETE runs against indexed checks
// (pinned by TestRepoGroupFKIndexesRunBeforeConsolidation).
//
// TestEveryRepoGroupsFKChildIsIndexed parses schema.sql for every
// column that REFERENCES repo_groups and requires it to appear here or
// in repoGroupFKCoveredElsewhere — a new repo_groups child FK cannot
// ship unindexed again.
type repoGroupFKIndex struct {
	table     string // child table (relative to aveloxis_data)
	column    string // FK column
	indexName string // idx_<table>_<column> convention
}

var repoGroupFKIndexes = []repoGroupFKIndex{
	{"repos", "repo_group_id", "idx_repos_repo_group_id"},
	{"email_message", "repo_group_id", "idx_email_message_repo_group_id"},
	{"email_message_ref", "repo_group_id", "idx_email_message_ref_repo_group_id"},
	{"repo_group_insights", "repo_group_id", "idx_repo_group_insights_repo_group_id"},
}

// repoGroupFKCoveredElsewhere lists repo_groups children whose
// repo_group_id already LEADS an existing index (so no new index is
// needed): child table → covering index name. The tripwire verifies the
// covering index is declared with the column in leading position.
var repoGroupFKCoveredElsewhere = map[string]string{
	"repo_groups_list_serve": "idx_rgls_group_email", // (repo_group_id, rgls_email)
}

func ensureRepoGroupFKIndexes(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, idx := range repoGroupFKIndexes {
		sql := fmt.Sprintf(
			`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON aveloxis_data.%s (%s)`,
			idx.indexName, idx.table, idx.column,
		)
		execCreateIndexConcurrently(ctx, pg, logger, errs,
			"aveloxis_data", idx.indexName, sql)
	}
}
