// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// repo_groups FK-child indexes (v0.28.15). The v0.27.17 "Default"
// consolidation deletes every loser group; each delete fires one DEFERRED
// FK check per child table at COMMIT, and an unindexed child column makes
// every check a sequential scan (12 GB email_message: 5.3 s; 1.7 GB
// email_message_ref: 1.2 s; measured 2026-08-26 on the `aveloxis` DB
// where 873 deleted groups ground for ~1.6 h). Migration-owned
// CONCURRENTLY (SR-2): the base DDL runs first, so a schema.sql
// declaration would block-build on a live fleet.
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

// coveredFK is a repo_groups FK child whose column is already the LEADING
// column of an index some earlier step builds.
type coveredFK struct {
	column    string
	indexName string
}

// repoGroupFKCoveredElsewhere: repo_groups_list_serve.repo_group_id leads
// the UNIQUE idx_rgls_group_email (repo_group_id, rgls_email), built in
// stage 2 — after the v0.28.18 dedupRepoGroupsListServe step that makes
// the UNIQUE buildable on fleets carrying Augur-era duplicate list rows.
var repoGroupFKCoveredElsewhere = map[string]coveredFK{
	"repo_groups_list_serve": {column: "repo_group_id", indexName: "idx_rgls_group_email"},
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

// repoGroupFKIndexesReady reports whether EVERY repo_groups FK child
// column — the four we build plus the covered ones — is the leading
// column of a VALID index. v0.28.18: the probe is by LEADING COLUMN, not
// index name (a fresh-context L13 finding): a UNIQUE that cannot build
// on a duplicate-bearing fleet, or an index an operator rebuilt under a
// different name, must not keep the v0.27.17 consolidation gated shut
// when the column is in fact indexed. A probe ERROR is not "ready"
// (SR-5) — the caller records it and skips the consolidation.
func repoGroupFKIndexesReady(ctx context.Context, pg *PostgresStore) (bool, error) {
	var tables, columns []string
	for _, idx := range repoGroupFKIndexes {
		tables = append(tables, idx.table)
		columns = append(columns, idx.column)
	}
	for tbl, c := range repoGroupFKCoveredElsewhere {
		tables = append(tables, tbl)
		columns = append(columns, c.column)
	}
	pairs := len(tables)
	var valid int
	err := pg.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM unnest($1::text[], $2::text[]) AS want(tbl, col)
		WHERE EXISTS (
			SELECT 1
			FROM pg_namespace n
			JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = want.tbl
			JOIN pg_index x ON x.indrelid = c.oid
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
			WHERE n.nspname = 'aveloxis_data'
			  AND a.attname = want.col
			  AND x.indisvalid
		)`, tables, columns).Scan(&valid)
	if err != nil {
		return false, err
	}
	return valid == pairs, nil
}
