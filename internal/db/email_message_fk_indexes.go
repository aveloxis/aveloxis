// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// email_message FK-child indexes (v0.28.18, a fresh-context L11 sweep of
// the v0.28.15 repo_groups fix — the same table, one column over).
// email_message.repo_id, signaled_repo_id and rgls_id are FK columns
// (→ repos ×2, → repo_groups_list_serve) added in v0.25.7, AFTER the
// v0.22.6/v0.22.7 FK-index audits, and shipped unindexed — the v0.25.34
// class. `dedup-repos` repoints them by loser repo_id (a sequential scan
// of the 12 GB table per pair) and then deletes the loser repos row,
// whose DEFERRED FK checks seq-scan the table twice more at COMMIT; the
// v0.28.18 repo_groups_list_serve dedup repoints rgls_id the same way.
// Partial on IS NOT NULL: the RI check and every repoint probe by
// equality, which implies NOT NULL, and most rows carry no
// signaled/list link. Migration-owned CONCURRENTLY (SR-2).
type emailMessageFKIndex struct {
	column    string
	indexName string
}

var emailMessageFKIndexes = []emailMessageFKIndex{
	{"repo_id", "idx_email_message_repo_id"},
	{"signaled_repo_id", "idx_email_message_signaled_repo_id"},
	{"rgls_id", "idx_email_message_rgls_id"},
}

func ensureEmailMessageFKIndexes(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	for _, idx := range emailMessageFKIndexes {
		sql := fmt.Sprintf(
			`CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON aveloxis_data.email_message (%s) WHERE %s IS NOT NULL`,
			idx.indexName, idx.column, idx.column,
		)
		execCreateIndexConcurrently(ctx, pg, logger, errs, "aveloxis_data", idx.indexName, sql)
	}
}

// dedupRepoGroupsListServe — SR-1 for idx_rgls_group_email (v0.28.18).
// The UNIQUE (repo_group_id, rgls_email) index is the arbiter of
// RegisterMailingList's ON CONFLICT and was built CONCURRENTLY since
// v0.25.7 with no dedup ahead of it; repo_groups_list_serve is an
// Augur-heritage table with a PK only, so a fleet carrying duplicate
// (group, list) registrations could never build the index — every
// migrate failed the CIC, RegisterMailingList failed with 42P10, and
// the v0.28.16 readiness gate would have kept the v0.27.17 consolidation
// shut. Winner = lowest rgls_id; losers' email_message and staging rows
// are repointed to the winner before the losers are deleted. A cheap
// existence probe (the table holds one row per list) skips the pass on
// clean fleets; a probe ERROR is collected, never treated as "clean".
func dedupRepoGroupsListServe(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	var dups bool
	err := pg.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM aveloxis_data.repo_groups_list_serve
			WHERE COALESCE(rgls_email, '') <> ''
			GROUP BY repo_group_id, rgls_email
			HAVING count(*) > 1)`).Scan(&dups)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("repo_groups_list_serve duplicate probe: %w", err))
		logger.Error("migration step failed", "label", "v0.28.18 repo_groups_list_serve duplicate probe", "error", err)
		return
	}
	if !dups {
		return
	}
	logger.Warn("repo_groups_list_serve carries duplicate (repo_group_id, rgls_email) rows — consolidating to the lowest rgls_id before the UNIQUE index builds")
	const losers = `
		SELECT rgls_id, MIN(rgls_id) OVER (PARTITION BY repo_group_id, rgls_email) AS winner
		FROM aveloxis_data.repo_groups_list_serve
		WHERE COALESCE(rgls_email, '') <> ''`
	execMigrationStep(ctx, pg, logger, errs,
		"v0.28.18 repoint email_message.rgls_id from duplicate list rows to the winner",
		`UPDATE aveloxis_data.email_message em SET rgls_id = w.winner
		 FROM (`+losers+`) w
		 WHERE em.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`)
	execMigrationStep(ctx, pg, logger, errs,
		"v0.28.18 repoint mailing_list_staging.rgls_id from duplicate list rows to the winner",
		`UPDATE aveloxis_ops.mailing_list_staging s SET rgls_id = w.winner
		 FROM (`+losers+`) w
		 WHERE s.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`)
	execMigrationStep(ctx, pg, logger, errs,
		"v0.28.18 delete duplicate repo_groups_list_serve rows (keep the lowest rgls_id per group + list)",
		`DELETE FROM aveloxis_data.repo_groups_list_serve r
		 USING (`+losers+`) w
		 WHERE r.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`)
}
