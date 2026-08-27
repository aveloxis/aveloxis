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
// shut. Winner = lowest rgls_id. Empty-string addresses collide on the
// UNIQUE exactly like real ones (only NULLs are distinct), so the
// partition is `rgls_email IS NOT NULL`, not "non-empty".
//
// ONE transaction (the L10 re-runs' findings): mailing_list_staging is
// UNIQUE (rgls_id, message_id_header) and every registration of a list
// stages the same archive months, so a plain repoint collides with
// 23505. Step 1 keeps ONE staging row per (winner, header) across the
// WHOLE partition — the winner's own copy if it has one, else the
// earliest loser copy (two losers sharing a header collide with each
// other, not just with the winner) — then the survivors are repointed,
// email_message is repointed, the losers' checkpoints are merged into
// the winner (GREATEST of the yyyy-mm resume point and last run; scan
// stays incomplete if any copy was), and the loser rows are deleted.
// Partitions where any copy holds a LIVE worker lock are skipped this
// migrate (WARN; rerun after the drain) — deleting a row under a
// running worker would strand its checkpoint and its staging. Any
// failure rolls the whole pass back. A cheap existence probe (one row
// per list) skips the pass on clean fleets; a probe ERROR is collected,
// never treated as "clean".
func dedupRepoGroupsListServe(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	const label = "v0.28.18 consolidate duplicate repo_groups_list_serve rows (lowest rgls_id wins)"
	stale := MailingListStaleLock.String()
	var dups, liveLocked int
	err := pg.pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE NOT live_locked), count(*) FILTER (WHERE live_locked)
		FROM (
			SELECT bool_or(mlls_locked_at IS NOT NULL AND mlls_locked_at >= NOW() - $1::interval) AS live_locked
			FROM aveloxis_data.repo_groups_list_serve
			WHERE rgls_email IS NOT NULL
			GROUP BY repo_group_id, rgls_email
			HAVING count(*) > 1) p`, stale).Scan(&dups, &liveLocked)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: duplicate probe: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	if liveLocked > 0 {
		logger.Warn("repo_groups_list_serve duplicate partitions held by a LIVE mailing-list worker are skipped this migrate — rerun `aveloxis migrate --skip-views` after the drain",
			"partitions_skipped", liveLocked)
	}
	if dups == 0 {
		return
	}
	logger.Warn("repo_groups_list_serve carries duplicate (repo_group_id, rgls_email) rows — consolidating to the lowest rgls_id before the UNIQUE index builds",
		"partitions", dups)
	// Every row of every unlocked partition, with its partition's winner.
	const members = `
		SELECT rgls_id, winner FROM (
			SELECT rgls_id,
			       MIN(rgls_id) OVER w AS winner,
			       bool_or(mlls_locked_at IS NOT NULL AND mlls_locked_at >= NOW() - $1::interval) OVER w AS live_locked
			FROM aveloxis_data.repo_groups_list_serve
			WHERE rgls_email IS NOT NULL
			WINDOW w AS (PARTITION BY repo_group_id, rgls_email)
		) p WHERE NOT live_locked`
	steps := []struct{ what, sql string }{
		{"delete duplicate staging rows across each partition (keep the winner's copy, else the earliest)",
			`DELETE FROM aveloxis_ops.mailing_list_staging s
			 USING (
				SELECT st.mls_id,
				       ROW_NUMBER() OVER (PARTITION BY w.winner, st.message_id_header
				                          ORDER BY (st.rgls_id = w.winner) DESC, st.mls_id) AS rn
				FROM aveloxis_ops.mailing_list_staging st
				JOIN (` + members + `) w ON w.rgls_id = st.rgls_id
			 ) d
			 WHERE s.mls_id = d.mls_id AND d.rn > 1`},
		{"repoint remaining loser staging rows",
			`UPDATE aveloxis_ops.mailing_list_staging s SET rgls_id = w.winner
			 FROM (` + members + `) w
			 WHERE s.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
		{"repoint email_message.rgls_id",
			`UPDATE aveloxis_data.email_message em SET rgls_id = w.winner
			 FROM (` + members + `) w
			 WHERE em.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
		{"merge loser checkpoints into the winner",
			`UPDATE aveloxis_data.repo_groups_list_serve r
			 SET mlls_last_month    = GREATEST(COALESCE(r.mlls_last_month, ''), agg.last_month),
			     mlls_last_run      = GREATEST(r.mlls_last_run, agg.last_run),
			     mlls_scan_complete = COALESCE(r.mlls_scan_complete, FALSE) AND agg.all_complete
			 FROM (
				SELECT w.winner,
				       MAX(COALESCE(l.mlls_last_month, '')) AS last_month,
				       MAX(l.mlls_last_run) AS last_run,
				       bool_and(COALESCE(l.mlls_scan_complete, FALSE)) AS all_complete
				FROM aveloxis_data.repo_groups_list_serve l
				JOIN (` + members + `) w ON w.rgls_id = l.rgls_id
				WHERE w.rgls_id <> w.winner
				GROUP BY w.winner
			 ) agg
			 WHERE r.rgls_id = agg.winner`},
		{"delete loser list rows",
			`DELETE FROM aveloxis_data.repo_groups_list_serve r
			 USING (` + members + `) w
			 WHERE r.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
	}
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: begin: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	touched := map[string]int64{}
	for _, st := range steps {
		tag, err := tx.Exec(ctx, st.sql, stale)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("%s: %s: %w", label, st.what, err))
			logger.Error("migration step failed", "label", label, "sub_step", st.what, "error", err)
			return
		}
		touched[st.what] = tag.RowsAffected()
	}
	if err := tx.Commit(ctx); err != nil {
		*errs = append(*errs, fmt.Errorf("%s: commit: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	logger.Info("migration step ok", "label", label,
		"staging_duplicates_deleted", touched[steps[0].what],
		"staging_repointed", touched[steps[1].what],
		"email_messages_repointed", touched[steps[2].what],
		"winners_with_merged_checkpoints", touched[steps[3].what],
		"list_rows_deleted", touched[steps[4].what])
}
