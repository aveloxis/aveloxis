// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
)

// email_message FK-child indexes (v0.28.18, a fresh-context L11 sweep of
// the v0.28.15 repo_groups fix — the same table, one column over).
// email_message.repo_id, signaled_repo_id and rgls_id are FK columns
// (→ repos ×2, → repo_groups_list_serve) added in v0.25.7, AFTER the
// v0.22.6/v0.22.7 FK-index audits, and shipped unindexed — the v0.25.34
// class. `dedup-repos` repoints them by loser repo_id (a sequential scan
// of the table per pair — 12 GB / 10.6M rows on the mailing-list
// deployment, the `aveloxis` DB; ~300 MB on aveloxis_large) and then
// deletes the loser repos row,
// whose DEFERRED FK checks seq-scan the table twice more at COMMIT; the
// v0.28.18 repo_groups_list_serve dedup repoints rgls_id the same way.
// Partial on IS NOT NULL: the RI check and every repoint probe by
// equality, which implies NOT NULL, and most rows carry no
// signaled/list link. Migration-owned CONCURRENTLY (SR-2).
type emailMessageFKIndex struct {
	column    string
	indexName string
	// parent is the referenced table — the readiness probe below is
	// keyed by it, so every consolidation that deletes a parent row
	// asks for exactly the indexes its deferred FK checks will probe.
	parent string
}

var emailMessageFKIndexes = []emailMessageFKIndex{
	{"repo_id", "idx_email_message_repo_id", "repos"},
	{"signaled_repo_id", "idx_email_message_signaled_repo_id", "repos"},
	{"rgls_id", "idx_email_message_rgls_id", "repo_groups_list_serve"},
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

// dupListPartitionsSQL is THE spelling of a duplicate (canonical group,
// list) partition — the dedup's membership, the stage-10 gate's pending
// probe and the tests all read it (SR-17: a second inline spelling is a
// defect). One row per list row with its partition's winner (lowest
// rgls_id) and the partition size.
const dupListPartitionsSQL = `
	SELECT l.rgls_id,
	       MIN(l.rgls_id) OVER w AS winner,
	       count(*) OVER w AS copies
	FROM aveloxis_data.repo_groups_list_serve l
	JOIN (
		SELECT repo_group_id,
		       CASE WHEN rg_name IS NULL THEN repo_group_id
		            ELSE MIN(repo_group_id) OVER (PARTITION BY rg_name) END AS canonical_group
		FROM aveloxis_data.repo_groups
	) g ON g.repo_group_id = l.repo_group_id
	WHERE l.rgls_email IS NOT NULL
	WINDOW w AS (PARTITION BY g.canonical_group, l.rgls_email)`

// lockQuerier is what the pending probe needs from either a transaction
// or the pool.
type lockQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// leadingColumnIndexValid reports whether a VALID index led by column
// exists on aveloxis_data.table (pg_index.indkey[0]; the
// repoGroupFKIndexesReady shape for one pair).
func leadingColumnIndexValid(ctx context.Context, q lockQuerier, table, column string) (bool, error) {
	var ok bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_namespace n
			JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = $1
			JOIN pg_index x ON x.indrelid = c.oid
			JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = x.indkey[0]
			WHERE n.nspname = 'aveloxis_data' AND a.attname = $2 AND x.indisvalid)`, table, column).Scan(&ok); err != nil {
		return false, fmt.Errorf("probing for an index on %s(%s): %w", table, column, err)
	}
	return ok, nil
}

// ErrEmailMessageIndexesNotReady wraps the gate's not-ready refusal so
// callers can tell "precondition unmet — a migrate to run" from a probe
// ERROR (both refuse; SR-5 says neither reads as ready, but the operator
// message and the log level differ).
var ErrEmailMessageIndexesNotReady = errors.New("email_message FK indexes not ready")

// emailMessageFKIndexesReadyFor is THE gate every consolidation that
// deletes a `parent` row passes before touching data (SR-18: the store
// enforces it, so neither `dedup-repos`, `reconcile-repos`, prelim's
// rename heal nor the list dedup inside the migrate can grind without
// it). Deleting a repos row fires the
// deferred email_message.repo_id / signaled_repo_id checks at COMMIT and
// the case-variant merge repoints both columns first — without VALID
// indexes each is a sequential scan of the table per row — 12 GB on the
// mailing-list deployment (the v0.25.34 hung-commit shape). The column set and the index names come
// from emailMessageFKIndexes (SR-17: one list; a rename there renames
// the refusal too). A probe ERROR is not "ready" (SR-5); an unknown
// parent is a caller bug and never reads as ready.
func emailMessageFKIndexesReadyFor(ctx context.Context, q lockQuerier, parent string) error {
	probed := 0
	for _, idx := range emailMessageFKIndexes {
		if idx.parent != parent {
			continue
		}
		probed++
		ready, err := leadingColumnIndexValid(ctx, q, "email_message", idx.column)
		if err != nil {
			return err
		}
		if !ready {
			return fmt.Errorf("%w: email_message(%s) has no valid index — run `aveloxis migrate --skip-views` first (v0.28.18 builds %s CONCURRENTLY); without it every %s consolidation sequential-scans email_message", ErrEmailMessageIndexesNotReady, idx.column, idx.indexName, parent)
		}
	}
	if probed == 0 {
		return fmt.Errorf("no email_message FK index is registered for parent %q", parent)
	}
	return nil
}

// dedupRepoGroupsListServe — SR-1 for idx_rgls_group_email (v0.28.18).
// The UNIQUE (repo_group_id, rgls_email) index is the arbiter of
// RegisterMailingList's ON CONFLICT and was built CONCURRENTLY since
// v0.25.7 with no dedup ahead of it; repo_groups_list_serve is an
// Augur-heritage table with a PK only, so a fleet carrying duplicate
// (group, list) registrations could never build the index — every
// migrate failed the CIC, RegisterMailingList failed with 42P10, and
// the v0.28.16 readiness gate would have kept the v0.27.17 consolidation
// shut. ONE transaction; any failure rolls the whole pass back. The
// body lives in dedupRepoGroupsListServeTx (the integration test drives
// it inside a transaction of its own and rolls back).
func dedupRepoGroupsListServe(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	const label = "v0.28.18 consolidate duplicate repo_groups_list_serve rows (lowest rgls_id wins)"
	// The loser DELETE's deferred FK checks probe email_message.rgls_id at
	// COMMIT — without a valid index that is one sequential scan of the
	// table per loser, 12 GB on the mailing-list deployment (the v0.28.16
	// decorative-gate class:
	// ensureEmailMessageFKIndexes only RECORDS a failed CONCURRENTLY
	// build). THE shared gate decides; a not-ready refusal is a skip the
	// next migrate retries (the CONCURRENTLY build above is re-attempted
	// every run), a probe ERROR is a failed step (SR-5).
	if err := emailMessageFKIndexesReadyFor(ctx, pg.pool, "repo_groups_list_serve"); err != nil {
		if errors.Is(err, ErrEmailMessageIndexesNotReady) {
			*errs = append(*errs, fmt.Errorf("%s skipped (its CONCURRENTLY index build failed above; rerun the migrate once it builds): %w", label, err))
			logger.Warn("list dedup skipped — email_message(rgls_id) index not valid", "label", label, "error", err)
			return
		}
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: begin: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	touched, err := dedupRepoGroupsListServeTx(ctx, tx, logger, pg.ownBackendPIDs)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	if touched == nil {
		return // clean fleet, or another serve is connected (skipped; WARN above)
	}
	if err := tx.Commit(ctx); err != nil {
		*errs = append(*errs, fmt.Errorf("%s: commit: %w", label, err))
		logger.Error("migration step failed", "label", label, "error", err)
		return
	}
	logger.Info("migration step ok", "label", label,
		"staging_duplicates_deleted", touched["staging_duplicates_deleted"],
		"staging_repointed", touched["staging_repointed"],
		"email_messages_repointed", touched["email_messages_repointed"],
		"winners_with_merged_checkpoints", touched["winners_with_merged_checkpoints"],
		"list_rows_deleted", touched["list_rows_deleted"])
}

// ServeApplicationName is the application_name `aveloxis serve` tags its
// pool with (v0.20.0; cmd/aveloxis/main.go runServe uses this symbol).
// The mailing-list worker pool lives only inside serve, so "an
// aveloxis-serve backend other than this process" is the one signal the
// list dedup needs.
const ServeApplicationName = "aveloxis-serve"

// serveBackendsBeyondOwnPool reports whether any `aveloxis serve` backend
// OTHER than this process's own pool connections is connected to THIS
// database. Own connections are excluded by server PID
// (PostgresStore.backendPIDs, fed by the pool's AfterConnect/BeforeClose
// hooks): exact under every topology — containers, NAT, dual-stack
// localhost — and under a transaction pooler the pooler's fake PIDs
// simply fail to match, which counts our own backends as another serve
// (the safe direction). pg_stat_activity is cluster-wide (the datname
// filter is load-bearing) and SNAPSHOTTED per transaction (the first
// read fixes the view; a same-statement pg_stat_clear_snapshot() runs
// AFTER the read — proven with EXPLAIN on PG 18), so the clear is its
// own statement first — on the SAME session, hence the pgx.Tx parameter.
// ownPIDs is a FUNC, snapshotted immediately before the read: the pool
// creates backends on demand (watchBlockers' 60 s poll, a MinConns
// refill), and a snapshot taken earlier would read such a backend as
// another serve (the twelfth pass; safe direction, but a false skip).
func serveBackendsBeyondOwnPool(ctx context.Context, tx pgx.Tx, ownPIDs func() []int32) (bool, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		return false, fmt.Errorf("clearing the activity snapshot: %w", err)
	}
	var own []int32
	if ownPIDs != nil {
		own = ownPIDs()
	}
	if own == nil {
		own = []int32{} // pgx encodes a nil slice as SQL NULL, and <> ALL(NULL) is NULL
	}
	var n int
	if err := tx.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity a
		WHERE a.datname = current_database() AND a.application_name = $1
		  AND a.pid <> ALL($2::int4[])`,
		ServeApplicationName, own).Scan(&n); err != nil {
		return false, fmt.Errorf("probing for a connected aveloxis-serve: %w", err)
	}
	return n > 0, nil
}

// dedupRepoGroupsListServeTx is the pass itself, inside the caller's tx.
//
// Partition = (CANONICAL group, rgls_email): the v0.27.17 consolidation
// repoints every same-named group's list rows onto MIN(repo_group_id)
// with a plain UPDATE, so two same-named groups that both registered a
// list would collide on the UNIQUE the moment it exists — they are one
// partition here (the surviving row may sit in the non-canonical group;
// the consolidation moves it). Winner = lowest rgls_id. Empty-string
// addresses collide on the UNIQUE like real ones (only NULLs are
// distinct), so the predicate is `rgls_email IS NOT NULL`. Only
// multi-row partitions are candidates.
//
// THE rule (the eleventh pass): while any `aveloxis serve` other than
// this process is connected to the database, NOTHING is consolidated.
// Only the scan stamps mlls_locked_at — the drain (DrainList) holds no
// database lock at all — so no lock-age rule can tell "idle" from
// "mid-drain", and repointing a loser's staging under a drain batch
// makes its next write fail 23503 and the batch get marked processed
// under the winner: lost messages. With no other serve there is no
// worker and no drain, and every young lock is a ghost of a stopped
// serve (`stop serve` → `migrate` is the upgrade ladder; serve's own
// startup migrate runs before its workers exist and its own backends
// are excluded by PID). The candidates are read once, locked FOR UPDATE
// (ClaimNextList uses SKIP LOCKED), the serve probe is repeated on the
// locked set, and every statement joins the frozen (rgls_id, winner)
// pairs. Steps: (1) keep ONE staging row per (winner, header) across
// the whole partition — the winner's own copy if it has one, else the
// earliest (mailing_list_staging is UNIQUE (rgls_id, message_id_header);
// two losers sharing a header collide with EACH OTHER, not just with
// the winner); (2) repoint the surviving loser staging — rgls_id AND
// repo_group_id, which DrainList reads as list identity; (3) repoint
// email_message.rgls_id; (4) merge the losers' checkpoints into the
// winner (GREATEST of the yyyy-mm resume point and last run; the scan
// stays incomplete if any copy was); (5) delete the losers. Returns nil
// counts when nothing was done (clean fleet, or skipped for a serve).
func dedupRepoGroupsListServeTx(ctx context.Context, tx pgx.Tx, logger *slog.Logger, ownPIDs func() []int32) (map[string]int64, error) {
	rows, err := tx.Query(ctx, `SELECT rgls_id, winner FROM (`+dupListPartitionsSQL+`) p
		WHERE copies > 1 ORDER BY winner, rgls_id`)
	if err != nil {
		return nil, fmt.Errorf("duplicate candidates: %w", err)
	}
	var ids, winners []int64
	partitions := map[int64]bool{}
	for rows.Next() {
		var id, winner int64
		if err := rows.Scan(&id, &winner); err != nil {
			rows.Close()
			return nil, fmt.Errorf("duplicate candidates: %w", err)
		}
		ids, winners = append(ids, id), append(winners, winner)
		partitions[winner] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duplicate candidates: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	const skipWarn = "repo_groups_list_serve carries duplicate (group, list) rows but another aveloxis-serve is connected to this database — consolidating nothing (a drain holds no lock, so no row is provably idle); the UNIQUE index and the repo_groups consolidation wait: rerun `aveloxis migrate --skip-views` with serve stopped"
	if other, err := serveBackendsBeyondOwnPool(ctx, tx, ownPIDs); err != nil {
		return nil, err
	} else if other {
		logger.Warn(skipWarn, "partitions", len(partitions))
		return nil, nil
	}
	// Lock the frozen members (no worker can claim one now), then repeat
	// the serve probe: a serve may have connected between read and lock.
	if _, err := tx.Exec(ctx, `
		SELECT rgls_id FROM aveloxis_data.repo_groups_list_serve
		WHERE rgls_id = ANY($1::bigint[]) FOR UPDATE`, ids); err != nil {
		return nil, fmt.Errorf("locking duplicate list rows: %w", err)
	}
	if other, err := serveBackendsBeyondOwnPool(ctx, tx, ownPIDs); err != nil {
		return nil, err
	} else if other {
		logger.Warn(skipWarn, "partitions", len(partitions))
		return nil, nil
	}
	logger.Warn("repo_groups_list_serve carries duplicate (group, list) rows and no other aveloxis-serve is connected — consolidating to the lowest rgls_id before the UNIQUE index builds (young worker locks are ghosts of a stopped serve)",
		"partitions", len(partitions), "losers", len(ids)-len(partitions), "member_rows_locked", len(ids))
	// Every statement joins the SAME frozen pairs.
	const members = `unnest($1::bigint[], $2::bigint[]) AS w(rgls_id, winner)`
	steps := []struct{ key, sql string }{
		{"staging_duplicates_deleted",
			`DELETE FROM aveloxis_ops.mailing_list_staging s
			 USING (
				SELECT st.mls_id,
				       ROW_NUMBER() OVER (PARTITION BY w.winner, st.message_id_header
				                          ORDER BY (st.rgls_id = w.winner) DESC, st.mls_id) AS rn
				FROM aveloxis_ops.mailing_list_staging st
				JOIN ` + members + ` ON w.rgls_id = st.rgls_id
			 ) d
			 WHERE s.mls_id = d.mls_id AND d.rn > 1`},
		{"staging_repointed",
			`UPDATE aveloxis_ops.mailing_list_staging s
			 SET rgls_id = w.winner,
			     repo_group_id = (SELECT repo_group_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = w.winner)
			 FROM ` + members + `
			 WHERE s.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
		{"email_messages_repointed",
			`UPDATE aveloxis_data.email_message em SET rgls_id = w.winner
			 FROM ` + members + `
			 WHERE em.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
		{"winners_with_merged_checkpoints",
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
				JOIN ` + members + ` ON w.rgls_id = l.rgls_id
				WHERE w.rgls_id <> w.winner
				GROUP BY w.winner
			 ) agg
			 WHERE r.rgls_id = agg.winner`},
		{"list_rows_deleted",
			`DELETE FROM aveloxis_data.repo_groups_list_serve r
			 USING ` + members + `
			 WHERE r.rgls_id = w.rgls_id AND w.rgls_id <> w.winner`},
	}
	touched := map[string]int64{}
	for _, st := range steps {
		tag, err := tx.Exec(ctx, st.sql, ids, winners)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", st.key, err)
		}
		touched[st.key] = tag.RowsAffected()
	}
	return touched, nil
}

// listDedupPending counts duplicate (canonical group, list) partitions
// still present — the ones a connected serve made the dedup skip, or a
// dedup step that failed left behind. The v0.27.17 consolidation's plain
// repoint of repo_groups_list_serve.repo_group_id would collide on the
// UNIQUE for exactly those, so the stage-10 gate consults this (SR-5: an
// ERROR is not "none pending"). Same spelling as the dedup's membership.
func listDedupPending(ctx context.Context, q lockQuerier) (int, error) {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(DISTINCT winner) FROM (`+dupListPartitionsSQL+`) p WHERE copies > 1`).Scan(&n); err != nil {
		return 0, fmt.Errorf("duplicate list partition probe: %w", err)
	}
	return n, nil
}
