// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// msg_ref_metadata.go — v0.27.15 one-shot repairs for the three
// message-bridge tables, all healed from data ALREADY IN THE DATABASE
// (zero API calls, no re-collection):
//
//  1. pull_request_review_message_ref DEDUP: the insert's bare
//     `ON CONFLICT DO NOTHING` had no unique constraint to arbitrate
//     against (dead code — the v0.27.7 repo_labor lesson), so every
//     re-collection cycle duplicated the review-body bridge rows.
//     Production: 10.47M rows, 5.26M duplicates.
//  2. uq_pr_review_msg_ref UNIQUE (pr_review_id, msg_id) — created
//     ONLY after the dedup drains (schema-DDL-ordering rule: NOT in
//     schema.sql, where it would fail against existing duplicate data
//     before this migration runs).
//  3. data_source backfill on issue_message_ref (21.7M),
//     pull_request_message_ref (32.5M), and
//     pull_request_review_message_ref — 100% empty since inception;
//     the messages row each ref points at has carried the exact
//     provenance all along.
//  4. Inline review comments backfilled INTO
//     pull_request_review_message_ref with their full line-anchoring
//     metadata from review_comments (23.3M rows) — the ref table's
//     line columns were 100% dark while review_comments held all of
//     it.
//
// Backfills batch by KEYSET WINDOWS over each table's PK (the
// v0.26.6 lesson: never LIMIT-rescan loops; window width sized
// against production PK ranges). Every window is its own implicit
// transaction — interrupted runs resume; all steps are idempotent.

import (
	"context"
	"fmt"
	"log/slog"
)

// MsgRefBackfillWindowSize is the PK-window width for the v0.27.15
// backfills. At production scale (PK maxes in the tens of millions)
// this walks ~25–60 windows per table, each a bounded index-range
// statement.
const MsgRefBackfillWindowSize = 1_000_000

// PRReviewMsgRefDedupSQL removes duplicate (pr_review_id, msg_id)
// bridge rows keeping the OLDEST (minimum PK). One full pass;
// idempotent (a deduped table deletes zero rows).
const PRReviewMsgRefDedupSQL = `
	DELETE FROM aveloxis_data.pull_request_review_message_ref t
	USING (
		SELECT pr_review_msg_ref_id,
		       ROW_NUMBER() OVER (PARTITION BY pr_review_id, msg_id
		                          ORDER BY pr_review_msg_ref_id) AS rn
		FROM aveloxis_data.pull_request_review_message_ref
	) d
	WHERE t.pr_review_msg_ref_id = d.pr_review_msg_ref_id AND d.rn > 1`

// msgRefDataSourceTargets enumerates the bridge tables whose
// data_source is backfilled from the messages row each ref points at.
var msgRefDataSourceTargets = []struct {
	Table string // fully qualified
	PK    string
}{
	{"aveloxis_data.issue_message_ref", "issue_msg_ref_id"},
	{"aveloxis_data.pull_request_message_ref", "pr_msg_ref_id"},
	{"aveloxis_data.pull_request_review_message_ref", "pr_review_msg_ref_id"},
}

// MsgRefDataSourceWindowSQL is the per-window UPDATE, instantiated per
// target table ($1 = exclusive lower PK bound, $2 = inclusive upper).
// The COALESCE(...) = '' filter makes re-runs no-ops.
func MsgRefDataSourceWindowSQL(table, pk string) string {
	return fmt.Sprintf(`
		UPDATE %s t
		SET data_source = m.data_source
		FROM aveloxis_data.messages m
		WHERE m.msg_id = t.msg_id
		  AND COALESCE(t.data_source, '') = ''
		  AND COALESCE(m.data_source, '') <> ''
		  AND t.%s > $1 AND t.%s <= $2`, table, pk, pk)
}

// PRReviewMsgRefInlineBackfillWindowSQL copies inline review comments
// into the Augur-compat ref table with their full line metadata
// ($1/$2 = keyset window over review_comments.review_comment_id).
// Comments whose review is not in the DB (pr_review_id IS NULL) are
// skipped — the ref's pr_review_id is NOT NULL. Idempotent via the
// uq_pr_review_msg_ref arbiter.
const PRReviewMsgRefInlineBackfillWindowSQL = `
	INSERT INTO aveloxis_data.pull_request_review_message_ref
		(pr_review_id, repo_id, msg_id, pr_review_msg_src_id, pr_review_msg_node_id,
		 pr_review_msg_diff_hunk, pr_review_msg_path, pr_review_msg_position,
		 pr_review_msg_original_position, pr_review_msg_commit_id,
		 pr_review_msg_original_commit_id, pr_review_msg_updated_at,
		 pr_review_msg_html_url, pr_review_msg_author_association,
		 pr_review_msg_start_line, pr_review_msg_original_start_line,
		 pr_review_msg_start_side, pr_review_msg_line, pr_review_msg_original_line,
		 pr_review_msg_side, data_source)
	SELECT rc.pr_review_id, rc.repo_id, rc.msg_id, rc.platform_src_id,
	       COALESCE(rc.node_id, ''),
	       COALESCE(rc.diff_hunk, ''), COALESCE(rc.file_path, ''), rc.position,
	       rc.original_position, COALESCE(rc.commit_id, ''),
	       COALESCE(rc.original_commit_id, ''), rc.updated_at,
	       COALESCE(rc.html_url, ''), COALESCE(rc.author_association, ''),
	       rc.start_line, rc.original_start_line, COALESCE(rc.start_side, ''),
	       rc.line, rc.original_line, COALESCE(rc.side, ''),
	       COALESCE(m.data_source, '')
	FROM aveloxis_data.review_comments rc
	JOIN aveloxis_data.messages m ON m.msg_id = rc.msg_id
	WHERE rc.pr_review_id IS NOT NULL
	  AND rc.review_comment_id > $1 AND rc.review_comment_id <= $2
	ON CONFLICT (pr_review_id, msg_id) DO NOTHING`

// runKeysetWindows walks (0, MAX(pk)] in MsgRefBackfillWindowSize
// steps, executing windowSQL($lo, $hi) per window. Each Exec is its
// own implicit transaction so interrupted runs resume.
func runKeysetWindows(ctx context.Context, pg *PostgresStore, logger *slog.Logger,
	label, boundsSQL, windowSQL string) error {
	var maxPK int64
	if err := pg.pool.QueryRow(ctx, boundsSQL).Scan(&maxPK); err != nil {
		return fmt.Errorf("%s: bounds: %w", label, err)
	}
	var touched int64
	for lo := int64(0); lo < maxPK; lo += MsgRefBackfillWindowSize {
		hi := lo + MsgRefBackfillWindowSize
		tag, err := pg.pool.Exec(ctx, windowSQL, lo, hi)
		if err != nil {
			return fmt.Errorf("%s: window (%d,%d]: %w", label, lo, hi, err)
		}
		touched += tag.RowsAffected()
	}
	logger.Info("migration keyset backfill complete", "label", label,
		"rows_touched", touched, "max_pk", maxPK, "window", MsgRefBackfillWindowSize)
	return nil
}

// ensureMsgRefMetadata runs the four v0.27.15 steps in dependency
// order. Errors land in the fail-closed collector (v0.19.4).
func ensureMsgRefMetadata(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	// Step 1: dedup — must precede the unique index.
	execMigrationStep(ctx, pg, logger, errs,
		"v0.27.15 dedup pull_request_review_message_ref (bare ON CONFLICT was dead — no unique arbiter)",
		PRReviewMsgRefDedupSQL)

	// Step 2: the arbiter the forward inserts name. After dedup, and
	// deliberately NOT in schema.sql (would fail against existing
	// duplicate data before step 1 runs).
	execCreateIndexConcurrently(ctx, pg, logger, errs,
		"aveloxis_data", "uq_pr_review_msg_ref",
		`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_pr_review_msg_ref
		 ON aveloxis_data.pull_request_review_message_ref (pr_review_id, msg_id)`)

	// Step 3: data_source backfills from messages (keyset windows).
	for _, t := range msgRefDataSourceTargets {
		if err := runKeysetWindows(ctx, pg, logger,
			"v0.27.15 backfill "+t.Table+".data_source from messages",
			fmt.Sprintf(`SELECT COALESCE(MAX(%s), 0) FROM %s`, t.PK, t.Table),
			MsgRefDataSourceWindowSQL(t.Table, t.PK)); err != nil {
			logger.Error("migration step failed", "error", err)
			*errs = append(*errs, err)
		}
	}

	// Step 4: inline review comments → ref rows with line metadata.
	// Needs the step-2 unique for its ON CONFLICT arbiter.
	if err := runKeysetWindows(ctx, pg, logger,
		"v0.27.15 backfill pull_request_review_message_ref inline-comment rows from review_comments",
		`SELECT COALESCE(MAX(review_comment_id), 0) FROM aveloxis_data.review_comments`,
		PRReviewMsgRefInlineBackfillWindowSQL); err != nil {
		logger.Error("migration step failed", "error", err)
		*errs = append(*errs, err)
	}
}
