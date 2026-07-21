// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.38 (summary/18 Phase 1a): messages msg_kind migration.
//
// Sequence (each step idempotent; the whole pass fast-skips once the
// old two-column constraint is gone, which is the LAST step — so an
// interrupted pass resumes in full on the next migrate):
//
//  1. add msg_kind SMALLINT NOT NULL DEFAULT 0 (addColumnIfMissing).
//  2. capture the collision worklist: msg_ids claimed by ≥2 kind
//     classes among {conversation bridges, review_comments,
//     review-body refs}. These rows' text/author were silently
//     overwritten cross-kind under the old arbiter — 198,237 on
//     aveloxis_large. `aveloxis heal-messages` consumes the worklist.
//  3. backfill msg_kind from bridge membership, keyset-windowed over
//     msg_id (the bulk-backfill house rule — production max msg_id is
//     ~56M). Priority inside a window: inline (2) > review body (3) >
//     conversation (1); platform 6 rows are email (4) by platform
//     alone. Collision rows therefore land on kind 2 or 3 in the
//     interim; the healer refetches both sides and fixes text either
//     way. Bridge-less orphans stay 0 (invisible to all read paths).
//  4. create UNIQUE (platform_msg_id, platform_id, msg_kind)
//     CONCURRENTLY (cannot collide: it is a superset of the old key).
//  5. drop the old UNIQUE (platform_msg_id, platform_id) — the write
//     paths' new arbiter takes over. Doing this LAST keeps uniqueness
//     continuous and doubles as the completed-pass marker for the
//     fast-skip.

package db

import (
	"context"
	"fmt"
	"log/slog"
)

// MsgKindBackfillWindowSize is the msg_id keyset window width for the
// kind backfill (~56 windows on production's 56M-row messages table).
const MsgKindBackfillWindowSize int64 = 1_000_000

// oldMessagesArbiterConstraint is the auto-generated name of the
// pre-v0.27.38 two-column unique.
const oldMessagesArbiterConstraint = "messages_platform_msg_id_platform_id_key"

func migrateMessageKinds(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	// Fast path: the old constraint is dropped as the FINAL step, so
	// its absence means the whole pass already completed (or this is a
	// fresh install whose schema.sql never had it). Skipping here keeps
	// every later migrate run O(1) instead of re-walking 56 windows.
	var oldConstraintExists bool
	if err := pg.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_constraint c
			JOIN pg_class t ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE n.nspname = 'aveloxis_data' AND t.relname = 'messages'
			  AND c.conname = $1)`, oldMessagesArbiterConstraint).Scan(&oldConstraintExists); err != nil {
		*errs = append(*errs, fmt.Errorf("msg_kind migration: probing old arbiter: %w", err))
		return
	}
	if !oldConstraintExists {
		return
	}

	logger.Info("msg_kind migration starting (v0.27.38 cross-kind collision fix)")

	captureMsgKindCollisions(ctx, pg, logger, errs)
	backfillMsgKinds(ctx, pg, logger, errs)

	// Step 4 — the new arbiter, CONCURRENTLY (superset of the old key,
	// so it cannot fail on existing data).
	execCreateIndexConcurrently(ctx, pg, logger, errs, "aveloxis_data",
		"uq_messages_platform_id_kind", `
		CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_messages_platform_id_kind
		ON aveloxis_data.messages (platform_msg_id, platform_id, msg_kind)`)

	// Step 5 — drop the old two-column arbiter. LAST: this is the
	// completed-pass marker for the fast-skip above.
	execMigrationStep(ctx, pg, logger, errs,
		"v0.27.38 drop old messages arbiter (platform_msg_id, platform_id)",
		`ALTER TABLE aveloxis_data.messages DROP CONSTRAINT IF EXISTS `+oldMessagesArbiterConstraint)

	logger.Info("msg_kind migration complete")
}

// captureMsgKindCollisions records msg_ids claimed by ≥2 kind classes
// into the heal worklist (step 2; idempotent via ON CONFLICT).
func captureMsgKindCollisions(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	// Step 2 — capture the collision worklist BEFORE anything mutates.
	// Conversation ∩ inline (the 198K-row class on production):
	execMigrationStep(ctx, pg, logger, errs,
		"v0.27.38 capture msg_kind collisions (conversation ∩ inline review)", `
		INSERT INTO aveloxis_ops.message_heal_worklist (msg_id)
		SELECT DISTINCT b.msg_id
		FROM (
			SELECT msg_id FROM aveloxis_data.issue_message_ref
			UNION
			SELECT msg_id FROM aveloxis_data.pull_request_message_ref
		) b
		WHERE EXISTS (SELECT 1 FROM aveloxis_data.review_comments rc WHERE rc.msg_id = b.msg_id)
		ON CONFLICT (msg_id) DO NOTHING`)
	// Conversation ∩ review-body (review-body refs = prrmr rows whose
	// msg is NOT an inline comment):
	execMigrationStep(ctx, pg, logger, errs,
		"v0.27.38 capture msg_kind collisions (conversation ∩ review body)", `
		INSERT INTO aveloxis_ops.message_heal_worklist (msg_id)
		SELECT DISTINCT b.msg_id
		FROM (
			SELECT msg_id FROM aveloxis_data.issue_message_ref
			UNION
			SELECT msg_id FROM aveloxis_data.pull_request_message_ref
		) b
		WHERE EXISTS (SELECT 1 FROM aveloxis_data.pull_request_review_message_ref pr WHERE pr.msg_id = b.msg_id)
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_data.review_comments rc WHERE rc.msg_id = b.msg_id)
		ON CONFLICT (msg_id) DO NOTHING`)

}

// backfillMsgKinds classifies every msg_kind=0 row from bridge
// membership, keyset-windowed over msg_id (step 3; idempotent via the
// msg_kind = 0 guard).
func backfillMsgKinds(ctx context.Context, pg *PostgresStore, logger *slog.Logger, errs *[]error) {
	var maxID int64
	if err := pg.pool.QueryRow(ctx,
		`SELECT COALESCE(MAX(msg_id), 0) FROM aveloxis_data.messages`).Scan(&maxID); err != nil {
		*errs = append(*errs, fmt.Errorf("msg_kind backfill: max msg_id: %w", err))
		return
	}
	for lo := int64(0); lo < maxID; lo += MsgKindBackfillWindowSize {
		hi := lo + MsgKindBackfillWindowSize
		tag, err := pg.pool.Exec(ctx, `
			UPDATE aveloxis_data.messages m
			SET msg_kind = CASE
				WHEN m.platform_id = 6 THEN 4
				WHEN EXISTS (SELECT 1 FROM aveloxis_data.review_comments rc WHERE rc.msg_id = m.msg_id) THEN 2
				WHEN EXISTS (SELECT 1 FROM aveloxis_data.pull_request_review_message_ref pr WHERE pr.msg_id = m.msg_id) THEN 3
				WHEN EXISTS (SELECT 1 FROM aveloxis_data.issue_message_ref ir WHERE ir.msg_id = m.msg_id)
				  OR EXISTS (SELECT 1 FROM aveloxis_data.pull_request_message_ref pm WHERE pm.msg_id = m.msg_id) THEN 1
				ELSE 0
			END
			WHERE m.msg_id > $1 AND m.msg_id <= $2 AND m.msg_kind = 0`, lo, hi)
		if err != nil {
			*errs = append(*errs, fmt.Errorf("msg_kind backfill window (%d,%d]: %w", lo, hi, err))
			return
		}
		logger.Info("msg_kind backfill window complete",
			"window_lo", lo, "window_hi", hi, "rows", tag.RowsAffected(), "max_id", maxID)
	}
}
