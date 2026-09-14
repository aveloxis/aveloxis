// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Store surface for `aveloxis heal-messages` (v0.27.38, summary/18
// Phase 1a). The worklist holds msg_ids that were claimed by more than
// one kind-class under the old two-column arbiter; healing = refetch
// BOTH parents' comments per row (the stored text belongs to at most
// one side — the later writer), then delete the stale cross-kind
// bridge links and stamp healed_at.

package db

import (
	"context"
	"fmt"
	"time"
)

// MessageHealItem is one pending worklist row with every parent that
// claims it, resolved to refetchable coordinates.
type MessageHealItem struct {
	MsgID   int64
	MsgKind int16
	// Conversation-side parents (stale links when MsgKind != 1).
	IssueRepoID  int64 // 0 = no issue-bridge claim
	IssueNumber  int64
	PRConvRepoID int64 // 0 = no PR-conversation-bridge claim
	PRConvNumber int64
	// Review-side parent (inline comment or review body).
	ReviewRepoID int64 // 0 = no review-side claim
	ReviewPRNum  int64
}

// CountMessageHealPending returns unhealed worklist rows.
func (s *PostgresStore) CountMessageHealPending(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM aveloxis_ops.message_heal_worklist WHERE healed_at IS NULL`).Scan(&n)
	return n, err
}

// GetMessageHealBatch resolves up to limit pending worklist rows
// ABOVE afterMsgID to their claiming parents (v0.28.8, Copilot round
// 4: the cursor is what lets a run advance PAST a fully-failing batch
// — without it, failed rows stay pending, the next pass reselects the
// same lowest 25K ids, and every higher-id row is permanently starved
// while the run reports success). Failed rows stay pending; a fresh
// run starts at cursor 0 and retries them. Rows whose parents cannot
// be resolved at all (deleted issues/PRs) still come back — the
// healer stamps them healed after deleting their stale links, since
// there is nothing to refetch.
func (s *PostgresStore) GetMessageHealBatch(ctx context.Context, afterMsgID int64, limit int) ([]MessageHealItem, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT w.msg_id, m.msg_kind,
		       COALESCE(i.repo_id, 0), COALESCE(i.issue_number, 0),
		       COALESCE(pc.repo_id, 0), COALESCE(pc.pr_number, 0),
		       COALESCE(rp.repo_id, 0), COALESCE(rp.pr_number, 0)
		FROM aveloxis_ops.message_heal_worklist w
		JOIN aveloxis_data.messages m ON m.msg_id = w.msg_id
		LEFT JOIN LATERAL (
			SELECT iss.repo_id, iss.issue_number
			FROM aveloxis_data.issue_message_ref ir
			JOIN aveloxis_data.issues iss ON iss.issue_id = ir.issue_id
			WHERE ir.msg_id = w.msg_id
			LIMIT 1
		) i ON TRUE
		LEFT JOIN LATERAL (
			SELECT pr.repo_id, pr.pr_number
			FROM aveloxis_data.pull_request_message_ref pm
			JOIN aveloxis_data.pull_requests pr ON pr.pull_request_id = pm.pull_request_id
			WHERE pm.msg_id = w.msg_id
			LIMIT 1
		) pc ON TRUE
		LEFT JOIN LATERAL (
			-- Inline comments resolve via review_comments → review →
			-- PR; review bodies via the review-body ref table. Either
			-- way the refetch coordinate is the PR number.
			SELECT pr.repo_id, pr.pr_number
			FROM (
				SELECT rc.pr_review_id FROM aveloxis_data.review_comments rc WHERE rc.msg_id = w.msg_id
				UNION
				SELECT pm.pr_review_id FROM aveloxis_data.pull_request_review_message_ref pm WHERE pm.msg_id = w.msg_id
			) refs
			JOIN aveloxis_data.pull_request_reviews rv ON rv.pr_review_id = refs.pr_review_id
			JOIN aveloxis_data.pull_requests pr ON pr.pull_request_id = rv.pull_request_id
			LIMIT 1
		) rp ON TRUE
		WHERE w.healed_at IS NULL
		  AND w.msg_id > $1
		ORDER BY w.msg_id
		LIMIT $2`, afterMsgID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MessageHealItem
	for rows.Next() {
		var it MessageHealItem
		if err := rows.Scan(&it.MsgID, &it.MsgKind,
			&it.IssueRepoID, &it.IssueNumber,
			&it.PRConvRepoID, &it.PRConvNumber,
			&it.ReviewRepoID, &it.ReviewPRNum); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// DeleteStaleConversationRefs removes the conversation-bridge links
// for a msg row whose kind is NOT conversation (2 or 3). The correct
// conversation-side rows were re-created against the NEW kind-1
// message rows by the healer's refetch; these old links made the
// issue/PR "own" a row holding the other kind's text.
func (s *PostgresStore) DeleteStaleConversationRefs(ctx context.Context, msgID int64) error {
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.issue_message_ref WHERE msg_id = $1`, msgID); err != nil {
		return fmt.Errorf("deleting stale issue refs for msg %d: %w", msgID, err)
	}
	if _, err := s.pool.Exec(ctx,
		`DELETE FROM aveloxis_data.pull_request_message_ref WHERE msg_id = $1`, msgID); err != nil {
		return fmt.Errorf("deleting stale PR refs for msg %d: %w", msgID, err)
	}
	return nil
}

// MarkMessagesHealed stamps healed_at on a set of worklist rows.
func (s *PostgresStore) MarkMessagesHealed(ctx context.Context, msgIDs []int64, at time.Time) error {
	if len(msgIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.message_heal_worklist
		SET healed_at = $2 WHERE msg_id = ANY($1::bigint[])`, msgIDs, at)
	return err
}
