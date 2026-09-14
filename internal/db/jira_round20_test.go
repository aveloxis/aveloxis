// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestLinkedMsgWritersHandle23505 (Copilot round 20 on PR #193): the
// uq_email_message_linked_msg backstop is only correct if BOTH writers
// of email_message.linked_msg_id handle the unique violation their
// anti-join can race into.
//   - UpsertJiraComment (forward order) links a notification to THIS
//     native comment's msg_id; a 23505 means it is already claimed, so
//     it is a no-op — never a failure that would defer the envelope.
//   - LinkCommentNotificationToNative (reverse order) claims the nearest
//     unclaimed native; a 23505 means a concurrent drain took it, so it
//     re-picks the next candidate (bounded by maxLinkClaimRetries).
func TestLinkedMsgWritersHandle23505(t *testing.T) {
	src := srctest.Read(t, "internal/db/jira_store.go")

	upsert := srctest.FuncBody(t, src, "func (s *PostgresStore) UpsertJiraComment(")
	if !strings.Contains(upsert, `pgErr.Code == "23505"`) {
		t.Error("UpsertJiraComment must treat a 23505 on the notification link as a no-op (the unique-index race backstop) — round 20")
	}

	rev := srctest.FuncBody(t, src, "func (s *PostgresStore) LinkCommentNotificationToNative(")
	if !strings.Contains(rev, `pgErr.Code == "23505"`) {
		t.Error("LinkCommentNotificationToNative must recognize 23505 to re-pick an unclaimed native — round 20")
	}
	if !strings.Contains(rev, "maxLinkClaimRetries") {
		t.Error("LinkCommentNotificationToNative's re-pick loop must be bounded by maxLinkClaimRetries — round 20")
	}
}

// TestLinkedMsgIDUniqueRejectsDoubleClaim (Copilot round 20): the hard
// backstop — two email_message rows cannot both claim the same native
// comment's msg_id via linked_msg_id. Mutation-provable: revert the
// migration's CREATE UNIQUE INDEX to a plain CREATE INDEX and the second
// claim succeeds instead of failing 23505.
func TestLinkedMsgIDUniqueRejectsDoubleClaim(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	// Guarantee the unique index exists regardless of the scratch DB's
	// stamp state (drains any pre-existing duplicate claims first).
	var mErrs []error
	ensureLinkedMsgIDUnique(ctx, store, slog.Default(), &mErrs)
	if len(mErrs) > 0 {
		t.Fatalf("ensureLinkedMsgIDUnique: %v", mErrs)
	}

	jsSeedRepo(t, ctx, store)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())

	// A native comment messages row to point at.
	var nativeMsgID int64
	mustQueryRowRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp, tool_source, tool_version)
		VALUES ($1, $2, $3, $4, 'native body', NOW(), 'test', '0.0.0')
		RETURNING msg_id`,
		&nativeMsgID, jsRepoID, time.Now().UnixNano(), JiraPlatformID, MsgKindComment)

	h1 := "<_avr20-a-" + tag + "@x>"
	h2 := "<_avr20-b-" + tag + "@x>"
	for _, h := range []string{h1, h2} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message
			(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class)
			VALUES ($1, 6, $2, 'jira@apache.org', '[jira] [Commented] (AVJR-5) t', NOW(), 'issue_event')`,
			jsRepoID, h)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE message_id_header IN ($1,$2)`, h1, h2)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, nativeMsgID)
	})

	// First claim lands.
	mustExecRetry(ctx, t, store,
		`UPDATE aveloxis_data.email_message SET linked_msg_id = $1 WHERE message_id_header = $2`, nativeMsgID, h1)

	// Second claim of the SAME native msg_id must be rejected by the
	// unique index — this is the concurrency invariant the reviewer flagged.
	_, err := store.pool.Exec(ctx,
		`UPDATE aveloxis_data.email_message SET linked_msg_id = $1 WHERE message_id_header = $2`, nativeMsgID, h2)
	var pgErr *pgconn.PgError
	if !(errors.As(err, &pgErr) && pgErr.Code == "23505") {
		t.Fatalf("second claim of native msg_id %d must fail 23505 (uq_email_message_linked_msg backstop); got: %v", nativeMsgID, err)
	}
}
