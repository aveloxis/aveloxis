// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDedupLinkedMsgIDsTxIsTransactional (Copilot round 24, Active#1 + S4 +
// S5): the migration that drains duplicate linked_msg_id claims must capture
// the affected issues, NULL the losers, and RECOUNT in ONE transaction. The
// pre-round-24 shape ran capture, NULL, and recount as separate statements
// outside a transaction — a crash between the NULL and the recount left
// comment_count stale forever, and the capture read state that the NULL then
// changed. Pin the atomic structure at the source.
func TestDedupLinkedMsgIDsTxIsTransactional(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"),
		"func dedupLinkedMsgIDsTx(")
	for _, needle := range []string{
		"pg.pool.Begin(ctx)",
		"tx.Rollback(ctx)",
		"tx.Commit(ctx)",
		"tx.Query(ctx,",
		"tx.Exec(ctx,",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("dedupLinkedMsgIDsTx must use %s (capture+NULL+recount are one transaction)", needle)
		}
	}
	// The recount and NULL must run against the tx, never a fresh pool
	// connection that could commit independently of the rollback.
	if strings.Contains(body, "pg.pool.Exec(") || strings.Contains(body, "pg.pool.Query(") {
		t.Error("dedupLinkedMsgIDsTx must not run capture/NULL/recount on pg.pool — that escapes the transaction (round 24)")
	}
}

// TestDedupLinkedMsgIDsDrainsAndRecounts (Copilot round 24, behavioral): a
// duplicate linked_msg_id claim wrongly excludes a notification from its
// issue's comment_count (a superseded notification is not counted, and the
// loser was wrongly marked superseded). dedupLinkedMsgIDsTx keeps the lowest
// email_message_id per linked value, NULLs the rest, and recounts the freed
// issue — all atomically.
func TestDedupLinkedMsgIDsDrainsAndRecounts(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round24-dedup")

	// Duplicates are insertable only without the unique index.
	mustExecRetry(ctx, t, store, `DROP INDEX IF EXISTS aveloxis_data.uq_email_message_linked_msg`)

	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR24-1", "t", "b", "JIRA", nil,
		r17Time(t, "2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	tag := time.Now().UnixNano()

	// The native comment (counts) and its issue_message_ref row.
	var native int64
	mustQueryRowRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp, tool_source, tool_version)
		VALUES ($1, $2, $3, $4, 'native', NOW(), 'test', '0.0.0') RETURNING msg_id`,
		&native, repoID, tag, JiraPlatformID, MsgKindComment)
	if err := store.bridgeEmailToIssueNoRecount(ctx, issueID, repoID, native); err != nil {
		t.Fatal(err)
	}

	// Two notification bodies, each bridged onto the issue, both claiming the
	// same native comment (the duplicate-claim bug the migration drains).
	mkNotif := func(n int) int64 {
		var bodyMsg int64
		mustQueryRowRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.messages
				(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp, tool_source, tool_version)
			VALUES ($1, $2, 6, $3, 'notif', NOW(), 'test', '0.0.0') RETURNING msg_id`,
			&bodyMsg, repoID, tag+int64(n), MsgKindComment)
		var em int64
		mustQueryRowRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class)
			VALUES ($1, 6, $2, 'jira@apache.org', '[jira] [Commented] (AVR24-1) t', NOW(), 'issue_event')
			RETURNING email_message_id`,
			&em, repoID, fmt.Sprintf("<_avr24-%d-%d@x>", n, tag))
		if err := store.InsertEmailMessageRef(ctx, em, bodyMsg, nil); err != nil {
			t.Fatal(err)
		}
		if err := store.bridgeEmailToIssueNoRecount(ctx, issueID, repoID, bodyMsg); err != nil {
			t.Fatal(err)
		}
		return em
	}
	emA := mkNotif(1) // lower email_message_id — the winner
	emB := mkNotif(2) // higher — the loser, must be NULLed

	mustExecRetry(ctx, t, store,
		`UPDATE aveloxis_data.email_message SET linked_msg_id = $1 WHERE email_message_id IN ($2, $3)`,
		native, emA, emB)

	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issue_message_ref WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message_ref WHERE email_message_id IN ($1,$2)`, emA, emB)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE email_message_id IN ($1,$2)`, emA, emB)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE repo_id = $1 AND msg_text IN ('native','notif')`, repoID)
	})

	// Wrong sentinel — the recount must overwrite it with the true count.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.issues SET comment_count = 99 WHERE issue_id = $1`, issueID)

	if err := dedupLinkedMsgIDsTx(ctx, store); err != nil {
		t.Fatalf("dedupLinkedMsgIDsTx: %v", err)
	}

	// Winner keeps the link; loser is NULLed.
	var aLink, bLink *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emA).Scan(&aLink); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emB).Scan(&bLink); err != nil {
		t.Fatal(err)
	}
	if aLink == nil || *aLink != native {
		t.Errorf("winner emA should keep linked_msg_id=%d, got %v", native, aLink)
	}
	if bLink != nil {
		t.Errorf("loser emB should be NULLed, got %v", *bLink)
	}

	// Recount: native (1) + freed notification B (1); notification A stays
	// superseded (still linked). Pre-round-24 the recount ran outside the tx.
	var cnt int
	if err := store.pool.QueryRow(ctx,
		`SELECT comment_count FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Errorf("comment_count = %d after dedup, want 2 (native + freed notification B; A still superseded)", cnt)
	}

	// Restore the unique index for later tests (no duplicates remain now).
	var mErrs []error
	ensureLinkedMsgIDUnique(ctx, store, slog.Default(), &mErrs)
	if len(mErrs) > 0 {
		t.Fatalf("rebuild uq_email_message_linked_msg after dedup: %v", mErrs)
	}
}
