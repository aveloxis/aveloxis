// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestApplyTrackerActionIgnoresZeroTimestamp (Copilot round 25, finding #2):
// a state action with no usable event timestamp cannot be ordered. Applying
// a zero-sentAt action would write updated_at = NULL, and the freshness
// guard's `updated_at IS NULL` arm then lets every later replay overwrite
// state with no ordering. The action must be dropped.
func TestApplyTrackerActionIgnoresZeroTimestamp(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round25-zerotime")
	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR25Z-1", "t", "b", "JIRA", nil,
		r17Time(t, "2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
	})

	// A Resolved with a ZERO timestamp must be a no-op.
	if err := store.ApplyTrackerAction(ctx, issueID, "Resolved", time.Time{}, 1); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := store.pool.QueryRow(ctx,
		`SELECT issue_state FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state == "closed" {
		t.Fatalf("a zero-timestamp Resolved must be a no-op; issue_state=%q (the un-orderable action applied)", state)
	}

	// A real-timestamped Resolved still applies — the guard is not blanket.
	if err := store.ApplyTrackerAction(ctx, issueID, "Resolved", r17Time(t, "2026-04-01T00:00:00Z"), 2); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT issue_state FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "closed" {
		t.Fatalf("a real-timestamped Resolved must apply; issue_state=%q", state)
	}
}

// TestReverseLinkRequiresAutomationSender (Copilot round 25, findings #3+#5,
// SR-18): the issue_event class is matched on SUBJECT ONLY, so a human list
// message with a "[jira] [Commented] (KEY-N)" subject reaches the reverse
// link. Only an AUTOMATION notification may supersede a native comment; the
// gate is enforced inside LinkCommentNotificationToNative so BOTH the
// processor and the keyed backfill callers are covered.
func TestReverseLinkRequiresAutomationSender(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round25-revgate")
	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR25R-1", "t", "b", "JIRA", nil,
		r17Time(t, "2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	tag := time.Now().UnixNano()
	ts := r17Time(t, "2026-03-02T12:00:00Z")

	// Native Jira comment on the issue.
	var native int64
	mustQueryRowRetry(ctx, t, store, `
		INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, msg_text, msg_timestamp, tool_source, tool_version)
		VALUES ($1, $2, $3, $4, 'native', $5, 'test', '0.0.0') RETURNING msg_id`,
		&native, repoID, tag, JiraPlatformID, MsgKindComment, ts)
	if err := store.bridgeEmailToIssueNoRecount(ctx, issueID, repoID, native); err != nil {
		t.Fatal(err)
	}

	mkNotif := func(sender, hdr string) int64 {
		var em int64
		mustQueryRowRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class, linked_external_key)
			VALUES ($1, 6, $2, $3, '[jira] [Commented] (AVR25R-1) t', $4, 'issue_event', 'AVR25R-1')
			RETURNING email_message_id`,
			&em, repoID, hdr, sender, ts)
		return em
	}
	emHuman := mkNotif("person@example.com", fmt.Sprintf("<h-%d@x>", tag))
	emJira := mkNotif("jira@apache.org", fmt.Sprintf("<j-%d@x>", tag))
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE email_message_id IN ($1,$2)`, emHuman, emJira)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issue_message_ref WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, native)
	})

	// The HUMAN notification must NOT claim the native comment.
	if err := store.LinkCommentNotificationToNative(ctx, emHuman, issueID, ts); err != nil {
		t.Fatalf("human reverse-link should be a clean no-op, got: %v", err)
	}
	var link *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emHuman).Scan(&link); err != nil {
		t.Fatal(err)
	}
	if link != nil {
		t.Fatalf("a HUMAN [jira]-subject message must not claim the native comment; linked_msg_id=%d", *link)
	}

	// The AUTOMATION notification links normally.
	if err := store.LinkCommentNotificationToNative(ctx, emJira, issueID, ts); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emJira).Scan(&link); err != nil {
		t.Fatal(err)
	}
	if link == nil || *link != native {
		t.Fatalf("the jira@ notification must link to the native comment %d, got %v", native, link)
	}
}

// TestForwardLinkRequiresAutomationSender (Copilot round 25, finding #1):
// UpsertJiraComment's collection-time link claims the nearest [Commented]
// NOTIFICATION for the comment — but the candidate must be an automation
// notification, not a human message whose subject merely looks like one.
func TestForwardLinkRequiresAutomationSender(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round25-fwdgate")
	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR25F-1", "t", "b", "JIRA", nil,
		r17Time(t, "2026-03-01T00:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	tag := time.Now().UnixNano()
	created := r17Time(t, "2026-03-02T12:00:00Z")

	mkNotif := func(sender, hdr string, sent time.Time) int64 {
		var em int64
		mustQueryRowRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.email_message
				(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class, linked_external_key)
			VALUES ($1, 6, $2, $3, '[jira] [Commented] (AVR25F-1) t', $4, 'issue_event', 'AVR25F-1')
			RETURNING email_message_id`,
			&em, repoID, hdr, sender, sent)
		return em
	}
	// The human notification is NEARER in time — without the gate it would win.
	emHuman := mkNotif("person@example.com", fmt.Sprintf("<hf-%d@x>", tag), created.Add(10*time.Second))
	emJira := mkNotif("jira@apache.org", fmt.Sprintf("<jf-%d@x>", tag), created.Add(40*time.Second))

	msgID, err := store.UpsertJiraComment(ctx, JiraAPIComment{
		RepoID: repoID, IssueID: issueID, ExternalKey: "AVR25F-1",
		CommentID: tag, Body: "body", Created: created, Updated: created,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE email_message_id IN ($1,$2)`, emHuman, emJira)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issue_message_ref WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, issueID)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE msg_id = $1`, msgID)
	})

	var hLink, jLink *int64
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emHuman).Scan(&hLink); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx,
		`SELECT linked_msg_id FROM aveloxis_data.email_message WHERE email_message_id = $1`, emJira).Scan(&jLink); err != nil {
		t.Fatal(err)
	}
	if hLink != nil {
		t.Fatalf("the NEARER human notification must not be claimed; linked_msg_id=%d (the gate is missing)", *hLink)
	}
	if jLink == nil || *jLink != msgID {
		t.Fatalf("the jira@ notification must be claimed by the comment %d, got %v", msgID, jLink)
	}
}

// TestBackfillContentionReturnsBeforeFinalMark (Copilot round 25, finding
// #4): on reverse-link contention the keyed backfill must RETURN the error,
// not `continue`. A `continue` makes an all-contention batch return n=0,
// which the CLI reads as "done" and then runs BackfillMarkRemainingProjected
// — permanently stamping the contention row (and everything beyond the
// batch) mailing_list_only.
func TestBackfillContentionReturnsBeforeFinalMark(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/mailinglist_projection_backfill.go"),
		"func (s *PostgresStore) BackfillKeyedIssueProjection("))
	// Region between the reverse-link call and the stamp (comment-stripped,
	// so an explanatory comment mentioning "continue" cannot false-match).
	call := strings.Index(body, "LinkCommentNotificationToNative(ctx, r.emID")
	stamp := strings.Index(body, "SET linked_issue_id = $2, projected_kind = 'issue'")
	if call < 0 || stamp < 0 || call >= stamp {
		t.Fatal("could not locate the reverse-link region in BackfillKeyedIssueProjection")
	}
	region := body[call:stamp]
	if !strings.Contains(region, "errLinkContentionExhausted") {
		t.Fatal("the reverse-link region must handle errLinkContentionExhausted")
	}
	// The contention arm must RETURN (stop the command) and must NOT `continue`
	// (which returns 0 and triggers the premature final mark).
	if !strings.Contains(region, "return n,") {
		t.Error("on contention the backfill must `return n, ...` so the CLI stops before the final mark — round 25")
	}
	if strings.Contains(region, "continue") {
		t.Error("the reverse-link contention arm must not `continue` — an all-contention batch returns 0 and the CLI marks the rest mailing_list_only (round 25 reverses round 23's continue)")
	}
}

// TestBackfillCLIStopsOnKeyedErrorBeforeFinalMark (Copilot round 25, finding
// #4): the CLI must return on a keyed-projection error BEFORE running
// BackfillMarkRemainingProjected, so a contention return never reaches the
// permanent mark.
func TestBackfillCLIStopsOnKeyedErrorBeforeFinalMark(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/backfill_mailing_list_projection.go"))
	keyed := strings.Index(src, "BackfillKeyedIssueProjection(")
	mark := strings.Index(src, "BackfillMarkRemainingProjected(")
	if keyed < 0 || mark < 0 || keyed >= mark {
		t.Fatal("expected BackfillKeyedIssueProjection to be driven before BackfillMarkRemainingProjected")
	}
	between := src[keyed:mark]
	if !strings.Contains(between, "if err != nil {") || !strings.Contains(between, "return err") {
		t.Error("the CLI must `return err` from the keyed loop before the final mark — round 25 (finding #4)")
	}
}
