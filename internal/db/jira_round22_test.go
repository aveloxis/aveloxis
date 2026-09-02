// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestReverseLinkExhaustionReturnsErrorForDeferral (Copilot round 22 on
// PR #193): after exhausting the 23505 re-pick retries the notification is
// STILL unlinked, so returning nil would let the processor mark the
// staging row processed and never retry (the comment stays double-counted).
// The exhaustion arm must return errLinkContentionExhausted so the caller
// defers the row; the RowsAffected()==0 arm (genuine surplus) still returns
// nil.
func TestReverseLinkExhaustionReturnsErrorForDeferral(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/jira_store.go"),
		"func (s *PostgresStore) LinkCommentNotificationToNative(")
	// the surplus arm keeps its nil...
	if !strings.Contains(body, "return nil // no unclaimed native in the window — done") {
		t.Error("the RowsAffected()==0 surplus arm must still return nil (no native to pair)")
	}
	// ...but the post-loop exhaustion arm must surface the sentinel.
	if !strings.Contains(body, "errLinkContentionExhausted") {
		t.Error("the exhaustion arm must return errLinkContentionExhausted so the caller defers the row (round 22)")
	}
	// and the after-loop return must NOT be a bare nil.
	tail := body[strings.LastIndex(body, "for attempt"):]
	if strings.Contains(tail, "\treturn nil\n}") {
		t.Error("the after-loop return must be the sentinel error, not nil (round 22)")
	}
}

// TestBackfillContinuesOnLinkContention (Copilot round 22): the re-runnable
// keyed-projection backfill must treat errLinkContentionExhausted as
// "leave the comment link for a future pass and continue" — NOT abort the
// whole window on transient concurrent-drain contention.
func TestBackfillContinuesOnLinkContention(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/mailinglist_projection_backfill.go"),
		"func (s *PostgresStore) BackfillKeyedIssueProjection(")
	if body == "" {
		// function name differs — scan the whole file for the guard.
		body = srctest.Read(t, "internal/db/mailinglist_projection_backfill.go")
	}
	if !strings.Contains(body, "errors.Is(err, errLinkContentionExhausted)") {
		t.Error("the backfill must let errLinkContentionExhausted continue (re-runnable heal), not abort the window (round 22)")
	}
}

// TestSenderBackfillNeverFabricatesAmbiguousAttribution (Copilot round 22,
// SR-6): the direct-match arm must GROUP BY the message and update only
// when exactly ONE distinct contributor matches — a DISTINCT ON pick over
// a multi-contributor email would fabricate an attribution.
func TestSenderBackfillNeverFabricatesAmbiguousAttribution(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/db/email_message_store.go"),
		"func (s *PostgresStore) BackfillMailingListSenderIDs(")
	if !strings.Contains(body, "HAVING COUNT(DISTINCT c.cntrb_id) = 1") {
		t.Error("the direct-match arm must require exactly one distinct contributor (SR-6, round 22)")
	}
	if strings.Contains(body, "SELECT DISTINCT ON (m2.msg_id) m2.msg_id, c.cntrb_id") {
		t.Error("the deterministic DISTINCT ON pick fabricates an ambiguous attribution — must be gone (round 22)")
	}
}

// TestSenderBackfillAmbiguousStaysNull (Copilot round 22, SR-6): a
// message whose sender email matches TWO active contributors must keep
// cntrb_id NULL; a message whose email matches exactly one is attributed.
// Mutation-provable: the pre-fix DISTINCT ON pick attributes the ambiguous
// one.
func TestSenderBackfillAmbiguousStaysNull(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	jsSeedRepo(t, ctx, store)
	tag := fmt.Sprintf("%d", time.Now().UnixNano())

	ambigEmail := "ambig-" + tag + "@example.org"
	soloEmail := "solo-" + tag + "@example.org"

	// Two active contributors share ambigEmail; one owns soloEmail.
	var cA, cB, cS string
	for _, seed := range []struct {
		id    *string
		login string
		email string
	}{{&cA, "avr22-a-" + tag, ambigEmail}, {&cB, "avr22-b-" + tag, ambigEmail}, {&cS, "avr22-s-" + tag, soloEmail}} {
		mustQueryRowRetry(ctx, t, store, `
			INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_email, cntrb_deleted)
			VALUES (gen_random_uuid(), $1, $2, 0) RETURNING cntrb_id::text`,
			seed.id, seed.login, seed.email)
	}

	seedMailMsg := func(header, email string) int64 {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message
			(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class)
			VALUES ($1, 6, $2, $3, 's', NOW(), 'discuss')`, jsRepoID, header, email)
		var mid int64
		mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.messages
			(repo_id, platform_msg_id, platform_id, msg_kind, node_id, msg_text, msg_timestamp, tool_source, tool_version)
			VALUES ($1, $2, 6, $3, $4, 'b', NOW(), 'test', '0.0.0') RETURNING msg_id`,
			&mid, jsRepoID, time.Now().UnixNano(), MsgKindComment, header)
		return mid
	}
	hAmb := "<avr22-amb-" + tag + "@x>"
	hSolo := "<avr22-solo-" + tag + "@x>"
	midAmb := seedMailMsg(hAmb, ambigEmail)
	midSolo := seedMailMsg(hSolo, soloEmail)
	lo := midAmb
	if midSolo < lo {
		lo = midSolo
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.messages WHERE msg_id IN ($1,$2)`, midAmb, midSolo)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.email_message WHERE message_id_header IN ($1,$2)`, hAmb, hSolo)
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text IN ($1,$2,$3)`, cA, cB, cS)
	})

	if _, err := store.BackfillMailingListSenderIDs(ctx, lo-1, 2); err != nil {
		t.Fatalf("BackfillMailingListSenderIDs: %v", err)
	}

	var ambCntrb *string
	mustQueryRowRetry(ctx, t, store, `SELECT cntrb_id::text FROM aveloxis_data.messages WHERE msg_id = $1`, &ambCntrb, midAmb)
	if ambCntrb != nil {
		t.Errorf("ambiguous-email message was attributed to %s — SR-6 requires it stay NULL (round 22)", *ambCntrb)
	}
	var soloCntrb *string
	mustQueryRowRetry(ctx, t, store, `SELECT cntrb_id::text FROM aveloxis_data.messages WHERE msg_id = $1`, &soloCntrb, midSolo)
	if soloCntrb == nil || *soloCntrb != cS {
		t.Errorf("single-contributor email must attribute to %s, got %v", cS, soloCntrb)
	}
}

// --- v0.29.1 (round 22 suppressed #1): lease heartbeat ---

func avr22RegisterProject(t *testing.T, ctx context.Context, store *PostgresStore) string {
	t.Helper()
	jsSeedRepo(t, ctx, store)
	key := fmt.Sprintf("AVR22P-%d", time.Now().UnixNano())
	repoID := jsRepoID
	if err := store.RegisterJiraProject(ctx, key, "https://issues.apache.org/jira", &repoID); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key = $1`, key)
	})
	return key
}

// TestJiraLeaseHeartbeatMeasuresInactivity: a long scan that keeps
// checkpointing (fresh heartbeat) is NOT reclaimed even after >2h; a
// stalled lock (no heartbeat for >2h) IS reclaimable. Mutation-provable:
// reverting the claim staleness to `jps_locked_at < NOW() - 2h` reclaims
// the active scan.
func TestJiraLeaseHeartbeatMeasuresInactivity(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	key := avr22RegisterProject(t, ctx, store)

	setState := func(lockedAgo, heartbeatAgo string) {
		mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.jira_project_serve
			SET jps_locked_at = NOW() - $2::interval, jps_heartbeat_at = NOW() - $3::interval
			WHERE project_key = $1`, key, lockedAgo, heartbeatAgo)
	}
	claimable := func() bool {
		job, err := store.ClaimNextJiraProject(ctx, time.Hour, key)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return job != nil
	}

	setState("3 hours", "1 minute") // long scan, still active
	if claimable() {
		t.Error("a long scan with a fresh heartbeat must NOT be reclaimed — the stale window measures inactivity, not total duration (round 22 S1)")
	}
	setState("3 hours", "3 hours") // stalled: no checkpoint for >2h
	if !claimable() {
		t.Error("a stalled lock (heartbeat >2h old) must be reclaimable")
	}
}

// TestJiraCheckpointHeartbeatOwnershipQualified: the OWNER's checkpoint
// renews the heartbeat; a STALE owner whose jps_locked_at has moved on
// (a successor reclaimed) does NOT. Mutation-provable: dropping the
// `jps_locked_at = $3` guard lets the stale owner renew.
func TestJiraCheckpointHeartbeatOwnershipQualified(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	key := avr22RegisterProject(t, ctx, store)

	job, err := store.ClaimNextJiraProject(ctx, time.Hour, key)
	if err != nil || job == nil {
		t.Fatalf("claim: job=%v err=%v", job, err)
	}
	readHB := func() time.Time {
		var hb time.Time
		mustQueryRowRetry(ctx, t, store, `SELECT jps_heartbeat_at FROM aveloxis_ops.jira_project_serve WHERE jps_id = $1`, &hb, job.JpsID)
		return hb
	}

	// Owner checkpoint renews (jps_locked_at still == job.LockedAt).
	if err := store.CheckpointJiraProject(ctx, job.JpsID, job.LockedAt, time.Now()); err != nil {
		t.Fatal(err)
	}
	if time.Since(readHB()) > 5*time.Minute {
		t.Error("the owner's checkpoint must renew jps_heartbeat_at")
	}

	// A successor reclaims: jps_locked_at moves on, heartbeat set old.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NOW() + INTERVAL '1 second', jps_heartbeat_at = NOW() - INTERVAL '1 hour'
		WHERE jps_id = $1`, job.JpsID)
	// Stale owner (still holds job.LockedAt) checkpoints — must NOT renew.
	if err := store.CheckpointJiraProject(ctx, job.JpsID, job.LockedAt, time.Now()); err != nil {
		t.Fatal(err)
	}
	if time.Since(readHB()) < 30*time.Minute {
		t.Error("a stale owner (jps_locked_at moved on) must NOT renew the heartbeat — ownership-qualified (round 22 S1)")
	}
}

// --- v0.29.1 (round 22 suppressed #2): API freshness resists stale replay ---

// TestJiraAPIFreshnessResistsStaleReplayAfterMailEvent: an API-owned row
// touched by a mail event (last_mail_event_id set, updated_at clobbered)
// must still REJECT a stale API replay — freshness keys on
// jira_api_updated_at, not updated_at. Mutation-provable: restoring the
// `OR last_mail_event_id IS NOT NULL` arm accepts the stale replay and
// regresses title/state.
func TestJiraAPIFreshnessResistsStaleReplayAfterMailEvent(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	jsSeedRepo(t, ctx, store)
	key := fmt.Sprintf("AVR22X%d-7", time.Now().UnixNano())
	t2 := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	// Newer API snapshot: title "new", closed, jira_api_updated_at = T2.
	id, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key, Title: "new", Status: "Closed",
		Resolution: "Fixed", Updated: t2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.issues WHERE issue_id = $1`, id) })

	// A mail event clobbers updated_at with a recent relay stamp and marks
	// the row mail-authored — jira_api_updated_at stays T2.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.issues
		SET last_mail_event_id = 999, updated_at = NOW()
		WHERE issue_id = $1`, id)

	// Stale API replay: older T1, title "old", open. Must be REJECTED.
	if _, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key, Title: "old", Status: "Open", Updated: t1,
	}); err != nil {
		t.Fatal(err)
	}

	var title, state string
	if err := store.pool.QueryRow(ctx, `SELECT issue_title, issue_state FROM aveloxis_data.issues WHERE issue_id = $1`, id).Scan(&title, &state); err != nil {
		t.Fatal(err)
	}
	if title != "new" || state != "closed" {
		t.Errorf("stale API replay regressed the row to title=%q state=%q — freshness must key on jira_api_updated_at, not updated_at (round 22 S2)", title, state)
	}
}
