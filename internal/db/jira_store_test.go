// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_store_test.go — C2/C3 store layer. The heart is the C3a
// provider-model proof: mail projection and the Jira collector minting
// the SAME ticket independently converge on ONE issues row at write
// time (deterministic negative id from the external key), in either
// arrival order; the API outranks mail on state/reporter; the forge
// outranks both (native rows untouched).
package db

import (
	"context"
	"fmt"
	"testing"
	"time"
)

const (
	jsRepoID = int64(944_149_010)
	jsKey    = "AVJR-5"
)

func jsSeedRepo(t *testing.T, ctx context.Context, store *PostgresStore) {
	t.Helper()
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
		VALUES ($1, 'https://github.com/_avjr/store', '_avjr', 'store', 1)
		ON CONFLICT (repo_id) DO NOTHING`, jsRepoID)
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.issue_message_ref WHERE repo_id = $1`, jsRepoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.messages WHERE repo_id = $1`, jsRepoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.email_message WHERE repo_id = $1`, jsRepoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, jsRepoID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.jira_identities WHERE jira_name LIKE '_avjr%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '_avjr%' OR cntrb_full_name LIKE '_avjr%'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.jira_staging WHERE project_key = 'AVJR'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key = 'AVJR'`)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, jsRepoID)
	})
}

// TestJiraProjectServeLifecycle — register → claim (cadence-gated,
// SKIP LOCKED) → checkpoint → complete → immediate reclaim refused by
// cadence.
func TestJiraProjectServeLifecycle(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)
	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", &[]int64{jsRepoID}[0]); err != nil {
		t.Fatal(err)
	}
	// Idempotent re-register.
	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", nil); err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "AVJR")
	if err != nil {
		t.Fatal(err)
	}
	if job == nil || job.ProjectKey != "AVJR" || job.BaseURL != "https://jira.example.org" {
		t.Fatalf("claim: %+v", job)
	}
	cp := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	if err := store.CheckpointJiraProject(ctx, job.JpsID, cp); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteJiraScan(ctx, job.JpsID, job.LockedAt); err != nil {
		t.Fatal(err)
	}
	// Within cadence: no reclaim.
	again, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "AVJR")
	if err != nil {
		t.Fatal(err)
	}
	if again != nil {
		t.Fatalf("cadence gate broken: reclaimed %+v", again)
	}
	var gotCp time.Time
	if err := store.pool.QueryRow(ctx, `SELECT jps_last_updated FROM aveloxis_ops.jira_project_serve WHERE jps_id = $1`, job.JpsID).Scan(&gotCp); err != nil {
		t.Fatal(err)
	}
	if !gotCp.Equal(cp) {
		t.Fatalf("checkpoint = %v, want %v", gotCp, cp)
	}
}

// TestJiraStagingNaturalKeyNoOp — re-staging the same (project, key,
// updated) is a true no-op; a NEWER updated stages a new row.
func TestJiraStagingNaturalKeyNoOp(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)
	if err := store.RegisterJiraProject(ctx, "AVJR", "https://jira.example.org", nil); err != nil {
		t.Fatal(err)
	}
	var jpsID int64
	if err := store.pool.QueryRow(ctx, `SELECT jps_id FROM aveloxis_ops.jira_project_serve WHERE project_key = 'AVJR'`).Scan(&jpsID); err != nil {
		t.Fatal(err)
	}
	up := time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC)
	for range 2 {
		if err := store.StageJiraIssue(ctx, jpsID, "AVJR", jsKey, up, nil, []byte(`{"key":"AVJR-5"}`)); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.jira_staging WHERE project_key = 'AVJR'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("re-stage duplicated: %d rows", n)
	}
	if err := store.StageJiraIssue(ctx, jpsID, "AVJR", jsKey, up.Add(time.Hour), nil, []byte(`{"key":"AVJR-5","v":2}`)); err != nil {
		t.Fatal(err)
	}
	batch, err := store.GetJiraStagingBatch(ctx, jpsID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 2 {
		t.Fatalf("batch = %d rows, want 2", len(batch))
	}
	if err := store.MarkJiraStagingProcessed(ctx, []int64{batch[0].JsID, batch[1].JsID}); err != nil {
		t.Fatal(err)
	}
	batch, err = store.GetJiraStagingBatch(ctx, jpsID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 0 {
		t.Fatal("processed rows must leave the batch")
	}
}

// TestJiraIdentityMatchMatrix — SR-6 at the identity layer: unambig
// login links, unambig display links, ambiguous stays NULL with the
// raw identity preserved, and a Jira-only identity can be MINTED as a
// contributor (the email-only-contributor precedent) so networks
// include the person.
func TestJiraIdentityMatchMatrix(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)
	// Contributors: one login-matchable, two sharing a display name.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, gh_login)
		VALUES ('944e0000-0000-4000-8000-0000000000f1'::uuid, '_avjr-login', '_avjr-login')
		ON CONFLICT (cntrb_id) DO NOTHING`)
	for i, id := range []string{"f2", "f3"} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_full_name)
			VALUES (('944e0000-0000-4000-8000-0000000000'||$1::text)::uuid, '_avjr-dup'||$2::text, '_avjr Same Name')
			ON CONFLICT (cntrb_id) DO NOTHING`, id, fmt.Sprintf("%d", i))
	}
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors (cntrb_id, cntrb_login, cntrb_full_name)
		VALUES ('944e0000-0000-4000-8000-0000000000f4'::uuid, '_avjr-disp', '_avjr Unique Name')
		ON CONFLICT (cntrb_id) DO NOTHING`)

	// (a) login match
	cntrb, method, ambig, err := store.ResolveJiraIdentity(ctx, "_avjr-login", "JIRAUSER1", "Whoever")
	if err != nil {
		t.Fatal(err)
	}
	if cntrb == "" || method != "login" || ambig {
		t.Fatalf("login match: (%q,%q,%v)", cntrb, method, ambig)
	}
	// (b) display-only unambiguous
	cntrb, method, ambig, err = store.ResolveJiraIdentity(ctx, "_avjr-nobody", "JIRAUSER2", "_avjr Unique Name")
	if err != nil {
		t.Fatal(err)
	}
	if cntrb == "" || method != "display" || ambig {
		t.Fatalf("display match: (%q,%q,%v)", cntrb, method, ambig)
	}
	// (c) ambiguous display → NULL, raw identity preserved
	cntrb, method, ambig, err = store.ResolveJiraIdentity(ctx, "_avjr-ambig", "JIRAUSER3", "_avjr Same Name")
	if err != nil {
		t.Fatal(err)
	}
	if cntrb != "" || method != "" || !ambig {
		t.Fatalf("ambiguous must stay unlinked AND report ambiguity: (%q,%q,%v)", cntrb, method, ambig)
	}
	var raw int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.jira_identities
		WHERE jira_name = '_avjr-ambig' AND cntrb_id IS NULL AND display_name = '_avjr Same Name'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw != 1 {
		t.Fatal("ambiguous identity must persist raw with NULL cntrb_id (SR-6)")
	}
	// (d) mint: a Jira-only person becomes a contributor
	minted, err := store.MintJiraContributor(ctx, "_avjr-only", "_avjr Only Person")
	if err != nil {
		t.Fatal(err)
	}
	if minted == "" {
		t.Fatal("mint must return a cntrb_id")
	}
	var mm string
	if err := store.pool.QueryRow(ctx, `SELECT match_method FROM aveloxis_data.jira_identities WHERE jira_name = '_avjr-only'`).Scan(&mm); err != nil {
		t.Fatal(err)
	}
	if mm != "minted" {
		t.Fatalf("mint method = %q", mm)
	}
	// Mint is idempotent (same row back).
	again, err := store.MintJiraContributor(ctx, "_avjr-only", "_avjr Only Person")
	if err != nil || again != minted {
		t.Fatalf("mint idempotency: %q vs %q (%v)", minted, again, err)
	}
}

// TestJiraIssueConvergesWithMailProjection — THE C3a proof, both
// orders:
//  1. mail-first: LinkOrCreateIssueFromEmail mints the synthetic, then
//     the API writer lands on the SAME row (issue_id stable, bridges
//     intact), overwriting mail-derived state and filling
//     jira_issue_id.
//  2. jira-first: the API writer mints, then mail projection LINKs to
//     the same row instead of creating a duplicate.
func TestJiraIssueConvergesWithMailProjection(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)

	// --- order 1: mail first ---
	mailID, created, err := store.LinkOrCreateIssueFromEmail(ctx, jsRepoID, jsKey, "[AVJR-5] t", "b", "JIRA", nil, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || !created {
		t.Fatalf("mail create: %v created=%v", err, created)
	}
	apiID, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: jsKey, JiraIssueID: 13657732,
		Title: "the real title", Status: "Resolved",
		ResolutionDate: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
		Created:        time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Updated:        time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if apiID != mailID {
		t.Fatalf("convergence broken: mail row %d vs api row %d — one logical ticket must be ONE row", mailID, apiID)
	}
	var state, ds string
	var jiraID *int64
	if err := store.pool.QueryRow(ctx, `SELECT issue_state, data_source, jira_issue_id FROM aveloxis_data.issues WHERE issue_id = $1`, mailID).Scan(&state, &ds, &jiraID); err != nil {
		t.Fatal(err)
	}
	if state != "closed" || ds != "JIRA API" || jiraID == nil || *jiraID != 13657732 {
		t.Fatalf("API precedence: state=%q ds=%q jira_id=%v", state, ds, jiraID)
	}

	// --- order 2: jira first (a second key) ---
	const key2 = "AVJR-6"
	apiID2, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key2, JiraIssueID: 13657733,
		Title: "api-first", Status: "Open",
		Created: time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC),
		Updated: time.Date(2026, 1, 6, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	mailID2, created2, err := store.LinkOrCreateIssueFromEmail(ctx, jsRepoID, key2, "[AVJR-6] t", "", "JIRA", nil, time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if created2 || mailID2 != apiID2 {
		t.Fatalf("jira-first convergence: mail created=%v id=%d vs api id=%d", created2, mailID2, apiID2)
	}
}

// TestJiraCommentNativeAndNotificationLink — a native comment lands as
// a platform-4 messages row bridged via issue_message_ref, and the
// matching [Commented] notification (±2 min, pilot-validated 94.4%)
// gets its email_message.linked_msg_id stamped — collection-time
// linking, the GitBox mirror precedent.
func TestJiraCommentNativeAndNotificationLink(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)
	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, jsRepoID, jsKey, "[AVJR-5] t", "", "JIRA", nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	commentAt := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	// The notification arrived 37s after the comment (the pilot's median).
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.email_message
		(repo_id, platform_id, message_id_header, sender_email, subject, sent_at, msg_class, linked_external_key)
		VALUES ($1, 6, '<_avjr-c1@x>', 'jira@apache.org', '[jira] [Commented] (AVJR-5) t', $2, 'issue_event', $3)
		ON CONFLICT (message_id_header) DO UPDATE SET linked_msg_id = NULL, sent_at = EXCLUDED.sent_at`,
		jsRepoID, commentAt.Add(37*time.Second), jsKey)

	msgID, err := store.UpsertJiraComment(ctx, JiraAPIComment{
		RepoID: jsRepoID, IssueID: issueID, ExternalKey: jsKey,
		CommentID: 998877, Body: "native body", AuthorCntrbID: "",
		Created: commentAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	var platID int16
	var kind int16
	if err := store.pool.QueryRow(ctx, `SELECT platform_id, msg_kind FROM aveloxis_data.messages WHERE msg_id = $1`, msgID).Scan(&platID, &kind); err != nil {
		t.Fatal(err)
	}
	if platID != 4 || kind != MsgKindComment {
		t.Fatalf("native comment row: platform=%d kind=%d", platID, kind)
	}
	var bridged int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.issue_message_ref WHERE issue_id = $1 AND msg_id = $2`, issueID, msgID).Scan(&bridged); err != nil {
		t.Fatal(err)
	}
	if bridged != 1 {
		t.Fatal("native comment must bridge via issue_message_ref")
	}
	var linked *int64
	if err := store.pool.QueryRow(ctx, `SELECT linked_msg_id FROM aveloxis_data.email_message WHERE message_id_header = '<_avjr-c1@x>'`).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked == nil || *linked != msgID {
		t.Fatalf("notification must be linked to the native comment (got %v) — read-time precedence depends on this stamp", linked)
	}
	// Idempotent re-upsert: same msg row, stamp survives.
	again, err := store.UpsertJiraComment(ctx, JiraAPIComment{
		RepoID: jsRepoID, IssueID: issueID, ExternalKey: jsKey,
		CommentID: 998877, Body: "native body v2", Created: commentAt,
	})
	if err != nil || again != msgID {
		t.Fatalf("comment upsert idempotency: %d vs %d (%v)", msgID, again, err)
	}
}

// TestUpsertJiraIssueNeverTouchesNativeForgeRows — order 3 of the
// provider model: a NATIVE GitHub issue carrying the external_key (the
// Jira→GitHub migration case) is LINKed, enriched with jira_issue_id,
// but its STATE stays forge-owned.
func TestUpsertJiraIssueNeverTouchesNativeForgeRows(t *testing.T) {
	store, ctx := sbConnect(t)
	jsSeedRepo(t, ctx, store)
	const key3 = "AVJR-7"
	var nativeID int64
	if err := store.pool.QueryRow(ctx, `INSERT INTO aveloxis_data.issues
		(repo_id, platform_issue_id, issue_number, issue_title, issue_state, external_key, data_source)
		VALUES ($1, 944149777, 7, '[AVJR-7] migrated', 'open', $2, 'GitHub API')
		ON CONFLICT (repo_id, platform_issue_id) DO UPDATE SET issue_state = 'open', jira_issue_id = NULL
		RETURNING issue_id`, jsRepoID, key3).Scan(&nativeID); err != nil {
		t.Fatal(err)
	}
	id, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: jsRepoID, ExternalKey: key3, JiraIssueID: 555,
		Title: "x", Status: "Resolved",
		ResolutionDate: time.Now(), Updated: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if id != nativeID {
		t.Fatalf("must LINK the native row, got %d vs %d", id, nativeID)
	}
	var state string
	var jiraID *int64
	if err := store.pool.QueryRow(ctx, `SELECT issue_state, jira_issue_id FROM aveloxis_data.issues WHERE issue_id = $1`, nativeID).Scan(&state, &jiraID); err != nil {
		t.Fatal(err)
	}
	if state != "open" {
		t.Fatalf("forge-owned state was overwritten to %q — provider rank 1 is untouchable", state)
	}
	if jiraID == nil || *jiraID != 555 {
		t.Fatalf("jira_issue_id enrichment must still land on native rows: %v", jiraID)
	}
}
