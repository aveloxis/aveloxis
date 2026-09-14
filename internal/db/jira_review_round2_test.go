// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review round on PR #193 (2026-09-01), db half: C3 (API snapshot
// freshness guard), C5 (notification-link idempotency on replay), C6
// (deterministic tie-break in the synthetic-state backfill).
package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func jr2Repo(t *testing.T, store *PostgresStore, name string) int64 {
	t.Helper()
	ctx := t.Context()
	id, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/_avjr2/" + name,
		Owner:    "_avjr2", Name: name,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message WHERE repo_id = $1`, id)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issue_message_ref WHERE issue_id IN
			(SELECT issue_id FROM aveloxis_data.issues WHERE repo_id = $1)`, id)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.messages WHERE repo_id = $1`, id)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.issues WHERE repo_id = $1`, id)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
	})
	return id
}

// TestJiraAPISnapshotFreshnessGuard (C3): the drain continues past one
// failed envelope, so an OLDER staged snapshot can replay AFTER a newer
// one already applied. The conflict update must not regress an API-owned
// row to older state — while a mail-owned row is always upgraded, and an
// equal-updated replay of the same snapshot stays idempotent.
func TestJiraAPISnapshotFreshnessGuard(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "fresh")

	newer := JiraAPIIssue{
		RepoID: repoID, ExternalKey: "AVJR2-1", JiraIssueID: 111,
		Title: "newer title", Status: "Resolved", Resolution: "Fixed",
		Created: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		Updated: time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC),
	}
	older := JiraAPIIssue{
		RepoID: repoID, ExternalKey: "AVJR2-1", JiraIssueID: 111,
		Title: "older title", Status: "Open",
		Created: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		Updated: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
	}
	if _, err := store.UpsertJiraIssueFromAPI(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertJiraIssueFromAPI(ctx, older); err != nil {
		t.Fatal(err)
	}
	var title, state string
	var updated time.Time
	if err := store.pool.QueryRow(ctx, `SELECT issue_title, issue_state, updated_at
		FROM aveloxis_data.issues WHERE repo_id = $1 AND external_key = 'AVJR2-1'`,
		repoID).Scan(&title, &state, &updated); err != nil {
		t.Fatal(err)
	}
	if state != "closed" || title != "newer title" || !updated.Equal(newer.Updated) {
		t.Errorf("stale replay regressed the row: title=%q state=%q updated=%v — an API-owned row must ignore API snapshots older than its stored updated_at (C3)", title, state, updated)
	}

	// Idempotent same-snapshot replay still applies (equal updated).
	if _, err := store.UpsertJiraIssueFromAPI(ctx, newer); err != nil {
		t.Fatalf("equal-updated replay must stay idempotent: %v", err)
	}

	// A MAIL-owned row is upgraded by ANY API snapshot regardless of age.
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, issue_title,
			issue_state, external_key, updated_at, data_source, tool_source, tool_version, data_collection_date)
		VALUES ($1, $2, 2, 'mail title', 'open', 'AVJR2-2',
			'2026-08-01 00:00:00+00', 'JIRA', 'test', '0', NOW())`,
		repoID, syntheticIssueID("AVJR2-2")); err != nil {
		t.Fatal(err)
	}
	mailUpgrade := older
	mailUpgrade.ExternalKey = "AVJR2-2"
	mailUpgrade.JiraIssueID = 222
	if _, err := store.UpsertJiraIssueFromAPI(ctx, mailUpgrade); err != nil {
		t.Fatal(err)
	}
	var ds string
	if err := store.pool.QueryRow(ctx, `SELECT data_source, issue_title FROM aveloxis_data.issues
		WHERE repo_id = $1 AND external_key = 'AVJR2-2'`, repoID).Scan(&ds, &title); err != nil {
		t.Fatal(err)
	}
	if ds != JiraAPIDataSource || title != "older title" {
		t.Errorf("mail-owned row must ALWAYS upgrade to the API snapshot (rank 2 over rank 3), got ds=%q title=%q", ds, title)
	}
}

// TestJiraCommentLinkIdempotentOnReplay (C5): the ±2-minute notification
// link must not consume a SECOND notification when an envelope replays
// (one comment linked, a later comment failed, the whole envelope
// re-drains). Once ANY notification is linked to this comment's msg_id,
// the link step is a no-op.
func TestJiraCommentLinkIdempotentOnReplay(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "link")
	rgls := xsRegisterList(t, store, "_avjr2_link", "dev@avjr2link.apache.org", "apache_ponymail")

	created := time.Date(2026, 5, 1, 10, 0, 30, 0, time.UTC)
	for i, mid := range []string{"<jr2-n1@example>", "<jr2-n2@example>"} {
		em := &model.EmailMessage{
			RglsID: &rgls, RepoID: &repoID, PlatformID: model.Platform(MailingListPlatformID),
			MLSystem: "apache_ponymail", ListAddress: "dev@avjr2link.apache.org",
			MessageIDHeader: mid, Subject: "[jira] [Commented] (AVJR2L-1) t",
			SenderEmail: "jira@apache.org",
			SentAt:      created.Add(time.Duration(i) * 20 * time.Second).Truncate(time.Minute),
			MsgClass:    "issue_event", LinkedExternalKey: "AVJR2L-1",
			ProjectedKind: "mailing_list_only", DataSource: "dev@avjr2link.apache.org",
		}
		if _, err := store.UpsertEmailMessage(ctx, em); err != nil {
			t.Fatal(err)
		}
	}
	issue := JiraAPIIssue{RepoID: repoID, ExternalKey: "AVJR2L-1", JiraIssueID: 333,
		Title: "t", Status: "Open", Created: created, Updated: created}
	issueID, err := store.UpsertJiraIssueFromAPI(ctx, issue)
	if err != nil {
		t.Fatal(err)
	}
	cm := JiraAPIComment{RepoID: repoID, IssueID: issueID, ExternalKey: "AVJR2L-1",
		CommentID: 987001, Body: "hello", Created: created, Updated: created}
	if _, err := store.UpsertJiraComment(ctx, cm); err != nil {
		t.Fatal(err)
	}
	// The replay (same comment, e.g. after a later comment failed).
	if _, err := store.UpsertJiraComment(ctx, cm); err != nil {
		t.Fatal(err)
	}
	var linked int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.email_message
		WHERE repo_id = $1 AND linked_msg_id IS NOT NULL`, repoID).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked != 1 {
		t.Errorf("linked notifications = %d, want exactly 1 — a replay must not consume a neighboring notification (C5)", linked)
	}
}

// TestSyntheticStateBackfillTieBreaksDeterministically (C6): Pony Mail
// timestamps tie at minute precision; the latest-action aggregates must
// order by a deterministic secondary key (email_message_id — mbox
// ingest order preserves send order within the minute) so reruns cannot
// flip issue state.
func TestSyntheticStateBackfillTieBreaksDeterministically(t *testing.T) {
	src := srctest.Read(t, "internal/db/mailinglist_projection_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) BackfillSyntheticJiraState(")
	for _, agg := range []string{
		"ORDER BY em.sent_at DESC, em.email_message_id DESC))[1] AS action",
		"ORDER BY em.sent_at DESC, em.email_message_id DESC))[1] AS at",
	} {
		if !strings.Contains(body, agg) {
			t.Errorf("BackfillSyntheticJiraState must tie-break its latest-action aggregates on email_message_id (Pony Mail minute-rounds sent_at; ties are the Part G 53-flip shape): missing %q", agg)
		}
	}
}

// TestJiraReporterFreshnessGuard (Copilot round 2 on PR #193, #5):
// reporter_id rides the SAME API-snapshot freshness predicate as the
// state trio. Pre-fix it was a bare COALESCE(EXCLUDED, stored) — a
// STALE replayed snapshot naming a different (or since-fixed) reporter
// regressed attribution while the rest of the row stayed new. Equal
// timestamps still FILL an unresolved reporter (the predicate is <=),
// and an incoming NULL never clobbers a resolved one.
func TestJiraReporterFreshnessGuard(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "repfresh")

	cntrbA, err := store.MintJiraContributor(ctx, "_avjr2-rep-a", "Rep A")
	if err != nil {
		t.Fatal(err)
	}
	cntrbB, err := store.MintJiraContributor(ctx, "_avjr2-rep-b", "Rep B")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.jira_identities WHERE jira_name LIKE '_avjr2-rep-%'`)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_identities WHERE cntrb_id IN ($1::uuid, $2::uuid)`, cntrbA, cntrbB)
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id IN ($1::uuid, $2::uuid)`, cntrbA, cntrbB)
	})

	readReporter := func() string {
		t.Helper()
		var rep *string
		if err := store.pool.QueryRow(ctx, `SELECT reporter_id::text FROM aveloxis_data.issues
			WHERE repo_id = $1 AND external_key = 'AVJR2-REP'`, repoID).Scan(&rep); err != nil {
			t.Fatal(err)
		}
		if rep == nil {
			return "<NULL>"
		}
		return *rep
	}

	t1 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)

	// Seed at t2 with NO reporter (identity unmatched on that pass).
	seed := JiraAPIIssue{RepoID: repoID, ExternalKey: "AVJR2-REP", JiraIssueID: 222,
		Title: "t", Status: "Open", Created: t1, Updated: t2}
	if _, err := store.UpsertJiraIssueFromAPI(ctx, seed); err != nil {
		t.Fatal(err)
	}

	// (1) EQUAL-timestamp snapshot fills the unresolved reporter.
	fill := seed
	fill.ReporterCntrb = cntrbA
	if _, err := store.UpsertJiraIssueFromAPI(ctx, fill); err != nil {
		t.Fatal(err)
	}
	if rep := readReporter(); rep != cntrbA {
		t.Fatalf("equal-timestamp snapshot must FILL the unresolved reporter, got %s", rep)
	}

	// (2) A STALE replay naming a different reporter must NOT regress.
	stale := seed
	stale.Updated = t1
	stale.ReporterCntrb = cntrbB
	if _, err := store.UpsertJiraIssueFromAPI(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if rep := readReporter(); rep != cntrbA {
		t.Fatalf("stale replay regressed reporter to %s, want the newer snapshot's %s kept", rep, cntrbA)
	}

	// (3) A FRESH snapshot with an unmatched reporter (NULL) must not
	// clobber the resolved one.
	fresher := seed
	fresher.Updated = t2.Add(time.Hour)
	fresher.ReporterCntrb = ""
	if _, err := store.UpsertJiraIssueFromAPI(ctx, fresher); err != nil {
		t.Fatal(err)
	}
	if rep := readReporter(); rep != cntrbA {
		t.Fatalf("fresh NULL clobbered the resolved reporter, got %s", rep)
	}

	// (4) A FRESH snapshot with a DIFFERENT resolved reporter advances.
	moved := seed
	moved.Updated = t2.Add(2 * time.Hour)
	moved.ReporterCntrb = cntrbB
	if _, err := store.UpsertJiraIssueFromAPI(ctx, moved); err != nil {
		t.Fatal(err)
	}
	if rep := readReporter(); rep != cntrbB {
		t.Fatalf("fresh snapshot's reporter change must advance, got %s", rep)
	}
}

// TestListJiraProjectRegistrations (Copilot round 2 on PR #193, #1):
// the identity backfill iterates the LIVE registrations — the
// operator-correctable mapping — so the lister must serve enabled
// rows with each registration's base_url + repo_id and hide disabled
// ones.
func TestListJiraProjectRegistrations(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "reglist")

	base := "https://issues.example.org/jira"
	cleanup := func() {
		store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key LIKE '_AVJRL%'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	// The one-instance guard: every registration must share the base.
	var existingBase string
	_ = store.pool.QueryRow(ctx, `SELECT base_url FROM aveloxis_ops.jira_project_serve LIMIT 1`).Scan(&existingBase)
	if existingBase != "" {
		base = existingBase
	}

	if err := store.RegisterJiraProject(ctx, "_AVJRL1", base, &repoID); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterJiraProject(ctx, "_AVJRL2", base, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.RegisterJiraProject(ctx, "_AVJRL3", base, &repoID); err != nil {
		t.Fatal(err)
	}
	var deadID int64
	if err := store.pool.QueryRow(ctx, `SELECT jps_id FROM aveloxis_ops.jira_project_serve
		WHERE project_key = '_AVJRL3'`).Scan(&deadID); err != nil {
		t.Fatal(err)
	}
	if err := store.DisableJiraProject(ctx, deadID); err != nil {
		t.Fatal(err)
	}

	regs, err := store.ListJiraProjectRegistrations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]JiraProjectRegistration{}
	for _, r := range regs {
		if strings.HasPrefix(r.ProjectKey, "_AVJRL") {
			got[r.ProjectKey] = r
		}
	}
	if _, dead := got["_AVJRL3"]; dead {
		t.Fatal("disabled registration must not list")
	}
	r1, ok := got["_AVJRL1"]
	if !ok || r1.BaseURL != base || r1.RepoID == nil || *r1.RepoID != repoID {
		t.Fatalf("registration 1 = %+v, want base %q + repo %d", r1, base, repoID)
	}
	r2, ok := got["_AVJRL2"]
	if !ok || r2.RepoID != nil {
		t.Fatalf("registration 2 = %+v, want listed with nil repo (the operator-heals-it case)", r2)
	}
}

// TestReleaseJiraClaimOwnershipAndReclaim (Copilot round 4 on
// PR #193): the shutdown claim release is ownership-qualified by the
// claim's OWN jps_locked_at stamp — a scan that outlived the stale
// window and was re-claimed elsewhere cannot clear the new holder's
// lock — and a correct release makes the project immediately
// reclaimable with counters and checkpoint untouched.
func TestReleaseJiraClaimOwnershipAndReclaim(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "release")

	base := "https://issues.example.org/jira"
	var existingBase string
	_ = store.pool.QueryRow(ctx, `SELECT base_url FROM aveloxis_ops.jira_project_serve LIMIT 1`).Scan(&existingBase)
	if existingBase != "" {
		base = existingBase
	}
	cleanup := func() {
		store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key = '_AVJRC1'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	if err := store.RegisterJiraProject(ctx, "_AVJRC1", base, &repoID); err != nil {
		t.Fatal(err)
	}
	job, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "_AVJRC1")
	if err != nil || job == nil {
		t.Fatalf("claim: %v (job=%v)", err, job)
	}
	if job.LockedAt.IsZero() {
		t.Fatal("claim must return its own jps_locked_at stamp — the release's ownership key")
	}

	// A WRONG stamp releases nothing (the straggler-release guard).
	if err := store.ReleaseJiraClaim(ctx, job.JpsID, job.LockedAt.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	var locked *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT jps_locked_at FROM aveloxis_ops.jira_project_serve
		WHERE jps_id = $1`, job.JpsID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if locked == nil {
		t.Fatal("a mismatched ownership stamp must release NOTHING")
	}

	// The claim's own stamp releases; the project is immediately
	// reclaimable (no 2h stale-window wait) with counters untouched.
	if err := store.ReleaseJiraClaim(ctx, job.JpsID, job.LockedAt); err != nil {
		t.Fatal(err)
	}
	var fails int
	var lastRun *time.Time
	if err := store.pool.QueryRow(ctx, `SELECT jps_locked_at, COALESCE(jps_failed_attempts, 0), jps_last_run
		FROM aveloxis_ops.jira_project_serve WHERE jps_id = $1`, job.JpsID).Scan(&locked, &fails, &lastRun); err != nil {
		t.Fatal(err)
	}
	if locked != nil {
		t.Fatal("the claim's own stamp must release the lock")
	}
	if fails != 0 || lastRun != nil {
		t.Fatalf("release must touch NOTHING but the lock (fails=%d lastRun=%v) — it is a rollback, not an outcome", fails, lastRun)
	}
	again, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "_AVJRC1")
	if err != nil || again == nil {
		t.Fatalf("released project must be immediately reclaimable, got %v (err=%v)", again, err)
	}
}

// TestCommentCountConvergesBothArrivalOrders (Copilot round 6 on
// PR #193, suppressed #1 + #2): a matched native/notification pair is
// ONE logical comment. The recount excludes superseded notifications
// (refs whose email_message carries linked_msg_id), and BOTH arrival
// orders converge — notification-first links at UpsertJiraComment
// (which now links BEFORE bridging so its own recount sees the
// supersession), native-first links via
// LinkCommentNotificationToNative when the notification is projected
// later.
func TestCommentCountConvergesBothArrivalOrders(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "ccount")
	rgls := xsRegisterList(t, store, "_avjr2_cc", "dev@avjr2cc.apache.org", "apache_ponymail")
	// This test creates email_message_ref rows, which jr2Repo's cleanup
	// does not know about — without deleting them first, the RESTRICT
	// FK silently blocks the email_message delete and the surviving
	// row's PRESERVED linked_msg_id poisons the next run's order-A arm
	// (the v0.27.76 residue class; t.Cleanup LIFO runs this before
	// jr2Repo's chain).
	t.Cleanup(func() {
		ctx := context.Background()
		store.pool.Exec(ctx, `DELETE FROM aveloxis_data.email_message_ref WHERE email_message_id IN
			(SELECT email_message_id FROM aveloxis_data.email_message WHERE repo_id = $1)`, repoID)
	})

	created := time.Date(2026, 5, 1, 10, 0, 30, 0, time.UTC)
	commentCount := func(issueID int64) int {
		t.Helper()
		var n int
		if err := store.pool.QueryRow(ctx, `SELECT COALESCE(comment_count, 0)
			FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	projectNotification := func(key, mid string, issueID int64) int64 {
		t.Helper()
		em := &model.EmailMessage{
			RglsID: &rgls, RepoID: &repoID, PlatformID: model.Platform(MailingListPlatformID),
			MLSystem: "apache_ponymail", ListAddress: "dev@avjr2cc.apache.org",
			MessageIDHeader: mid, Subject: "[jira] [Commented] (" + key + ") t",
			SenderEmail: "jira@apache.org", SentAt: created.Truncate(time.Minute),
			MsgClass: "issue_event", LinkedExternalKey: key,
			ProjectedKind: "mailing_list_only", DataSource: "dev@avjr2cc.apache.org",
		}
		emID, err := store.UpsertEmailMessage(ctx, em)
		if err != nil {
			t.Fatal(err)
		}
		msgID, err := store.UpsertMailingListMessageBody(ctx, repoID, mid, "dev@avjr2cc.apache.org",
			"jira@apache.org", "notification body", created.Truncate(time.Minute), nil, "clean", "qs-v1")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.InsertEmailMessageRef(ctx, emID, msgID, nil); err != nil {
			t.Fatal(err)
		}
		if err := store.BridgeEmailToIssue(ctx, issueID, repoID, msgID); err != nil {
			t.Fatal(err)
		}
		return emID
	}

	// ORDER A: notification first, native second.
	issueA, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{RepoID: repoID,
		ExternalKey: "AVJRCC-1", JiraIssueID: 441, Title: "t", Status: "Open",
		Created: created, Updated: created})
	if err != nil {
		t.Fatal(err)
	}
	projectNotification("AVJRCC-1", "<cc-a1@example>", issueA)
	if n := commentCount(issueA); n != 1 {
		t.Fatalf("order A after notification: comment_count = %d, want 1", n)
	}
	if _, err := store.UpsertJiraComment(ctx, JiraAPIComment{RepoID: repoID, IssueID: issueA,
		ExternalKey: "AVJRCC-1", CommentID: 988001, Body: "hello",
		Created: created, Updated: created}); err != nil {
		t.Fatal(err)
	}
	if n := commentCount(issueA); n != 1 {
		t.Fatalf("order A after native twin: comment_count = %d, want 1 — the matched pair is ONE logical comment", n)
	}

	// ORDER B: native first, notification second.
	issueB, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{RepoID: repoID,
		ExternalKey: "AVJRCC-2", JiraIssueID: 442, Title: "t", Status: "Open",
		Created: created, Updated: created})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertJiraComment(ctx, JiraAPIComment{RepoID: repoID, IssueID: issueB,
		ExternalKey: "AVJRCC-2", CommentID: 988002, Body: "hello",
		Created: created, Updated: created}); err != nil {
		t.Fatal(err)
	}
	emB := projectNotification("AVJRCC-2", "<cc-b1@example>", issueB)
	if n := commentCount(issueB); n != 2 {
		t.Fatalf("order B pre-link: comment_count = %d, want 2 (nothing linked yet)", n)
	}
	if err := store.LinkCommentNotificationToNative(ctx, emB, issueB, created.Truncate(time.Minute)); err != nil {
		t.Fatal(err)
	}
	var linked *int64
	if err := store.pool.QueryRow(ctx, `SELECT linked_msg_id FROM aveloxis_data.email_message
		WHERE email_message_id = $1`, emB).Scan(&linked); err != nil {
		t.Fatal(err)
	}
	if linked == nil {
		t.Fatal("order B: the reverse link must claim the already-collected native comment")
	}
	if n := commentCount(issueB); n != 1 {
		t.Fatalf("order B after reverse link: comment_count = %d, want 1", n)
	}
	// Idempotent replay of the reverse link.
	if err := store.LinkCommentNotificationToNative(ctx, emB, issueB, created.Truncate(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n := commentCount(issueB); n != 1 {
		t.Fatalf("reverse-link replay changed comment_count to %d", n)
	}
}

// TestJiraClaimOwnedWritesRefuseStaleOwner (Copilot round 6 on
// PR #193, active): after the 2h stale window a claim can be STOLEN;
// the original worker's completion/failure writes must then match
// zero rows and surface ErrJiraClaimLost instead of clearing the
// replacement's lock or stamping outcomes it no longer owns.
func TestJiraClaimOwnedWritesRefuseStaleOwner(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "steal")

	base := "https://issues.example.org/jira"
	var existingBase string
	_ = store.pool.QueryRow(ctx, `SELECT base_url FROM aveloxis_ops.jira_project_serve LIMIT 1`).Scan(&existingBase)
	if existingBase != "" {
		base = existingBase
	}
	cleanup := func() {
		store.pool.Exec(context.Background(),
			`DELETE FROM aveloxis_ops.jira_project_serve WHERE project_key = '_AVJRO1'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	if err := store.RegisterJiraProject(ctx, "_AVJRO1", base, &repoID); err != nil {
		t.Fatal(err)
	}
	job1, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "_AVJRO1")
	if err != nil || job1 == nil {
		t.Fatalf("claim1: %v %v", job1, err)
	}
	// The scan STALLS past the stale window (v0.29.1: staleness measures
	// inactivity, so both the lock and the heartbeat must be aged); a
	// second worker steals it.
	if _, err := store.pool.Exec(ctx, `UPDATE aveloxis_ops.jira_project_serve
		SET jps_locked_at = NOW() - INTERVAL '3 hours',
		    jps_heartbeat_at = NOW() - INTERVAL '3 hours' WHERE jps_id = $1`, job1.JpsID); err != nil {
		t.Fatal(err)
	}
	job2, err := store.ClaimNextJiraProject(ctx, 24*time.Hour, "_AVJRO1")
	if err != nil || job2 == nil {
		t.Fatalf("steal claim: %v %v", job2, err)
	}

	if err := store.CompleteJiraScan(ctx, job1.JpsID, job1.LockedAt); !errors.Is(err, ErrJiraClaimLost) {
		t.Fatalf("stale owner's completion = %v, want ErrJiraClaimLost", err)
	}
	if err := store.RecordJiraFailure(ctx, job1.JpsID, job1.LockedAt); !errors.Is(err, ErrJiraClaimLost) {
		t.Fatalf("stale owner's failure record = %v, want ErrJiraClaimLost", err)
	}
	var locked *time.Time
	var lastRun *time.Time
	var fails int
	if err := store.pool.QueryRow(ctx, `SELECT jps_locked_at, jps_last_run, COALESCE(jps_failed_attempts, 0)
		FROM aveloxis_ops.jira_project_serve WHERE jps_id = $1`, job1.JpsID).Scan(&locked, &lastRun, &fails); err != nil {
		t.Fatal(err)
	}
	if locked == nil || !locked.Equal(job2.LockedAt) {
		t.Fatalf("lock = %v, want the REPLACEMENT holder's stamp %v intact", locked, job2.LockedAt)
	}
	if lastRun != nil || fails != 0 {
		t.Fatalf("stale owner recorded outcomes (last_run=%v fails=%d) — it owns nothing", lastRun, fails)
	}
	// The rightful holder's writes work.
	if err := store.CompleteJiraScan(ctx, job2.JpsID, job2.LockedAt); err != nil {
		t.Fatalf("rightful completion: %v", err)
	}
}

// TestMintJiraContributorIsAtomic (Copilot round 9 on PR #193): the
// pre-fix three-statement mint committed the contributor before the
// identity link, so a concurrent double-mint's LOSER abandoned an
// active contributor row — and those rows are NOT invisible:
// GetPublicFleetStats publishes COUNT(*) of contributors on the
// landing page. The mint is one transaction now: the loser deletes
// its own never-handed-out row before returning the winner's id.
// Raced 20 rounds with a start barrier; every round must end with
// BOTH callers agreeing on one winner and exactly ONE contributor
// row minted for the name.
func TestMintJiraContributorIsAtomic(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)

	cleanup := func() {
		c := context.Background()
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.jira_identities WHERE jira_name LIKE '_avjr2-race-%'`)
		cleanupExecRetry(c, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_full_name LIKE '_avjr2 Race %'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	for round := 0; round < 20; round++ {
		name := fmt.Sprintf("_avjr2-race-%d", round)
		display := fmt.Sprintf("_avjr2 Race %d", round)
		start := make(chan struct{})
		results := make(chan string, 2)
		errs := make(chan error, 2)
		for g := 0; g < 2; g++ {
			go func() {
				<-start
				id, err := store.MintJiraContributor(ctx, name, display)
				results <- id
				errs <- err
			}()
		}
		close(start)
		a, b := <-results, <-results
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				t.Fatalf("round %d: mint: %v", round, err)
			}
		}
		if a != b {
			t.Fatalf("round %d: winners diverge (%s vs %s) — both callers must receive the identity row's winner", round, a, b)
		}
		var rows int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_data.contributors
			WHERE cntrb_full_name = $1`, display).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("round %d: %d contributor rows for one identity — the loser's orphan inflates the PUBLISHED contributor count", round, rows)
		}
	}
}
