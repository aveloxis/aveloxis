// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review round on PR #193 (2026-09-01), db half: C3 (API snapshot
// freshness guard), C5 (notification-link idempotency on replay), C6
// (deterministic tie-break in the synthetic-state backfill).
package db

import (
	"context"
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
