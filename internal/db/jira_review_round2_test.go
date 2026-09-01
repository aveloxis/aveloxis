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
