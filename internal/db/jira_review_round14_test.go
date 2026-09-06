// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"crypto/md5"
	"fmt"
	"testing"
	"time"
)

// TestJiraCommentStaleReplayNeverRegressesText (Copilot round 14 on
// PR #193): the drain continues past failed envelopes, so an OLDER
// staged snapshot can replay AFTER a newer edited comment landed —
// the bare EXCLUDED overwrite regressed msg_text (the issue-level
// freshness guard never covered comments). messages.msg_updated now
// persists the provider edit time; text advances only on
// equal-or-newer snapshots.
func TestJiraCommentStaleReplayNeverRegressesText(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round14-comment-fresh")

	issueID, _, err := store.LinkOrCreateIssueFromEmail(ctx, repoID, "AVR14-1", "t", "b", "JIRA", nil,
		time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("seed issue: %v", err)
	}

	older := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	newCm := JiraAPIComment{RepoID: repoID, IssueID: issueID, ExternalKey: "AVR14-1",
		CommentID: 914001, Body: "edited body", Created: older, Updated: newer}
	msgID, err := store.UpsertJiraComment(ctx, newCm)
	if err != nil {
		t.Fatal(err)
	}

	// The stale replay: an older snapshot of the same comment.
	oldCm := newCm
	oldCm.Body = "original body"
	oldCm.Updated = older
	if _, err := store.UpsertJiraComment(ctx, oldCm); err != nil {
		t.Fatal(err)
	}
	var text string
	var updated *time.Time
	if err := store.pool.QueryRow(ctx, `
		SELECT msg_text, msg_updated FROM aveloxis_data.messages WHERE msg_id = $1`,
		msgID).Scan(&text, &updated); err != nil {
		t.Fatal(err)
	}
	if text != "edited body" {
		t.Fatalf("msg_text = %q — a stale replayed snapshot regressed the edited comment", text)
	}
	if updated == nil || !updated.Equal(newer) {
		t.Fatalf("msg_updated = %v, want the newer edit time retained", updated)
	}

	// Equal-timestamp replay stays idempotent AND FILLS an unresolved
	// author (author is immutable across edits, so even a stale
	// snapshot's author is correct — reasoned at the guard). Copilot
	// round 24 (PR #193): the previous version seeded AuthorCntrbID="",
	// which maps to SQL NULL and only asserted msg_text — a regression
	// in the COALESCE(..., EXCLUDED.cntrb_id) author arm would still
	// pass. Seed a real contributor, replay its id, and assert the
	// stored cntrb_id actually filled.
	var authorID string
	mustQueryRowRetry(ctx, t, store, `INSERT INTO aveloxis_data.contributors
		(cntrb_id, cntrb_login, cntrb_email, cntrb_deleted)
		VALUES (gen_random_uuid(), $1, '', 0) RETURNING cntrb_id::text`,
		&authorID, "r14-author-"+fmt.Sprintf("%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		cleanupExecRetry(ctx, store, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id::text = $1`, authorID)
	})
	fill := newCm
	fill.AuthorCntrbID = authorID
	if _, err := store.UpsertJiraComment(ctx, fill); err != nil {
		t.Fatal(err)
	}
	var gotAuthor *string
	if err := store.pool.QueryRow(ctx, `
		SELECT msg_text, cntrb_id::text FROM aveloxis_data.messages WHERE msg_id = $1`, msgID).Scan(&text, &gotAuthor); err != nil {
		t.Fatal(err)
	}
	if text != "edited body" {
		t.Fatalf("equal-timestamp replay changed text to %q", text)
	}
	if gotAuthor == nil || *gotAuthor != authorID {
		t.Fatalf("equal-timestamp replay must FILL the unresolved author to %s, got %v", authorID, gotAuthor)
	}
}

// TestStripBatchCASSkipsConcurrentlyReingestedRows (Copilot round 14
// on PR #193): the strip CLI computes clean text from a snapshot read
// in a previous statement; a concurrent drain can re-ingest a
// CORRECTED raw body (writing its own fresh clean) between the read
// and the write. The unconditional batch update overwrote that fresh
// clean with old-body output and stamped the current rule — making
// the row invisible to --rule-rerun forever. The md5 compare-and-set
// leaves the changed row untouched.
func TestStripBatchCASSkipsConcurrentlyReingestedRows(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "round14-strip-cas")

	seed := func(pid int64, body string) int64 {
		var id int64
		if err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.messages (repo_id, platform_msg_id, platform_id, msg_kind, msg_text)
			VALUES ($1, $2, 6, 0, $3) RETURNING msg_id`, repoID, pid, body).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	racedID := seed(914101, "old raw body")
	cleanID := seed(914102, "quiet body")

	// The CLI read both rows; before its write lands, the drain
	// re-ingests the first with a corrected body + its OWN clean.
	mustExecRetry(ctx, t, store, `
		UPDATE aveloxis_data.messages
		SET msg_text = 'corrected raw body', msg_text_clean = 'corrected clean', msg_text_clean_rule = 'qs-v1'
		WHERE msg_id = $1`, racedID)

	oldMD5 := fmt.Sprintf("%x", md5.Sum([]byte("old raw body")))
	quietMD5 := fmt.Sprintf("%x", md5.Sum([]byte("quiet body")))
	if err := store.UpdateMessageCleanBatch(ctx,
		[]int64{racedID, cleanID},
		[]string{"stale clean from old body", "quiet clean"},
		[]string{oldMD5, quietMD5}, "qs-v1"); err != nil {
		t.Fatal(err)
	}

	var clean string
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(msg_text_clean, '') FROM aveloxis_data.messages WHERE msg_id = $1`,
		racedID).Scan(&clean); err != nil {
		t.Fatal(err)
	}
	if clean != "corrected clean" {
		t.Fatalf("raced row clean = %q — the CAS must leave the drain's fresh clean in place (pre-fix: stale clean + current rule = invisible to --rule-rerun)", clean)
	}
	if err := store.pool.QueryRow(ctx, `
		SELECT COALESCE(msg_text_clean, '') FROM aveloxis_data.messages WHERE msg_id = $1`,
		cleanID).Scan(&clean); err != nil {
		t.Fatal(err)
	}
	if clean != "quiet clean" {
		t.Fatalf("unraced row clean = %q — the CAS must still stamp unchanged rows", clean)
	}
}
