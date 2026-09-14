// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
	"time"
)

// TestTrackerGuardClockDomains (fresh-context adversarial round
// 2026-09-02, #1): ApplyTrackerAction writes a MAIL sent_at into
// updated_at without demoting data_source, so on an API-owned row the
// pre-fix guard then arbitrated mail-vs-mail ties against the API's
// strict-< arm — the second half of a same-minute pair was blocked
// (the round-15 tie bug one state over) and jiraAPISnapshotFreshSQL
// refused the healing API snapshot whenever the relay stamp exceeded
// the snapshot's updated (the F4 skew, no longer harmless on an
// API-labeled row). last_mail_event_id IS NOT NULL now marks the
// stored stamp as mail-authored across BOTH guards, and an applying
// API write clears it.
func TestTrackerGuardClockDomains(t *testing.T) {
	store, ctx := emConnect(t)
	t.Cleanup(store.Close)
	repoID := jr2Repo(t, store, "fc1-clock-domains")

	apiT0 := time.Date(2026, 5, 1, 9, 0, 0, 0, time.UTC)
	issueID, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: repoID, ExternalKey: "AVFC1-1", JiraIssueID: 501,
		Title: "t", Status: "Open", Created: apiT0, Updated: apiT0,
	})
	if err != nil {
		t.Fatal(err)
	}

	state := func() (string, *int64) {
		var st string
		var em *int64
		if err := store.pool.QueryRow(ctx, `
			SELECT issue_state, last_mail_event_id FROM aveloxis_data.issues WHERE issue_id = $1`,
			issueID).Scan(&st, &em); err != nil {
			t.Fatal(err)
		}
		return st, em
	}

	// A same-minute mail pair, strictly newer than the API stamp.
	mailT := apiT0.Add(30 * time.Minute)
	if err := store.ApplyTrackerAction(ctx, issueID, "Reopened", mailT, 100); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyTrackerAction(ctx, issueID, "Resolved", mailT, 101); err != nil {
		t.Fatal(err)
	}
	if st, _ := state(); st != "closed" {
		t.Fatalf("state = %q after the pair — the second half of a same-minute pair on an ex-API row was blocked (the tie-breaker never fired against a mail-authored stamp)", st)
	}

	// The deferred OLDER half replays: refused (emid 100 < 101).
	if err := store.ApplyTrackerAction(ctx, issueID, "Reopened", mailT, 100); err != nil {
		t.Fatal(err)
	}
	if st, _ := state(); st != "closed" {
		t.Fatalf("deferred replay regressed the tie winner: %q", st)
	}

	// The healing API snapshot carries updated < the relay stamp (the
	// F4 skew) — it must still APPLY (rank 2 beats mail-authored
	// state) and re-establish the API clock domain.
	if _, err := store.UpsertJiraIssueFromAPI(ctx, JiraAPIIssue{
		RepoID: repoID, ExternalKey: "AVFC1-1", JiraIssueID: 501,
		Title: "t2", Status: "Resolved", Resolution: "Fixed",
		ResolutionDate: mailT.Add(-time.Minute), Created: apiT0, Updated: mailT.Add(-time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
	var title string
	if err := store.pool.QueryRow(ctx, `
		SELECT issue_title FROM aveloxis_data.issues WHERE issue_id = $1`, issueID).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "t2" {
		t.Fatalf("title = %q — the API snapshot was refused against a mail-authored relay stamp (the F4 skew on an API-labeled row)", title)
	}
	if _, em := state(); em != nil {
		t.Fatalf("last_mail_event_id = %v after an applying API write — the marker must clear so ties refuse against the genuine API stamp", *em)
	}

	// And a mail tie against the GENUINE API stamp still refuses (the
	// pinned rank rule): same timestamp as the API's updated.
	if err := store.ApplyTrackerAction(ctx, issueID, "Reopened", mailT.Add(-time.Minute), 300); err != nil {
		t.Fatal(err)
	}
	if st, _ := state(); st != "closed" {
		t.Fatalf("a mail tie against a genuine API stamp flipped state to %q — the rank rule regressed", st)
	}
}
