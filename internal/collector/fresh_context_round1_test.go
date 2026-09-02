// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Fresh-context adversarial round 2026-09-02 (the operator-requested
// own-review pass): intra-source row keysets (#4) and the thread-
// inheritance deferral arm (#6).

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// A full window of persistently failing envelopes inside ONE project
// must not head-block that project's own tail: the row cursor skips
// them this drain (they retry next drain from the top). Pre-fix, the
// all-failed batch broke the loop and the tail never drained.
func TestJiraDrainRowCursorPassesFailingWindow(t *testing.T) {
	repoID := int64(42)
	rows := make([]db.JiraStagingRow, 0, jiraDrainBatchSize+1)
	for i := 1; i <= jiraDrainBatchSize; i++ {
		// Unparseable envelopes: processEnvelope fails, row stays staged.
		rows = append(rows, db.JiraStagingRow{JsID: int64(i), IssueKey: fmt.Sprintf("BAD-%d", i),
			RepoID: &repoID, Envelope: []byte(`{broken`)})
	}
	rows = append(rows, db.JiraStagingRow{JsID: int64(jiraDrainBatchSize + 1), IssueKey: "AVJP-1",
		RepoID: &repoID, Envelope: []byte(jiraEnvelope)})
	store := &fakeJiraProcStore{
		stagedProjects: []int64{7},
		perProject:     map[int64][]db.JiraStagingRow{7: rows},
		identities:     map[string][3]any{"alice-gh": {"cntrb-alice", "login", false}},
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 1 || len(store.processed) != 1 || store.processed[0] != int64(jiraDrainBatchSize+1) {
		t.Fatalf("drained=%d processed=%v — the tail row behind a full failing window must drain in the SAME pass (pre-fix the all-failed batch broke the loop)", n, store.processed)
	}
}

// The mailing-list sibling: an all-deferred batch advances the row
// cursor to the list's tail instead of returning early.
func TestMailingListDrainRowCursorPassesDeferredBatch(t *testing.T) {
	rg := int64(3)
	deferRow := func(mls int64) db.StagedMailingListRow {
		return db.StagedMailingListRow{MlsID: mls, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
			MessageID: fmt.Sprintf("m-fc4-%d@x", mls), ListAddress: "dev@x.apache.org",
			SenderEmail: "jira@apache.org", Subject: fmt.Sprintf("[jira] [Resolved] (AVFC4-%d) done", mls),
			MsgClass: mailinglist.ClassIssueEvent, ExternalKey: fmt.Sprintf("AVFC4-%d", mls), Body: "b",
		}}
	}
	goodRow := db.StagedMailingListRow{MlsID: 9, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
		MessageID: "m-fc4-good@x", ListAddress: "dev@x.apache.org", SenderEmail: "a@x",
		Subject: "plain discussion", MsgClass: mailinglist.ClassDiscuss, Body: "b",
	}}
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		chunk:            2,
		rows:             []db.StagedMailingListRow{deferRow(1), deferRow(2), goodRow},
		applyActionFails: 2, // the whole first chunk defers
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if n != 1 || len(store.processed) != 1 || store.processed[0] != 9 {
		t.Fatalf("processed=%v (n=%d) — the tail row behind an all-deferred batch must drain in the SAME pass", store.processed, n)
	}
}

// #6: a transient FindIssueForThread failure defers the row like the
// other three projection-side calls — the pre-fix swallow marked it
// processed with the thread bridge permanently missing.
func TestThreadInheritanceLookupFailureDefersRow(t *testing.T) {
	rg := int64(3)
	row := db.StagedMailingListRow{MlsID: 1, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
		MessageID: "m-fc6@x", ListAddress: "dev@x.apache.org", SenderEmail: "a@x",
		Subject: "Re: something", MsgClass: mailinglist.ClassDiscuss, Body: "b",
		ThreadRoot: "root-fc6@x",
	}}
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:          []db.StagedMailingListRow{row},
		findThreadErr: errors.New("transient"),
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.processed) != 0 {
		t.Fatalf("processed = %v — a failed thread-inheritance lookup must leave the row UNPROCESSED for replay", store.processed)
	}
	// Replay converges (the knob cleared itself).
	store.rows = []db.StagedMailingListRow{row}
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(store.processed) != 1 {
		t.Fatalf("processed = %v after replay, want the row marked", store.processed)
	}
}
