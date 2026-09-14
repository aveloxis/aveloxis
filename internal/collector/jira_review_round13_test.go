// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot round 13 on PR #193: drain-window starvation. Oldest-first
// project/list windows let head-blockers (nil-repo rows awaiting the
// operator heal, persistently failing envelopes) permanently occupy
// the head once they fill a window — newer healthy sources never got
// a turn. Both drains rotate a keyset cursor now.

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// One more head-blocking project than the drain window holds: the
// healthy tail project must still drain within two passes. Pre-fix,
// every DrainOnce re-asked for the same oldest window and the tail
// starved forever.
func TestJiraDrainRotatesPastHeadBlockers(t *testing.T) {
	repoID := int64(42)
	staged := make([]int64, 0, jiraDrainProjectLimit+1)
	per := map[int64][]db.JiraStagingRow{}
	for i := 1; i <= jiraDrainProjectLimit; i++ {
		id := int64(i)
		staged = append(staged, id)
		// nil-repo: skipped, stays staged — the head-blocker shape.
		per[id] = []db.JiraStagingRow{{JsID: id, IssueKey: fmt.Sprintf("BLK-%d", i), RepoID: nil,
			Envelope: []byte(`{"key":"BLK","fields":{"summary":"x","updated":"2026-01-01T00:00:00.000+0000"}}`)}}
	}
	tail := int64(jiraDrainProjectLimit + 1)
	staged = append(staged, tail)
	per[tail] = []db.JiraStagingRow{{JsID: 9999, IssueKey: "AVJP-1", RepoID: &repoID, Envelope: []byte(jiraEnvelope)}}

	store := &fakeJiraProcStore{
		stagedProjects: staged,
		perProject:     per,
		identities:     map[string][3]any{"alice-gh": {"cntrb-alice", "login", false}},
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))

	n1, err := p.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("first drain: %v", err)
	}
	n2, err := p.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n1+n2 != 1 || len(store.processed) != 1 || store.processed[0] != 9999 {
		t.Fatalf("drained=%d+%d processed=%v — the tail project past a full head-blocker window must drain within two passes (pre-fix it starved forever)", n1, n2, store.processed)
	}
}

// The mailing-list sibling: with a window of 1 and two staged lists,
// consecutive DrainOnce calls must visit BOTH (the cursor advances);
// the pre-fix oldest-first window revisited the first list forever
// when it could not drain.
func TestMailingListDrainRotatesLists(t *testing.T) {
	rg := int64(3)
	row := func(mls int64) db.StagedMailingListRow {
		return db.StagedMailingListRow{MlsID: mls, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
			MessageID: fmt.Sprintf("m-rot-%d@x", mls), ListAddress: "dev@x.apache.org",
			SenderEmail: "a@x", Subject: "hello", MsgClass: mailinglist.ClassDiscuss, Body: "b",
		}}
	}
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		stagedLists: []int64{7, 8},
		rows:        []db.StagedMailingListRow{row(1)},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainOnce(context.Background(), 1); err != nil {
		t.Fatalf("first drain: %v", err)
	}
	store.rows = []db.StagedMailingListRow{row(2)}
	if _, err := p.DrainOnce(context.Background(), 1); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	distinct := []int64{}
	for _, id := range store.drainedLists { // DrainList polls each list to empty, so ids repeat
		if len(distinct) == 0 || distinct[len(distinct)-1] != id {
			distinct = append(distinct, id)
		}
	}
	if len(distinct) != 2 || distinct[0] != 7 || distinct[1] != 8 {
		t.Fatalf("drained lists = %v (distinct %v) — consecutive window-1 drains must rotate 7 then 8, not revisit the head", store.drainedLists, distinct)
	}
}
