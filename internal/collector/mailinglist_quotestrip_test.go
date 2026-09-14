// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// mailinglist_quotestrip_test.go — Part B ingest wiring: the processor
// must STRIP the body through the shared pattern library before the
// body write, so every newly drained message carries msg_text_clean +
// the current rule version from day one (history is the
// strip-quoted-history CLI's job).
package collector

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

func TestProcessorStripsQuotedHistoryAtIngest(t *testing.T) {
	raw := "Own text.\n> quoted history line\nOn Mon, Jan 5, 2026 at 3:14 PM Alice <a@x.org> wrote:"
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-strip@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "a@x.org",
				MsgClass: mailinglist.ClassDiscuss, Body: raw,
			}},
		},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.cleanBodies) != 1 {
		t.Fatalf("expected 1 body write, got %d", len(store.cleanBodies))
	}
	wantClean, wantRule := mailinglist.StripQuotedHistory(raw)
	if store.cleanBodies[0] != wantClean {
		t.Errorf("clean body = %q, want %q — the processor must route the body through StripQuotedHistory", store.cleanBodies[0], wantClean)
	}
	if wantClean != "Own text." {
		t.Fatalf("fixture sanity: strip of the raw body = %q", wantClean)
	}
	if store.cleanRules[0] != wantRule || wantRule != mailinglist.QuoteStripRuleVersion {
		t.Errorf("clean rule = %q, want %q", store.cleanRules[0], wantRule)
	}
}

// TestProcessorAppliesTrackerActionOnKeyedPath — C1 drain wiring: a
// [Resolved] notification that LINKs its issue must reach
// ApplyTrackerAction with the parsed action (the DO UPDATE can never
// see it — the LINK path returns before the INSERT).
func TestProcessorAppliesTrackerActionOnKeyedPath(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-resolve@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
				MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-99",
				Subject: "[jira] [Resolved] (ARROW-99) fix the thing", Body: "resolved body",
			}},
		},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.appliedActions) != 1 || !strings.HasSuffix(store.appliedActions[0], ":Resolved") {
		t.Fatalf("applied actions = %v, want one <issueID>:Resolved", store.appliedActions)
	}
}
