// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// fakeMLStore records the routing decisions.
type fakeMLStore struct {
	emails     []*model.EmailMessage
	bodies     int
	refs       int
	checkpts   []string
	completed  bool
	resolvable map[string]string // email → cntrb_id
	nextMsgID  int64
}

func (f *fakeMLStore) ClaimNextList(context.Context, string, time.Duration, int, string) (*db.ListJob, error) {
	return nil, nil
}
func (f *fakeMLStore) CheckpointListMonth(_ context.Context, _ int64, m string) error {
	f.checkpts = append(f.checkpts, m)
	return nil
}
func (f *fakeMLStore) CompleteListScan(context.Context, int64, bool) error {
	f.completed = true
	return nil
}
func (f *fakeMLStore) RecordListFailure(context.Context, int64) error { return nil }
func (f *fakeMLStore) GetPrimaryRepoForGroup(context.Context, int64) (int64, bool, error) {
	return 42, true, nil
}
func (f *fakeMLStore) UpsertEmailMessage(_ context.Context, em *model.EmailMessage) (int64, error) {
	f.emails = append(f.emails, em)
	return int64(len(f.emails)), nil
}
func (f *fakeMLStore) UpsertMailingListMessageBody(context.Context, int64, string, string, string, string, time.Time, *string) (int64, error) {
	f.bodies++
	f.nextMsgID++
	return f.nextMsgID, nil
}
func (f *fakeMLStore) InsertEmailMessageRef(context.Context, int64, int64, *int64) error {
	f.refs++
	return nil
}
func (f *fakeMLStore) ResolveContributorIDByEmail(_ context.Context, email string) (string, bool, error) {
	if id, ok := f.resolvable[email]; ok {
		return id, true, nil
	}
	return "", false, nil
}

// fakeBackend returns canned messages for one month, empty thereafter.
type fakeBackend struct {
	msgs map[string][]mailinglist.ArchiveMessage
}

func (b *fakeBackend) Name() string { return "apache_ponymail" }
func (b *fakeBackend) FetchMonth(_ context.Context, _ string, yyyymm string) ([]mailinglist.ArchiveMessage, time.Duration, error) {
	return b.msgs[yyyymm], 0, nil
}

func TestWorkerRoutesMirrorVsBody(t *testing.T) {
	systems, err := mailinglist.LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	apache := systems["apache_ponymail"]

	now := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	month := "2026-03"
	store := &fakeMLStore{resolvable: map[string]string{"alice@example.org": "cntrb-alice"}}
	backend := &fakeBackend{msgs: map[string][]mailinglist.ArchiveMessage{
		month: {
			// github mirror → metadata-only, NO body
			{MessageID: "m-mirror@x", ListAddress: "dev@arrow.apache.org", Subject: "[PR] fix [arrow-rs]", Sender: "x (via GitHub)", SenderEmail: "x@example.org", Body: "URL: https://github.com/apache/arrow-rs/pull/1\n"},
			// discussion → body + ref, sender resolves
			{MessageID: "m-discuss@x", ListAddress: "dev@arrow.apache.org", Subject: "[DISCUSS] design", Sender: "Alice <alice@example.org>", SenderEmail: "alice@example.org", Body: "let's discuss"},
			// jira issue_event → not a mirror → body + ref, external_key captured
			{MessageID: "m-jira@x", ListAddress: "dev@arrow.apache.org", Subject: "[jira] [Created] (ARROW-99) thing", Sender: "Bot (Jira)", SenderEmail: "jira@apache.org", Body: "ticket body"},
		},
	}}

	w := NewMailingListWorker(store, apache, backend,
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond),
		mailinglist.NewBreaker(10, time.Hour),
		90*24*time.Hour, 1 /*backfillMonths*/, 1, "boot", slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.now = func() time.Time { return now }

	if err := w.ProcessList(context.Background(), &db.ListJob{RglsID: 7, RepoGroupID: 3, ListAddress: "dev@arrow.apache.org", System: "apache_ponymail"}); err != nil {
		t.Fatalf("ProcessList: %v", err)
	}

	if len(store.emails) != 3 {
		t.Errorf("expected 3 email_message rows (one per message), got %d", len(store.emails))
	}
	// Only discuss + jira get bodies; the github_mirror is metadata-only.
	if store.bodies != 2 {
		t.Errorf("expected 2 bodies (mirror is metadata-only), got %d", store.bodies)
	}
	if store.refs != 2 {
		t.Errorf("expected 2 email_message_ref rows, got %d", store.refs)
	}
	if !store.completed {
		t.Error("scan should be marked complete")
	}

	// Verify per-message classification + mirror flags landed on the entities.
	byID := map[string]*model.EmailMessage{}
	for _, em := range store.emails {
		byID[em.MessageIDHeader] = em
	}
	if m := byID["m-mirror@x"]; m == nil || !m.IsMirror || m.MsgClass != mailinglist.ClassGitHubMirror {
		t.Errorf("mirror message: %+v", m)
	}
	if m := byID["m-mirror@x"]; m != nil && m.SignaledRepoURL != "https://github.com/apache/arrow-rs" {
		t.Errorf("mirror signaled_repo_url = %q", m.SignaledRepoURL)
	}
	if m := byID["m-jira@x"]; m == nil || m.IsMirror || m.MsgClass != mailinglist.ClassIssueEvent || m.LinkedExternalKey != "ARROW-99" {
		t.Errorf("jira message: %+v", m)
	}
	if m := byID["m-discuss@x"]; m == nil || m.DataSource != "dev@arrow.apache.org" {
		t.Errorf("discuss message data_source = %v", m)
	}
}

func TestWorkerFailsWhenGroupHasNoRepo(t *testing.T) {
	systems, _ := mailinglist.LoadSystems()
	store := &fakeMLStoreNoRepo{}
	w := NewMailingListWorker(store, systems["apache_ponymail"], &fakeBackend{},
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond), mailinglist.NewBreaker(10, time.Hour),
		time.Hour, 1, 1, "boot", slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := w.ProcessList(context.Background(), &db.ListJob{RglsID: 1, RepoGroupID: 9, ListAddress: "dev@x.apache.org"})
	if err == nil {
		t.Error("ProcessList must fail when the repo_group has no repo (messages.repo_id is NOT NULL)")
	}
	if !store.failed {
		t.Error("must RecordListFailure when no repo is available")
	}
}

// fakeMLStoreNoRepo embeds the routing fake but reports no repo in the group.
type fakeMLStoreNoRepo struct {
	fakeMLStore
	failed bool
}

func (f *fakeMLStoreNoRepo) GetPrimaryRepoForGroup(context.Context, int64) (int64, bool, error) {
	return 0, false, nil
}
func (f *fakeMLStoreNoRepo) RecordListFailure(context.Context, int64) error {
	f.failed = true
	return nil
}
