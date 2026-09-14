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

// fakeStageStore records the messages the stage-only worker stages. With the
// v0.25.x split (summary/12 §11) the worker no longer touches the hot tables —
// it fetches, classifies, and stages. Resolution + writes are the
// MailingListProcessor's job (see mailinglist_processor_test.go).
type fakeStageStore struct {
	staged    []model.MailingListStagedMessage
	checkpts  []string
	completed bool
}

func (f *fakeStageStore) ClaimNextList(context.Context, string, time.Duration, int, string) (*db.ListJob, error) {
	return nil, nil
}
func (f *fakeStageStore) CheckpointListMonth(_ context.Context, _ int64, m string) error {
	f.checkpts = append(f.checkpts, m)
	return nil
}
func (f *fakeStageStore) CompleteListScan(context.Context, int64, bool) error {
	f.completed = true
	return nil
}
func (f *fakeStageStore) RecordListFailure(context.Context, int64) error { return nil }
func (f *fakeStageStore) ReleaseListLock(context.Context, int64, time.Time) error {
	return nil
}
func (f *fakeStageStore) StageMailingListMessage(_ context.Context, _ int64, _, _ *int64, msg model.MailingListStagedMessage) error {
	f.staged = append(f.staged, msg)
	return nil
}

// fakeBackend returns canned messages for one month, empty thereafter.
type fakeBackend struct {
	msgs map[string][]mailinglist.ArchiveMessage
}

func (b *fakeBackend) Name() string { return "apache_ponymail" }
func (b *fakeBackend) FetchMonth(_ context.Context, _ string, yyyymm string) ([]mailinglist.ArchiveMessage, time.Duration, error) {
	return b.msgs[yyyymm], 0, nil
}
func (b *fakeBackend) EnumerateLists(context.Context, string) ([]mailinglist.ListInfo, error) {
	return nil, nil
}
func (b *fakeBackend) FirstMonth(context.Context, string) (string, error) { return "", nil }

// TestWorkerStagesClassifiedMessages pins the stage-only contract: every
// fetched message is classified (cheap, no DB) and staged with the right
// MsgClass / IsMirror / SignaledRepoURL / ExternalKey / mirror captures. No
// body / ref / sender resolution happens in the worker anymore.
func TestWorkerStagesClassifiedMessages(t *testing.T) {
	systems, err := mailinglist.LoadSystems()
	if err != nil {
		t.Fatal(err)
	}
	apache := systems["apache_ponymail"]

	now := time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC)
	month := "2026-03"
	store := &fakeStageStore{}
	backend := &fakeBackend{msgs: map[string][]mailinglist.ArchiveMessage{
		month: {
			// github mirror → IsMirror, signaled URL, mirror captures
			{MessageID: "m-mirror@x", ListAddress: "dev@arrow.apache.org", Subject: "[PR] fix [arrow-rs]", Sender: "x (via GitHub)", SenderEmail: "x@example.org", Body: "URL: https://github.com/apache/arrow-rs/pull/1\n"},
			// discussion → not a mirror, no external key
			{MessageID: "m-discuss@x", ListAddress: "dev@arrow.apache.org", Subject: "[DISCUSS] design", Sender: "Alice <alice@example.org>", SenderEmail: "alice@example.org", Body: "let's discuss"},
			// jira issue_event → not a mirror, external_key captured
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

	if len(store.staged) != 3 {
		t.Fatalf("expected 3 staged messages, got %d", len(store.staged))
	}
	if !store.completed {
		t.Error("scan should be marked complete")
	}

	byID := map[string]model.MailingListStagedMessage{}
	for _, m := range store.staged {
		byID[m.MessageID] = m
	}

	if m := byID["m-mirror@x"]; !m.IsMirror || m.MsgClass != mailinglist.ClassGitHubMirror {
		t.Errorf("mirror message classified wrong: %+v", m)
	}
	if m := byID["m-mirror@x"]; m.SignaledRepoURL != "https://github.com/apache/arrow-rs" {
		t.Errorf("mirror signaled_repo_url = %q", m.SignaledRepoURL)
	}
	if m := byID["m-mirror@x"]; m.MirrorRepo != "arrow-rs" || m.MirrorNumber != 1 {
		t.Errorf("mirror captures wrong: repo=%q number=%d", m.MirrorRepo, m.MirrorNumber)
	}
	if m := byID["m-jira@x"]; m.IsMirror || m.MsgClass != mailinglist.ClassIssueEvent || m.ExternalKey != "ARROW-99" {
		t.Errorf("jira message classified wrong: %+v", m)
	}
	if m := byID["m-discuss@x"]; m.IsMirror || m.ExternalKey != "" {
		t.Errorf("discuss message classified wrong: %+v", m)
	}
}

// TestNewMailingListWorkerPreservesZeroBackfill pins the v0.25.13 fix: the
// constructor must NOT clamp backfillMonths 0 → 6. A 0 is the explicit
// "full history" signal monthsToScan's default branch depends on; the config
// layer already maps absent → 6 (MailingListBackfillMonthsOrDefault, nil→6).
func TestNewMailingListWorkerPreservesZeroBackfill(t *testing.T) {
	systems, _ := mailinglist.LoadSystems()
	w := NewMailingListWorker(&fakeStageStore{}, systems["apache_ponymail"], &fakeBackend{},
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond), mailinglist.NewBreaker(10, time.Hour),
		time.Hour, 0 /*full history*/, 1, "boot", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if w.backfill != 0 {
		t.Fatalf("constructor clamped backfill 0 → %d; 0 must be preserved as the full-history signal", w.backfill)
	}
}

// TestMonthsToScanFullHistoryWhenBackfillZero is the END-TO-END behavioral
// guard: with backfill=0 and no checkpoint, the worker scans far more than a
// 6-month window (fakeBackend.FirstMonth returns "", so monthsToScan uses its
// 30-year floor — ~360 months — proving it is NOT the bounded window).
func TestMonthsToScanFullHistoryWhenBackfillZero(t *testing.T) {
	systems, _ := mailinglist.LoadSystems()
	w := NewMailingListWorker(&fakeStageStore{}, systems["apache_ponymail"], &fakeBackend{},
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond), mailinglist.NewBreaker(10, time.Hour),
		time.Hour, 0, 1, "boot", slog.New(slog.NewTextHandler(io.Discard, nil)))
	months := w.monthsToScan(context.Background(), &db.ListJob{RglsID: 1, RepoGroupID: 1, ListAddress: "dev@x.apache.org"})
	if len(months) <= 6 {
		t.Fatalf("backfill=0 must scan full history; got %d months — a 6-month window means the constructor clamp is back", len(months))
	}
}
