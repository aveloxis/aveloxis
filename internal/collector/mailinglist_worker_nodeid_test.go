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
)

// TestWorkerStagesMirrorNodeID pins that the worker recovers the GitHub node
// ID from the GitBox Message-ID for github_mirror messages, INDEPENDENTLY of
// the body-URL captures.
//
// The production shape (2026-08-29) is the second message here: subject-rule
// classification, no canonical URL in the body. Before v0.28.20 that staged
// no link key at all, which is why linked_* was NULL on all 396,809 mirror
// rows. A non-mirror message must stage no node ID even when its Message-ID
// happens to look like one.
func TestWorkerStagesMirrorNodeID(t *testing.T) {
	systems, err := mailinglist.LoadSystems()
	if err != nil {
		t.Fatalf("LoadSystems: %v", err)
	}
	apache := systems["apache_ponymail"]

	month := "2026-03"
	store := &fakeStageStore{}
	backend := &fakeBackend{msgs: map[string][]mailinglist.ArchiveMessage{
		month: {
			// Mirror WITH a body URL (the documented path) — node ID also staged.
			{MessageID: "PR_kwDOAAA111@gitbox.apache.org", ListAddress: "dev@arrow.apache.org",
				Subject: "[PR] fix [arrow-rs]", Sender: "x (via GitHub)", SenderEmail: "x@example.org",
				Body: "URL: https://github.com/apache/arrow-rs/pull/1\n"},
			// Mirror WITHOUT a body URL — the real production shape, reply form.
			{MessageID: "PR_kwDOAAA222-f9312bc0-9406-4f3d-95fd-e0ae8915b4e9@gitbox.apache.org",
				ListAddress: "dev@arrow.apache.org", Subject: "Re: [PR] fix again [arrow-rs]",
				Sender: "y <y@example.org>", SenderEmail: "y@example.org", Body: "looks good to me"},
			// Non-mirror: must NOT carry a mirror node ID.
			{MessageID: "PR_kwDOAAA333@gitbox.apache.org", ListAddress: "dev@arrow.apache.org",
				Subject: "[DISCUSS] design", Sender: "Alice <alice@example.org>",
				SenderEmail: "alice@example.org", Body: "let's discuss"},
		},
	}}

	w := NewMailingListWorker(store, apache, backend,
		mailinglist.NewPacer(time.Nanosecond, time.Millisecond),
		mailinglist.NewBreaker(10, time.Hour),
		90*24*time.Hour, 1, 1, "boot", slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.now = func() time.Time { return time.Date(2026, 3, 15, 0, 0, 0, 0, time.UTC) }

	if err := w.ProcessList(context.Background(), &db.ListJob{
		RglsID: 7, RepoGroupID: 3, ListAddress: "dev@arrow.apache.org", System: "apache_ponymail",
	}); err != nil {
		t.Fatalf("ProcessList: %v", err)
	}

	got := map[string]string{}
	for _, m := range store.staged {
		got[m.MessageID] = m.MirrorNodeID
	}
	if len(got) != 3 {
		t.Fatalf("staged %d messages, want 3", len(got))
	}
	if v := got["PR_kwDOAAA111@gitbox.apache.org"]; v != "PR_kwDOAAA111" {
		t.Errorf("body-URL mirror node id = %q, want %q", v, "PR_kwDOAAA111")
	}
	// The one that matters: subject-classified, no body URL.
	const replyID = "PR_kwDOAAA222-f9312bc0-9406-4f3d-95fd-e0ae8915b4e9@gitbox.apache.org"
	if v := got[replyID]; v != "PR_kwDOAAA222" {
		t.Errorf("subject-rule mirror node id = %q, want %q — this is the production shape that regressed", v, "PR_kwDOAAA222")
	}
	if v := got["PR_kwDOAAA333@gitbox.apache.org"]; v != "" {
		t.Errorf("non-mirror staged node id %q, want empty", v)
	}
}
