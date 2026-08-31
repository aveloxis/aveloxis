// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// mirrorRow builds a github_mirror staged row with the given node ID and
// optional body-URL captures.
func mirrorRow(mlsID int64, messageID, nodeID, repo string, number int) db.StagedMailingListRow {
	return db.StagedMailingListRow{MlsID: mlsID, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
		MessageID: messageID, ListAddress: "dev@arrow.apache.org", SenderEmail: "x@example.org",
		MsgClass: mailinglist.ClassGitHubMirror, IsMirror: true,
		SignaledRepoURL: "https://github.com/apache/arrow-rs",
		MirrorNodeID:    nodeID,
		MirrorRepo:      repo, MirrorNumber: number, MirrorKind: "pull", MirrorOwner: "apache",
	}}
}

func newProcessor(store *fakeProcStore) *MailingListProcessor {
	return NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// TestMirrorLinksViaNodeID is the regression test for the 2026-08-29 finding:
// every one of 396,809 production github_mirror rows had linked_issue_id and
// linked_pull_request_id NULL, because the body-URL rule that supplies
// MirrorRepo/MirrorNumber never fires on Apache mail. A mirror row carrying
// ONLY a node ID (the shape the worker now always produces) must link.
func TestMirrorLinksViaNodeID(t *testing.T) {
	pr := int64(555)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		nodePRID: &pr,
		// No MirrorRepo / MirrorNumber — the production shape.
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "PR_kwDOBCyuKc8AAAABBMldbw@gitbox.apache.org", "PR_kwDOBCyuKc8AAAABBMldbw", "", 0),
		},
	}
	if _, err := newProcessor(store).DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.emails) != 1 {
		t.Fatalf("expected 1 email_message row, got %d", len(store.emails))
	}
	got := store.emails[0]
	if got.LinkedPullRequestID == nil || *got.LinkedPullRequestID != pr {
		t.Errorf("LinkedPullRequestID = %v, want %d — the node-ID path did not link", got.LinkedPullRequestID, pr)
	}
	if len(store.nodeIDsAsked) != 1 || store.nodeIDsAsked[0] != "PR_kwDOBCyuKc8AAAABBMldbw" {
		t.Errorf("node IDs asked = %v, want exactly the extracted node ID", store.nodeIDsAsked)
	}
}

// TestMirrorNodeIDPreferredOverBodyURL pins the ORDER. Both paths are
// available here and they disagree; the node ID is the exact platform
// identifier and must win. Without the ordering the body-URL path's guessed
// owner ("apache") could key the row onto the wrong entity.
func TestMirrorNodeIDPreferredOverBodyURL(t *testing.T) {
	nodePR, bodyPR := int64(111), int64(999)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		nodePRID:   &nodePR,
		mirrorPRID: &bodyPR,
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "PR_abc@gitbox.apache.org", "PR_abc", "arrow-rs", 7),
		},
	}
	if _, err := newProcessor(store).DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	got := store.emails[0]
	if got.LinkedPullRequestID == nil || *got.LinkedPullRequestID != nodePR {
		t.Errorf("LinkedPullRequestID = %v, want %d (node ID must win over body URL)", got.LinkedPullRequestID, nodePR)
	}
}

// TestMirrorFallsBackToBodyURLWithoutNodeID pins that the pre-existing
// body-URL path still works for systems whose mail carries a canonical URL
// but whose Message-ID is not a node ID — removing that path would be a
// silent regression for non-GitBox relays.
func TestMirrorFallsBackToBodyURLWithoutNodeID(t *testing.T) {
	bodyPR := int64(999)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		mirrorPRID: &bodyPR,
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "CADkQ1tYq@mail.gmail.com", "", "arrow-rs", 7),
		},
	}
	if _, err := newProcessor(store).DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	got := store.emails[0]
	if got.LinkedPullRequestID == nil || *got.LinkedPullRequestID != bodyPR {
		t.Errorf("LinkedPullRequestID = %v, want %d (body-URL fallback)", got.LinkedPullRequestID, bodyPR)
	}
	if len(store.nodeIDsAsked) != 0 {
		t.Errorf("node resolver called with %v; an empty node ID must not be queried", store.nodeIDsAsked)
	}
}

// TestMirrorNodeIDResolveErrorKeepsProvenance pins SR-5 handling at the call
// site: a resolver FAILURE must not be read as "no link". The message still
// lands (provenance is not lost) with the link left NULL so a later heal can
// fill it, and the batch keeps draining.
func TestMirrorNodeIDResolveErrorKeepsProvenance(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		nodeErr: errors.New("connection reset"),
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "PR_abc@gitbox.apache.org", "PR_abc", "", 0),
		},
	}
	n, err := newProcessor(store).DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v — a resolver error must not fail the drain", err)
	}
	if n != 1 || len(store.emails) != 1 {
		t.Fatalf("processed=%d emails=%d, want 1/1 (provenance must survive)", n, len(store.emails))
	}
	if got := store.emails[0]; got.LinkedPullRequestID != nil || got.LinkedIssueID != nil {
		t.Errorf("links = (%v,%v), want both NULL on resolver error",
			got.LinkedIssueID, got.LinkedPullRequestID)
	}
}

// TestMirrorNodeIDFailuresLogOncePerBatch pins the v0.27.91 flood rule for the
// new resolver. Apache dev@ lists are ~100% mirror traffic and DrainList works
// in 500-row batches, so a per-message WARN would emit one line per row across
// ~397K rows during a systemic resolver failure (DB restart, statement
// timeout, pool exhaustion). One aggregate line per batch instead.
//
// This drives FOUR batches with failures confined to the FIRST, which is what
// makes the assertion meaningful: per-batch counters and per-DRAIN counters
// are indistinguishable in a single-batch drain, and a drain-scoped counter
// would re-log the same stale count on every later batch.
func TestMirrorNodeIDFailuresLogOncePerBatch(t *testing.T) {
	const chunk, batches = 5, 4
	var rows []db.StagedMailingListRow
	for i := int64(1); i <= chunk*batches; i++ {
		rows = append(rows, mirrorRow(i, "PR_abc@gitbox.apache.org", "PR_abc", "", 0))
	}
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		chunk: chunk,
		rows:  rows,
		// Fail only while the first batch is in flight.
		nodeErrUntil: chunk,
	}
	var buf bytes.Buffer
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true,
		slog.New(slog.NewTextHandler(&buf, nil)))

	n, err := p.DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if want := chunk * batches; n != want || len(store.emails) != want {
		t.Fatalf("processed=%d emails=%d, want %d/%d — provenance must survive", n, len(store.emails), want, want)
	}
	if got := strings.Count(buf.String(), "mirror link resolve failed"); got != 1 {
		t.Errorf("logged the mirror-resolve failure %d times across %d batches, want exactly 1 "+
			"(per-message logging is the v0.27.91 flood class; a DRAIN-scoped counter re-logs "+
			"a stale count on every later batch)", got, batches)
	}
	// The count must describe THIS batch, not the whole drain.
	if !strings.Contains(buf.String(), "failures=5") {
		t.Errorf("aggregate line must carry the failing batch's own count (failures=5); got: %s", buf.String())
	}
}

// TestMirrorNodeIDErrorDoesNotFallBackToGuessedOwner pins that a resolver
// ERROR does not degrade to the body-URL path, which guesses an owner
// (defaulting to "apache"). Falling back from the exact key to a guess exactly
// when the exact lookup is failing inverts the release's own ordering. A clean
// MISS still falls through — that is covered by
// TestMirrorFallsBackToBodyURLWithoutNodeID.
func TestMirrorNodeIDErrorDoesNotFallBackToGuessedOwner(t *testing.T) {
	bodyPR := int64(999)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		nodeErr:    errors.New("connection reset"),
		mirrorPRID: &bodyPR, // body-URL path would resolve if consulted
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "PR_abc@gitbox.apache.org", "PR_abc", "arrow-rs", 7),
		},
	}
	if _, err := newProcessor(store).DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if got := store.emails[0]; got.LinkedPullRequestID != nil {
		t.Errorf("LinkedPullRequestID = %d — a node-ID resolver ERROR must not degrade to the "+
			"owner-guessing body-URL path", *got.LinkedPullRequestID)
	}
}

// TestMirrorNodeIDCancellationSkipsAggregateLog pins the shutdown contract for
// the mirror resolver behaviorally.
//
// Moving the WARN out of processRow (to fix the per-message flood) took the
// site out of the scripts/ shutdown-classification ratchet's view, so the
// `errors.Is(nodeErr, context.Canceled)` early return is no longer pinned by
// that analyzer. This pins it directly: on a canceled context the drain must
// surface the cancellation and log NOTHING — a `stop serve` is not a failure,
// and a flood of "resolve failed" lines on every shutdown is the exact noise
// the v0.27.91 class covers.
func TestMirrorNodeIDCancellationSkipsAggregateLog(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		nodeErr: context.Canceled,
		rows: []db.StagedMailingListRow{
			mirrorRow(1, "PR_abc@gitbox.apache.org", "PR_abc", "", 0),
			mirrorRow(2, "PR_def@gitbox.apache.org", "PR_def", "", 0),
		},
	}
	var buf bytes.Buffer
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true,
		slog.New(slog.NewTextHandler(&buf, nil)))

	_, err := p.DrainList(context.Background(), 7)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("DrainList err = %v, want context.Canceled — a cancelled resolve must "+
			"surface as shutdown, not be swallowed as a per-row failure", err)
	}
	if strings.Contains(buf.String(), "mirror link resolve failed") {
		t.Errorf("logged a resolve failure on shutdown; a stop serve is not a failure. got: %s", buf.String())
	}
	if len(store.emails) != 0 {
		t.Errorf("wrote %d rows on a cancelled drain, want 0 (nothing marked, rows re-drain next start)", len(store.emails))
	}
}
