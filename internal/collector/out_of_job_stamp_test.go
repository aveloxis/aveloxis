// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// D7 (0.29.120, summary/63): the collector's runs outside a queued job — the
// mailing-list and Jira drains (refreshRepoActivity) and heal-messages —
// stamp every repository they wrote, so the API's cached answers, durable
// until the repository's state moves, follow them.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

type fakeDrainRefresher struct {
	refreshErr error
	stampErr   error
	refreshed  []int64
	stamped    [][]int64
}

func (f *fakeDrainRefresher) RefreshQueueGatheredCounts(ctx context.Context, repoID int64) error {
	f.refreshed = append(f.refreshed, repoID)
	return f.refreshErr
}

func (f *fakeDrainRefresher) StampReposChangedOutsideJob(ctx context.Context, repoIDs []int64) error {
	f.stamped = append(f.stamped, repoIDs)
	return f.stampErr
}

// The stamp is not best-effort the way the activity refresh is: the drain
// already landed its rows, so a canceled drain (a stop) and a failed refresh
// still stamp (the store makes the stamp itself outlast the cancel), and a
// failed stamp is logged.
func TestDrainRefreshStampsTheRepository(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, c := range map[string]struct {
		ctx        context.Context
		refreshErr error
	}{
		"live":           {context.Background(), nil},
		"canceled":       {canceled, nil},
		"refresh failed": {context.Background(), errors.New("refresh boom")},
	} {
		f := &fakeDrainRefresher{refreshErr: c.refreshErr}
		var logs bytes.Buffer
		refreshRepoActivity(c.ctx, f, slog.New(slog.NewTextHandler(&logs, nil)), 42, "test drain", true)
		if !reflect.DeepEqual(f.stamped, [][]int64{{42}}) {
			t.Errorf("%s: stamped %v, want [[42]]: a drain's rows must move the repository's cache state", name, f.stamped)
		}
	}
	f := &fakeDrainRefresher{stampErr: errors.New("stamp boom")}
	var logs bytes.Buffer
	refreshRepoActivity(context.Background(), f, slog.New(slog.NewTextHandler(&logs, nil)), 7, "test drain", true)
	if !strings.Contains(logs.String(), "stamp boom") || !strings.Contains(logs.String(), "repo_id=7") {
		t.Errorf("a failed stamp must be logged with its repository and error, got %q", logs.String())
	}
	if !reflect.DeepEqual(f.refreshed, []int64{7}) {
		t.Errorf("a failed stamp must not skip the activity refresh, refreshed %v", f.refreshed)
	}
	// Reached but nothing final written: the activity refresh, no stamp.
	f = &fakeDrainRefresher{}
	refreshRepoActivity(context.Background(), f, slog.New(slog.NewTextHandler(&logs, nil)), 8, "test drain", false)
	if len(f.stamped) != 0 || !reflect.DeepEqual(f.refreshed, []int64{8}) {
		t.Errorf("wrote=false: stamped %v (want none), refreshed %v (want [8])", f.stamped, f.refreshed)
	}
}

// heal-messages writes the conversations of every repository its batch names
// (the refetched parents and the stale links it deletes): that set, each
// once, in a stable order.
func TestHealBatchReposIsEveryRepositoryTheBatchNames(t *testing.T) {
	items := []db.MessageHealItem{
		{MsgID: 1, IssueRepoID: 5},
		{MsgID: 2, PRConvRepoID: 9, ReviewRepoID: 5},
		{MsgID: 3, ReviewRepoID: 3},
		{MsgID: 4}, // no parent anywhere: its cleanup touches no conversation
	}
	if got := healBatchRepos(items); !reflect.DeepEqual(got, []int64{3, 5, 9}) {
		t.Errorf("healBatchRepos = %v, want [3 5 9]", got)
	}
	if got := healBatchRepos(nil); len(got) != 0 {
		t.Errorf("an empty batch names no repository, got %v", got)
	}
}

// The wiring: the pass stamps that set after its last write (the refetch and
// the stale-link cleanup) and before the healed mark, whose failure returns
// an error after the rows were written; a dry run writes nothing and returns
// before it.
func TestMessageHealPassStampsAfterItsWrites(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func runMessageHealPass("))
	// Deferred, so every return after the first write reaches it — a stop
	// mid-pass included (round 1 finding 2: the per-repository loop returned
	// on ctx.Err() before a stamp placed after it).
	stamp := strings.Index(body, "defer stampHealBatch(ctx, store, logger, items)")
	if stamp < 0 {
		t.Fatal("runMessageHealPass must defer stampHealBatch(ctx, store, logger, items)")
	}
	dry := strings.Index(body, "if dryRun {")
	if dry < 0 || dry > stamp {
		t.Error("a dry run returns before the deferred stamp (it writes nothing)")
	}
	for _, write := range []string{"sw.Flush(ctx)", "proc.ProcessRepo(", "store.DeleteStaleConversationRefs("} {
		if i := strings.Index(body, write); i < 0 || i < stamp {
			t.Errorf("the stamp must be deferred before the pass's first write (%s)", write)
		}
	}
	fn := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func stampHealBatch("))
	if !strings.Contains(fn, "StampReposChangedOutsideJob(") || !strings.Contains(fn, "healBatchRepos(items)") {
		t.Error("stampHealBatch must stamp healBatchRepos(items)")
	}
}

// Whole-branch review of 0.29.119–0.29.123, A5: a staged row can commit
// part of its writes and then fail (a Jira envelope whose issue landed and
// whose comment did not; a mail row whose issue and message landed and
// whose tracker action did not). The partial write is in the tables the
// API's answers read, and a repository without a queue row (a gone one,
// its data kept) has no next collection to show it — so the first partial
// write of a row stamps its repository. Only the first: the row is retried
// every drain, and a lasting failure must not move the state each time
// (round 1 finding 3). Its final write stamps again.
func TestJiraDrainStampsAPartialWriteOnce(t *testing.T) {
	repoID := int64(42)
	store := &fakeJiraProcStore{
		perProject: map[int64][]db.JiraStagingRow{7: {{JsID: 1, IssueKey: "AVJP-1", RepoID: &repoID, Envelope: []byte(jiraEnvelope)}}},
		identities: map[string][3]any{"alice-gh": {"cntrb-alice", "login", false}},
		commentErr: errors.New("deadlock detected"), // the issue lands, its comments do not
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	drain := func() {
		t.Helper()
		if _, err := p.DrainOnce(context.Background()); err != nil {
			t.Fatalf("DrainOnce: %v", err)
		}
	}
	drain()
	if len(store.issues) == 0 || len(store.stampedRepos) != 1 {
		t.Fatalf("a committed issue whose comments failed: issues %d, stampedRepos %v — want the repository stamped once", len(store.issues), store.stampedRepos)
	}
	drain()
	if len(store.stampedRepos) != 1 {
		t.Errorf("the same row failing again stamped again: %v (a lasting failure must not move the state every drain)", store.stampedRepos)
	}
	store.commentErr = nil
	drain()
	if len(store.stampedRepos) != 2 {
		t.Errorf("the row's final write must stamp: %v", store.stampedRepos)
	}
}

func TestMailingListDrainStampsAPartialWriteOnce(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:             []db.StagedMailingListRow{mlRetryRow(1)}, // [Resolved]: commits its issue, then the action fails
		applyActionFails: 2,
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	drain := func() {
		t.Helper()
		// A deferred row stays staged in the real store; the fake serves
		// each row once, so it is staged again before every drain.
		store.rows = []db.StagedMailingListRow{mlRetryRow(1)}
		if _, err := p.DrainList(context.Background(), 7); err != nil {
			t.Fatalf("DrainList: %v", err)
		}
	}
	drain()
	if len(store.stampedRepos) != 1 || store.stampedRepos[0] != 42 {
		t.Fatalf("a row whose issue and message landed before its deferral: stampedRepos %v, want [42]", store.stampedRepos)
	}
	drain()
	if len(store.stampedRepos) != 1 {
		t.Errorf("the same row deferring again stamped again: %v", store.stampedRepos)
	}
	drain()
	if len(store.stampedRepos) != 2 {
		t.Errorf("the row's final write must stamp: %v (processed %v, tracker-action calls %d, applied %v)", store.stampedRepos, store.processed, store.applyActionCalls, store.appliedActions)
	}
}

// Copilot review of PR #246 (0.29.124), the two Highs on A5: a retry can
// commit MORE than the attempt before it and still fail — a Jira envelope
// fails on its first comment, then writes that comment and fails on the
// second — and a stop can land between a row's first write and the one that
// flagged "committed". The partial-write signal is the row's progress (how
// many writes landed), not a first-time flag: a retry that got further
// stamps, a retry that got no further does not.
func TestJiraDrainStampsEachTimeARetryGetsFurther(t *testing.T) {
	repoID := int64(42)
	store := &fakeJiraProcStore{
		perProject:   map[int64][]db.JiraStagingRow{7: {{JsID: 1, IssueKey: "AVJP-1", RepoID: &repoID, Envelope: []byte(jiraEnvelope)}}},
		identities:   map[string][3]any{"alice-gh": {"cntrb-alice", "login", false}},
		failComments: map[int64]bool{111: true, 112: true}, // the issue lands, no comment does
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	drain := func(want int, why string) {
		t.Helper()
		if _, err := p.DrainOnce(context.Background()); err != nil {
			t.Fatalf("DrainOnce: %v", err)
		}
		if len(store.stampedRepos) != want {
			t.Fatalf("%s: stamped %v, want %d stamps", why, store.stampedRepos, want)
		}
	}
	drain(1, "the issue committed")
	drain(1, "the same failure again: no progress")
	store.failComments = map[int64]bool{112: true} // the first comment lands now
	drain(2, "the retry got further (one comment more)")
	drain(2, "the same failure again")
	store.failComments = nil
	drain(3, "the final write")
}

func TestMailingListDrainStampsAnIssueCreatedBeforeAStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:           []db.StagedMailingListRow{mlRetryRow(1)}, // [Resolved]: creates its issue first
		onIssueCreated: cancel,                                   // the stop lands right after
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(ctx, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("DrainList: %v, want the cancellation", err)
	}
	if len(store.createdIssues) != 1 || len(store.emails) != 0 {
		t.Fatalf("premise: the issue was created and nothing after it: issues %v emails %d", store.createdIssues, len(store.emails))
	}
	if len(store.stampedRepos) != 1 || store.stampedRepos[0] != 42 {
		t.Errorf("an issue committed before the stop must stamp its repository: %v", store.stampedRepos)
	}
}

// L10 round on 0.29.125, F1: a LINK is progress, not a write — a keyed row
// whose issue already exists, stopped before its message is written, wrote
// nothing and stamps nothing.
func TestMailingListDrainDoesNotStampALinkStoppedBeforeAnyWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:          []db.StagedMailingListRow{mlRetryRow(1)},
		issueByKey:    map[string]int64{"ARROW-77": 5}, // the issue exists: a LINK
		onIssueLinked: cancel,                          // the stop lands right after
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(ctx, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("DrainList: %v, want the cancellation", err)
	}
	if len(store.emails) != 0 {
		t.Fatalf("premise: nothing was written after the link: emails %d", len(store.emails))
	}
	if len(store.stampedRepos) != 0 {
		t.Errorf("a LINK followed by a stop wrote nothing and must not stamp: %v", store.stampedRepos)
	}
}
