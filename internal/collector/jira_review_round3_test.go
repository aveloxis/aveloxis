// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_review_round3_test.go — Copilot round 2 on PR #193 (the second
// review round of the v0.29.0 Jira/mailing-list code): truncated
// inline comment blocks are completed by the WORKER before staging
// (#2), the assignee identity is BANKED like the reporter's
// (suppressed #1), and a projection-side write failure DEFERS the
// mailing-list row for replay instead of being swallowed while the
// row is marked processed (#3).
package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform/jira"
)

// TestJiraWorkerCompletesTruncatedCommentBlocks — Jira caps the inline
// comment block independently of the search page size; an envelope
// staged with a truncated block loses the tail FOREVER (the envelope
// is immutable and the incremental JQL never re-serves an unchanged
// issue). The worker must page the dedicated comment endpoint and
// stage the COMPLETE block.
func TestJiraWorkerCompletesTruncatedCommentBlocks(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	commentEndpointHits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/issue/AVCT-1/comment") {
			commentEndpointHits++
			start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
			all := []string{
				`{"id":"901","author":{"name":"u1","displayName":"U One"},"body":"c1","created":"2026-05-01T09:00:00.000+0000"}`,
				`{"id":"902","author":{"name":"u2","displayName":"U Two"},"body":"c2","created":"2026-05-01T09:01:00.000+0000"}`,
				`{"id":"903","author":{"name":"u3","displayName":"U Three"},"body":"c3","created":"2026-05-01T09:02:00.000+0000"}`,
			}
			if start > len(all) {
				start = len(all)
			}
			page := all[start:]
			_, _ = io.WriteString(w, `{"startAt":`+strconv.Itoa(start)+`,"maxResults":100,"total":3,"comments":[`+strings.Join(page, ",")+`]}`)
			return
		}
		// Search page: ONE issue whose inline block is TRUNCATED
		// (total 3, one embedded comment). Honors the round-9 walk's
		// jql pagination: a keyset clause or a window past the issue's
		// minute serves empty (the tie drain re-lists the minute once,
		// then its keyset finds it drained).
		jql := r.URL.Query().Get("jql")
		if strings.Contains(jql, "issuekey > '") || strings.Contains(jql, "updated >= '2026-05-01 10:01'") {
			_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":0,"issues":[]}`)
			return
		}
		up := base.Format("2006-01-02T15:04:05.000-0700")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":1,"issues":[
			{"id":"777","key":"AVCT-1","fields":{"summary":"s","status":{"name":"Open"},
			 "updated":"`+up+`","created":"`+up+`",
			 "comment":{"total":3,"comments":[
			   {"id":"901","author":{"name":"u1","displayName":"U One"},"body":"c1","created":"2026-05-01T09:00:00.000+0000"}]}}}]}`)
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVCT", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if !store.completed {
		t.Fatal("scan must complete")
	}
	if commentEndpointHits == 0 {
		t.Fatal("worker never paged the comment endpoint for the truncated block")
	}
	env, ok := store.envelopes["AVCT-1@10:00"]
	if !ok {
		t.Fatalf("AVCT-1 not staged; staged = %v", store.staged)
	}
	var is jira.Issue
	if err := json.Unmarshal(env, &is); err != nil {
		t.Fatal(err)
	}
	if is.Fields.Comment == nil || len(is.Fields.Comment.Comments) != 3 {
		t.Fatalf("staged envelope carries %d comments, want the COMPLETE 3 (truncated tail lost forever otherwise)",
			len(is.Fields.Comment.Comments))
	}
	if is.Fields.Comment.Total != 3 {
		t.Fatalf("staged Total = %d, want 3 (the processor's truncation backstop must read the block as complete)",
			is.Fields.Comment.Total)
	}
}

// TestJiraWorkerFailsScanWhenCommentTailFetchFails — SR-3 shape: an
// issue that cannot stage COMPLETE must fail the scan (backoff +
// retry), never stage the truncated envelope or skip past it.
func TestJiraWorkerFailsScanWhenCommentTailFetchFails(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/comment") {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		jql := r.URL.Query().Get("jql")
		if strings.Contains(jql, "issuekey > '") || strings.Contains(jql, "updated >= '2026-05-01 10:01'") {
			_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":0,"issues":[]}`)
			return
		}
		up := base.Format("2006-01-02T15:04:05.000-0700")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":1,"issues":[
			{"id":"777","key":"AVCT-2","fields":{"summary":"s","status":{"name":"Open"},
			 "updated":"`+up+`","created":"`+up+`",
			 "comment":{"total":2,"comments":[]}}}]}`)
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVCT", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if store.completed {
		t.Fatal("a scan that could not complete a comment block must not stamp complete")
	}
	if store.failures != 1 {
		t.Fatalf("failures = %d, want 1 (RecordJiraFailure backoff)", store.failures)
	}
	if len(store.staged) != 0 {
		t.Fatalf("staged = %v — a truncated envelope must never stage", store.staged)
	}
}

// TestJiraProcessorBanksAssigneeIdentity (suppressed #1): the assignee
// is requested and serialized precisely so its identity gets banked —
// ResolveJiraIdentity records the perishable Server-era username
// whether or not it links, and an unambiguous Jira-only assignee is
// minted like a reporter would be.
func TestJiraProcessorBanksAssigneeIdentity(t *testing.T) {
	repoID := int64(42)
	env := `{"id":"1001","key":"AVAS-1","fields":{
		"summary":"assignee only",
		"assignee":{"name":"assignee-only-person","key":"JIRAUSER9","displayName":"Assignee Only"},
		"status":{"name":"Open"},
		"created":"2026-01-01T09:00:00.000+0000",
		"updated":"2026-02-01T10:00:00.000+0000"}}`
	store := &fakeJiraProcStore{
		batches: [][]db.JiraStagingRow{{
			{JsID: 1, IssueKey: "AVAS-1", RepoID: &repoID, Envelope: []byte(env)},
		}},
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainOnce(context.Background()); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}

	banked := false
	for _, name := range store.resolved {
		if name == "assignee-only-person" {
			banked = true
		}
	}
	if !banked {
		t.Fatalf("assignee identity never reached ResolveJiraIdentity (resolved = %v) — the perishable username was not banked", store.resolved)
	}
	minted := false
	for _, name := range store.minted {
		if name == "assignee-only-person" {
			minted = true
		}
	}
	if !minted {
		t.Fatalf("unambiguous Jira-only assignee must mint a contributor row like a reporter would (minted = %v)", store.minted)
	}
	if len(store.processed) == 0 {
		t.Fatal("the envelope must still process")
	}
}

// mlRetryRow builds the standard keyed issue_event row the deferral
// tests drive.
func mlRetryRow(mlsID int64) db.StagedMailingListRow {
	rg := int64(3)
	return db.StagedMailingListRow{MlsID: mlsID, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
		MessageID: "m-retry@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
		Subject:  "[jira] [Resolved] (ARROW-77) fix it",
		MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-77", Body: "notification body",
	}}
}

// TestMailingListTrackerActionFailureDefersRow (#3): a transient
// ApplyTrackerAction failure must leave the staging row UNPROCESSED so
// the next drain replays it — the swallowed-WARN shape permanently
// lost a Resolved/Reopened transition (the ledgered historical
// backfill only repairs rows that existed when it ran).
func TestMailingListTrackerActionFailureDefersRow(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:             []db.StagedMailingListRow{mlRetryRow(1)},
		applyActionFails: 1,
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.processed) != 0 {
		t.Fatalf("processed = %v — a row whose tracker action failed must stay UNPROCESSED for replay", store.processed)
	}
	if len(store.appliedActions) != 0 {
		t.Fatalf("appliedActions = %v, want none on the failed drain", store.appliedActions)
	}

	// The row replays on the next drain (the real store re-selects
	// unprocessed rows; the fake hands them back explicitly) and every
	// write converges idempotently — the action lands, the row marks.
	store.rows = []db.StagedMailingListRow{mlRetryRow(1)}
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList replay: %v", err)
	}
	if len(store.processed) != 1 {
		t.Fatalf("processed = %v after replay, want the row marked", store.processed)
	}
	if len(store.appliedActions) != 1 || !strings.HasSuffix(store.appliedActions[0], ":Resolved") {
		t.Fatalf("appliedActions = %v, want the Resolved action applied on replay", store.appliedActions)
	}
}

// TestMailingListProjectionFailureDefersRow — the sibling arm: a
// transient LinkOrCreateIssueFromEmail failure defers the row too
// (its projection would otherwise land as mailing_list_only with no
// automatic heal).
func TestMailingListProjectionFailureDefersRow(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows:           []db.StagedMailingListRow{mlRetryRow(1)},
		linkIssueFails: 1,
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))

	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.processed) != 0 {
		t.Fatalf("processed = %v — a row whose projection failed must stay UNPROCESSED for replay", store.processed)
	}

	store.rows = []db.StagedMailingListRow{mlRetryRow(1)}
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList replay: %v", err)
	}
	if len(store.processed) != 1 {
		t.Fatalf("processed = %v after replay, want the row marked", store.processed)
	}
	if len(store.createdIssues) == 0 {
		t.Fatal("replay must project the issue")
	}
	if len(store.appliedActions) != 1 {
		t.Fatalf("appliedActions = %v, want the action applied on the replay's successful projection", store.appliedActions)
	}
}

// TestFetchAllJiraCommentsHonorsServerCappedPages (Copilot round 3 on
// PR #193, #1): Jira Server admins can cap the comment endpoint's
// effective maxResults BELOW the requested size; the response echoes
// the effective value. A short-page check against the REQUESTED size
// reads the first server-capped page as final and permanently drops
// the tail.
func TestFetchAllJiraCommentsHonorsServerCappedPages(t *testing.T) {
	const serverCap = 2
	total := 5
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		w.Header().Set("Content-Type", "application/json")
		var rows []string
		for i := start; i < total && i < start+serverCap; i++ {
			rows = append(rows, `{"id":"`+strconv.Itoa(9000+i)+`","author":{"name":"u"},"body":"c`+strconv.Itoa(i)+`","created":"2026-05-01T09:00:00.000+0000"}`)
		}
		_, _ = io.WriteString(w, `{"startAt":`+strconv.Itoa(start)+`,"maxResults":`+strconv.Itoa(serverCap)+`,"total":`+strconv.Itoa(total)+`,"comments":[`+strings.Join(rows, ",")+`]}`)
	}))
	defer srv.Close()

	client := jira.New(srv.URL, "")
	all, err := fetchAllJiraComments(context.Background(), client, "AVCP-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != total {
		t.Fatalf("fetched %d comments, want all %d — a server-capped page must not read as the final page", len(all), total)
	}
}

// TestJiraShutdownReleasesTheClaim (Copilot round 4 on PR #193): a
// scan cut down by shutdown must release its claim on a bounded
// background context — every Canceled exit used to leave
// jps_locked_at held, stranding the project for the claim query's
// 2-hour stale window on every restart (the pass-37 mailing-list
// ReleaseListLock class). No failure recorded, no completion stamped:
// the release is a rollback, not an outcome.
func TestJiraShutdownReleasesTheClaim(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("a canceled scan must not reach the server")
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repoID := int64(42)
	lockedAt := time.Date(2026, 5, 1, 11, 30, 0, 0, time.UTC)
	store := &fakeJiraStore{
		job:           &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVRL", BaseURL: srv.URL, RepoID: &repoID, LockedAt: lockedAt},
		cancelOnClaim: cancel, // shutdown lands right after the claim
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(ctx)

	if len(store.released) != 1 || store.released[0] != "7@11:30" {
		t.Fatalf("released = %v, want exactly one ownership-stamped release (7@11:30)", store.released)
	}
	if store.failures != 0 {
		t.Fatalf("failures = %d — shutdown is a rollback, never a strike", store.failures)
	}
	if store.completed {
		t.Fatal("a canceled scan must not stamp complete")
	}
}

// TestJiraResumeFromCheckpointCompletes — the SR-19 driver for "the
// per-page checkpoint is the resume state" (the shutdown claim
// release's contract, Copilot round 4 on PR #193): a scan cut down
// mid-corpus leaves jps_last_updated at its last checkpointed page;
// the released claim's next run starts its window THERE and completes
// the tail (the boundary minute re-lists as a natural-key no-op).
// Driven as the resumed run: LastUpdated = the mid-corpus checkpoint
// a killed scan would have left.
func TestJiraResumeFromCheckpointCompletes(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	all := []string{"AVRS2-1", "AVRS2-2", "AVRS2-3"}
	ups := []time.Time{base, base.Add(time.Minute), base.Add(time.Hour)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jiraJQLServe(r.URL.Query().Get("jql"), all, ups, 2))
	}))
	defer srv.Close()

	repoID := int64(42)
	checkpoint := ups[1] // the killed scan checkpointed through B@10:01
	store := &fakeJiraStore{job: &db.JiraProjectJob{
		JpsID: 7, ProjectKey: "AVRS2", BaseURL: srv.URL, RepoID: &repoID,
		LastUpdated: &checkpoint,
	}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if !store.completed {
		t.Fatal("the resumed scan must run to done")
	}
	// The resume window is `updated >= checkpoint`: B re-lists (no-op
	// under the natural key) and the tail C is collected. A must NOT
	// re-list — the checkpoint's whole point is skipping drained work.
	sawA, sawC := false, false
	for _, s := range store.staged {
		if strings.HasPrefix(s, "AVRS2-1@") {
			sawA = true
		}
		if strings.HasPrefix(s, "AVRS2-3@") {
			sawC = true
		}
	}
	if sawA {
		t.Fatalf("staged = %v — the resume window re-walked BEFORE the checkpoint", store.staged)
	}
	if !sawC {
		t.Fatalf("staged = %v — the resumed scan never completed the tail", store.staged)
	}
}

// TestJiraCompletionFailureRecordsFailureNotSuccess (Copilot round 5
// on PR #193): a NON-cancel CompleteJiraScan failure used to warn and
// fall through to the "project synced" log with jps_locked_at still
// held — false success plus a 2h claim strand. It must instead record
// a failure (which clears the lock and paces the retry) and stamp
// nothing as synced.
func TestJiraCompletionFailureRecordsFailureNotSuccess(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		jql := r.URL.Query().Get("jql")
		if strings.Contains(jql, "issuekey > '") || strings.Contains(jql, "updated >= '2026-05-01 10:01'") {
			_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":0,"issues":[]}`)
			return
		}
		up := base.Format("2006-01-02T15:04:05.000-0700")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":1,"issues":[
			{"id":"801","key":"AVCF-1","fields":{"summary":"s","status":{"name":"Open"},
			 "updated":"`+up+`","created":"`+up+`"}}]}`)
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{
		job:         &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVCF", BaseURL: srv.URL, RepoID: &repoID},
		completeErr: fmt.Errorf("connection reset by peer"),
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if store.completed {
		t.Fatal("a failed completion write must not read as synced")
	}
	if store.failures != 1 {
		t.Fatalf("failures = %d, want 1 — the failure record clears the lock and paces the retry", store.failures)
	}
	if len(store.released) != 0 {
		t.Fatalf("released = %v — the failure record already cleared the lock; release is the fallback only", store.released)
	}
}

// TestCommentedNotificationTriggersReverseLink (Copilot round 6 on
// PR #193, suppressed #2): when the mailing-list processor projects a
// [Commented] notification, it must attempt the reverse comment link
// (the native twin may already be collected — nothing else ever
// revisits the pair).
func TestCommentedNotificationTriggersReverseLink(t *testing.T) {
	rg := int64(3)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{{MlsID: 1, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
			MessageID: "m-cmt@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
			Subject:  "[jira] [Commented] (ARROW-88) note",
			MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-88", Body: "n",
		}}},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.reverseLinks) != 1 {
		t.Fatalf("reverseLinks = %v, want exactly one attempt for the [Commented] projection", store.reverseLinks)
	}
	// A non-Commented action must NOT probe.
	store2 := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{mlRetryRow(1)}, // [Resolved]
	}
	p2 := NewMailingListProcessor(store2, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p2.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store2.reverseLinks) != 0 {
		t.Fatalf("reverseLinks = %v for a [Resolved] notification, want none", store2.reverseLinks)
	}
}
