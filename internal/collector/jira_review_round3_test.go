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
		// (total 3, one embedded comment). Honors startAt like real
		// Jira — the drift-safe walk re-lists the boundary minute at
		// startAt=0 and then offsets past it to find the window empty.
		if start, _ := strconv.Atoi(r.URL.Query().Get("startAt")); start >= 1 {
			_, _ = io.WriteString(w, `{"startAt":1,"maxResults":50,"total":1,"issues":[]}`)
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
		if start, _ := strconv.Atoi(r.URL.Query().Get("startAt")); start >= 1 {
			_, _ = io.WriteString(w, `{"startAt":1,"maxResults":50,"total":1,"issues":[]}`)
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
