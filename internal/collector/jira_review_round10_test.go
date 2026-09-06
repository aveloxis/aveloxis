// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot round 10 on PR #193: (2) a checkpoint write failure must
// fail the scan instead of letting CompleteJiraScan stamp success over
// a stale jps_last_updated; (3) drains that create issue rows outside
// collection jobs must refresh the queue's cached activity so the home
// ranking sees them.

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

func round10JiraServer(t *testing.T) *httptest.Server {
	t.Helper()
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		jql := r.URL.Query().Get("jql")
		if strings.Contains(jql, "issuekey > '") || strings.Contains(jql, "updated >= '2026-05-01 10:01'") {
			_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":0,"issues":[]}`)
			return
		}
		up := base.Format("2006-01-02T15:04:05.000-0700")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":50,"total":1,"issues":[
			{"id":"901","key":"AVRX-1","fields":{"summary":"s","status":{"name":"Open"},
			 "updated":"`+up+`","created":"`+up+`"}}]}`)
	}))
}

// A NON-cancel CheckpointJiraProject failure must fail the scan: the
// pre-fix warn-and-continue let the walk finish and CompleteJiraScan
// stamp jps_last_synced_at over a STALE jps_last_updated, so a
// transient checkpoint failure silently converted the next cadence
// into a re-walk of everything past the last stamp (the whole history
// on a first sync). Failing records backoff; the checkpointed prefix
// is the resume state.
func TestJiraCheckpointFailureFailsTheScan(t *testing.T) {
	srv := round10JiraServer(t)
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{
		job:        &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVRX", BaseURL: srv.URL, RepoID: &repoID},
		checkptErr: fmt.Errorf("connection reset by peer"),
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if store.completed {
		t.Fatal("a scan whose checkpoint write failed must NOT stamp as synced — the stale jps_last_updated would replay the whole window next cadence")
	}
	if store.failures != 1 {
		t.Fatalf("failures = %d, want 1 (backoff pacing; the checkpointed prefix is the resume state)", store.failures)
	}
	if len(store.released) != 0 {
		t.Fatalf("released = %v — the failure record already cleared the lock", store.released)
	}
}

// A CANCELED checkpoint write is a shutdown, not a failure: the %w
// wrap must keep errors.Is(err, context.Canceled) intact so the
// worker's shutdown arm releases the claim and records nothing.
func TestJiraCheckpointCancellationReleasesClaim(t *testing.T) {
	srv := round10JiraServer(t)
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{
		job:        &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVRX", BaseURL: srv.URL, RepoID: &repoID},
		checkptErr: context.Canceled,
	}
	w := NewJiraWorker(store, 24*time.Hour, "", 50, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if store.completed || store.failures != 0 {
		t.Fatalf("completed=%v failures=%d — a canceled checkpoint is a shutdown, not an outcome", store.completed, store.failures)
	}
	if len(store.released) != 1 {
		t.Fatalf("released = %v, want exactly the shutdown claim release", store.released)
	}
}

// The Jira drain refreshes the queue's cached activity for every repo
// whose envelopes landed — and a refresh failure never fails the
// drain that already committed its rows.
func TestJiraDrainRefreshesActivityCache(t *testing.T) {
	repoID := int64(42)
	store := &fakeJiraProcStore{
		batches: [][]db.JiraStagingRow{{
			{JsID: 1, IssueKey: "AVJP-1", RepoID: &repoID, Envelope: []byte(jiraEnvelope)},
			{JsID: 2, IssueKey: "AVJP-2", RepoID: nil, Envelope: []byte(`{"key":"AVJP-2","fields":{"summary":"x","updated":"2026-01-01T00:00:00.000+0000"}}`)},
		}},
		identities: map[string][3]any{"alice-gh": {"cntrb-alice", "login", false}},
		refreshErr: errors.New("transient"),
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainOnce(context.Background())
	if err != nil {
		t.Fatalf("DrainOnce: %v (a failed activity refresh must never fail the drain)", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	if len(store.refreshedRepos) != 1 || store.refreshedRepos[0] != repoID {
		t.Fatalf("refreshedRepos = %v, want exactly [%d] — one refresh per drained repo, none for the nil-repo row", store.refreshedRepos, repoID)
	}
}

// The mailing-list drain refreshes the list's repo once per drained
// DrainList — and skips the refresh entirely when nothing drained.
func TestMailingListDrainRefreshesActivityCache(t *testing.T) {
	rg := int64(3)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{{MlsID: 1, RepoGroupID: &rg, Message: model.MailingListStagedMessage{
			MessageID: "m-r10@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
			Subject:  "[jira] [Created] (ARROW-91) note",
			MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-91", Body: "n",
		}}},
		refreshErr: errors.New("transient"),
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v (a failed activity refresh must never fail the drain)", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	if len(store.refreshedRepos) != 1 || store.refreshedRepos[0] != 42 {
		t.Fatalf("refreshedRepos = %v, want exactly [42]", store.refreshedRepos)
	}

	// Empty drain: no refresh (the cache only moves when rows landed).
	store2 := &fakeProcStore{primaryRepoID: 42, primaryRepoOK: true}
	p2 := NewMailingListProcessor(store2, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p2.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("empty DrainList: %v", err)
	}
	if len(store2.refreshedRepos) != 0 {
		t.Fatalf("refreshedRepos = %v on an empty drain, want none", store2.refreshedRepos)
	}
}
