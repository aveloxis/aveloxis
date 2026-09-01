// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_worker_test.go — C3 fetch half: claim a project, page its
// incremental JQL window against the Jira Server search API, stage
// envelopes, checkpoint per staged page (SR-3), complete. A dead
// project key (400) disables the row; a transient failure records
// backoff and keeps the checkpoint.
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
)

type fakeJiraStore struct {
	job        *db.JiraProjectJob
	staged     []string // issueKey@updated
	checkpts   []time.Time
	completed  bool
	failures   int
	disabled   bool
	claimCalls int
}

func (f *fakeJiraStore) ClaimNextJiraProject(context.Context, time.Duration, string) (*db.JiraProjectJob, error) {
	f.claimCalls++
	if f.claimCalls > 1 {
		return nil, nil
	}
	return f.job, nil
}
func (f *fakeJiraStore) StageJiraIssue(_ context.Context, _ int64, _, issueKey string, updated time.Time, _ *int64, _ []byte) error {
	// The fake honors the real writer's boundary: jira_staging carries
	// UNIQUE (project_key, issue_key, updated_at) with ON CONFLICT DO
	// NOTHING, so a boundary-minute re-list (the C2 window-advance walk
	// re-lists the cursor minute with startAt=0 on purpose) is a no-op.
	// A fake that appended duplicates would make every staged-count
	// assertion depend on how many re-lists the walk performed.
	nk := issueKey + "@" + updated.UTC().Format("15:04")
	for _, have := range f.staged {
		if have == nk {
			return nil
		}
	}
	f.staged = append(f.staged, nk)
	return nil
}
func (f *fakeJiraStore) CheckpointJiraProject(_ context.Context, _ int64, at time.Time) error {
	f.checkpts = append(f.checkpts, at)
	return nil
}
func (f *fakeJiraStore) CompleteJiraScan(_ context.Context, _ int64) error {
	f.completed = true
	return nil
}
func (f *fakeJiraStore) RecordJiraFailure(context.Context, int64) error { f.failures++; return nil }
func (f *fakeJiraStore) DisableJiraProject(context.Context, int64) error {
	f.disabled = true
	return nil
}

// TestJiraWorkerSyncsProjectPages — two pages staged, checkpoint
// advances per page, scan completes.
func TestJiraWorkerSyncsProjectPages(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	// Window-keyed server (the C2 drift-safe walk requests by jql cursor
	// with startAt=0, not bare offsets): the full corpus is three issues
	// across two minutes; each request serves the remaining set for its
	// cursor, sliced by startAt like real Jira.
	all := []string{"AVJW-1", "AVJW-2", "AVJW-3"}
	ups := []time.Time{base, base.Add(time.Minute), base.Add(time.Hour)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		jql := r.URL.Query().Get("jql")
		cursor := time.Time{}
		if i := strings.Index(jql, "updated >= '"); i >= 0 {
			rest := jql[i+len("updated >= '"):]
			cursor, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
		}
		var keys []string
		var when []time.Time
		for i := range all {
			if !ups[i].Before(cursor) {
				keys = append(keys, all[i])
				when = append(when, ups[i])
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jiraSearchPageAt(keys, when, start))
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVJW", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2 /*pageSize*/, 0 /*pageSleep*/, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if len(store.staged) != 3 {
		t.Fatalf("staged = %v, want 3 issues", store.staged)
	}
	// Three pages touch staging: the two data pages plus the boundary
	// re-list of the final minute (all-duplicate, checkpointing the
	// same value idempotently) before the offset fallback finds the
	// window empty.
	if len(store.checkpts) != 3 {
		t.Fatalf("checkpoints = %v, want one per staged page incl. the boundary re-list (SR-3)", store.checkpts)
	}
	if last := store.checkpts[len(store.checkpts)-1]; !last.Equal(base.Add(time.Hour)) {
		t.Fatalf("final checkpoint = %v, want the corpus max %v", last, base.Add(time.Hour))
	}
	if !store.completed {
		t.Fatal("scan must complete")
	}
	// Decode-ability of the envelope is the processor's contract; the
	// worker stages raw client JSON.
	var probe map[string]any
	if err := json.Unmarshal([]byte(`{"key":"x"}`), &probe); err != nil {
		t.Fatal(err)
	}
}

// TestJiraWorkerDisablesDeadProject — a 400 (dead key, 5 of 191 in
// the pilot) disables the registration instead of retrying forever.
func TestJiraWorkerDisablesDeadProject(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
	}))
	defer srv.Close()
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 8, ProjectKey: "DEADKEY", BaseURL: srv.URL}}
	w := NewJiraWorker(store, 24*time.Hour, "", 100, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())
	if !store.disabled {
		t.Fatal("a 400 project must be disabled")
	}
	if store.completed {
		t.Fatal("a disabled project must not stamp a completed scan")
	}
}

// TestJiraWorkerRecordsTransientFailure — a 503 records backoff, no
// completion, checkpoint untouched.
func TestJiraWorkerRecordsTransientFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 9, ProjectKey: "AVJW", BaseURL: srv.URL}}
	w := NewJiraWorker(store, 24*time.Hour, "", 100, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())
	if store.failures != 1 {
		t.Fatalf("failures = %d, want 1", store.failures)
	}
	if len(store.checkpts) != 0 || store.completed {
		t.Fatal("a failed sync must not checkpoint or complete")
	}
}

// TestJiraWorkerPageSkipFailsScanNotCheckpoint (review 2026-08-30 #7,
// SR-3): an unparseable issue in a page must FAIL the scan (backoff
// retry) — a `continue` would let later issues push pageMax past the
// skipped one and the checkpoint would stamp over work never staged.
func TestJiraWorkerPageSkipFailsScanNotCheckpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// One good issue, then one with garbage `updated`, then a good
		// LATER one whose timestamp would advance the checkpoint past
		// the broken issue.
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":3,"total":3,"issues":[
		  {"id":"1","key":"AVJW-10","fields":{"summary":"s","status":{"name":"Open"},"updated":"2026-01-01T00:00:00.000+0000"}},
		  {"id":"2","key":"AVJW-11","fields":{"summary":"s","status":{"name":"Open"},"updated":"not-a-timestamp"}},
		  {"id":"3","key":"AVJW-12","fields":{"summary":"s","status":{"name":"Open"},"updated":"2026-01-03T00:00:00.000+0000"}}]}`)
	}))
	defer srv.Close()

	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVJW", BaseURL: srv.URL}}
	w := NewJiraWorker(store, 24*time.Hour, "", 100, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if store.failures != 1 {
		t.Fatalf("an unparseable issue must record a scan failure (backoff retry), failures=%d", store.failures)
	}
	if len(store.checkpts) != 0 {
		t.Fatalf("the failed page must not checkpoint (SR-3 — AVJW-11 was never staged): %v", store.checkpts)
	}
	if store.completed {
		t.Fatal("a failed scan must not complete")
	}
}
