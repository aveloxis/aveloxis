// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Copilot review round on PR #193 (2026-09-01), collector half:
//
//	C1 — identity-store FAILURES were swallowed into a successful empty
//	attribution: the issue/comment write succeeded, the staging row was
//	marked processed, and a transient resolve/mint error permanently
//	lost the attribution (and the raw Jira identity the mint banks).
//	SR-5: a resolution ERROR is not "no identity" — the envelope must
//	fail so the row stays staged and retries.
//
//	C2 — offset pagination over ORDER BY updated ASC permanently skips
//	issues: an issue updated mid-scan moves later in the result set,
//	shifting every offset left; the next page omits an unseen issue
//	while the per-page checkpoint advances past it, and the next
//	cycle's `updated >=` never re-lists it. The walk now advances the
//	WINDOW (jql cursor, startAt=0) instead of the offset, holds a scan
//	CEILING so a busy project can't chase its own tail, and falls back
//	to in-window offsets only for a same-minute cohort larger than a
//	page (recoverable: the checkpoint equals that minute, so the next
//	cycle re-lists it).
package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// failingIdentityStore wraps fakeJiraProcStore to make identity calls fail.
type failingIdentityStore struct {
	fakeJiraProcStore
	resolveErr error
	mintErr    error
}

func (f *failingIdentityStore) ResolveJiraIdentity(ctx context.Context, name, key, disp string) (string, string, bool, error) {
	if f.resolveErr != nil {
		return "", "", false, f.resolveErr
	}
	return f.fakeJiraProcStore.ResolveJiraIdentity(ctx, name, key, disp)
}

func (f *failingIdentityStore) MintJiraContributor(ctx context.Context, name, disp string) (string, error) {
	if f.mintErr != nil {
		return "", f.mintErr
	}
	return f.fakeJiraProcStore.MintJiraContributor(ctx, name, disp)
}

func TestIdentityResolveErrorFailsTheEnvelope(t *testing.T) {
	f := &failingIdentityStore{resolveErr: errors.New("deadlock detected")}
	p := NewJiraProcessor(f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.processEnvelope(context.Background(), 1, []byte(jiraEnvelope)); err == nil {
		t.Fatal("a ResolveJiraIdentity ERROR must fail the envelope (stays staged, retries) — swallowing it permanently loses the attribution (SR-5)")
	}
	if len(f.issues) != 0 {
		t.Errorf("no issue write may land on an identity-store failure, got %d", len(f.issues))
	}
}

func TestMintErrorFailsTheEnvelope(t *testing.T) {
	f := &failingIdentityStore{mintErr: errors.New("connection refused")}
	p := NewJiraProcessor(f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.processEnvelope(context.Background(), 1, []byte(jiraEnvelope)); err == nil {
		t.Fatal("a MintJiraContributor ERROR must fail the envelope — the mint is what banks the raw Jira identity")
	}
}

func TestAmbiguousIdentityStillResolvesEmpty(t *testing.T) {
	// Ambiguity is an ANSWER (SR-6: stays NULL), not an error — the
	// envelope must succeed with empty attribution.
	f := &failingIdentityStore{}
	f.identities = map[string][3]any{"alice-gh": {"", "", true}}
	p := NewJiraProcessor(f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := p.processEnvelope(context.Background(), 1, []byte(jiraEnvelope)); err != nil {
		t.Fatalf("ambiguous identity must not fail the envelope: %v", err)
	}
	if len(f.issues) != 1 {
		t.Fatalf("issue write must land, got %d", len(f.issues))
	}
	if f.issues[0].ReporterCntrb != "" {
		t.Errorf("ambiguous reporter must stay unattributed, got %q", f.issues[0].ReporterCntrb)
	}
}

// jiraWindowServer serves issues by WINDOW (the jql cursor), not by
// offset — the shape the drift-safe walk must request.
type jiraWindowServer struct {
	mu       sync.Mutex
	requests []string // "cursor|startAt"
	base     time.Time
}

func (s *jiraWindowServer) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jql := r.URL.Query().Get("jql")
		start, _ := strconv.Atoi(r.URL.Query().Get("startAt"))
		cursor := ""
		if i := strings.Index(jql, "updated >= '"); i >= 0 {
			rest := jql[i+len("updated >= '"):]
			cursor = rest[:strings.Index(rest, "'")]
		}
		s.mu.Lock()
		s.requests = append(s.requests, fmt.Sprintf("%s|%d", cursor, start))
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		// Serve `updated >= cursor` (boundary minute INCLUSIVE — real
		// Jira semantics), sliced by startAt and capped like real pages.
		all := []string{"AVW2-1", "AVW2-2", "AVW2-3"}
		ups := []time.Time{s.base, s.base.Add(time.Minute), s.base.Add(10 * time.Minute)}
		var cur time.Time
		if cursor != "" {
			cur, _ = time.Parse("2006-01-02 15:04", cursor)
		}
		var keys []string
		var when []time.Time
		for i := range all {
			if !ups[i].Before(cur) {
				keys = append(keys, all[i])
				when = append(when, ups[i])
			}
		}
		_, _ = io.WriteString(w, jiraSearchPageAt(keys, when, start))
	}
}

func TestJiraSyncAdvancesWindowNotOffset(t *testing.T) {
	ws := &jiraWindowServer{base: time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)}
	srv := httptest.NewServer(ws.handler(t))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVW2", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if len(store.staged) != 3 {
		t.Fatalf("staged = %v, want all 3 issues across two windows", store.staged)
	}
	if !store.completed {
		t.Fatal("scan must complete")
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if len(ws.requests) < 2 {
		t.Fatalf("requests = %v, want >= 2 (window advance)", ws.requests)
	}
	// The walk must ADVANCE THE WINDOW: some later request carries a
	// non-empty jql cursor. A pure offset walk never moves the cursor
	// (every request would read "cursor|N" with an empty cursor) — the
	// permanent-skip class over a mutable ORDER BY.
	advanced := false
	seen := map[string]bool{}
	for _, req := range ws.requests {
		if seen[req] {
			t.Errorf("request %q repeated — the walk must make progress on every request", req)
		}
		seen[req] = true
		if !strings.HasPrefix(req, "|") {
			advanced = true
		}
	}
	if !advanced {
		t.Error("no request ever advanced the jql cursor — the walk is still bare offsets (the permanent-skip class)")
	}
}

// jiraSearchPageAt renders a window's remaining issues with explicit
// per-issue updated times, sliced by startAt and capped at maxResults
// like real Jira.
func jiraSearchPageAt(keys []string, updated []time.Time, startAt int) string {
	return jiraSearchPageAtN(keys, updated, startAt, 2)
}

func jiraSearchPageAtN(keys []string, updated []time.Time, startAt, maxResults int) string {
	total := len(keys)
	if startAt > len(keys) {
		startAt = len(keys)
	}
	keys, updated = keys[startAt:], updated[startAt:]
	if len(keys) > maxResults {
		keys, updated = keys[:maxResults], updated[:maxResults]
	}
	var issues []string
	for i, k := range keys {
		up := updated[i].Format("2006-01-02T15:04:05.000-0700")
		issues = append(issues, fmt.Sprintf(
			`{"id":"%d","key":"%s","fields":{"summary":"s","status":{"name":"Open"},"updated":"%s","created":"%s"}}`,
			2000+startAt+i, k, up, up))
	}
	return fmt.Sprintf(`{"startAt":%d,"maxResults":%d,"total":%d,"issues":[%s]}`,
		startAt, len(keys), total, strings.Join(issues, ","))
}

func TestJiraSyncHoldsScanCeiling(t *testing.T) {
	ws := &jiraWindowServer{base: time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)}
	srv := httptest.NewServer(ws.handler(t))
	defer srv.Close()

	var jqls []string
	var mu sync.Mutex
	probe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		jqls = append(jqls, r.URL.Query().Get("jql"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":2,"total":0,"issues":[]}`)
	}))
	defer probe.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVW3", BaseURL: probe.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(jqls) == 0 {
		t.Fatal("no requests")
	}
	for _, jql := range jqls {
		if !strings.Contains(jql, "updated <= '") {
			t.Errorf("jql %q lacks the scan CEILING — a busy project would chase its own tail forever (new updates keep re-entering the window)", jql)
		}
	}
}
