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
	"sort"
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
		cursor := ""
		if i := strings.Index(jql, "updated >= '"); i >= 0 {
			rest := jql[i+len("updated >= '"):]
			cursor = rest[:strings.Index(rest, "'")]
		}
		s.mu.Lock()
		// Record the FULL jql: the round-9 tie drain legally revisits a
		// (cursor, key) pair with a DIFFERENT jql shape (the frozen
		// minute window + ORDER BY issuekey), so uniqueness holds at
		// the jql level, not the coarse pair.
		s.requests = append(s.requests, cursor+"|"+jql)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		all := []string{"AVW2-1", "AVW2-2", "AVW2-3"}
		ups := []time.Time{s.base, s.base.Add(time.Minute), s.base.Add(10 * time.Minute)}
		_, _ = io.WriteString(w, jiraJQLServe(jql, all, ups, 2))
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
	_ = seen
	if !advanced {
		t.Error("no request ever advanced the jql cursor — the walk is still bare offsets (the permanent-skip class)")
	}
}

// jiraJQLServe emulates real Jira Server jql semantics for worker
// fixtures: `updated >= 'X'`, `updated < 'Y'`, `issuekey > 'K'`
// (numeric by issue number) and the two ORDER BY forms. The
// drift-safe walk paginates purely by cursor/keyset (startAt is
// always 0 — round 9 retired the offset fallback), so a fixture that
// ignored these clauses would re-serve one page forever and trip the
// walker's own termination bounds.
func jiraJQLServe(jql string, keys []string, ups []time.Time, maxResults int) string {
	type row struct {
		key string
		up  time.Time
	}
	keyNum := func(k string) int {
		i := strings.LastIndex(k, "-")
		n, _ := strconv.Atoi(k[i+1:])
		return n
	}
	var ge, lt time.Time
	if i := strings.Index(jql, "updated >= '"); i >= 0 {
		rest := jql[i+len("updated >= '"):]
		ge, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
	}
	if i := strings.Index(jql, "updated < '"); i >= 0 {
		rest := jql[i+len("updated < '"):]
		lt, _ = time.Parse("2006-01-02 15:04", rest[:strings.Index(rest, "'")])
	}
	keyGT := ""
	if i := strings.Index(jql, "issuekey > '"); i >= 0 {
		rest := jql[i+len("issuekey > '"):]
		keyGT = rest[:strings.Index(rest, "'")]
	}
	var rows []row
	for i := range keys {
		if !ge.IsZero() && ups[i].Before(ge) {
			continue
		}
		if !lt.IsZero() && !ups[i].Before(lt) {
			continue
		}
		if keyGT != "" && keyNum(keys[i]) <= keyNum(keyGT) {
			continue
		}
		rows = append(rows, row{keys[i], ups[i]})
	}
	if strings.Contains(jql, "ORDER BY issuekey") {
		sort.Slice(rows, func(i, j int) bool { return keyNum(rows[i].key) < keyNum(rows[j].key) })
	} else {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].up.Before(rows[j].up) })
	}
	total := len(rows)
	if len(rows) > maxResults {
		rows = rows[:maxResults]
	}
	var issues []string
	for _, r := range rows {
		up := r.up.Format("2006-01-02T15:04:05.000-0700")
		issues = append(issues, fmt.Sprintf(
			`{"id":"%d","key":"%s","fields":{"summary":"s","status":{"name":"Open"},"updated":"%s","created":"%s"}}`,
			2000+keyNum(r.key), r.key, up, up))
	}
	return fmt.Sprintf(`{"startAt":0,"maxResults":%d,"total":%d,"issues":[%s]}`,
		len(rows), total, strings.Join(issues, ","))
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

// TestJiraSyncBoundaryDriftStillStagesSibling — the L10 F1 red-proof.
// Corpus: A@10:00, B@10:30, C@10:30 with pageSize 2. Page 1 serves
// [A, B]; BETWEEN pages, B is touched (updated → 10:45). The first C2
// walk advanced the cursor to 10:30 with startAt = count-of-boundary-
// issues-in-page (1) — an OFFSET ASSUMPTION: the drifted window is
// [C@10:30, B@10:45], and slicing it at 1 serves [B] only. C was never
// listed while the checkpoint advanced past 10:30: a PERMANENT skip
// (empirically reproduced by the reviewer). The fix is TRUE startAt=0
// on cursor advance — the boundary minute re-lists whole and the
// staging natural key no-ops the duplicates.
func TestJiraSyncBoundaryDriftStillStagesSibling(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	requests := 0
	bUpdated := base.Add(30 * time.Minute) // drifts to 10:45 after page 1
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		if requests == 2 {
			bUpdated = base.Add(45 * time.Minute) // the mid-walk touch
		}
		keys := []string{"AVD3-1", "AVD3-2", "AVD3-3"}
		when := []time.Time{base, bUpdated, base.Add(30 * time.Minute)}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, jiraJQLServe(r.URL.Query().Get("jql"), keys, when, 2))
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVD3", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.RunOnce(context.Background())

	if !store.completed {
		t.Fatal("scan must complete")
	}
	sawC := false
	for _, s := range store.staged {
		if strings.HasPrefix(s, "AVD3-3@") {
			sawC = true
		}
	}
	if !sawC {
		t.Fatalf("staged = %v — the boundary-minute sibling AVD3-3 was skipped (the F1 permanent-skip class)", store.staged)
	}
}

// TestJiraSyncFailsOnReServingServer — the L10 F2 termination bound.
// A misbehaving server returns the SAME non-empty page regardless of
// jql cursor or startAt. The first C2 walk had no reachable bound
// (the (cursor, startAt) no-progress guard could never fire — every
// non-empty page strictly advances one of the pair — and the rewrite
// dropped the old `startAt < total` loop condition): the reviewer's
// probe ran 8,779 requests before the ctx timeout. The staleness bound
// keys on what the loop cannot self-satisfy: consecutive pages whose
// every (key, updated) pair was already seen this scan.
func TestJiraSyncFailsOnReServingServer(t *testing.T) {
	base := time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		// Deliberately jql-BLIND: the same full page regardless of
		// cursor/keyset — the misbehaving-server shape the walk's
		// termination bounds exist for (post-round-9 it trips the
		// tie drain's key-progress guard).
		u1 := base.Format("2006-01-02T15:04:05.000-0700")
		u2 := base.Add(time.Minute).Format("2006-01-02T15:04:05.000-0700")
		_, _ = io.WriteString(w, `{"startAt":0,"maxResults":2,"total":2,"issues":[`+
			`{"id":"2001","key":"AVRS-1","fields":{"summary":"s","status":{"name":"Open"},"updated":"`+u1+`","created":"`+u1+`"}},`+
			`{"id":"2002","key":"AVRS-2","fields":{"summary":"s","status":{"name":"Open"},"updated":"`+u2+`","created":"`+u2+`"}}]}`)
	}))
	defer srv.Close()

	repoID := int64(42)
	store := &fakeJiraStore{job: &db.JiraProjectJob{JpsID: 7, ProjectKey: "AVRS", BaseURL: srv.URL, RepoID: &repoID}}
	w := NewJiraWorker(store, 24*time.Hour, "", 2, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan struct{})
	go func() { defer close(done); w.RunOnce(context.Background()) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("walk did not terminate against a re-serving server (the F2 unbounded-loop class)")
	}
	if store.completed {
		t.Fatal("a scan that could not converge must not stamp complete")
	}
	if store.failures != 1 {
		t.Fatalf("failures = %d, want exactly 1 (RecordJiraFailure backoff)", store.failures)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests > 10 {
		t.Fatalf("requests = %d — the bound must trip within a handful of stale pages", requests)
	}
}
