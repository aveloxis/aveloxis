// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.26.3 regression tests for the ETag self-aliasing event loss
// (found 2026-07-09): the pre-v0.26.3 design paginated
// /repos/{o}/{r}/issues/events TWICE per cycle (ListIssueEvents, then
// ListPREvents). The first pass primed the ETag cache; on any repo with
// no new event in the seconds between passes, the second pass got
// 304 Not Modified, the paginator ended with zero items, and the
// ENTIRE PR-event history was silently dropped — permanently, because
// incremental cycles never re-walk history. 209 production repos with
// 50+ PRs had zero PR events. ListRepoEvents makes the aliasing
// structurally impossible: one pass, both kinds.
//
// The mock server here EMITS an ETag and honors If-None-Match exactly
// like GitHub, so any future return to a two-pass design fails these
// tests instead of silently losing data.

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func newEventsTestServer(t *testing.T, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/r/issues/events", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") == `"ev-1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"ev-1"`)
		json.NewEncoder(w).Encode([]map[string]any{
			{
				"id": 1001, "event": "closed",
				"actor":      map[string]any{"login": "alice", "id": 1},
				"created_at": "2026-01-01T00:00:00Z",
				"issue":      map[string]any{"number": 7},
			},
			{
				"id": 1002, "event": "merged",
				"actor":      map[string]any{"login": "bob", "id": 2},
				"created_at": "2026-01-02T00:00:00Z",
				"issue": map[string]any{
					"number":       9,
					"pull_request": map[string]any{},
				},
			},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("[]"))
	})
	return httptest.NewServer(mux)
}

func TestListRepoEventsSinglePassYieldsBothKinds(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var hits atomic.Int64
	srv := newEventsTestServer(t, &hits)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)

	var issueEvents, prEvents int
	var prNumber int64
	for ev, err := range client.ListRepoEvents(t.Context(), "o", "r", time.Time{}) {
		if err != nil {
			t.Fatalf("ListRepoEvents: %v", err)
		}
		switch {
		case ev.PR != nil:
			prEvents++
			prNumber = ev.PR.PlatformPRID
		case ev.Issue != nil:
			issueEvents++
		default:
			t.Error("RepoEvent with neither side set")
		}
	}

	if issueEvents != 1 || prEvents != 1 {
		t.Errorf("expected 1 issue event + 1 PR event from ONE pass, got issue=%d pr=%d — "+
			"a two-pass regression loses one kind to the ETag 304 (2026-07-09 incident)",
			issueEvents, prEvents)
	}
	if prNumber != 9 {
		t.Errorf("PR event must carry the PR NUMBER in PlatformPRID for processor "+
			"resolution, got %d want 9", prNumber)
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("the events endpoint must be paginated exactly ONCE per cycle, got %d "+
			"requests — a second pass both wastes API budget and re-arms the ETag trap", got)
	}
}

func TestListRepoEventsSecondCycle304IsCleanZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var hits atomic.Int64
	srv := newEventsTestServer(t, &hits)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)

	// Cycle 1 populates and primes the ETag.
	for _, err := range client.ListRepoEvents(t.Context(), "o", "r", time.Time{}) {
		if err != nil {
			t.Fatalf("cycle 1: %v", err)
		}
	}
	// Cycle 2 (INCREMENTAL — a real since): nothing changed upstream →
	// 304 → clean zero items, no error. That's the CORRECT incremental
	// semantic — the ETag layer is only pathological when two passes
	// alias within one cycle.
	var items int
	for _, err := range client.ListRepoEvents(t.Context(), "o", "r", time.Now().Add(-time.Hour)) {
		if err != nil {
			t.Fatalf("cycle 2 must be a clean zero, got error: %v", err)
		}
		items++
	}
	if items != 0 {
		t.Errorf("cycle 2 should yield 0 items on 304, got %d", items)
	}
	// Cycle 3 (FULL SNAPSHOT — a zero since, the force-full / gap-filler
	// shape): the listing is a truth set and bypasses the cache — it
	// must yield the events again, never the cached "nothing new"
	// (Copilot round 5, v0.28.18).
	items = 0
	for _, err := range client.ListRepoEvents(t.Context(), "o", "r", time.Time{}) {
		if err != nil {
			t.Fatalf("cycle 3 (full snapshot): %v", err)
		}
		items++
	}
	if items == 0 {
		t.Error("a full-snapshot listing (zero since) must bypass the ETag cache and yield the events again, got 0")
	}
}
