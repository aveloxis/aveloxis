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

	// Cycles 1 and 2 share ONE fixed, non-zero since, and it predates
	// the fixture events. Both halves matter:
	//
	//   - non-zero, because a zero since runs under WithoutETag, which
	//     suppresses the cache WRITE — cycle 1 would prime nothing and
	//     cycle 2 could never send If-None-Match;
	//   - identical, because the since is part of the URL and the ETag
	//     cache is keyed by URL;
	//   - predating the events, because the client also filters by
	//     since. Until pass 52 this test used a zero since for cycle 1
	//     and `now-1h` for cycle 2, and its "0 items on 304" assertion
	//     was satisfied by that FILTER dropping two 2026-dated events —
	//     it passed with the server's 304 branch deleted entirely.
	since := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)

	var primed int
	for _, err := range client.ListRepoEvents(t.Context(), "o", "r", since) {
		if err != nil {
			t.Fatalf("cycle 1: %v", err)
		}
		primed++
	}
	if primed == 0 {
		t.Fatal("cycle 1 yielded nothing, so it cannot have primed an ETag and cycle 2 proves nothing — the fixture events must post-date the since")
	}

	// Cycle 2 (INCREMENTAL, same since): nothing changed upstream →
	// 304 → clean zero items, no error. That's the CORRECT incremental
	// semantic — the ETag layer is only pathological when two passes
	// alias within one cycle.
	var items int
	for _, err := range client.ListRepoEvents(t.Context(), "o", "r", since) {
		if err != nil {
			t.Fatalf("cycle 2 must be a clean zero, got error: %v", err)
		}
		items++
	}
	if items != 0 {
		t.Errorf("cycle 2 should yield 0 items on 304, got %d", items)
	}
	if got := hits.Load(); got != 2 {
		t.Errorf("expected exactly 2 requests after two incremental cycles (a fetch then a revalidation), got %d — if cycle 2 did not reach the server at all, the zero above is not a 304", got)
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
