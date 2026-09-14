// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// Copilot round 5 on PR #191 (v0.28.18): a full-snapshot listing (zero
// since) has a STABLE URL, so the second walk in one process used to be
// answered from the ETag cache with an empty result — the gap filler's
// expected set read empty ("no gaps") and a force-full recollect staged
// nothing. Against an If-None-Match-honoring mock: two zero-since walks
// both yield the items and neither sends If-None-Match; two same-since
// INCREMENTAL walks keep the documented 304 = nothing-new semantic.
func TestFullSnapshotListingsBypassETagIncrementalKeeps304(t *testing.T) {
	var conditional, hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/pulls"):
			_, _ = w.Write([]byte(`[{"id":11,"number":1,"title":"pr","state":"open","updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		default:
			_, _ = w.Write([]byte(`[{"id":21,"number":2,"title":"issue","state":"open","updated_at":"2026-08-01T00:00:00Z","created_at":"2026-08-01T00:00:00Z"}]`))
		}
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)

	countIssues := func(since time.Time) int {
		t.Helper()
		n := 0
		for _, err := range client.ListIssues(t.Context(), "o", "r", since) {
			if err != nil {
				t.Fatalf("ListIssues: %v", err)
			}
			n++
		}
		return n
	}
	countPRs := func(since time.Time) int {
		t.Helper()
		n := 0
		for _, err := range client.ListPullRequests(t.Context(), "o", "r", since) {
			if err != nil {
				t.Fatalf("ListPullRequests: %v", err)
			}
			n++
		}
		return n
	}

	for i := 1; i <= 2; i++ {
		if n := countIssues(time.Time{}); n != 1 {
			t.Errorf("full-snapshot ListIssues walk %d yielded %d items, want 1 (never a 304)", i, n)
		}
		if n := countPRs(time.Time{}); n != 1 {
			t.Errorf("full-snapshot ListPullRequests walk %d yielded %d items, want 1 (never a 304)", i, n)
		}
	}
	if c := conditional.Load(); c != 0 {
		t.Errorf("full-snapshot walks must not send If-None-Match, sent %d", c)
	}

	// Incremental: the same since twice → the second walk is a clean 304.
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if n := countIssues(since); n != 1 {
		t.Fatalf("first incremental ListIssues yielded %d, want 1", n)
	}
	if n := countIssues(since); n != 0 {
		t.Errorf("second incremental ListIssues yielded %d, want 0 (304 = nothing new is the incremental contract)", n)
	}
	if conditional.Load() == 0 {
		t.Error("the repeat incremental walk must have sent If-None-Match")
	}
	if hits.Load() < 6 {
		t.Errorf("expected at least 6 requests (4 full + 2 incremental), got %d", hits.Load())
	}
}
