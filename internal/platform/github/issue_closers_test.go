// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.26.5 — FetchIssueClosers: batched per-issue timeline lookup for
// the closed_by backfill sweep (Phase 3). GitHub's repo-wide
// /issues/events feed truncates history on large repos (3.23M closers
// unreachable on production), but the per-issue
// timelineItems(itemTypes:[CLOSED_EVENT], last:1) is uncapped. Batched
// ~100 issues per query via aliases, the whole historical sweep costs
// ~100K GraphQL points.

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestFetchIssueClosersBatchesAndDecodes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var queries int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		queries++
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		// One aliased issue() lookup per requested number.
		if !strings.Contains(req.Query, "timelineItems") ||
			!strings.Contains(req.Query, "CLOSED_EVENT") {
			t.Errorf("query must select timelineItems(itemTypes:[CLOSED_EVENT]...), got: %.200s", req.Query)
		}
		// issue 7: closed by a user; issue 9: closer deleted (null actor);
		// issue 11: never closed (empty timeline).
		fmt.Fprint(w, `{"data":{"repository":{
			"i0":{"timelineItems":{"nodes":[{"actor":{"__typename":"User","login":"closer-login","databaseId":424242}}]}},
			"i1":{"timelineItems":{"nodes":[{"actor":null}]}},
			"i2":{"timelineItems":{"nodes":[]}}
		}}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	closers, err := client.FetchIssueClosers(t.Context(), "o", "r", []int{7, 9, 11})
	if err != nil {
		t.Fatalf("FetchIssueClosers: %v", err)
	}
	if queries != 1 {
		t.Errorf("3 issues must fit in ONE batched query, got %d queries", queries)
	}
	got, ok := closers[7]
	if !ok || got.Login != "closer-login" || got.PlatformID != 424242 {
		t.Errorf("issue 7 closer: got %+v", got)
	}
	// Deleted-user closer and never-closed issues: absent from the map
	// (operator decision: no data → report no data).
	if _, ok := closers[9]; ok {
		t.Error("issue 9 (null actor / deleted closer) must be absent from the result")
	}
	if _, ok := closers[11]; ok {
		t.Error("issue 11 (no closed event) must be absent from the result")
	}
}
