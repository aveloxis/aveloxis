// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Issue #245: in the GitHub REST API, `watchers_count` (and `watchers`) are
// legacy aliases of `stargazers_count`. The number of people watching a
// repository is `subscribers_count`. The GraphQL path reads
// `watchers { totalCount }`, so the REST fallback must read
// `subscribers_count` to keep repo_info.watcher_count on the same meaning.

package github

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestFetchRepoInfoRESTReadsWatchersFromSubscribersCount(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/chaoss/augur", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"full_name":"chaoss/augur","stargazers_count":620,"watchers_count":620,"watchers":620,"subscribers_count":31}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	info, err := client.fetchRepoInfoREST(t.Context(), "chaoss", "augur")
	if err != nil {
		t.Fatalf("fetchRepoInfoREST: %v", err)
	}
	if info.StarCount != 620 {
		t.Errorf("StarCount = %d, want 620 (stargazers_count)", info.StarCount)
	}
	if info.WatcherCount != 31 {
		t.Errorf("WatcherCount = %d, want 31 (subscribers_count) — watchers_count is a legacy alias of stargazers_count", info.WatcherCount)
	}
}
