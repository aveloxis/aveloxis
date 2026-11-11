// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestLiveRESTRepoInfoReadsSubscribers is the network canary paired with the
// REST fallback's watcher mapping (issue #245, 0.29.143): the fallback reads
// `subscribers_count` from GET /repos/{owner}/{repo}, because REST's
// `watchers_count` aliases `stargazers_count`. A mock cannot tell whether
// GitHub still sends the field; if it stopped, the decode would leave 0 and
// every fallback would store watcher_count = 0 with the unit test green. On
// golang/go the watchers are a small fraction of the stars, so a watcher
// count equal to the star count means the alias came back.
// Skipped unless AVELOXIS_TEST_NETWORK=1 and a token is set.
func TestLiveRESTRepoInfoReadsSubscribers(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the REST repo-info canary")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := New("https://api.github.com", platform.NewKeyPool([]string{tok}, logger), logger)
	info, err := client.fetchRepoInfoREST(t.Context(), "golang", "go")
	if err != nil {
		t.Fatalf("GET /repos/golang/go: %v", err)
	}
	if info.WatcherCount <= 0 {
		t.Errorf("watcher count %d: subscribers_count missing from the REST answer", info.WatcherCount)
	}
	if info.WatcherCount == info.StarCount {
		t.Errorf("watcher count equals the star count (%d): the REST fallback is reading the stargazers alias", info.StarCount)
	}
}
