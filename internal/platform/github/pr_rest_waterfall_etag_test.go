// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestGitHubPRRESTWaterfallSurvivesETag — the GitHub side of the class
// Copilot round 2 on PR #191 named: FetchPRByNumber, FetchPRMeta, and
// FetchPRRepos all GET /repos/o/r/pulls/N. The pr_child_mode=rest
// waterfall, gap fill's REST branch, and the v0.20.20 huge-body REST
// fallback call them back-to-back; pre-v0.28.17 the second and third
// GETs carried the cached ETag, GitHub answered 304, and meta/repos
// came back as an error (or, in the fallback, were silently skipped).
// GetJSON is ETag-free now, so all three must return data.
func TestGitHubPRRESTWaterfallSurvivesETag(t *testing.T) {
	var bodies int32
	const tag = `W/"pr1-v1"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !strings.HasSuffix(r.URL.Path, "/pulls/1") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
			return
		}
		w.Header().Set("ETag", tag)
		if r.Header.Get("If-None-Match") == tag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		atomic.AddInt32(&bodies, 1)
		_, _ = w.Write([]byte(`{
		  "id": 501, "number": 1, "state": "open", "title": "Fix", "body": "b",
		  "html_url": "https://github.com/o/r/pull/1", "diff_url": "https://github.com/o/r/pull/1.diff",
		  "user": {"login": "alice", "id": 7},
		  "created_at": "2026-01-02T03:04:05Z", "updated_at": "2026-01-03T03:04:05Z",
		  "head": {"ref": "feature", "sha": "abc123", "label": "alice:feature",
		           "repo": {"id": 11, "name": "r", "full_name": "alice/r", "private": false, "owner": {"login": "alice", "id": 7}}},
		  "base": {"ref": "main", "sha": "def456", "label": "o:main",
		           "repo": {"id": 12, "name": "r", "full_name": "o/r", "private": false, "owner": {"login": "o", "id": 9}}}
		}`))
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := &Client{http: platform.NewHTTPClient(srv.URL, platform.NewKeyPool([]string{"k"}, logger), logger, platform.AuthGitHub), logger: logger}
	ctx := context.Background()

	pr, err := client.FetchPRByNumber(ctx, "o", "r", 1)
	if err != nil || pr == nil || pr.Number != 1 {
		t.Fatalf("FetchPRByNumber = %+v, %v", pr, err)
	}
	head, base, err := client.FetchPRMeta(ctx, "o", "r", 1)
	if err != nil {
		t.Fatalf("FetchPRMeta right after FetchPRByNumber: %v (the 304 class)", err)
	}
	if head == nil || head.Ref != "feature" || base == nil || base.Ref != "main" {
		t.Errorf("meta = %+v / %+v, want feature / main", head, base)
	}
	headRepo, baseRepo, err := client.FetchPRRepos(ctx, "o", "r", 1)
	if err != nil {
		t.Fatalf("FetchPRRepos as the third same-URL GET: %v", err)
	}
	if headRepo == nil || baseRepo == nil {
		t.Errorf("repos = %+v / %+v, want both populated from head.repo/base.repo", headRepo, baseRepo)
	}
	if b := atomic.LoadInt32(&bodies); b != 3 {
		t.Errorf("PR body served %d times, want 3 (never a 304)", b)
	}
}
