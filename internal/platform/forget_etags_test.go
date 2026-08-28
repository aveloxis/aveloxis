// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// Pass 30 (v0.28.18): a job that fetched a repo's listing pages and then
// FAILED left their ETags cached, so the retry in the same process was
// answered 304 on those pages — zero items, and last_collected advanced
// past a window that was never stored. ForgetETagsWithPrefix drops the
// repo's entries: the next read of a forgotten path is unconditional,
// a path outside the prefix keeps revalidating.
func TestForgetETagsWithPrefixMakesTheNextReadUnconditional(t *testing.T) {
	var mu sync.Mutex
	conditional := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if r.Header.Get("If-None-Match") != "" {
			conditional[r.URL.Path]++
		}
		mu.Unlock()
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"t"}, logger), logger, AuthGitHub)

	prime := func(path string) {
		t.Helper()
		resp, err := c.Get(t.Context(), path)
		if err != nil {
			t.Fatalf("prime %s: %v", path, err)
		}
		resp.Body.Close()
	}
	for _, p := range []string{"/repos/o/r/issues?state=all", "/repos/o/r/pulls?state=all", "/repos/o/other/issues?state=all"} {
		prime(p)
	}
	if n := c.ForgetETagsWithPrefix("/repos/o/r/"); n != 2 {
		t.Fatalf("ForgetETagsWithPrefix dropped %d entries, want 2 (the two under the prefix)", n)
	}
	// Forgotten paths read unconditionally (a 200 body, no If-None-Match);
	// the untouched path still revalidates (304).
	for _, p := range []string{"/repos/o/r/issues?state=all", "/repos/o/r/pulls?state=all"} {
		resp, err := c.Get(t.Context(), p)
		if err != nil {
			t.Fatalf("re-read %s after forgetting: %v", p, err)
		}
		resp.Body.Close()
	}
	if _, err := c.Get(t.Context(), "/repos/o/other/issues?state=all"); err == nil || !strings.Contains(err.Error(), "304") {
		t.Errorf("the path outside the prefix must still revalidate to a 304, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if conditional["/repos/o/r/issues"] != 0 || conditional["/repos/o/r/pulls"] != 0 {
		t.Errorf("forgotten paths must not send If-None-Match on the re-read: %v", conditional)
	}
	if conditional["/repos/o/other/issues"] != 1 {
		t.Errorf("the untouched path must have revalidated once, got %d", conditional["/repos/o/other/issues"])
	}
}
