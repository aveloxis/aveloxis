// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 1g (v0.27.37): REST pagination must retry a page
// whose BODY read/decode fails with a retryable transport error.
// Pre-fix, `paginate` decoded the body OUTSIDE Get's retry loop, so an
// HTTP/2 RST_STREAM/CANCEL mid-body — routine at GitHub's edge —
// surfaced as `decoding page: stream error` and killed the whole
// collection job. On force-full walks of pytorch-class repos
// (~10–15K sequential pages of review comments/events) the per-page
// failure probability compounded to near-certain job death: THE
// "largest repos never complete" anomaly.
//
// The load-bearing subtlety: Get caches the response ETag at HEADER
// time, before the body is read. A naive re-fetch would send
// If-None-Match, receive 304, and paginate treats 304 as clean
// end-of-iteration — converting a loud failure into SILENT TRUNCATION
// (and the stale cached ETag would poison the NEXT cycle the same
// way). The retry must therefore forget the cached ETag and fetch the
// page unconditionally.

package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type prItem struct {
	ID int `json:"id"`
}

func pageRetryClient(t *testing.T, srvURL string) *HTTPClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(pageRetryLogWriter{t}, nil))
	return NewHTTPClient(srvURL, NewKeyPool([]string{"test-token"}, logger), logger, AuthGitHub)
}

type pageRetryLogWriter struct{ t *testing.T }

func (w pageRetryLogWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// TestPaginateRetriesMidBodyFailure: page 2's first serve aborts
// mid-body (Content-Length larger than the bytes written → the client
// sees io.ErrUnexpectedEOF). The iteration must re-fetch page 2 on a
// fresh request WITHOUT If-None-Match and complete with every item.
func TestPaginateRetriesMidBodyFailure(t *testing.T) {
	var page2Hits atomic.Int32
	var retriedWithETag atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/r/things", func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		switch page {
		case "", "1":
			w.Header().Set("Link", fmt.Sprintf(`<%s?page=2&per_page=100>; rel="next"`, "/repos/o/r/things"))
			w.Header().Set("ETag", `"page1-etag"`)
			_, _ = w.Write([]byte(`[{"id":1},{"id":2}]`))
		case "2":
			n := page2Hits.Add(1)
			if n == 1 {
				// First attempt: declare 1000 bytes, write a fragment,
				// return — the client's decoder hits unexpected EOF.
				w.Header().Set("ETag", `"page2-etag"`)
				w.Header().Set("Content-Length", "1000")
				_, _ = w.Write([]byte(`[{"id":3},`))
				return
			}
			// Retry: MUST arrive unconditional. If the stale cached
			// ETag were replayed, real GitHub would answer 304 and the
			// tail of the dataset would silently vanish.
			if r.Header.Get("If-None-Match") != "" {
				retriedWithETag.Store(true)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			_, _ = w.Write([]byte(`[{"id":3},{"id":4}]`))
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := pageRetryClient(t, srv.URL)
	var got []int
	for item, err := range PaginateGitHub[prItem](context.Background(), c, "/repos/o/r/things") {
		if err != nil {
			t.Fatalf("pagination error: %v", err)
		}
		got = append(got, item.ID)
	}
	if retriedWithETag.Load() {
		t.Fatal("retry sent If-None-Match with the stale cached ETag — a 304 there is SILENT TRUNCATION (the v0.25.0 data-loss class)")
	}
	if page2Hits.Load() < 2 {
		t.Fatalf("page 2 must be re-fetched after the mid-body abort; hits=%d", page2Hits.Load())
	}
	if len(got) != 4 {
		t.Fatalf("expected all 4 items after retry, got %v", got)
	}
}

// TestPaginateReadRetryExhaustionIsTransient: a page that aborts
// mid-body on EVERY attempt must surface an error that classifies
// ClassTransient, so the job-level force_full_collect recovery
// machinery reacts correctly.
func TestPaginateReadRetryExhaustionIsTransient(t *testing.T) {
	var hits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/o/r/things", func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte(`[{"id":1},`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := pageRetryClient(t, srv.URL)
	var lastErr error
	for _, err := range PaginateGitHub[prItem](context.Background(), c, "/repos/o/r/things") {
		if err != nil {
			lastErr = err
		}
	}
	if lastErr == nil {
		t.Fatal("persistent mid-body aborts must surface an error")
	}
	if ClassifyError(lastErr) != ClassTransient {
		t.Fatalf("exhausted read retries must classify ClassTransient, got %v (err: %v)", ClassifyError(lastErr), lastErr)
	}
	// 1 initial + maxPageReadRetries attempts.
	if got := hits.Load(); got != int32(1+maxPageReadRetries) {
		t.Fatalf("expected %d attempts (1 + %d retries), got %d", 1+maxPageReadRetries, maxPageReadRetries, got)
	}
	if !errors.Is(lastErr, ErrTransient) {
		t.Fatalf("error must wrap ErrTransient: %v", lastErr)
	}
}
