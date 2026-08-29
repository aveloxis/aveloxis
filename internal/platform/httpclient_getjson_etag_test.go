// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// etagServer answers every GET with an ETag and honours If-None-Match
// with a 304, counting bodies served and conditional requests received.
func etagServer(bodies, conditional *int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const tag = `"v1"`
		w.Header().Set("ETag", tag)
		if r.Header.Get("If-None-Match") != "" {
			atomic.AddInt32(conditional, 1)
			if r.Header.Get("If-None-Match") == tag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		atomic.AddInt32(bodies, 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id": 7}`))
	}))
}

// TestGetJSONNeverUsesConditionalRequests — v0.28.17: two GetJSON calls
// to the same path must BOTH get a body; the second must not carry
// If-None-Match (a decoding reader cannot use a 304). Red before the
// fix: the second call returned ErrNotModified.
func TestGetJSONNeverUsesConditionalRequests(t *testing.T) {
	var bodies, conditional int32
	srv := etagServer(&bodies, &conditional)
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)

	for i := 1; i <= 2; i++ {
		var got struct{ ID int }
		if err := c.GetJSON(context.Background(), "/repos/o/r/pulls/1", &got); err != nil {
			t.Fatalf("GetJSON call %d: %v", i, err)
		}
		if got.ID != 7 {
			t.Fatalf("GetJSON call %d decoded %+v, want id 7", i, got)
		}
	}
	if b := atomic.LoadInt32(&bodies); b != 2 {
		t.Errorf("bodies served = %d, want 2 (one per GetJSON)", b)
	}
	if cnd := atomic.LoadInt32(&conditional); cnd != 0 {
		t.Errorf("GetJSON sent %d conditional requests, want 0", cnd)
	}
}

// The bypass is scoped to GetJSON: a bare Get on the same path still
// caches the ETag and revalidates — that is what paginate relies on
// for "304 = nothing new, stop paginating".
func TestBareGetStillRevalidatesWithETag(t *testing.T) {
	var bodies, conditional int32
	srv := etagServer(&bodies, &conditional)
	defer srv.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
	ctx := context.Background()

	resp, err := c.Get(ctx, "/repos/o/r/issues?per_page=100")
	if err != nil {
		t.Fatalf("first Get: %v", err)
	}
	resp.Body.Close()
	if _, err := c.Get(ctx, "/repos/o/r/issues?per_page=100"); err == nil || !isNotModified(err) {
		t.Fatalf("second bare Get should revalidate and surface ErrNotModified, got %v", err)
	}
	if cnd := atomic.LoadInt32(&conditional); cnd != 1 {
		t.Errorf("bare Get sent %d conditional requests, want 1", cnd)
	}
}

func isNotModified(err error) bool { return err != nil && ClassifyError(err) == ClassNotModified }
