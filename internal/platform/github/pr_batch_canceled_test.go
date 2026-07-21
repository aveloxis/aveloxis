// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// v0.27.28 — cancellation must never fight the shutdown. Pre-fix,
// context.Canceled classified as ClassTransient, so every in-flight
// PR batch at `aveloxis stop` ran the full 10→5→2→1 subdivision
// cascade and fired the size-1 REST fallback against a dead context
// (85 of each in one minute of the 2026-07-21 production log).

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestFetchPRBatchCancellationBubblesWithoutFightingShutdown drives
// FetchPRBatch with an already-cancelled context and asserts the
// pre-fix pathology is gone on all three axes:
//   - the error bubbles as context.Canceled (not swallowed/reshaped)
//   - NO subdivision cascade (log carries no size-1 exhaustion)
//   - NO REST fallback attempts (zero requests reach the server)
func TestFetchPRBatchCancellationBubblesWithoutFightingShutdown(t *testing.T) {
	var serverHits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"repository":null}}`))
	}))
	defer server.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	keys := platform.NewKeyPool([]string{"tok"}, logger)
	client := New(server.URL, keys, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // shutdown already happened

	_, err := client.FetchPRBatch(ctx, "owner", "repo", []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	if err == nil {
		t.Fatal("expected an error from a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled to bubble", err)
	}

	logs := buf.String()
	if strings.Contains(logs, "exhausted at size 1") {
		t.Error("subdivision cascaded to size 1 on a cancelled context — ClassCanceled must bubble at the gate, never subdivide")
	}
	if strings.Contains(logs, "falling back to REST") || strings.Contains(logs, "REST fallback") {
		t.Error("REST fallback fired against a dead context — every fallback call inherits the same cancellation and is doomed")
	}
	if hits := serverHits.Load(); hits > 1 {
		// 0 is ideal (NewRequestWithContext fails pre-dial); tolerate
		// one racy in-flight request, but a cascade means many.
		t.Errorf("server received %d requests from a cancelled batch — the pre-fix cascade fired ~dozens", hits)
	}
}
