// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// captureLogger returns a slog.Logger that records every record to
// a thread-safe bytes.Buffer, plus the buffer for inspection. Used
// by the 5xx tests to assert which log line was emitted (the real
// semantic signal — timing alone can't distinguish the default arm
// from the 5xx branch when only one retry happens).
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), &buf
}

// v0.22.12 — status 500 must be classified as a transient server
// error, in the same case branch as 502/503/504. Before this release
// 500 fell into the default "unexpected status" arm with linear
// backoff. Production log on 2026-05-18 showed 1,429 WARN entries
// from a transient GitHub /events incident; classifying 500
// consistently with the other 5xx codes gives operators a clearer
// log signal AND uses GitHub's recommended exponential backoff.
//
// Empirical evidence the fix is needed: the production log emitted
// `level=WARN msg="unexpected status" url=... status=500
// body_snippet="" attempt=1`, identical to a generic
// uncategorized status. After the fix it should emit
// `level=WARN msg="server error, retrying with backoff" url=...
// status=500 wait=... attempt=1` — same message shape as 502/503/504.

// fiveHundredThenTwoHundred is a test handler that returns N copies of
// `firstStatus` then 200. Used by the transient-5xx tests below to
// prove the client retries and eventually succeeds.
func fiveHundredThenTwoHundred(failCount int32, firstStatus int) (http.HandlerFunc, *int32) {
	var hits int32
	h := func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&hits, 1)
		if n <= failCount {
			w.WriteHeader(firstStatus)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}
	return h, &hits
}

// TestHTTPClientTreats500AsTransient is the v0.22.12 regression
// guard for the 2026-05-18 incident handling fix. Asserts a 500
// response is routed through the SAME case branch as 502/503/504
// (the "server error, retrying with backoff" branch), not the
// generic "unexpected status" default arm.
//
// The semantic test is on the LOG MESSAGE, not timing. Both branches
// happen to retry and eventually succeed; timing alone can't
// distinguish them when only one retry is needed. The branches emit
// distinct WARN lines:
//
//	5xx branch:  msg="server error, retrying with backoff"
//	default arm: msg="unexpected status"
//
// If a future refactor moves 500 back into the default arm, the
// log assertion below fires.
func TestHTTPClientTreats500AsTransient(t *testing.T) {
	handler, hits := fiveHundredThenTwoHundred(1, http.StatusInternalServerError)
	server := httptest.NewServer(handler)
	defer server.Close()

	logger, logBuf := captureLogger()
	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, logger), logger, AuthGitHub)
	resp, err := client.Get(context.Background(), "/")
	if err != nil {
		t.Fatalf("Get returned error after 1×500 + 1×200: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("final status = %d, want 200 — client should have retried past the 500", resp.StatusCode)
	}
	if got := atomic.LoadInt32(hits); got != 2 {
		t.Errorf("server hit %d times, want 2 — client should have retried exactly once", got)
	}

	logStr := logBuf.String()
	if strings.Contains(logStr, `msg="unexpected status"`) {
		t.Errorf("got `unexpected status` WARN — 500 is being routed to the default arm. "+
			"v0.22.12 contract requires 500 to be in the 5xx branch alongside 502/503/504. "+
			"Full log:\n%s", logStr)
	}
	if !strings.Contains(logStr, `msg="server error, retrying with backoff"`) {
		t.Errorf("missing `server error, retrying with backoff` WARN — 500 should hit the "+
			"5xx branch's standard log line. Full log:\n%s", logStr)
	}
}

// TestHTTPClientTreats502503504AsTransient locks in the existing
// behavior so a future refactor (e.g., the one that adds 500 to the
// case branch) doesn't accidentally drop 502/503/504. Same test
// shape as the 500 case, repeated for each status code.
func TestHTTPClientTreats502503504AsTransient(t *testing.T) {
	for _, status := range []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			handler, hits := fiveHundredThenTwoHundred(1, status)
			server := httptest.NewServer(handler)
			defer server.Close()

			client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
			resp, err := client.Get(context.Background(), "/")
			if err != nil {
				t.Fatalf("Get returned error after 1×%d + 1×200: %v", status, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("final status = %d, want 200", resp.StatusCode)
			}
			if got := atomic.LoadInt32(hits); got != 2 {
				t.Errorf("server hit %d times, want 2", got)
			}
		})
	}
}
