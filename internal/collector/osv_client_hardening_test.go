// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — the OSV client was the last external caller on
// http.DefaultClient, which has NO timeout: a stalled read held its
// goroutine forever (the same hang class v0.27.5's --max-time 30 fixed
// for the curl registry lookups). It also sent no User-Agent — the
// anonymous-UA class that silently 403'd every crates.io lookup since
// inception (v0.27.19). These tests pin both fixes.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestOSVBatchRespectsClientTimeout proves queryOSVBatch goes through
// osvHTTPClient (not http.DefaultClient): swap in a short-timeout
// client, serve a slow handler, and the call must error instead of
// hanging. Against DefaultClient the swap would have no effect and the
// request would succeed after the sleep.
func TestOSVBatchRespectsClientTimeout(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.Write([]byte(`{"results":[]}`))
	}))
	defer slow.Close()

	origURL, origClient := osvBatchURL, osvHTTPClient
	osvBatchURL = slow.URL
	osvHTTPClient = &http.Client{Timeout: 100 * time.Millisecond}
	defer func() { osvBatchURL, osvHTTPClient = origURL, origClient }()

	_, err := queryOSVBatch(context.Background(), osvBatchRequest{})
	if err == nil {
		t.Fatal("expected timeout error from a 2s server against a 100ms client; got nil — queryOSVBatch is not using osvHTTPClient")
	}
}

// TestOSVRequestsCarryUserAgent asserts both request paths identify
// themselves. The batch path is exercised end-to-end; the detail path
// is covered by the same source tripwire below plus the shared
// osvUserAgent var.
func TestOSVRequestsCarryUserAgent(t *testing.T) {
	var gotUA string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		w.Write([]byte(`{"results":[]}`))
	}))
	defer srv.Close()

	origURL := osvBatchURL
	osvBatchURL = srv.URL
	defer func() { osvBatchURL = origURL }()

	if _, err := queryOSVBatch(context.Background(), osvBatchRequest{}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(gotUA, "aveloxis/") {
		t.Errorf("OSV batch request User-Agent = %q, want aveloxis/<version> prefix", gotUA)
	}
}

// TestOSVClientSourceContract is the drift tripwire:
//   - http.DefaultClient must never return to vulnerability.go
//     (no-timeout = the goroutine-hang class)
//   - both request sites must set a User-Agent
//   - the shared client must carry an explicit timeout
func TestOSVClientSourceContract(t *testing.T) {
	src, err := os.ReadFile("vulnerability.go")
	if err != nil {
		t.Fatal(err)
	}
	// Strip // line comments before scanning — the doc comment on
	// osvHTTPClient legitimately NAMES http.DefaultClient to explain
	// why it's banned (the v0.21.5 / v0.27.4 false-match lesson).
	var code []string
	for line := range strings.SplitSeq(string(src), "\n") {
		if idx := strings.Index(line, "//"); idx >= 0 {
			line = line[:idx]
		}
		code = append(code, line)
	}
	s := strings.Join(code, "\n")

	if strings.Contains(s, "http.DefaultClient") {
		t.Error("vulnerability.go uses http.DefaultClient — it has NO timeout; use osvHTTPClient (30s house standard)")
	}
	if got := strings.Count(s, `Header.Set("User-Agent", osvUserAgent)`); got < 2 {
		t.Errorf("found %d User-Agent header sets, want 2 (querybatch + per-id detail fetch)", got)
	}
	if !strings.Contains(s, "osvHTTPClient = &http.Client{Timeout:") {
		t.Error("osvHTTPClient must declare an explicit Timeout")
	}
}

// TestOSVBatchLogsItsResponseSize — v0.29.71 review round 1 (F6): the OSV
// batch answer is the largest data body the vulnerability scan reads, and
// the operator's rule (2026-09-30) is no size limit but the largest sizes
// logged. The body is padded to 64 MiB, plus one byte per earlier run in
// this process, so it is a new high-water mark whatever an earlier test or
// run served (the marks are process-wide; whole-branch review F1).
func TestOSVBatchLogsItsResponseSize(t *testing.T) {
	osvSizeTestRuns++
	size := 64<<20 + osvSizeTestRuns
	// Padded INSIDE the JSON value: a decoder stops reading at the value's
	// end, so trailing padding would never be read (or counted).
	body := `{"results":[],"pad":"` + strings.Repeat("x", size-len(`{"results":[],"pad":""}`)) + `"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	origURL := osvBatchURL
	osvBatchURL = srv.URL
	t.Cleanup(func() { osvBatchURL = origURL })
	var buf lockedBuffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	if _, err := queryOSVBatch(context.Background(), osvBatchRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); !strings.Contains(got, "response size high-water mark") || !strings.Contains(got, "source=osv-batch") || !strings.Contains(got, "bytes="+strconv.Itoa(size)) {
		t.Errorf("the OSV batch body's size must be logged as a high-water mark; log:\n%s", got)
	}
}

// osvSizeTestRuns counts TestOSVBatchLogsItsResponseSize's runs in this
// process (-count=N).
var osvSizeTestRuns int
