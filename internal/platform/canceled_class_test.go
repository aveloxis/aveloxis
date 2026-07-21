// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

// v0.27.28 — context cancellation is ClassCanceled, never Transient.
// The phase-0 mapping of cancellation to Transient was deliberate
// ("keeps shutdown logs free of spurious ERROR lines") and harmless
// until subdivision (v0.20.8) and the size-1 REST fallback (v0.20.20)
// started keying RETRY behavior off the class; once v0.27.25 made
// `aveloxis stop` actually cancel contexts, every shutdown fought
// itself: 85 pointless 10→5→2→1 cascades + 85 doomed REST fallbacks
// + 87 misleading "retrying" WARNs in one minute of the 2026-07-21
// production log.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClassifyErrorCanceledIsTerminalNotTransient(t *testing.T) {
	cases := []error{
		context.Canceled,
		fmt.Errorf("graphql PR batch: %w", context.Canceled), // the exact production wrap
		fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", context.Canceled)),
	}
	for _, err := range cases {
		if got := ClassifyError(err); got != ClassCanceled {
			t.Errorf("ClassifyError(%v) = %v, want ClassCanceled — Transient tells subdivision/fallback machinery to FIGHT the shutdown", err, got)
		}
	}
}

// TestClassifyErrorDeadlineStaysTransient pins the deliberate split:
// a timed-out request is retry-worthy; a cancelled one never is.
func TestClassifyErrorDeadlineStaysTransient(t *testing.T) {
	if got := ClassifyError(context.DeadlineExceeded); got != ClassTransient {
		t.Errorf("ClassifyError(DeadlineExceeded) = %v, want ClassTransient — request timeouts are legitimately retryable", got)
	}
}

func TestClassCanceledString(t *testing.T) {
	if ClassCanceled.String() != "canceled" {
		t.Errorf("ClassCanceled.String() = %q, want %q", ClassCanceled.String(), "canceled")
	}
}

// TestHTTPClientQuietOnCanceledContext — a cancelled request must
// return promptly WITHOUT the "retrying" WARN it will never honor.
func TestHTTPClientQuietOnCanceledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // hold the request so cancel lands mid-flight
	}))
	defer srv.Close()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	keys := NewKeyPool([]string{"tok"}, logger)
	client := NewHTTPClient(srv.URL, keys, logger, AuthGitHub)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()

	start := time.Now()
	_, err := client.Get(ctx, "/") // Get prepends the base URL

	if err == nil {
		t.Fatal("expected error from cancelled request")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v — cancellation must bail immediately, not ride the retry chain", elapsed)
	}
	if strings.Contains(buf.String(), "retrying") {
		t.Errorf("log claims a retry that will never happen:\n%s", buf.String())
	}
}
