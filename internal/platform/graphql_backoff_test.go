// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// graphql_backoff_test.go — v0.27.87 (Copilot review round, PR #173):
// (a) the retry loop must NOT sleep after the FINAL allowed attempt —
// every exhausted chain paid one full tail backoff before surfacing
// the error, which partially defeated WithGraphQLFastFail (whose
// whole point is failing fast so subdivision can take over, and
// subdivision MULTIPLIES exhausted chains); (b) the backoff sleep is
// an injectable seam so backoff-shape tests stop paying real
// wall-clock (TestFetchContributorActivityServerErrorFastFailAndSkip
// measured 18.12s of genuine jittered sleeps pre-seam).

package platform

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestRetrySleepSkipsFinalAttempt: on the last allowed attempt the
// helper returns immediately — sleeping there only delays the
// caller's failure.
func TestRetrySleepSkipsFinalAttempt(t *testing.T) {
	start := time.Now()
	if err := retrySleep(context.Background(), 5*time.Second, 2, 3); err != nil {
		t.Fatalf("final-attempt retrySleep must be a no-op, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("final-attempt retrySleep slept %v — it must return immediately", elapsed)
	}
}

// TestSetGraphQLSleepForTest pins the seam contract: override + restore.
func TestSetGraphQLSleepForTest(t *testing.T) {
	called := 0
	restore := SetGraphQLSleepForTest(func(ctx context.Context, d time.Duration) error {
		called++
		return nil
	})
	if err := retrySleep(context.Background(), time.Hour, 0, 3); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("override must be used, called=%d", called)
	}
	restore()
	start := time.Now()
	_ = retrySleep(context.Background(), 10*time.Millisecond, 0, 3)
	if called != 1 {
		t.Error("restore must reinstate the real sleep")
	}
	if time.Since(start) < 10*time.Millisecond {
		t.Error("restored sleep must actually wait")
	}
}

// TestGraphQLSleepsBetweenAttemptsOnly is the deterministic shape
// proof: a persistent-5xx endpoint under the fast-fail budget of N
// attempts must sleep exactly N-1 times (between attempts), never
// after the last one.
func TestGraphQLSleepsBetweenAttemptsOnly(t *testing.T) {
	var mu sync.Mutex
	sleeps := 0
	restore := SetGraphQLSleepForTest(func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		sleeps++
		mu.Unlock()
		return nil
	})
	defer restore()

	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := NewHTTPClient(srv.URL, NewKeyPool([]string{"t"}, quiet), quiet, AuthGitHub)
	ctx := WithGraphQLFastFail(t.Context())
	var dest json.RawMessage
	if err := c.GraphQL(ctx, "query { x }", nil, &dest); err == nil {
		t.Fatal("persistent 502s must exhaust the fast-fail budget")
	}
	if requests != graphqlFastFailRetries {
		t.Fatalf("expected exactly %d attempts, got %d", graphqlFastFailRetries, requests)
	}
	if sleeps != graphqlFastFailRetries-1 {
		t.Errorf("expected %d between-attempt sleeps (never one after the final attempt), got %d",
			graphqlFastFailRetries-1, sleeps)
	}
}
