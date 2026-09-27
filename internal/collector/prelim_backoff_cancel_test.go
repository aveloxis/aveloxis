// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// refusingTransport fails every round trip with a transient error, so
// headWithRetry enters its backoff.
type refusingTransport struct{ attempts int }

func (t *refusingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	t.attempts++
	return nil, errors.New("dial tcp 127.0.0.1:1: connection refused")
}

// TestHeadWithRetryBackoffStopsOnCancel pins batch 7b review round 4: the
// probe's retry backoff (1 s, 3 s, 9 s) waits on the context. Before, a
// plain time.Sleep noticed a cancel only after the NEXT attempt failed —
// up to 9 s late, at the one probe prelim, mark-gone-repos and
// reconcile-repos share, longer than most legal shutdown graces.
func TestHeadWithRetryBackoffStopsOnCancel(t *testing.T) {
	saved := probeTransport
	rt := &refusingTransport{}
	probeTransport = rt
	t.Cleanup(func() { probeTransport = saved })

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond) // the first attempt has failed; the 1 s backoff is running
		cancel()
	}()
	start := time.Now()
	_, err := headWithRetry(ctx, "http://127.0.0.1:1/x")
	took := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("headWithRetry after a cancel mid-backoff = %v; want context.Canceled", err)
	}
	if took > 500*time.Millisecond {
		t.Errorf("headWithRetry returned %s after the cancel; want at once (the 1 s backoff must wait on ctx)", took)
	}
	if rt.attempts != 1 {
		t.Errorf("%d attempts; want 1 (no retry after the cancel)", rt.attempts)
	}
}
