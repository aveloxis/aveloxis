// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package ecosystems

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// v0.25.0 — source-level circuit breaker on the ecosyste.ms client.
//
// Mirrors the v0.22.12 BreadthWorker circuit breaker. Trips after
// CircuitBreakerThreshold consecutive transient (5xx or transport)
// failures across distinct calls; pauses the source for
// CircuitBreakerPause; reopens for probing afterward.
//
// Per the v0.25.0 scanner contract: when the breaker is open,
// LookupPackages returns (nil, nil) — treated by CompositeScanner
// as "source returned no data" rather than "source errored." So
// the rest of the scan proceeds, last_run gets stamped, and the
// fleet stops generating one ERROR log line per repo against a
// broken upstream.

func TestCircuitBreakerThresholdConstantsExist(t *testing.T) {
	if CircuitBreakerThreshold <= 0 {
		t.Errorf("CircuitBreakerThreshold must be positive, got %d", CircuitBreakerThreshold)
	}
	if CircuitBreakerPause <= 0 {
		t.Errorf("CircuitBreakerPause must be positive, got %v", CircuitBreakerPause)
	}
}

// TestCircuitBreakerTripsAfterThresholdConsecutive5xx pins the trip
// behavior. We point the client at a server that always returns
// 500. After CircuitBreakerThreshold calls each returning a 5xx
// error, the next call should short-circuit with (nil, nil) and
// the upstream server should NOT receive that probe.
func TestCircuitBreakerTripsAfterThresholdConsecutive5xx(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "down", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	c := New(Options{BaseURL: server.URL})

	// Burn exactly CircuitBreakerThreshold failures.
	for i := 0; i < CircuitBreakerThreshold; i++ {
		_, err := c.LookupPackages(context.Background(), "https://github.com/x/y")
		if err == nil {
			t.Fatalf("call %d: expected error from 5xx, got nil", i+1)
		}
	}
	hitsAtTrip := hits.Load()
	if hitsAtTrip != int64(CircuitBreakerThreshold) {
		t.Fatalf("expected %d hits before trip, got %d", CircuitBreakerThreshold, hitsAtTrip)
	}

	// Next call: breaker must short-circuit (no upstream hit, no error).
	dists, err := c.LookupPackages(context.Background(), "https://github.com/x/y")
	if err != nil {
		t.Errorf("post-trip call must return nil err (treated by scanner as no-data), got %v", err)
	}
	if len(dists) != 0 {
		t.Errorf("post-trip call must return zero distributions, got %d", len(dists))
	}
	if hits.Load() != hitsAtTrip {
		t.Errorf("post-trip call must NOT hit upstream — fleet-wide pullback. Got %d total hits, expected %d", hits.Load(), hitsAtTrip)
	}
}

// TestCircuitBreakerResetsOnSuccess pins that a successful response
// (or 404) clears the consecutive-failure counter so a flaky upstream
// can't slowly accumulate enough failures to trip the breaker over
// the course of hours.
func TestCircuitBreakerResetsOnSuccess(t *testing.T) {
	var hitNum atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hitNum.Add(1)
		// Pattern: alternate 500 / 200. Counter never reaches threshold.
		if n%2 == 1 {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	c := New(Options{BaseURL: server.URL})

	// Run 4× threshold calls — half error, half succeed. Breaker
	// must NOT trip because consecutive failures never reach the
	// threshold.
	for i := 0; i < 4*CircuitBreakerThreshold; i++ {
		_, _ = c.LookupPackages(context.Background(), "https://github.com/x/y")
	}

	// Final assertion: breaker is not open (a fresh call would
	// reach the upstream and we'd see an extra hit).
	preFinal := hitNum.Load()
	_, _ = c.LookupPackages(context.Background(), "https://github.com/x/y")
	if hitNum.Load() == preFinal {
		t.Error("breaker tripped under alternating success/fail pattern — must reset counter on each success so consecutive-failure semantics are preserved")
	}
}

// TestCircuitBreakerProbesAfterPauseElapses pins the reopen
// behavior. We set CircuitBreakerPause via the package var, trip
// the breaker, fast-forward by setting cbOpenUntil to a past time,
// and confirm the next call reaches the upstream.
//
// Without a clock-injection layer the test directly munges client
// state — the alternative (sleeping for an hour) is intolerable in
// CI, and adding a Clock interface for one test is overkill.
func TestCircuitBreakerProbesAfterPauseElapses(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)

	c := New(Options{BaseURL: server.URL})

	// Manually open the breaker into the past (simulating elapsed pause).
	c.cbMu.Lock()
	c.cbConsecutive5xx = CircuitBreakerThreshold
	c.cbOpenUntil = time.Now().Add(-time.Minute)
	c.cbMu.Unlock()

	preCall := hits.Load()
	_, err := c.LookupPackages(context.Background(), "https://github.com/x/y")
	if err != nil {
		t.Fatalf("post-pause call: %v", err)
	}
	if hits.Load() != preCall+1 {
		t.Errorf("post-pause call must reach upstream (breaker reopened) — got %d hits, expected %d", hits.Load(), preCall+1)
	}

	// State should be reset after the probe.
	c.cbMu.Lock()
	openUntil := c.cbOpenUntil
	consecutive := c.cbConsecutive5xx
	c.cbMu.Unlock()
	if !openUntil.IsZero() {
		t.Errorf("cbOpenUntil must be zero after successful probe, got %v", openUntil)
	}
	if consecutive != 0 {
		t.Errorf("cbConsecutive5xx must be zero after successful probe, got %d", consecutive)
	}
}

// TestCircuitBreaker429DoesNotTripBreaker pins that 429 (rate limit)
// is handled by per-call retry policy, NOT by the source-level
// breaker. The upstream is healthy when it 429s; it's just asking
// us to slow down for this specific call.
func TestCircuitBreaker429DoesNotTripBreaker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	c := New(Options{BaseURL: server.URL})

	// Burn 2× threshold calls with 429. Breaker must NOT trip.
	for i := 0; i < 2*CircuitBreakerThreshold; i++ {
		_, _ = c.LookupPackages(context.Background(), "https://github.com/x/y")
	}

	c.cbMu.Lock()
	openUntil := c.cbOpenUntil
	consecutive := c.cbConsecutive5xx
	c.cbMu.Unlock()

	if !openUntil.IsZero() {
		t.Errorf("429 must NOT trip the breaker (upstream is healthy, just throttling) — got cbOpenUntil=%v", openUntil)
	}
	if consecutive != 0 {
		t.Errorf("429 must NOT increment the consecutive-failure counter — got %d", consecutive)
	}
}

// TestCircuitBreakerTransportErrorsCountToward429 pins that
// connection-level errors (DNS, ECONNREFUSED, TLS) DO count as
// transient failures and trip the breaker. The upstream is
// effectively unreachable — same operational signal as 5xx.
func TestCircuitBreakerTransportErrorsTripBreaker(t *testing.T) {
	// Point the client at an unreachable port. http.Get will fail
	// at the transport layer.
	c := New(Options{
		BaseURL: "http://127.0.0.1:1", // port 1 typically refuses connections
		HTTPClient: &http.Client{
			Timeout: 100 * time.Millisecond, // fast-fail
		},
	})

	for i := 0; i < CircuitBreakerThreshold; i++ {
		_, _ = c.LookupPackages(context.Background(), "https://github.com/x/y")
	}

	c.cbMu.Lock()
	openUntil := c.cbOpenUntil
	c.cbMu.Unlock()

	if openUntil.IsZero() {
		t.Error("transport-level errors must trip the circuit breaker — upstream is unreachable, same signal as 5xx")
	}
}
