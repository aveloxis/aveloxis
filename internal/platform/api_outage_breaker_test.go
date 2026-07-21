// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.34 fleet-level API-outage circuit breaker tests. Motivating
// incident: the 2026-07-21 GitHub 502 storm (2 hours, 1,044/1,045
// failures on POST /graphql, 160 exhausted retry budgets) — per-request
// backoff can never outlast an incident longer than its own budget, so
// the pool-level breaker pauses new CLAIMS while in-flight retries act
// as the probes that reopen it.

package platform

import (
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func breakerPool() *KeyPool {
	return NewKeyPool([]string{"tok1", "tok2"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func ok200(kp *KeyPool) {
	kp.UpdateFromResponse(kp.keys[0], &http.Response{StatusCode: 200, Header: http.Header{}})
}

func TestAPIOutageBreakerTripsOnlyAtThreshold(t *testing.T) {
	kp := breakerPool()
	for i := 0; i < APIOutageThreshold-1; i++ {
		kp.NoteServerError()
	}
	if !kp.APIHealthy() {
		t.Fatalf("breaker must stay closed below %d consecutive failures", APIOutageThreshold)
	}
	kp.NoteServerError()
	if kp.APIHealthy() {
		t.Fatalf("breaker must open at %d consecutive failures", APIOutageThreshold)
	}
}

// The measured 2026-07-21 storm had 58%% per-retry success — a brownout
// like that must keep grinding through per-request backoff, never
// pausing the fleet. Consecutive-without-success is the signal.
func TestAPIOutageBreakerBrownoutNeverTrips(t *testing.T) {
	kp := breakerPool()
	for round := 0; round < 10; round++ {
		for i := 0; i < APIOutageThreshold-1; i++ {
			kp.NoteServerError()
		}
		ok200(kp) // one success anywhere in the fleet resets the count
	}
	if !kp.APIHealthy() {
		t.Fatal("interleaved successes must keep the breaker closed (brownout ≠ outage)")
	}
}

func TestAPIOutageBreakerSuccessClosesInstantly(t *testing.T) {
	kp := breakerPool()
	for i := 0; i < APIOutageThreshold; i++ {
		kp.NoteServerError()
	}
	if kp.APIHealthy() {
		t.Fatal("precondition: breaker open")
	}
	ok200(kp)
	if !kp.APIHealthy() {
		t.Fatal("ANY 2xx must close the breaker immediately — in-flight retries are the probes")
	}
	// And the counter restarts from zero: one more error must not re-trip.
	kp.NoteServerError()
	if !kp.APIHealthy() {
		t.Fatal("recovery must reset the consecutive counter")
	}
}

func TestAPIOutageBreakerPauseExtendsWhileErrorsContinue(t *testing.T) {
	kp := breakerPool()
	for i := 0; i < APIOutageThreshold; i++ {
		kp.NoteServerError()
	}
	kp.mu.Lock()
	first := kp.apiPauseUntil
	// Simulate the probe window having nearly elapsed while in-flight
	// jobs are still failing.
	kp.apiPauseUntil = time.Now().Add(time.Second)
	kp.mu.Unlock()
	kp.NoteServerError() // still at/above threshold → pause re-extends
	kp.mu.Lock()
	second := kp.apiPauseUntil
	kp.mu.Unlock()
	if !second.After(first.Add(-APIOutagePause)) || time.Until(second) < APIOutagePause-time.Minute {
		t.Fatal("continued errors at/above threshold must keep extending the pause — a long outage stays paused without timers")
	}
}

func TestAPIOutageBreakerProbeWindowExpiry(t *testing.T) {
	kp := breakerPool()
	for i := 0; i < APIOutageThreshold; i++ {
		kp.NoteServerError()
	}
	kp.mu.Lock()
	kp.apiPauseUntil = time.Now().Add(-time.Second) // window elapsed, traffic stopped
	kp.mu.Unlock()
	if !kp.APIHealthy() {
		t.Fatal("an elapsed probe window must reopen claims so probe jobs can test the API")
	}
}

func TestAPIOutageBreakerNilPoolSafe(t *testing.T) {
	var kp *KeyPool
	kp.NoteServerError() // must not panic
	if !kp.APIHealthy() {
		t.Fatal("nil pool (keyless deployment) must always report healthy")
	}
}

func TestAPIOutageBreakerConcurrentAccess(t *testing.T) {
	kp := breakerPool()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				kp.NoteServerError()
				kp.APIHealthy()
				ok200(kp)
			}
		}()
	}
	wg.Wait()
}

// Source pins: BOTH 5xx retry branches feed the breaker (the storm was
// GraphQL-dominated; a REST-only hook would have missed it), and the
// 2xx path in UpdateFromResponse is what closes it.
func TestAPIOutageBreakerWiring(t *testing.T) {
	httpSrc, err := os.ReadFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(httpSrc), "c.keys.NoteServerError()") {
		t.Error("httpclient.go's 5xx branch must call c.keys.NoteServerError()")
	}
	gqlSrc, err := os.ReadFile("graphql.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gqlSrc), "c.keys.NoteServerError()") {
		t.Error("graphql.go's 5xx branch must call c.keys.NoteServerError() — the 2026-07-21 storm was 1,044/1,045 GraphQL")
	}
	rlSrc, err := os.ReadFile("ratelimit.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(rlSrc)
	twoxx := strings.Index(s, "resp.StatusCode >= 200 && resp.StatusCode < 300")
	if twoxx < 0 || !strings.Contains(s[twoxx:twoxx+700], "consecutive5xx = 0") {
		t.Error("UpdateFromResponse's 2xx branch must reset the breaker (any success closes it)")
	}
}
