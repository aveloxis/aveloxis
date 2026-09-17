// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bytes"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestKeyPoolSummaryReportsStandingRefusals: on chaoss.tv 2026-09-17 every
// summary line read core_remaining_min=4305 while one key was refused
// 4,388 times — the tracked balance was the only per-key signal. The
// summary must report keys whose refusal still stands, per bucket.
func TestKeyPoolSummaryReportsStandingRefusals(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	kp := platform.NewKeyPool([]string{"refused", "healthy"}, logger)
	keys, _ := kp.Snapshot()
	if len(keys) != 2 {
		t.Fatalf("snapshot has %d keys", len(keys))
	}
	h := http.Header{}
	h.Set("X-RateLimit-Resource", "core")
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10))
	key, release, err := kp.Acquire(t.Context(), platform.ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	kp.UpdateFromResponse(key, &http.Response{StatusCode: http.StatusForbidden, Header: h})
	release()

	s := &Scheduler{ghKeys: kp, logger: logger}
	s.logKeyPoolSummary()
	line := buf.String()
	for _, want := range []string{"refused_core_now=1", "refused_graphql_now=0", "refused_search_now=0", "refusals_lifetime=1"} {
		if !strings.Contains(line, want) {
			t.Errorf("summary line lacks %q:\n%s", want, line)
		}
	}
}

// TestKeyPoolSummaryLifetimeCountersSurviveInvalidation (Copilot review
// 5237013602 on PR #209): the lifetime counters were summed after the
// invalid-key skip, so invalidating a key subtracted its history and a
// "lifetime" total went down. Current-state gauges stay limited to live keys.
func TestKeyPoolSummaryLifetimeCountersSurviveInvalidation(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	kp := platform.NewKeyPool([]string{"only"}, logger)
	key, release, err := kp.Acquire(t.Context(), platform.ResourceCore)
	if err != nil {
		t.Fatal(err)
	}
	refusal := http.Header{}
	refusal.Set("X-RateLimit-Remaining", "0")
	refusal.Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(10*time.Minute).Unix(), 10))
	kp.UpdateFromResponse(key, &http.Response{StatusCode: http.StatusForbidden, Header: refusal})
	secondary := http.Header{}
	secondary.Set("Retry-After", "1")
	kp.UpdateFromResponse(key, &http.Response{StatusCode: http.StatusTooManyRequests, Header: secondary})
	release()
	kp.InvalidateKey(key)

	buf.Reset()
	(&Scheduler{ghKeys: kp, logger: logger}).logKeyPoolSummary()
	line := buf.String()
	for _, want := range []string{"refusals_lifetime=1", "secondary_hits_lifetime=1", "keys_alive=0", "refused_core_now=0"} {
		if !strings.Contains(line, want) {
			t.Errorf("summary line lacks %q:\n%s", want, line)
		}
	}
}
