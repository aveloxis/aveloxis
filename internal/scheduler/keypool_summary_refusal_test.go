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
