// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRefreshAllAggregatesExists verifies the DB has a method to refresh
// dm_ tables for all repos at once, for use during the weekly matview rebuild.
func TestRefreshAllAggregatesExists(t *testing.T) {
	src, err := os.ReadFile("../db/aggregates.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "RefreshAllRepoAggregates") {
		t.Error("aggregates.go must contain RefreshAllRepoAggregates for weekly rebuild")
	}
}

// TestMatviewRebuildCallsAggregates verifies the scheduler's matview rebuild
// also refreshes dm_ aggregate tables.
func TestMatviewRebuildCallsAggregates(t *testing.T) {
	// Pass 28 (v0.28.18): was a fixed 1,200-char window after the first
	// "weekly matview rebuild" mention — the legacy scan-window class the
	// srctest ratchet retires; one added log line pushed the call out of
	// the window. The function body is the contract.
	body := srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) rebuildMatviews(")
	if !strings.Contains(body, "RefreshAllRepoAggregates(") {
		t.Error("weekly matview rebuild must also call RefreshAllRepoAggregates to populate dm_ tables")
	}
}
