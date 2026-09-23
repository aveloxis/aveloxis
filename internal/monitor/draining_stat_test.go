// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package monitor

import (
	"os"
	"strings"
	"testing"
)

// TestDashboardShowsDrainingSeparately — v0.29.64: QueueStats counts
// drain-parked rows as "draining" (they hold no worker slot); the
// dashboard renders that count next to Collecting, so Collecting can no
// longer exceed the worker count. The store behavior is
// TestQueueStatsCountsDrainParkedSeparately (internal/db).
func TestDashboardShowsDrainingSeparately(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `<div class="label">Parked (drain / heal)</div></div>`+"`"+`, stats["draining"])`) {
		t.Error(`monitor.go must render stats["draining"] as "Parked (drain / heal)"`)
	}
}
