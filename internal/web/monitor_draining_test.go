// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"os"
	"strings"
	"testing"
)

// TestWebMonitorShowsDrainingSeparately — v0.29.64: the web monitor page
// renders QueueStats' "draining" count beside Collecting (see
// TestQueueStatsCountsDrainParkedSeparately in internal/db).
func TestWebMonitorShowsDrainingSeparately(t *testing.T) {
	data, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, `{{.Stats.draining}}</div><div style="color:#666;font-size:0.85rem">Parked (drain / heal)</div>`) {
		t.Error(`the monitor template must render {{.Stats.draining}} as "Parked (drain / heal)"`)
	}
}
