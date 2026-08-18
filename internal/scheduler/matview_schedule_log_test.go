// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"strings"
	"testing"
)

// v0.27.96 — log-the-effective-value applied to the matview schedule. The
// 2026-08-18 incident: the operator set matview_rebuild_day to "disable"
// (an unrecognized value pre-v0.27.96) and believed rebuilds were off; the
// silent Saturday fallback kept them firing and there was NO startup line
// stating the effective schedule, so the misconfiguration was invisible
// until the next 11-hour rebuild showed up in pg_stat_statements.
func TestSchedulerLogsEffectiveMatviewSchedule(t *testing.T) {
	data, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatalf("read scheduler.go: %v", err)
	}
	src := string(data)

	if !strings.Contains(src, "matview rebuild schedule") {
		t.Error("scheduler startup must log the EFFECTIVE matview schedule " +
			"(\"matview rebuild schedule\" line with the raw config value, " +
			"the effective weekday, and the dm_-skip flag) — the operator's " +
			"only runtime verification that a disable actually took effect")
	}
	if !strings.Contains(src, "MatviewRebuildDayRecognized()") {
		t.Error("scheduler startup must WARN on an unrecognized " +
			"matview_rebuild_day value (via MatviewRebuildDayRecognized) — " +
			"the silent Saturday fallback is how \"disable\" kept 11-hour " +
			"rebuilds running")
	}
}
