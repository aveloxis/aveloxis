// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// TestReconcileHealRefusalFallsBackToConsolidation pins the v0.27.49
// fallback: a "dataless" dup (last_collected NULL) whose heal is
// refused (healed=false, nil error — residual children tripped the FK
// fail-safe, the 2026-07-22 apache/baremaps class) must route to
// DedupRenamedRepoPair instead of stranding the row.
func TestReconcileHealRefusalFallsBackToConsolidation(t *testing.T) {
	src, err := os.ReadFile("reconcile_repos.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	at := strings.Index(code, "heal refused (residual children)")
	if at < 0 {
		t.Fatal("reconcile must log the heal-refused fallback — the healed=false branch may not silently skip")
	}
	end := at + 700
	if end > len(code) {
		end = len(code)
	}
	window := code[at:end]
	if !strings.Contains(window, "DedupRenamedRepoPair(") {
		t.Error("the heal-refused branch must fall back to db.DedupRenamedRepoPair — " +
			"the consolidation machinery is what handles residual children")
	}
}
