// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import "testing"

// v0.27.96 — the "disable" trap, found live 2026-08-18: the operator set
// matview_rebuild_day to "disable" believing rebuilds were off, but the
// accessor accepted only "disabled"/"none"/"off" and SILENTLY fell back to
// Saturday for anything else — rebuilds kept firing (the Aug 8 rebuild ran
// 11h23m; another queued Aug 14). Classic silent-coercion class
// ([[feedback_test_config_knobs_end_to_end]]).
func TestMatviewRebuildWeekdayDisableAliases(t *testing.T) {
	for _, v := range []string{"disabled", "disable", "none", "off", "DISABLE", "Disabled"} {
		c := CollectionConfig{MatviewRebuildDay: v}
		if got := c.MatviewRebuildWeekday(); got != -1 {
			t.Errorf("MatviewRebuildWeekday(%q) = %d, want -1 (disabled) — "+
				"a silently-ignored disable value means the weekly rebuild "+
				"keeps firing on the fallback day", v, got)
		}
	}
	// Unrecognized values still fall back to Saturday (6) — the startup
	// effective-value log is what makes that fallback visible.
	c := CollectionConfig{MatviewRebuildDay: "sabado"}
	if got := c.MatviewRebuildWeekday(); got != 6 {
		t.Errorf("MatviewRebuildWeekday(unrecognized) = %d, want 6 (Saturday fallback)", got)
	}
}

// TestMatviewRebuildDayRecognized reports whether a raw config value maps to
// a deliberate schedule choice (a real day or a disable alias) as opposed to
// the silent Saturday fallback. The scheduler's startup log uses it to WARN
// on typos.
func TestMatviewRebuildDayRecognized(t *testing.T) {
	for raw, want := range map[string]bool{
		"saturday": true, "thursday": true, "disabled": true, "disable": true,
		"none": true, "off": true, "": true, // empty = deliberate default
		"sabado": false, "disbaled": false,
	} {
		c := CollectionConfig{MatviewRebuildDay: raw}
		if got := c.MatviewRebuildDayRecognized(); got != want {
			t.Errorf("MatviewRebuildDayRecognized(%q) = %v, want %v", raw, got, want)
		}
	}
}
