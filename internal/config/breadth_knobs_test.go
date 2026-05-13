// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"strings"
	"testing"
)

// v0.20.17: breadth_interval_minutes, breadth_batch_size,
// breadth_cooldown_days are now configurable. Pre-fix these
// were hardcoded to 6h / 100 / no-cooldown, which capped
// throughput to 400 contributors/day = 9.6 years to cover a
// 1.4M-contributor fleet once.

func TestCollectionConfigHasBreadthKnobs(t *testing.T) {
	data, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	needles := []string{
		`BreadthIntervalMinutes`,
		`"breadth_interval_minutes"`,
		`BreadthBatchSize`,
		`"breadth_batch_size"`,
		`BreadthCooldownDays`,
		`"breadth_cooldown_days"`,
	}
	for _, n := range needles {
		if !strings.Contains(src, n) {
			t.Errorf("config.go missing breadth knob %q — pre-v0.20.17 the breadth worker was hardcoded to 6h cycles + 100 batch + no cooldown, giving 400 contributors/day = 9.6 years for one pass over 1.4M contributors", n)
		}
	}
}

// TestBreadthIntervalDurationFallback pins the fallback default
// (matches the EnrichInterval / SearchResolveInterval pattern).
// Operators with an older aveloxis.json missing the new keys
// still get sensible behavior.
func TestBreadthIntervalDurationFallback(t *testing.T) {
	c := &CollectionConfig{}
	// 15-minute default matches the new throughput target
	// (2000 batch × 96 ticks/day = 192K contribs/day).
	got := c.BreadthIntervalDuration()
	wantMins := 15
	if int(got.Minutes()) != wantMins {
		t.Errorf("BreadthIntervalDuration() default = %v, want %d minutes — first-pass coverage on a 1.4M-contributor fleet needs throughput around %d/day, which the 15-minute default achieves with the 2000 batch default", got, wantMins, wantMins)
	}
}

func TestBreadthBatchSizeDefault(t *testing.T) {
	c := &CollectionConfig{}
	got := c.BreadthBatchSizeOrDefault()
	if got != 2000 {
		t.Errorf("BreadthBatchSizeOrDefault() default = %d, want 2000", got)
	}
}

func TestBreadthCooldownDurationFallback(t *testing.T) {
	c := &CollectionConfig{}
	got := c.BreadthCooldownDuration()
	wantHours := 24 * 7
	if int(got.Hours()) != wantHours {
		t.Errorf("BreadthCooldownDuration() default = %v, want 7 days (168 hours)", got)
	}
}
