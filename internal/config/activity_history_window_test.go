// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

// activity_history_window_test.go — TDD for the v0.27.58
// activity_history_window_days knob (operator decision 2026-07-30:
// "parameterizable windows, with the default being 180 day windows").
// The GitHub contributionsCollection window may not exceed one year,
// so the accessor clamps at 365; non-positive falls back to the 180
// default. Per the config-knobs-end-to-end rule, the value→behavior
// path (config JSON → window spans actually generated) is tested in
// internal/platform/github/history_windows_test.go.

import (
	"encoding/json"
	"testing"
)

func TestActivityHistoryWindowDaysDefault(t *testing.T) {
	var c CollectionConfig
	if got := c.ActivityHistoryWindowDaysOrDefault(); got != 180 {
		t.Errorf("zero-value accessor = %d, want 180", got)
	}
	if DefaultConfig().Collection.ActivityHistoryWindowDays != 180 {
		t.Error("DefaultConfig must set activity_history_window_days to 180")
	}
}

func TestActivityHistoryWindowDaysClamps(t *testing.T) {
	c := CollectionConfig{ActivityHistoryWindowDays: 500}
	if got := c.ActivityHistoryWindowDaysOrDefault(); got != 365 {
		t.Errorf("window above the GitHub 1-year API limit must clamp to 365, got %d", got)
	}
	c.ActivityHistoryWindowDays = -3
	if got := c.ActivityHistoryWindowDaysOrDefault(); got != 180 {
		t.Errorf("non-positive window must fall back to the 180 default, got %d", got)
	}
	c.ActivityHistoryWindowDays = 90
	if got := c.ActivityHistoryWindowDaysOrDefault(); got != 90 {
		t.Errorf("in-range value must pass through, got %d", got)
	}
}

func TestActivityHistoryWindowDaysJSONTag(t *testing.T) {
	var c CollectionConfig
	if err := json.Unmarshal([]byte(`{"activity_history_window_days": 90}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.ActivityHistoryWindowDays != 90 {
		t.Error(`the JSON key must be "activity_history_window_days"`)
	}
}
