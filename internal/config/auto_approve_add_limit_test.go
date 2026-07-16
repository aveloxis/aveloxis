// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// The v0.27.20 per-add approval knob: 0 (default) = every non-admin
// addition of not-yet-tracked repos requires admin approval; > 0
// auto-approves batches at or under the limit. Config-knobs rule:
// exercise config value → effective behavior via the accessor, and
// pin the JSON surface.
func TestAutoApproveAddLimitKnob(t *testing.T) {
	// Zero-value default: fully gated.
	var w WebConfig
	if got := w.AutoApproveAddLimitValue(); got != 0 {
		t.Errorf("default auto_approve_add_limit must be 0 (fully gated), got %d", got)
	}
	// Negative collapses to 0 — a negative limit has no meaning.
	w.AutoApproveAddLimit = -5
	if got := w.AutoApproveAddLimitValue(); got != 0 {
		t.Errorf("negative limit must collapse to 0, got %d", got)
	}
	// Positive passes through.
	w.AutoApproveAddLimit = 25
	if got := w.AutoApproveAddLimitValue(); got != 25 {
		t.Errorf("configured limit must pass through, got %d", got)
	}
	// JSON surface: the key must round-trip under its documented name.
	var parsed WebConfig
	if err := json.Unmarshal([]byte(`{"auto_approve_add_limit": 7}`), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.AutoApproveAddLimitValue() != 7 {
		t.Errorf("auto_approve_add_limit JSON key must map to the field, got %d",
			parsed.AutoApproveAddLimitValue())
	}
}
