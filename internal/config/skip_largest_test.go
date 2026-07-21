// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// v0.27.35 large-repo skip knob: percent-to-fraction conversion with
// misconfiguration collapsing to "disabled" (never to a surprise
// clamp), and the JSON surface.
func TestSkipLargestFraction(t *testing.T) {
	var c CollectionConfig
	if got := c.SkipLargestFraction(); got != 0 {
		t.Errorf("default must be disabled (0), got %v", got)
	}
	c.SkipLargestPercent = 0.5
	if got := c.SkipLargestFraction(); got != 0.005 {
		t.Errorf("0.5%% must convert to fraction 0.005, got %v", got)
	}
	c.SkipLargestPercent = -1
	if got := c.SkipLargestFraction(); got != 0 {
		t.Errorf("negative percent must disable, got %v", got)
	}
	c.SkipLargestPercent = 100
	if got := c.SkipLargestFraction(); got != 0 {
		t.Errorf("percent >= 100 is certainly a misconfiguration — must disable, got %v", got)
	}
	var parsed CollectionConfig
	if err := json.Unmarshal([]byte(`{"skip_largest_percent": 2.5}`), &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.SkipLargestFraction() != 0.025 {
		t.Errorf("skip_largest_percent JSON key must map to the field, got %v", parsed.SkipLargestFraction())
	}
}
