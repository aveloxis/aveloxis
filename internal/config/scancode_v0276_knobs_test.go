// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// v0.27.6 — two new scancode knobs.
//
// scancode_timeout_cap_strikes: sideline a repo after N consecutive
// wall-clock timeouts AT the adaptive-timeout cap. The v0.23.8 design
// deliberately never sidelined timeout-class failures; the June 2026
// production logs showed the consequence — pytorch/docs and
// WHO/smart-html claimed 27× each (24h at-cap timeout → re-claim →
// repeat), each cycle burning a full worker-day.
//
// scancode_ignore_globs: operator-supplied path globs passed to the
// scancode subprocess as repeated `--ignore <glob>` flags. Empty by
// default so pre-v0.27.6 behavior is unchanged.

func TestScancodeTimeoutCapStrikesDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeTimeoutCapStrikes != 3 {
		t.Errorf("DefaultConfig must set ScancodeTimeoutCapStrikes = 3, got %d",
			cfg.Collection.ScancodeTimeoutCapStrikes)
	}
}

func TestScancodeTimeoutCapStrikesAccessorClampsNonPositive(t *testing.T) {
	var c CollectionConfig
	if got := c.ScancodeTimeoutCapStrikesOrDefault(); got != 3 {
		t.Errorf("zero-value accessor must return 3, got %d", got)
	}
	c.ScancodeTimeoutCapStrikes = -5
	if got := c.ScancodeTimeoutCapStrikesOrDefault(); got != 3 {
		t.Errorf("negative input must clamp to 3, got %d — an at-cap timeout can never be cured by another attempt, so 'unlimited retries' must be unreachable", got)
	}
	c.ScancodeTimeoutCapStrikes = 7
	if got := c.ScancodeTimeoutCapStrikesOrDefault(); got != 7 {
		t.Errorf("explicit value must pass through, got %d", got)
	}
}

func TestScancodeIgnoreGlobsDefaultEmpty(t *testing.T) {
	cfg := DefaultConfig()
	if len(cfg.Collection.ScancodeIgnoreGlobs) != 0 {
		t.Errorf("ScancodeIgnoreGlobs must default to empty (no --ignore flags — pre-v0.27.6 behavior), got %v",
			cfg.Collection.ScancodeIgnoreGlobs)
	}
	if cfg.Collection.ScancodeIgnoreGlobsOrDefault() != nil {
		t.Error("accessor must normalize an empty list to nil")
	}
}

func TestScancodeV0276KnobsJSONRoundTrip(t *testing.T) {
	raw := []byte(`{"collection": {
		"scancode_timeout_cap_strikes": 5,
		"scancode_ignore_globs": ["*.min.js", "*/node_modules/*"]
	}}`)
	cfg := DefaultConfig()
	if err := json.Unmarshal(raw, cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Collection.ScancodeTimeoutCapStrikesOrDefault() != 5 {
		t.Errorf("scancode_timeout_cap_strikes must parse from JSON, got %d",
			cfg.Collection.ScancodeTimeoutCapStrikesOrDefault())
	}
	globs := cfg.Collection.ScancodeIgnoreGlobsOrDefault()
	if len(globs) != 2 || globs[0] != "*.min.js" || globs[1] != "*/node_modules/*" {
		t.Errorf("scancode_ignore_globs must parse from JSON, got %v", globs)
	}
}

// TestScancodeIgnoreGlobsAbsentVsEmptyEquivalent pins the
// config-knob end-to-end lesson: "[]" in aveloxis.example.json and an
// absent key must produce identical effective behavior through the
// accessor (both nil — no --ignore flags).
func TestScancodeIgnoreGlobsAbsentVsEmptyEquivalent(t *testing.T) {
	absent := DefaultConfig()
	explicit := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"collection": {"scancode_ignore_globs": []}}`), explicit); err != nil {
		t.Fatal(err)
	}
	if a, e := absent.Collection.ScancodeIgnoreGlobsOrDefault(), explicit.Collection.ScancodeIgnoreGlobsOrDefault(); a != nil || e != nil {
		t.Errorf("absent (%v) and explicit-empty (%v) must both be effective-nil", a, e)
	}
}
