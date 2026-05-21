// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// v0.23.8 — operator-configurable scancode wall-clock timeout +
// adaptive cap. Pre-v0.23.8 these were hardcoded constants in
// internal/collector/scancode_worker.go (scancodeRunTimeout = 2h,
// no cap because no adaptive scaling).

func TestCollectionConfigHasScancodeRunTimeoutHours(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeRunTimeoutHours != 2 {
		t.Errorf("DefaultConfig().Collection.ScancodeRunTimeoutHours = %d; want 2 (v0.23.8 default matches the pre-v0.23.8 hardcoded constant; operators with kernel-class repos override)", cfg.Collection.ScancodeRunTimeoutHours)
	}
}

func TestCollectionConfigHasScancodeRunTimeoutCapHours(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeRunTimeoutCapHours != 24 {
		t.Errorf("DefaultConfig().Collection.ScancodeRunTimeoutCapHours = %d; want 24 (the adaptive formula min(base * 2^attempts, cap) needs an upper bound to prevent a runaway scan from consuming a worker slot indefinitely)", cfg.Collection.ScancodeRunTimeoutCapHours)
	}
}

func TestScancodeRunTimeoutAccessorDefaults(t *testing.T) {
	c := CollectionConfig{}
	if c.ScancodeRunTimeout() != 2*time.Hour {
		t.Errorf("ScancodeRunTimeout() on zero-value config = %v; want 2h (default for legacy aveloxis.json without the knob set)", c.ScancodeRunTimeout())
	}
	if c.ScancodeRunTimeoutCap() != 24*time.Hour {
		t.Errorf("ScancodeRunTimeoutCap() on zero-value config = %v; want 24h", c.ScancodeRunTimeoutCap())
	}
}

func TestScancodeRunTimeoutAccessorHonorsExplicit(t *testing.T) {
	c := CollectionConfig{
		ScancodeRunTimeoutHours:    8,
		ScancodeRunTimeoutCapHours: 48,
	}
	if c.ScancodeRunTimeout() != 8*time.Hour {
		t.Errorf("ScancodeRunTimeout() = %v; want 8h", c.ScancodeRunTimeout())
	}
	if c.ScancodeRunTimeoutCap() != 48*time.Hour {
		t.Errorf("ScancodeRunTimeoutCap() = %v; want 48h", c.ScancodeRunTimeoutCap())
	}
}

func TestScancodeRunTimeoutJSONTags(t *testing.T) {
	// Confirm the JSON tags so an operator can set the knobs in
	// aveloxis.json. The TestConfigurationDocsCoverEveryJSONField
	// tripwire will catch a missing docs entry separately; this test
	// just confirms the tags exist on the struct.
	srcBytes, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, `json:"scancode_run_timeout_hours"`) {
		t.Error("CollectionConfig.ScancodeRunTimeoutHours must carry the JSON tag scancode_run_timeout_hours")
	}
	if !strings.Contains(src, `json:"scancode_run_timeout_cap_hours"`) {
		t.Error("CollectionConfig.ScancodeRunTimeoutCapHours must carry the JSON tag scancode_run_timeout_cap_hours")
	}
}
