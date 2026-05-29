// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"strings"
	"testing"
)

// v0.25.2 — operator-configurable scancode --max-in-memory cap.
// Pre-v0.25.2 this was hardcoded to 5000 inside
// internal/collector/scancode_worker.go's runOne. The default is
// conservative for low-memory dev machines; production hosts with
// hundreds of GB of RAM can safely raise it for faster scans on
// monorepos with tens of thousands of files. scancode passes the
// flag through to the per-process memory cap before it spills
// intermediate scan results to a tempfile on disk.

func TestCollectionConfigHasScancodeMaxInMemory(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeMaxInMemory != 5000 {
		t.Errorf("DefaultConfig().Collection.ScancodeMaxInMemory = %d; want 5000 (v0.25.2 default matches the pre-v0.25.2 hardcoded constant so legacy aveloxis.json files keep working unchanged)", cfg.Collection.ScancodeMaxInMemory)
	}
}

func TestScancodeMaxInMemoryAccessorDefaults(t *testing.T) {
	c := CollectionConfig{}
	if c.ScancodeMaxInMemoryOrDefault() != 5000 {
		t.Errorf("ScancodeMaxInMemoryOrDefault() on zero-value config = %d; want 5000 (default for legacy aveloxis.json without the knob set — must match the pre-v0.25.2 hardcoded constant so no behavior changes on existing fleets)", c.ScancodeMaxInMemoryOrDefault())
	}
}

func TestScancodeMaxInMemoryAccessorHonorsExplicit(t *testing.T) {
	c := CollectionConfig{ScancodeMaxInMemory: 50000}
	if c.ScancodeMaxInMemoryOrDefault() != 50000 {
		t.Errorf("ScancodeMaxInMemoryOrDefault() = %d; want 50000", c.ScancodeMaxInMemoryOrDefault())
	}
}

func TestScancodeMaxInMemoryRejectsNegative(t *testing.T) {
	// A negative value would pass through to scancode as a bogus
	// command-line argument. The accessor must clamp non-positive
	// values to the safe default.
	c := CollectionConfig{ScancodeMaxInMemory: -100}
	if got := c.ScancodeMaxInMemoryOrDefault(); got != 5000 {
		t.Errorf("ScancodeMaxInMemoryOrDefault() on negative input = %d; want 5000 (must clamp to default, never pass a negative through to scancode)", got)
	}
}

func TestScancodeMaxInMemoryJSONTag(t *testing.T) {
	srcBytes, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, `json:"scancode_max_in_memory"`) {
		t.Error("CollectionConfig.ScancodeMaxInMemory must carry the JSON tag scancode_max_in_memory")
	}
}
