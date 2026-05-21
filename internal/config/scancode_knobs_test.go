// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// v0.21.0 — Scancode is now run by a dedicated ScancodeWorker pool
// rather than inline in the per-job analysis phase. Five new config
// knobs control the pool's behavior. Pre-v0.21.0 the cadence (30 days)
// and concurrency (2) were hardcoded in internal/collector/scancode.go;
// the 2026-05-14 production incident showed that for a fleet-scale
// install the operator needs to be able to tune both. The other knobs
// (start interval, clone directory, shutdown grace) are new
// requirements of the decoupled worker.

func TestCollectionConfigHasScancodeKnobs(t *testing.T) {
	data, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin field names + JSON tags. Whitespace between field name and
	// type is gofmt-controlled and not stable across formatter
	// upgrades, so check each token independently.
	required := []string{
		"ScancodeWorkers",
		`json:"scancode_workers"`,
		"ScancodeStartIntervalSec",
		`json:"scancode_start_interval_s"`,
		"ScancodeCadenceDays",
		`json:"scancode_cadence_days"`,
		"ScancodeCloneDir",
		`json:"scancode_clone_dir"`,
		"ScancodeShutdownGraceMinutes",
		`json:"scancode_shutdown_grace_minutes"`,
	}
	for _, needle := range required {
		if !strings.Contains(src, needle) {
			t.Errorf("CollectionConfig must declare the v0.21.0 scancode knobs. Missing: %q", needle)
		}
	}
}

func TestScancodeWorkersDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeWorkers != 2 {
		t.Errorf("DefaultConfig().Collection.ScancodeWorkers = %d; want 2. Default matches pre-v0.21.0 concurrency so operators upgrading don't surprise themselves with a sudden increase in scancode load. Operators on large fleets are expected to raise this.", cfg.Collection.ScancodeWorkers)
	}
}

func TestScancodeStartIntervalDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeStartIntervalSec != 90 {
		t.Errorf("DefaultConfig().Collection.ScancodeStartIntervalSec = %d; want 90. 90 seconds between scancode worker claims paces clone-bandwidth bursts on large fleets without throttling small fleets perceptibly.", cfg.Collection.ScancodeStartIntervalSec)
	}
	d := cfg.Collection.ScancodeStartInterval()
	if d != 90*time.Second {
		t.Errorf("ScancodeStartInterval() = %v; want 90s", d)
	}
}

func TestScancodeCadenceDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeCadenceDays != 180 {
		t.Errorf("DefaultConfig().Collection.ScancodeCadenceDays = %d; want 180 (6 months). Pre-v0.21.0 was 30 days; 6 months is a better fit because per-file license + copyright headers in source files are near-immutable on the timescale we care about, and the I/O cost of scanning a Linux-kernel-scale repo doesn't justify monthly re-scans.", cfg.Collection.ScancodeCadenceDays)
	}
	d := cfg.Collection.ScancodeCadence()
	want := 180 * 24 * time.Hour
	if d != want {
		t.Errorf("ScancodeCadence() = %v; want %v", d, want)
	}
}

func TestScancodeCloneDirDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeCloneDir == "" {
		t.Error("DefaultConfig().Collection.ScancodeCloneDir must be non-empty. The ScancodeWorker creates fresh per-run clones under this directory; an empty default would leave operators creating /etc/aveloxis-scancode by accident on first scan.")
	}
}

func TestScancodeShutdownGraceDefault(t *testing.T) {
	// v0.23.7: default flipped from 30 min to 0 (immediate kill).
	// Rationale: a subprocess that outlives `aveloxis stop` cannot
	// deliver its output back — aveloxis Go code is what reads the
	// scancode JSON file and writes to the DB. A scan that finishes
	// after aveloxis is gone produces a file no one ingests. The
	// v0.21.0 recoverOrphans path on next startup notices the
	// orphaned lock row, attempts to ingest from disk if a file
	// exists, otherwise clears the lock. So the grace period was
	// buying nothing in practice — the scan either finished AND was
	// ingested (success) or didn't AND was discarded (fail). Letting
	// the subprocess linger past stop just delayed the inevitable
	// while adding ghost-process risk.
	cfg := DefaultConfig()
	if cfg.Collection.ScancodeShutdownGraceMinutes != 0 {
		t.Errorf("DefaultConfig().Collection.ScancodeShutdownGraceMinutes = %d; want 0. v0.23.7 flipped the default to 0 (immediate kill on stop) because subprocesses that outlive aveloxis can't deliver their output and just become ghosts. Operators who genuinely want the old behavior set the value explicitly in aveloxis.json.", cfg.Collection.ScancodeShutdownGraceMinutes)
	}
	d := cfg.Collection.ScancodeShutdownGrace()
	if d != 0 {
		t.Errorf("ScancodeShutdownGrace() = %v; want 0 (immediate kill)", d)
	}
}

func TestScancodeAccessorsFallBackToDefaults(t *testing.T) {
	// Zero values for the int fields should map to the documented
	// defaults (operators with a pre-v0.21.0 aveloxis.json keep
	// working without edits).
	c := CollectionConfig{}
	if c.ScancodeWorkersOrDefault() != 2 {
		t.Errorf("ScancodeWorkersOrDefault() with zero = %d; want 2", c.ScancodeWorkersOrDefault())
	}
	if c.ScancodeStartInterval() != 90*time.Second {
		t.Errorf("ScancodeStartInterval() with zero = %v; want 90s", c.ScancodeStartInterval())
	}
	if c.ScancodeCadence() != 180*24*time.Hour {
		t.Errorf("ScancodeCadence() with zero = %v; want 180d", c.ScancodeCadence())
	}
	// v0.23.7: zero now maps to zero (immediate kill), not a 30-min
	// fallback. Pre-v0.23.7 the accessor returned 30 min on a zero
	// input field to provide a sane default for operators who hadn't
	// set the knob; v0.23.7 makes the default itself "kill immediately
	// on stop" since lingering subprocesses can't deliver output anyway.
	if c.ScancodeShutdownGrace() != 0 {
		t.Errorf("ScancodeShutdownGrace() with zero = %v; want 0 (immediate kill, v0.23.7)", c.ScancodeShutdownGrace())
	}
	if c.ScancodeCloneDirOrDefault() == "" {
		t.Error("ScancodeCloneDirOrDefault() with zero must return a non-empty default path")
	}
}

func TestScancodeAccessorsHonorExplicitValues(t *testing.T) {
	c := CollectionConfig{
		ScancodeWorkers:              12,
		ScancodeStartIntervalSec:     45,
		ScancodeCadenceDays:          90,
		ScancodeCloneDir:             "/var/lib/aveloxis-scancode",
		ScancodeShutdownGraceMinutes: 60,
	}
	if c.ScancodeWorkersOrDefault() != 12 {
		t.Errorf("ScancodeWorkersOrDefault() = %d; want 12", c.ScancodeWorkersOrDefault())
	}
	if c.ScancodeStartInterval() != 45*time.Second {
		t.Errorf("ScancodeStartInterval() = %v; want 45s", c.ScancodeStartInterval())
	}
	if c.ScancodeCadence() != 90*24*time.Hour {
		t.Errorf("ScancodeCadence() = %v; want 90d", c.ScancodeCadence())
	}
	if c.ScancodeCloneDirOrDefault() != "/var/lib/aveloxis-scancode" {
		t.Errorf("ScancodeCloneDirOrDefault() = %q; want /var/lib/aveloxis-scancode", c.ScancodeCloneDirOrDefault())
	}
	if c.ScancodeShutdownGrace() != 60*time.Minute {
		t.Errorf("ScancodeShutdownGrace() = %v; want 60m", c.ScancodeShutdownGrace())
	}
}

func TestScancodeJSONRoundTrip(t *testing.T) {
	// Ensure aveloxis.json's `collection.scancode_*` keys actually
	// land on the config struct's fields. A typo on either side would
	// silently no-op without this test.
	in := `{
		"collection": {
			"scancode_workers": 12,
			"scancode_start_interval_s": 45,
			"scancode_cadence_days": 90,
			"scancode_clone_dir": "/var/lib/aveloxis-scancode",
			"scancode_shutdown_grace_minutes": 60
		}
	}`
	var cfg Config
	if err := json.Unmarshal([]byte(in), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Collection.ScancodeWorkers != 12 {
		t.Errorf("ScancodeWorkers from JSON = %d; want 12", cfg.Collection.ScancodeWorkers)
	}
	if cfg.Collection.ScancodeStartIntervalSec != 45 {
		t.Errorf("ScancodeStartIntervalSec from JSON = %d; want 45", cfg.Collection.ScancodeStartIntervalSec)
	}
	if cfg.Collection.ScancodeCadenceDays != 90 {
		t.Errorf("ScancodeCadenceDays from JSON = %d; want 90", cfg.Collection.ScancodeCadenceDays)
	}
	if cfg.Collection.ScancodeCloneDir != "/var/lib/aveloxis-scancode" {
		t.Errorf("ScancodeCloneDir from JSON = %q", cfg.Collection.ScancodeCloneDir)
	}
	if cfg.Collection.ScancodeShutdownGraceMinutes != 60 {
		t.Errorf("ScancodeShutdownGraceMinutes from JSON = %d; want 60", cfg.Collection.ScancodeShutdownGraceMinutes)
	}
}
