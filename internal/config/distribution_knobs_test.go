// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// v0.24.0 — Distribution Tracking config knobs.
//
// The subsystem is OFF BY DEFAULT (distribution_tracking_enabled = false).
// When enabled, a periodic worker pool examines each repo against
// deps.dev, ecosyste.ms, GitHub Packages, GitHub release assets, and
// the GitHub Contents API to determine whether/where the repo is
// distributed via package managers.
//
// Default cadence is 180 days (6 months) — package-distribution
// mappings are stable on this timescale.

func TestCollectionConfigHasDistributionFields(t *testing.T) {
	// Source-contract pin: every JSON tag must exist on CollectionConfig.
	// Verified by round-tripping through encoding/json since the v0.20.12
	// docs-coverage tripwire AST-parses the same source file.
	cfg := &CollectionConfig{}
	blob, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(blob)

	for _, needle := range []string{
		`"distribution_tracking_enabled"`,
		`"distribution_tracking_interval_days"`,
		`"distribution_tracking_workers"`,
		`"distribution_tracking_start_interval_s"`,
		`"distribution_tracking_polite_email"`,
		`"distribution_tracking_user_agent"`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("CollectionConfig must declare JSON field %s for the v0.24.0 distribution tracking subsystem", needle)
		}
	}
}

func TestDistributionTrackingDefaults(t *testing.T) {
	cfg := DefaultConfig()

	// Off by default — operators must explicitly opt in.
	if cfg.Collection.DistributionTrackingEnabled {
		t.Error("DefaultConfig must set DistributionTrackingEnabled = false. The subsystem makes outbound calls to deps.dev + ecosyste.ms + GitHub Packages and operators should explicitly opt in.")
	}

	// 180 days = 6 months, the user-specified default.
	if cfg.Collection.DistributionTrackingIntervalDays != 180 {
		t.Errorf("DefaultConfig must set DistributionTrackingIntervalDays = 180 (6 months). Got %d", cfg.Collection.DistributionTrackingIntervalDays)
	}

	// 4 workers — modest concurrency for outbound HTTP work.
	if cfg.Collection.DistributionTrackingWorkers != 4 {
		t.Errorf("DefaultConfig must set DistributionTrackingWorkers = 4. Got %d", cfg.Collection.DistributionTrackingWorkers)
	}

	// 30s between claims — paces outbound traffic; well under any
	// known registry rate limit.
	if cfg.Collection.DistributionTrackingStartIntervalSec != 30 {
		t.Errorf("DefaultConfig must set DistributionTrackingStartIntervalSec = 30. Got %d", cfg.Collection.DistributionTrackingStartIntervalSec)
	}
}

func TestDistributionTrackingIntervalAccessor(t *testing.T) {
	// Accessor falls back to 180 days when the field is zero.
	c := &CollectionConfig{}
	got := c.DistributionTrackingInterval()
	want := 180 * 24 * time.Hour
	if got != want {
		t.Errorf("DistributionTrackingInterval() with zero field = %v, want %v (180 days fallback)", got, want)
	}

	// Honors a non-zero value.
	c.DistributionTrackingIntervalDays = 30
	got = c.DistributionTrackingInterval()
	want = 30 * 24 * time.Hour
	if got != want {
		t.Errorf("DistributionTrackingInterval() with 30 = %v, want %v", got, want)
	}
}

func TestDistributionTrackingWorkersOrDefault(t *testing.T) {
	c := &CollectionConfig{}
	if c.DistributionTrackingWorkersOrDefault() != 4 {
		t.Errorf("Zero field must fall back to 4 workers, got %d", c.DistributionTrackingWorkersOrDefault())
	}
	c.DistributionTrackingWorkers = 8
	if c.DistributionTrackingWorkersOrDefault() != 8 {
		t.Errorf("Non-zero field must be honored, got %d", c.DistributionTrackingWorkersOrDefault())
	}
}

func TestDistributionTrackingStartIntervalAccessor(t *testing.T) {
	c := &CollectionConfig{}
	got := c.DistributionTrackingStartInterval()
	want := 30 * time.Second
	if got != want {
		t.Errorf("Zero field must fall back to 30s, got %v", got)
	}
}
