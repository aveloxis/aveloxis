// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// v0.25.0 — cross-check-sources flag for distribution tracking.
//
// Operator-mandated: an aveloxis.json knob that guarantees BOTH
// deps.dev AND ecosyste.ms are queried per repo, with separate
// rows persisted per source. Default true (lock in v0.24.0
// behavior) so existing deployments keep cross-checking even if
// they don't update their config files.

func TestCollectionConfigHasCrossCheckSourcesField(t *testing.T) {
	// JSON-tag pin via marshaling. omitempty means an unset field
	// won't appear in the output, so we set the value first.
	val := true
	cfg := &CollectionConfig{
		DistributionTrackingCrossCheckSources: &val,
	}
	blob, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !contains(string(blob), `"distribution_tracking_cross_check_sources"`) {
		t.Errorf("CollectionConfig must declare JSON field 'distribution_tracking_cross_check_sources' — operator direction v0.25.0")
	}
}

func TestDistributionTrackingCrossCheckSourcesDefaultsTrueWhenAbsent(t *testing.T) {
	// Pointer-to-bool: nil means "field absent from aveloxis.json,
	// use the v0.25.0 default which is true." A bare bool would
	// default to false and silently break the lock-in.
	cfg := &CollectionConfig{}
	if cfg.DistributionTrackingCrossCheckSources != nil {
		t.Fatalf("expected nil (field absent), got %v", *cfg.DistributionTrackingCrossCheckSources)
	}
	if !cfg.DistributionTrackingCrossCheckSourcesValue() {
		t.Error("DistributionTrackingCrossCheckSourcesValue() must default to true when JSON field is absent — v0.25.0 lock-in guarantee for existing aveloxis.json deployments")
	}
}

func TestDistributionTrackingCrossCheckSourcesRespectsExplicitFalse(t *testing.T) {
	f := false
	cfg := &CollectionConfig{
		DistributionTrackingCrossCheckSources: &f,
	}
	if cfg.DistributionTrackingCrossCheckSourcesValue() {
		t.Error("explicit false in aveloxis.json must produce false from accessor — operator opt-out path")
	}
}

func TestDistributionTrackingCrossCheckSourcesRespectsExplicitTrue(t *testing.T) {
	tr := true
	cfg := &CollectionConfig{
		DistributionTrackingCrossCheckSources: &tr,
	}
	if !cfg.DistributionTrackingCrossCheckSourcesValue() {
		t.Error("explicit true in aveloxis.json must produce true from accessor")
	}
}

func TestDistributionTrackingCrossCheckSourcesParsesFromJSON(t *testing.T) {
	// Round-trip: a JSON config with the field set to false should
	// produce a nil-checked-false from the accessor.
	src := `{"distribution_tracking_cross_check_sources": false}`
	var cfg CollectionConfig
	if err := json.Unmarshal([]byte(src), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if cfg.DistributionTrackingCrossCheckSources == nil {
		t.Fatal("expected non-nil pointer after parsing explicit false")
	}
	if *cfg.DistributionTrackingCrossCheckSources {
		t.Error("expected false after parsing 'false', got true")
	}
	if cfg.DistributionTrackingCrossCheckSourcesValue() {
		t.Error("accessor must reflect parsed-false")
	}
}

// contains is a stdlib-strings.Contains alias to avoid an import
// dance with the existing config tests (they already cover the
// same import surface in distribution_knobs_test.go).
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
