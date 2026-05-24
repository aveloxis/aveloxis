// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// v0.25.3 — operator escape hatch for the v0.25.0 immediate-reclaim
// behavior. Default true preserves current behavior; operators flip
// to false once their fleet is through the v0.25.0/v0.25.1
// transition cohort.
//
// Pointer-to-bool (not bare bool) so the JSON decoder can
// distinguish "absent" (use the v0.25.3 default = true) from
// "explicitly false". A bare bool would default to false on
// missing-key, silently disabling the v0.25.0 design for every
// aveloxis.json that pre-dates v0.25.3 — exactly the wrong
// migration story. Same shape as v0.25.0's
// distribution_tracking_cross_check_sources field.

func TestCollectionConfigHasImmediatePartialReclaimField(t *testing.T) {
	srcBytes, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	if !strings.Contains(src, `DistributionTrackingImmediatePartialReclaim *bool`) {
		t.Error("CollectionConfig must declare DistributionTrackingImmediatePartialReclaim as *bool (pointer-to-bool) so the JSON decoder can distinguish 'absent' from 'explicitly false' — same shape as DistributionTrackingCrossCheckSources")
	}
	if !strings.Contains(src, `json:"distribution_tracking_immediate_partial_reclaim,omitempty"`) {
		t.Error("CollectionConfig.DistributionTrackingImmediatePartialReclaim must carry the JSON tag distribution_tracking_immediate_partial_reclaim,omitempty")
	}
}

func TestImmediatePartialReclaimAccessorDefaultsToTrue(t *testing.T) {
	c := CollectionConfig{}
	if !c.DistributionTrackingImmediatePartialReclaimValue() {
		t.Error("DistributionTrackingImmediatePartialReclaimValue() on zero-value config must return true — preserves v0.25.0/v0.25.1 behavior on aveloxis.json files that pre-date v0.25.3")
	}
}

func TestImmediatePartialReclaimAccessorHonorsExplicitFalse(t *testing.T) {
	v := false
	c := CollectionConfig{DistributionTrackingImmediatePartialReclaim: &v}
	if c.DistributionTrackingImmediatePartialReclaimValue() {
		t.Error("DistributionTrackingImmediatePartialReclaimValue() must honor explicit false — when an operator sets the JSON field to false, the claim query drops the OR scan_complete = FALSE clause")
	}
}

func TestImmediatePartialReclaimAccessorHonorsExplicitTrue(t *testing.T) {
	v := true
	c := CollectionConfig{DistributionTrackingImmediatePartialReclaim: &v}
	if !c.DistributionTrackingImmediatePartialReclaimValue() {
		t.Error("DistributionTrackingImmediatePartialReclaimValue() must honor explicit true")
	}
}

func TestImmediatePartialReclaimJSONRoundTrip(t *testing.T) {
	// Pointer-to-bool semantics: 'absent in JSON' should produce nil
	// AND the accessor returns the default (true). 'explicit false'
	// produces a non-nil pointer to false AND the accessor returns
	// false. This is the entire point of the *bool shape.
	cases := []struct {
		name       string
		jsonFrag   string
		wantNilPtr bool
		wantEffect bool
	}{
		{"absent", `{}`, true, true},
		{"explicit false", `{"distribution_tracking_immediate_partial_reclaim": false}`, false, false},
		{"explicit true", `{"distribution_tracking_immediate_partial_reclaim": true}`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c CollectionConfig
			if err := json.Unmarshal([]byte(tc.jsonFrag), &c); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			gotNil := c.DistributionTrackingImmediatePartialReclaim == nil
			if gotNil != tc.wantNilPtr {
				t.Errorf("pointer-nil = %v, want %v", gotNil, tc.wantNilPtr)
			}
			if got := c.DistributionTrackingImmediatePartialReclaimValue(); got != tc.wantEffect {
				t.Errorf("accessor = %v, want %v", got, tc.wantEffect)
			}
		})
	}
}
