// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// vuln_scan_transitive_flip_test.go — v0.27.136: the C1 default flip,
// decided on the 2026-08-21 canary evidence. The knob moves to the
// pointer-to-bool pattern (v0.25.0 cross_check_sources precedent) so
// the decoder distinguishes "absent" (new default = TRUE) from an
// explicit false (the opt-out escape hatch). SR-10 discipline: the
// accessor is the SINGLE default layer, the JSON round-trip covers all
// three input states, and the consumer pins prove nothing reads the
// raw pointer.

package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestVulnScanTransitiveDefaultsTrue(t *testing.T) {
	var c CollectionConfig
	if !c.VulnScanTransitiveValue() {
		t.Error("zero-value config must default vuln_scan_transitive to TRUE (the v0.27.136 flip)")
	}
}

func TestVulnScanTransitiveExplicitValuesHonored(t *testing.T) {
	f, tr := false, true
	c := CollectionConfig{VulnScanTransitive: &f}
	if c.VulnScanTransitiveValue() {
		t.Error("explicit false is the opt-out escape hatch — it must be honored")
	}
	c.VulnScanTransitive = &tr
	if !c.VulnScanTransitiveValue() {
		t.Error("explicit true must be honored")
	}
}

// The three-state JSON round-trip: absent → nil pointer → true;
// explicit false → false; explicit true → true. This is the case a
// bare bool cannot express — absent and false are indistinguishable —
// and exactly why the flip requires the pointer form.
func TestVulnScanTransitiveJSONRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		blob    string
		wantNil bool
		want    bool
	}{
		{"absent", `{}`, true, true},
		{"explicit false", `{"vuln_scan_transitive": false}`, false, false},
		{"explicit true", `{"vuln_scan_transitive": true}`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var c CollectionConfig
			if err := json.Unmarshal([]byte(tc.blob), &c); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if (c.VulnScanTransitive == nil) != tc.wantNil {
				t.Errorf("pointer nil-ness = %v, want %v", c.VulnScanTransitive == nil, tc.wantNil)
			}
			if got := c.VulnScanTransitiveValue(); got != tc.want {
				t.Errorf("effective value = %v, want %v", got, tc.want)
			}
		})
	}
}

// Consumer pins: every read goes through the accessor. A consumer
// reading the raw pointer would either fail to compile against bool
// parameters (caught) or — worse, after a future refactor — compare
// the pointer itself. One default layer, at the accessor (SR-10).
func TestVulnScanTransitiveConsumersUseAccessor(t *testing.T) {
	for _, f := range []string{
		"../scheduler/scheduler.go",
		"../../cmd/aveloxis/heal_vulnerabilities.go",
	} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		s := string(b)
		if !strings.Contains(s, "VulnScanTransitiveValue()") {
			t.Errorf("%s must read the knob via VulnScanTransitiveValue()", f)
		}
		if strings.Contains(s, "Collection.VulnScanTransitive)") || strings.Contains(s, "Collection.VulnScanTransitive\n") {
			t.Errorf("%s reads the RAW pointer — the accessor is the single default layer", f)
		}
	}
}
