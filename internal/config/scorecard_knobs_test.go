// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.5 scorecard knobs: per-attempt wall-clock timeout + pool-token
// count for the remote-primary scorecard phase. Behavioral tests on the
// accessors — the config-knob end-to-end lesson says the accessor is
// the ONLY place a default may live, so these pin the accessor
// semantics directly.

package config

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestCollectionConfigHasScorecardKnobs(t *testing.T) {
	typ := reflect.TypeOf(CollectionConfig{})

	f, ok := typ.FieldByName("ScorecardTimeoutMinutes")
	if !ok {
		t.Fatal("CollectionConfig must declare ScorecardTimeoutMinutes")
	}
	if got := f.Tag.Get("json"); got != "scorecard_timeout_minutes" {
		t.Errorf("ScorecardTimeoutMinutes json tag = %q, want scorecard_timeout_minutes", got)
	}

	f, ok = typ.FieldByName("ScorecardTokenCount")
	if !ok {
		t.Fatal("CollectionConfig must declare ScorecardTokenCount")
	}
	if got := f.Tag.Get("json"); got != "scorecard_token_count" {
		t.Errorf("ScorecardTokenCount json tag = %q, want scorecard_token_count", got)
	}
}

func TestScorecardTimeoutDefaultsTo15Minutes(t *testing.T) {
	cases := []struct {
		in   int
		want time.Duration
	}{
		{0, 15 * time.Minute},  // unset → default
		{-3, 15 * time.Minute}, // bogus → default
		{15, 15 * time.Minute}, // explicit default
		{45, 45 * time.Minute}, // operator override
		{1, 1 * time.Minute},   // small but valid
		{120, 2 * time.Hour},   // large but valid
	}
	for _, tc := range cases {
		c := CollectionConfig{ScorecardTimeoutMinutes: tc.in}
		if got := c.ScorecardTimeout(); got != tc.want {
			t.Errorf("ScorecardTimeout(%d) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestScorecardTokenCountDefaultsToAll(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 0},   // 0 = all tokens (the default)
		{-7, 0},  // bogus → all tokens, never a negative slice bound
		{1, 1},   // single token
		{40, 40}, // subset of a large pool
	}
	for _, tc := range cases {
		c := CollectionConfig{ScorecardTokenCount: tc.in}
		if got := c.ScorecardTokenCountOrDefault(); got != tc.want {
			t.Errorf("ScorecardTokenCountOrDefault(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestScorecardKnobsJSONRoundTrip exercises the config value → effective
// behavior path end-to-end at the JSON layer (the mailing-list backfill
// 0→6 clamp bug class: per-layer tests can all pass while the parsed
// value is clobbered downstream).
func TestScorecardKnobsJSONRoundTrip(t *testing.T) {
	var c CollectionConfig
	blob := []byte(`{"scorecard_timeout_minutes": 30, "scorecard_token_count": 5}`)
	if err := json.Unmarshal(blob, &c); err != nil {
		t.Fatal(err)
	}
	if got := c.ScorecardTimeout(); got != 30*time.Minute {
		t.Errorf("parsed ScorecardTimeout = %v, want 30m", got)
	}
	if got := c.ScorecardTokenCountOrDefault(); got != 5 {
		t.Errorf("parsed ScorecardTokenCountOrDefault = %d, want 5", got)
	}

	// Absent keys → defaults through the accessors.
	var empty CollectionConfig
	if err := json.Unmarshal([]byte(`{}`), &empty); err != nil {
		t.Fatal(err)
	}
	if got := empty.ScorecardTimeout(); got != 15*time.Minute {
		t.Errorf("absent scorecard_timeout_minutes → %v, want 15m", got)
	}
	if got := empty.ScorecardTokenCountOrDefault(); got != 0 {
		t.Errorf("absent scorecard_token_count → %d, want 0 (all tokens)", got)
	}
}
