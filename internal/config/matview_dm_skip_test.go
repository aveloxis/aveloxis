// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

// matview_dm_skip_test.go — TDD suite for the v0.27.56
// matview_rebuild_skip_dm_aggregates knob.
//
// Motivating incident (2026-07-27→30, aveloxis_large): the weekly
// rebuild's SECOND step — RefreshAllRepoAggregates, a 93,222-repo ×
// two-pass per-repo DELETE+INSERT loop — ran for 3+ days while
// MatviewRebuildActive stayed set, silently pausing all collection
// claims with 74K repos due. The matview step itself took ~20h; the
// dm_ step was the multi-day tail. This knob lets operators keep the
// weekly matview refresh but skip the dm_ aggregate pass entirely
// (dm_ tables then update only via `aveloxis refresh-views` /
// `aveloxis migrate`, which are explicit operator commands and
// deliberately NOT gated by this knob). The full rebuild off-switch
// already exists: matview_rebuild_day: "disabled".

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestMatviewSkipDMAggregatesField(t *testing.T) {
	var c CollectionConfig
	c.MatviewRebuildSkipDMAggregates = true // compile-time field pin
	if !c.MatviewRebuildSkipDMAggregates {
		t.Fatal("field must be settable")
	}
}

// Zero-value = false = pre-v0.27.56 behavior, so operators who never
// touch aveloxis.json see no change. A plain bool (not *bool) is
// deliberate: the default IS the zero value, no absent-vs-false
// distinction needed.
func TestMatviewSkipDMAggregatesJSONRoundTrip(t *testing.T) {
	var absent CollectionConfig
	if err := json.Unmarshal([]byte(`{}`), &absent); err != nil {
		t.Fatal(err)
	}
	if absent.MatviewRebuildSkipDMAggregates {
		t.Error("absent key must default to false (dm_ refresh stays part of the weekly rebuild)")
	}
	var set CollectionConfig
	if err := json.Unmarshal([]byte(`{"matview_rebuild_skip_dm_aggregates": true}`), &set); err != nil {
		t.Fatal(err)
	}
	if !set.MatviewRebuildSkipDMAggregates {
		t.Error("matview_rebuild_skip_dm_aggregates: true must parse into the field")
	}
}

// The scheduler's weekly rebuild must consult the knob AND log the
// skip (log-the-effective-value rule: operators reading the log must
// see that the dm_ step was skipped by config, not silently absent).
// The gate must sit around the RefreshAllRepoAggregates call — the
// matview step is deliberately not affected by this knob.
func TestRebuildMatviewsConsultsSkipKnob(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	idx := strings.Index(body, "func (s *Scheduler) rebuildMatviews(")
	if idx < 0 {
		t.Fatal("cannot find rebuildMatviews")
	}
	body = body[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:1+end]
	}
	if !strings.Contains(body, "MatviewRebuildSkipDMAggregates") {
		t.Fatal("rebuildMatviews must consult Collection.MatviewRebuildSkipDMAggregates around the dm_ aggregate step")
	}
	if !strings.Contains(body, "RefreshAllRepoAggregates") {
		t.Fatal("rebuildMatviews lost its RefreshAllRepoAggregates call — the knob must gate it, not delete it")
	}
	skipPos := strings.Index(body, "MatviewRebuildSkipDMAggregates")
	refreshPos := strings.Index(body, "RefreshAllRepoAggregates")
	if skipPos > refreshPos {
		t.Error("the skip check must come BEFORE the RefreshAllRepoAggregates call so a skipped run never starts the multi-day loop")
	}
	if !strings.Contains(body, "skipped by config") && !strings.Contains(body, "skipped (matview_rebuild_skip_dm_aggregates") {
		t.Error("the skip path must log that the dm_ step was skipped by config — silence here reads as 'the rebuild forgot the aggregates'")
	}
}
