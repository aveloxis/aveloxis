// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.37 (tech-debt Action 1): the three-layer config wiring is
// collapsed. Before: every knob was declared in CollectionConfig,
// MIRRORED in scheduler.Config (45 fields), and hand-copied in
// main.go (42 assignment lines). Adding a knob was a 3-file edit;
// forgetting a layer silently dropped the value with no compile error
// — the documented mailing_list_backfill_months incident, where a
// duplicate clamp at two layers defeated the operator's setting for
// weeks while every per-layer unit test passed.
//
// After: scheduler.Config carries ONLY scheduler-internal runtime
// inputs plus a *config.CollectionConfig pointer. Every operator knob
// is read through the CollectionConfig accessor at the point of use,
// so each default/clamp exists in exactly one place.

package scheduler

import (
	"reflect"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

// The mirror detector: scheduler.Config must not re-declare any field
// that exists (by name) on config.CollectionConfig. "Workers" is the
// single deliberate exception — it is the FLAG-RESOLVED worker count
// (serve's --workers overrides collection.workers in main.go), i.e. a
// runtime input, not a mirror.
func TestSchedulerConfigHasNoCollectionMirror(t *testing.T) {
	collectionFields := map[string]bool{}
	ct := reflect.TypeOf(config.CollectionConfig{})
	for i := 0; i < ct.NumField(); i++ {
		collectionFields[ct.Field(i).Name] = true
	}

	allowed := map[string]bool{"Workers": true}

	st := reflect.TypeOf(Config{})
	for i := 0; i < st.NumField(); i++ {
		name := st.Field(i).Name
		if collectionFields[name] && !allowed[name] {
			t.Errorf("scheduler.Config re-declares CollectionConfig field %q — the "+
				"mirror pattern is banned (v0.25.37). Read the value through "+
				"cfg.Collection at the point of use instead; mirrors are how the "+
				"mailing_list_backfill_months double-clamp incident happened.", name)
		}
	}

	if st.NumField() > 7 {
		t.Errorf("scheduler.Config has %d fields — it should stay a SLIM struct of "+
			"scheduler-internal runtime inputs (workers, poll/lock/org intervals, "+
			"force-full, Collection pointer). New operator knobs belong on "+
			"config.CollectionConfig only.", st.NumField())
	}
}

// The scheduler must consume the operator's CollectionConfig directly.
func TestSchedulerConfigCarriesCollectionConfig(t *testing.T) {
	f, ok := reflect.TypeOf(Config{}).FieldByName("Collection")
	if !ok {
		t.Fatal("scheduler.Config must have a Collection *config.CollectionConfig field")
	}
	if f.Type != reflect.TypeOf(&config.CollectionConfig{}) {
		t.Fatalf("Config.Collection must be *config.CollectionConfig, got %v", f.Type)
	}
}

// End-to-end knob check for the incident-class knob: an explicit
// mailing_list_backfill_months = 0 must survive to the effective value
// the scheduler wiring consumes (0 = full history), and an absent value
// must default to 6 — with NO re-clamping anywhere in the scheduler.
func TestBackfillMonthsKnobSurvivesToSchedulerLayer(t *testing.T) {
	zero := 0
	explicit := config.CollectionConfig{MailingListBackfillMonths: &zero}
	if got := explicit.MailingListBackfillMonthsOrDefault(); got != 0 {
		t.Fatalf("explicit 0 must stay 0 (full history), got %d", got)
	}
	var absent config.CollectionConfig
	if got := absent.MailingListBackfillMonthsOrDefault(); got != 6 {
		t.Fatalf("absent must default to 6, got %d", got)
	}
}
