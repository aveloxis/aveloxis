// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.35 value-checking tripwire. The v0.20.12 docs-coverage tests
// verify every JSON KEY appears in the docs and example config — but
// never the VALUES. That gap let aveloxis.example.json ship
// scancode_shutdown_grace_minutes: 30 for two months after v0.23.7
// flipped the default to 0: operators who `cp aveloxis.example.json
// aveloxis.json` got obsolete behavior and CI stayed green.
//
// This test closes the gap semantically: the example config's
// `collection` block must produce IDENTICAL EFFECTIVE BEHAVIOR to an
// untouched DefaultConfig. Effective = through the accessor for
// accessor-backed fields (many defaults are applied there, not in
// DefaultConfig), raw field value otherwise. If an example value is
// ever intentionally non-default, add it to the allowlist WITH A
// REASON — silence is not an option.

package config

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// exampleValueAllowlist: fields where aveloxis.example.json may
// legitimately differ from the compiled default. Keep this SHORT.
var exampleValueAllowlist = map[string]string{
	"RepoCloneDir": "default is $HOME-dependent (defaultCloneDir); the example uses the portable ~ form",
}

// effectiveAccessors maps accessor-backed fields to their effective
// getter. Comparing through the accessor is what makes zero-in-example
// vs zero-in-default equivalence work (defaults applied at the
// accessor layer, per the config-knob end-to-end lesson).
var effectiveAccessors = map[string]func(c *CollectionConfig) any{
	"VulnScanTransitive":                          func(c *CollectionConfig) any { return c.VulnScanTransitiveValue() },
	"MailingListProcessorWorkers":                 func(c *CollectionConfig) any { return c.MailingListProcessorWorkersOrDefault() },
	"MailingListCadenceDays":                      func(c *CollectionConfig) any { return c.MailingListCadenceDuration() },
	"MailingListWorkers":                          func(c *CollectionConfig) any { return c.MailingListWorkersOrDefault() },
	"MailingListBackfillMonths":                   func(c *CollectionConfig) any { return c.MailingListBackfillMonthsOrDefault() },
	"MailingListMirrorHandling":                   func(c *CollectionConfig) any { return c.MailingListMirrorHandlingOrDefault() },
	"PhaseWatchdogMinutes":                        func(c *CollectionConfig) any { return c.PhaseWatchdogDuration() },
	"StagingRetentionHours":                       func(c *CollectionConfig) any { return c.StagingRetentionDuration() },
	"EnrichIntervalMinutes":                       func(c *CollectionConfig) any { return c.EnrichIntervalDuration() },
	"SearchResolveIntervalMinutes":                func(c *CollectionConfig) any { return c.SearchResolveIntervalDuration() },
	"AffiliationIntervalMinutes":                  func(c *CollectionConfig) any { return c.AffiliationIntervalDuration() },
	"ShutdownGraceSeconds":                        func(c *CollectionConfig) any { return c.ShutdownGraceDuration() },
	"BreadthIntervalMinutes":                      func(c *CollectionConfig) any { return c.BreadthIntervalDuration() },
	"BreadthBatchSize":                            func(c *CollectionConfig) any { return c.BreadthBatchSizeOrDefault() },
	"BreadthCooldownDays":                         func(c *CollectionConfig) any { return c.BreadthCooldownDuration() },
	"BreadthFetchConcurrency":                     func(c *CollectionConfig) any { return c.BreadthFetchConcurrencyOrDefault() },
	"DistributionTrackingIntervalDays":            func(c *CollectionConfig) any { return c.DistributionTrackingInterval() },
	"DistributionTrackingWorkers":                 func(c *CollectionConfig) any { return c.DistributionTrackingWorkersOrDefault() },
	"DistributionTrackingStartIntervalSec":        func(c *CollectionConfig) any { return c.DistributionTrackingStartInterval() },
	"DistributionTrackingCrossCheckSources":       func(c *CollectionConfig) any { return c.DistributionTrackingCrossCheckSourcesValue() },
	"DistributionTrackingImmediatePartialReclaim": func(c *CollectionConfig) any { return c.DistributionTrackingImmediatePartialReclaimValue() },
	"ScancodeWorkers":                             func(c *CollectionConfig) any { return c.ScancodeWorkersOrDefault() },
	"ScancodeStartIntervalSec":                    func(c *CollectionConfig) any { return c.ScancodeStartInterval() },
	"ScancodeCadenceDays":                         func(c *CollectionConfig) any { return c.ScancodeCadence() },
	"ScancodeCloneDir":                            func(c *CollectionConfig) any { return c.ScancodeCloneDirOrDefault() },
	"ScancodeShutdownGraceMinutes":                func(c *CollectionConfig) any { return c.ScancodeShutdownGrace() },
	"ScancodeRunTimeoutHours":                     func(c *CollectionConfig) any { return c.ScancodeRunTimeout() },
	"ScancodeRunTimeoutCapHours":                  func(c *CollectionConfig) any { return c.ScancodeRunTimeoutCap() },
	"ScancodeMaxInMemory":                         func(c *CollectionConfig) any { return c.ScancodeMaxInMemoryOrDefault() },
	"ScorecardTimeoutMinutes":                     func(c *CollectionConfig) any { return c.ScorecardTimeout() },
	"ScorecardTokenCount":                         func(c *CollectionConfig) any { return c.ScorecardTokenCountOrDefault() },
	"ScancodeTimeoutCapStrikes":                   func(c *CollectionConfig) any { return c.ScancodeTimeoutCapStrikesOrDefault() },
	"ScancodeIgnoreGlobs":                         func(c *CollectionConfig) any { return c.ScancodeIgnoreGlobsOrDefault() },
	"MatviewRebuildDay":                           func(c *CollectionConfig) any { return c.MatviewRebuildWeekday() },
}

func TestExampleConfigCollectionValuesMatchEffectiveDefaults(t *testing.T) {
	raw, err := os.ReadFile("../../aveloxis.example.json")
	if err != nil {
		t.Fatalf("read aveloxis.example.json: %v", err)
	}
	var envelope struct {
		Collection json.RawMessage `json:"collection"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("parse example: %v", err)
	}

	exampleCfg := DefaultConfig()
	if err := json.Unmarshal(envelope.Collection, &exampleCfg.Collection); err != nil {
		t.Fatalf("apply example collection block: %v", err)
	}
	defaultCfg := DefaultConfig()

	typ := reflect.TypeOf(CollectionConfig{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if _, allowed := exampleValueAllowlist[field.Name]; allowed {
			continue
		}

		var got, want any
		if getter, ok := effectiveAccessors[field.Name]; ok {
			got = getter(&exampleCfg.Collection)
			want = getter(&defaultCfg.Collection)
		} else {
			got = reflect.ValueOf(exampleCfg.Collection).Field(i).Interface()
			want = reflect.ValueOf(defaultCfg.Collection).Field(i).Interface()
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("aveloxis.example.json teaches a NON-DEFAULT value for %s "+
				"(json %q): example-effective=%v default-effective=%v. Either fix "+
				"the example to match the compiled default, or add the field to "+
				"exampleValueAllowlist with a reason.",
				field.Name, jsonTag(field), got, want)
		}
	}
}

func jsonTag(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name
	}
	return tag
}
