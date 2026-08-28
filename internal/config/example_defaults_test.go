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
	"strings"
	"testing"
)

// exampleValueAllowlist: per-file fields where a committed example
// may legitimately differ from the compiled default. Keep each list
// SHORT — a value that is not a deliberate tuning choice belongs in
// the file, not in here. Pass 45 widened this from one file to all
// three: TestExampleConfigsCarryNoEnvVarSyntax already treated the
// three as one copyable set, and README.md tells operators to
// `cp aveloxis.docker.example.json aveloxis.docker.json` — but only
// aveloxis.example.json was value-checked, so both siblings shipped
// scancode_shutdown_grace_minutes: 30 for the same year-plus after
// v0.23.7 flipped the default to 0. In the sharded profile that also
// inflated shutdownBudget() to ~31m50s against the TimeoutStopSec=180
// that docs/guide/running-as-a-service.md tells operators to set.
const (
	exampleDefaultsFile = "aveloxis.example.json"
	exampleDockerFile   = "aveloxis.docker.example.json"
	exampleShardedFile  = "aveloxis.sharded.example.json"
	docsSnippetLabel    = "docs/getting-started/configuration.md snippet"
)

var exampleValueAllowlist = map[string]map[string]string{
	exampleDefaultsFile: {
		// v0.27.147 (round 26): the example previously advertised
		// "$HOME/aveloxis-repos/" — but config.Load never expands env
		// vars, so a copied config created a relative directory
		// literally named $HOME. The example now uses a real absolute
		// path, which legitimately differs from the home-dir default.
		"RepoCloneDir": "default is home-dir-dependent (defaultCloneDir); the example uses a literal absolute path because aveloxis.json values are never env-expanded",
	},
	exampleDockerFile: {
		"RepoCloneDir": "container-local path for the compose bind mount; the default is home-dir-dependent and meaningless inside the image",
	},
	exampleShardedFile: {
		// This file is a TUNING PROFILE for a large sharded fleet, so
		// its throughput knobs deliberately differ. Anything NOT about
		// that profile (a retired default, a stale flag value) is a
		// bug in the example and must be fixed there.
		"RepoCloneDir":                 "placeholder the operator fills in; the default is home-dir-dependent",
		"DaysUntilRecollect":           "sharded profile: a 100K-repo fleet cannot re-walk daily",
		"Workers":                      "sharded profile: sized for the documented 180-worker deployment",
		"MatviewRebuildDay":            "sharded profile: Sunday rebuild to clear the weekday window",
		"ThreadingMode":                "sharded profile: this file exists to demonstrate threading_mode=sharded",
		"ShardSize":                    "sharded profile: paired with threading_mode above",
		"SearchResolveIntervalMinutes": "sharded profile: a larger contributor pool wants a tighter search-resolve cadence",
		"ShutdownGraceSeconds":         "sharded profile: 180 workers need longer than the 10s default to drain",
		"ScancodeWorkers":              "sharded profile: sized for the dedicated scancode host",
	},
	docsSnippetLabel: {
		"RepoCloneDir": "same reason as aveloxis.example.json: the snippet cannot print a home-dir-dependent default",
	},
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
	for _, name := range []string{exampleDefaultsFile, exampleDockerFile, exampleShardedFile} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile("../../" + name)
			if err != nil {
				t.Fatalf("read %s: %v", name, err)
			}
			assertCollectionMatchesEffectiveDefaults(t, name, raw)
		})
	}
}

// assertCollectionMatchesEffectiveDefaults applies doc's collection
// block over a pristine DefaultConfig and compares every field through
// its accessor. Shared by the three example configs and the docs
// snippet so the four can never drift apart from each other either.
func assertCollectionMatchesEffectiveDefaults(t *testing.T, label string, doc []byte) {
	t.Helper()
	var envelope struct {
		Collection json.RawMessage `json:"collection"`
	}
	if err := json.Unmarshal(doc, &envelope); err != nil {
		t.Fatalf("%s: parse: %v", label, err)
	}
	if len(envelope.Collection) == 0 {
		t.Fatalf("%s: must carry a collection block", label)
	}
	cfg := DefaultConfig()
	if err := json.Unmarshal(envelope.Collection, &cfg.Collection); err != nil {
		t.Fatalf("%s: apply collection block: %v", label, err)
	}
	defaultCfg := DefaultConfig()
	allow := exampleValueAllowlist[label]

	// Track which allowlisted fields actually differ, so an entry that
	// stopped suppressing anything fails instead of lingering as
	// permission nobody needs (the Phase-3 policy-registry pattern).
	stillDiffers := map[string]bool{}

	typ := reflect.TypeOf(CollectionConfig{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if _, allowed := allow[field.Name]; allowed {
			if getter, ok := effectiveAccessors[field.Name]; ok {
				stillDiffers[field.Name] = !reflect.DeepEqual(getter(&cfg.Collection), getter(&defaultCfg.Collection))
			} else {
				stillDiffers[field.Name] = !reflect.DeepEqual(
					reflect.ValueOf(cfg.Collection).Field(i).Interface(),
					reflect.ValueOf(defaultCfg.Collection).Field(i).Interface())
			}
			continue
		}
		var got, want any
		if getter, ok := effectiveAccessors[field.Name]; ok {
			got, want = getter(&cfg.Collection), getter(&defaultCfg.Collection)
		} else {
			got = reflect.ValueOf(cfg.Collection).Field(i).Interface()
			want = reflect.ValueOf(defaultCfg.Collection).Field(i).Interface()
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s teaches a NON-DEFAULT value for %s (json %q): "+
				"effective=%v default-effective=%v. Operators copy this file verbatim; "+
				"either fix the value, or add the field to exampleValueAllowlist[%q] with a reason.",
				label, field.Name, jsonTag(field), got, want, label)
		}
	}

	for name := range allow {
		if !stillDiffers[name] {
			t.Errorf("exampleValueAllowlist[%q][%q] suppresses nothing — %s now matches the compiled default. "+
				"Delete the entry; a stale exemption is standing permission for a future drift nobody reviewed.",
				label, name, name)
		}
	}
}

// TestConfigurationDocSnippetMatchesEffectiveDefaults — pass 44. The
// docs' "every supported option" snippet is a defaults document that
// nothing checked: docs_coverage_test.go matches KEYS only, so the
// snippet taught scancode_shutdown_grace_minutes: 30 for three months
// after the flip to 0, and threading_mode: "sharded" for three and a
// half against its own reference table. Run its collection block
// through the same effective-accessor comparison as the example JSON.
func TestConfigurationDocSnippetMatchesEffectiveDefaults(t *testing.T) {
	doc, err := os.ReadFile("../../docs/getting-started/configuration.md")
	if err != nil {
		t.Fatalf("read configuration.md: %v", err)
	}
	block := configurationDocSnippet(t, string(doc))
	assertCollectionMatchesEffectiveDefaults(t, docsSnippetLabel, []byte(block))

	// Values alone cannot police the snippet's "every supported
	// option" claim: an OMITTED key silently keeps the default, so the
	// comparison above passes and TestConfigurationDocsCoverEveryJSONField
	// passes too (it scans the whole document, and the reference table
	// below the snippet mentions every tag). Pass 45 proved the gap by
	// deleting staging_retention_hours from the snippet with both
	// tests still green. So assert presence explicitly.
	typ := reflect.TypeOf(CollectionConfig{})
	for i := 0; i < typ.NumField(); i++ {
		tag := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			continue
		}
		if !strings.Contains(block, `"`+tag+`"`) {
			t.Errorf("the configuration.md full-configuration snippet omits %q — it claims to carry "+
				"EVERY supported option, and an omitted key is invisible to the value check above "+
				"(absent means default). Add it to the snippet, or soften the claim.", tag)
		}
	}
}

// configurationDocSnippet returns the first fenced ```json block that
// carries a "collection" key — the "every supported option" example.
func configurationDocSnippet(t *testing.T, doc string) string {
	t.Helper()
	rest := doc
	for {
		open := strings.Index(rest, "```json\n")
		if open < 0 {
			t.Fatal("configuration.md must contain a fenced ```json full-configuration snippet with a collection block")
		}
		body := rest[open+len("```json\n"):]
		end := strings.Index(body, "\n```")
		if end < 0 {
			t.Fatal("unterminated ```json fence in configuration.md")
		}
		if block := body[:end]; strings.Contains(block, `"collection"`) {
			return block
		}
		rest = body[end:]
	}
}

func jsonTag(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		return f.Name
	}
	return tag
}

// TestExampleConfigsCarryNoEnvVarSyntax — v0.27.147 (round 26,
// suppressed finding): config.Load only JSON-unmarshals; nothing ever
// calls os.ExpandEnv. aveloxis.example.json advertised
// "$HOME/aveloxis-repos/", which a copying operator got LITERALLY — a
// relative directory named $HOME under the working directory. The
// committed example configs must never teach env-var syntax the
// loader does not implement.
func TestExampleConfigsCarryNoEnvVarSyntax(t *testing.T) {
	for _, f := range []string{
		"../../aveloxis.example.json",
		"../../aveloxis.docker.example.json",
		"../../aveloxis.sharded.example.json",
	} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("reading %s: %v", f, err)
		}
		for _, needle := range []string{"$HOME", "${", "\"~/"} {
			if strings.Contains(string(b), needle) {
				t.Errorf("%s contains %q — aveloxis.json values are used literally (no env/tilde expansion); use a real absolute path or omit the key for the computed default", f, needle)
			}
		}
	}
}
