// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// TestStagingRetentionConfigKnob — v0.22.4 item 5.
//
// The pre-v0.22.4 PurgeStagedProcessed query hardcoded `INTERVAL '7
// days'`. For a frequently-re-collected repo this stacked 3–5 cycles
// of processed JSONB tombstones in the staging table (verified
// empirically against aveloxis_large on 2026-05-16: zephyr had 84K
// staged issues against an actual 28K count). Not a correctness bug
// (Processor reads WHERE NOT processed) but real disk waste and
// diagnostic confusion.
//
// Pin: new CollectionConfig.StagingRetentionHours field with a
// fallback duration accessor defaulting to 1 hour.
func TestStagingRetentionConfigKnob(t *testing.T) {
	src, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Field declaration with json tag. Whitespace-tolerant — gofmt
	// aligns fields in columns so the exact spacing varies.
	fieldRE := regexp.MustCompile(`\bStagingRetentionHours\s+int\s+` + "`" + `json:"staging_retention_hours"` + "`")
	if !fieldRE.MatchString(code) {
		t.Error(`config.go: CollectionConfig must declare ` +
			"`StagingRetentionHours int `" + `json:"staging_retention_hours"` +
			"` so operators can tune the staging-cleanup window without recompiling")
	}

	// Accessor with a sensible default. Look for the function name +
	// a return path that yields a time.Duration. We don't constrain
	// the implementation shape (could be `if <=0 return default`,
	// `max(...)`, etc.) but the function must exist and return a
	// Duration.
	if !strings.Contains(code, "StagingRetentionDuration() time.Duration") {
		t.Error("config.go: CollectionConfig must expose " +
			"StagingRetentionDuration() time.Duration so the staging " +
			"cleanup goroutine can read a tuned interval")
	}
}

// TestStagingRetentionDurationDefault — behavioral test for the
// accessor's fallback when the field is unset (zero). 1 hour by
// design — operators who need forensic retention can raise it; the
// previous 7-day window was defensive overkill that wasted disk space
// across the whole fleet.
func TestStagingRetentionDurationDefault(t *testing.T) {
	c := &CollectionConfig{}
	got := c.StagingRetentionDuration()
	want := time.Hour
	if got != want {
		t.Errorf("default StagingRetentionDuration = %v, want %v", got, want)
	}
}

// TestStagingRetentionDurationHonorsExplicit — operator-tunable case.
func TestStagingRetentionDurationHonorsExplicit(t *testing.T) {
	c := &CollectionConfig{StagingRetentionHours: 24}
	got := c.StagingRetentionDuration()
	want := 24 * time.Hour
	if got != want {
		t.Errorf("StagingRetentionDuration with hours=24 = %v, want %v", got, want)
	}
}
