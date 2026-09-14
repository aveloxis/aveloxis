// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_knobs_test.go — C3: the Jira collector's config surface. Off by
// default (the mailing-list posture); politeness is self-imposed
// (issues.apache.org emits no rate-limit headers), so the polite email
// in the User-Agent is the operator's contact contract.
package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestJiraKnobsDefaults(t *testing.T) {
	var c CollectionConfig
	if c.JiraEnabled {
		t.Error("jira_enabled must default OFF")
	}
	if got := c.JiraWorkersOrDefault(); got != 1 {
		t.Errorf("workers default = %d, want 1", got)
	}
	if got := c.JiraCadenceDuration(); got != 24*time.Hour {
		t.Errorf("cadence default = %v, want 24h", got)
	}
}

func TestJiraKnobsEndToEnd(t *testing.T) {
	raw := []byte(`{"collection": {"jira_enabled": true, "jira_workers": 2,
		"jira_cadence_hours": 6, "jira_polite_email": "ops@x.org"}}`)
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if !cfg.Collection.JiraEnabled ||
		cfg.Collection.JiraWorkersOrDefault() != 2 ||
		cfg.Collection.JiraCadenceDuration() != 6*time.Hour ||
		cfg.Collection.JiraPoliteEmail != "ops@x.org" {
		t.Fatalf("end-to-end decode: %+v", cfg.Collection)
	}
}
