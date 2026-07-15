// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMailConfigVulnDigestKnobs(t *testing.T) {
	var m MailConfig
	if got := m.VulnDigestMinSeverityOrDefault(); got != "HIGH" {
		t.Errorf("default severity floor must be HIGH, got %q", got)
	}
	if got := m.VulnDigestInterval(); got != 24*time.Hour {
		t.Errorf("default interval must be 24h, got %v", got)
	}
	m.VulnDigestMinSeverity = "CRITICAL"
	m.VulnDigestIntervalHours = 6
	if got := m.VulnDigestMinSeverityOrDefault(); got != "CRITICAL" {
		t.Errorf("explicit severity floor lost, got %q", got)
	}
	if got := m.VulnDigestInterval(); got != 6*time.Hour {
		t.Errorf("explicit interval lost, got %v", got)
	}
	m.VulnDigestIntervalHours = -3
	if got := m.VulnDigestInterval(); got != 24*time.Hour {
		t.Errorf("negative interval must clamp to default, got %v", got)
	}
}

func TestMailConfigVulnDigestJSONRoundTrip(t *testing.T) {
	raw := `{"operator_email":"ops@x.io","vuln_digest_min_severity":"MEDIUM","vuln_digest_interval_hours":12}`
	var m MailConfig
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	if m.OperatorEmail != "ops@x.io" || m.VulnDigestMinSeverityOrDefault() != "MEDIUM" || m.VulnDigestInterval() != 12*time.Hour {
		t.Errorf("JSON round-trip lost values: %+v", m)
	}
}
