// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// TestSeveritiesAtOrAbove pins the digest severity floor expansion.
func TestSeveritiesAtOrAbove(t *testing.T) {
	cases := []struct {
		min  string
		want string
	}{
		{"CRITICAL", "CRITICAL"},
		{"HIGH", "CRITICAL,HIGH"},
		{"high", "CRITICAL,HIGH"},
		{"MEDIUM", "CRITICAL,HIGH,MEDIUM"},
		{"LOW", "CRITICAL,HIGH,MEDIUM,LOW"},
		{"ALL", "CRITICAL,HIGH,MEDIUM,LOW,UNKNOWN"},
		{"UNKNOWN", "CRITICAL,HIGH,MEDIUM,LOW,UNKNOWN"},
		{"", "CRITICAL,HIGH"},        // empty → conservative HIGH default
		{"bogus", "CRITICAL,HIGH"},   // unrecognized → HIGH default
		{"  High ", "CRITICAL,HIGH"}, // whitespace + case folded
	}
	for _, c := range cases {
		got := strings.Join(SeveritiesAtOrAbove(c.min), ",")
		if got != c.want {
			t.Errorf("SeveritiesAtOrAbove(%q) = %q, want %q", c.min, got, c.want)
		}
	}
}

// TestGetNewVulnerabilityFindingsQueryContract pins the load-bearing
// predicates: window on first_detected_at, unresolved only, severity
// set filter, most-severe-first ordering.
func TestGetNewVulnerabilityFindingsQueryContract(t *testing.T) {
	src, err := os.ReadFile("vuln_digest_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"v.first_detected_at > $1",
		"v.resolved_at IS NULL",
		"= ANY($2)",
		"WHEN 'CRITICAL' THEN 4",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("digest query missing %q", needle)
		}
	}
}
