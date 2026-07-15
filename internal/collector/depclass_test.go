// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.11 — version-resolution classifier tests. The classes (in
// purl-construction precedence order): locked > exact > bounded-range
// > range-floor > unpinned. 'locked' is applied by the scan when a
// lockfile resolution exists (or for Go, exact-by-construction under
// MVS) — the pure classifier here handles the other four from the raw
// requirement string.

import "testing"

func TestClassifyRequirement(t *testing.T) {
	cases := []struct {
		requirement string
		version     string
		want        string
	}{
		// exact — ==X or bare version.
		{"==2.31.0", "2.31.0", resolutionExact},
		{"requests==2.31.0", "2.31.0", resolutionExact},
		{"1.2.3", "1.2.3", resolutionExact},
		{"=4.17.1", "4.17.1", resolutionExact},             // npm single-equals pin
		{`serde = "1.0.188"`, "1.0.188", resolutionExact},  // Cargo.toml line
		{`gem 'rails', '5.2.0'`, "5.2.0", resolutionExact}, // Gemfile line
		{"", "2.0.0", resolutionExact},                     // bare version, requirement unrecorded
		{"flask!=1.5,==1.4", "1.4", resolutionExact},       // == present, no bounds

		// bounded-range — upper bound exists: ~= / ^ / ~ / compound with < or <=.
		{"~=1.4.2", "1.4.2", resolutionBoundedRange},             // PEP 440 compatible release
		{"^4.17.1", "4.17.1", resolutionBoundedRange},            // npm caret
		{"~1.2.3", "1.2.3", resolutionBoundedRange},              // npm tilde
		{`gem 'rails', '~> 5.0'`, "5.0", resolutionBoundedRange}, // ruby pessimistic
		{">=1.0,<2.0", "1.0", resolutionBoundedRange},            // compound with upper bound
		{"apache-airflow>=2.0.0, <3.0", "2.0.0", resolutionBoundedRange},
		{"<=3.1", "3.1", resolutionBoundedRange},
		{">=1.2 <2.0", "1.2", resolutionBoundedRange},      // npm space-joined range
		{"1.2.x", "1.2.x", resolutionBoundedRange},         // npm x-range
		{"==1.*", "1.", resolutionBoundedRange},            // pypi wildcard pin admits a range
		{"1.2.3 - 2.3.4", "1.2.3", resolutionBoundedRange}, // npm hyphen range

		// range-floor — lower bound only.
		{">=3.0.0", "3.0.0", resolutionRangeFloor},
		{"apache-airflow>=3.0.0", "3.0.0", resolutionRangeFloor},
		{">1.5", "1.5", resolutionRangeFloor},
		{"requests >= 2.20", "2.20", resolutionRangeFloor},

		// unpinned — no version at all. Produces no findings today; the
		// classifier just must not panic and must label honestly.
		{"", "", resolutionUnpinned},
		{"*", "*", resolutionUnpinned},
		{"latest", "latest", resolutionUnpinned},
		{`gem 'rails'`, "", resolutionUnpinned},
	}
	for _, c := range cases {
		if got := classifyRequirement(c.requirement, c.version); got != c.want {
			t.Errorf("classifyRequirement(%q, %q) = %q, want %q", c.requirement, c.version, got, c.want)
		}
	}
}

// Garbage in, no panic out — the classifier sits on the scan hot path.
func TestClassifyRequirementNeverPanics(t *testing.T) {
	for _, req := range []string{"", " ", "\x00", "<><><>", "== == ==", "~", "^", ">=", "🎉", "a\nb"} {
		_ = classifyRequirement(req, "")
		_ = classifyRequirement(req, "1.0")
	}
}
