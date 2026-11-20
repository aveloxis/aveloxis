// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import "testing"

func TestClassifyRequirement(t *testing.T) {
	cases := []struct {
		requirement string
		version     string
		want        string
	}{
		// exact — ==X or bare version.
		{"==2.31.0", "2.31.0", Exact},
		{"requests==2.31.0", "2.31.0", Exact},
		{"1.2.3", "1.2.3", Exact},
		{"=4.17.1", "4.17.1", Exact},             // npm single-equals pin
		{`serde = "1.0.188"`, "1.0.188", Exact},  // Cargo.toml line
		{`gem 'rails', '5.2.0'`, "5.2.0", Exact}, // Gemfile line
		{"", "2.0.0", Exact},                     // bare version, requirement unrecorded
		{"flask!=1.5,==1.4", "1.4", Exact},       // == present, no bounds

		// bounded-range — upper bound exists: ~= / ^ / ~ / compound with < or <=.
		{"~=1.4.2", "1.4.2", BoundedRange},             // PEP 440 compatible release
		{"^4.17.1", "4.17.1", BoundedRange},            // npm caret
		{"~1.2.3", "1.2.3", BoundedRange},              // npm tilde
		{`gem 'rails', '~> 5.0'`, "5.0", BoundedRange}, // ruby pessimistic
		{">=1.0,<2.0", "1.0", BoundedRange},            // compound with upper bound
		{"apache-airflow>=2.0.0, <3.0", "2.0.0", BoundedRange},
		{"<=3.1", "3.1", BoundedRange},
		{">=1.2 <2.0", "1.2", BoundedRange},      // npm space-joined range
		{"1.2.x", "1.2.x", BoundedRange},         // npm x-range
		{"==1.*", "1.", BoundedRange},            // pypi wildcard pin admits a range
		{"1.2.3 - 2.3.4", "1.2.3", BoundedRange}, // npm hyphen range

		// range-floor — lower bound only.
		{">=3.0.0", "3.0.0", RangeFloor},
		{"apache-airflow>=3.0.0", "3.0.0", RangeFloor},
		{">1.5", "1.5", RangeFloor},
		{"requests >= 2.20", "2.20", RangeFloor},

		// unpinned — no version at all. Produces no findings today; the
		// classifier just must not panic and must label honestly.
		{"", "", Unpinned},
		{"*", "*", Unpinned},
		{"latest", "latest", Unpinned},
		{`gem 'rails'`, "", Unpinned},
	}
	for _, c := range cases {
		if got := ClassifyRequirement(c.requirement, c.version); got != c.want {
			t.Errorf("classifyRequirement(%q, %q) = %q, want %q", c.requirement, c.version, got, c.want)
		}
	}
}

// Garbage in, no panic out — the classifier sits on the scan hot path.
func TestClassifyRequirementNeverPanics(t *testing.T) {
	for _, req := range []string{"", " ", "\x00", "<><><>", "== == ==", "~", "^", ">=", "🎉", "a\nb"} {
		_ = ClassifyRequirement(req, "")
		_ = ClassifyRequirement(req, "1.0")
	}
}
