// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// oauth_name_test.go — v0.27.87 (Copilot round, PR #173):
// splitOAuthName must trim before splitting. Pre-fix, a
// whitespace-only provider name (" ") passed the UPDATE path's
// `$9 = ''` preserve-guard and overwrote first_name with " " and
// last_name with '' — junk clobbering real names; a trailing space
// ("Sean ") clobbered last_name the same way.

package db

import "testing"

func TestSplitOAuthNameTrimsWhitespace(t *testing.T) {
	cases := []struct {
		in          string
		first, last string
	}{
		{"Sean Goggins", "Sean", "Goggins"},
		{"Sean Patrick Goggins", "Sean", "Goggins"}, // middle names fall away (v0.19.0 rule)
		{"Sean", "Sean", ""},
		{"", "", ""},
		// The Copilot-flagged edges: whitespace-only must behave as
		// EMPTY (so the caller's preserve-stored guard engages), and
		// leading/trailing spaces must not fabricate name tokens.
		{" ", "", ""},
		{"   ", "", ""},
		{"Sean ", "Sean", ""},
		{" Sean Goggins ", "Sean", "Goggins"},
	}
	for _, c := range cases {
		first, last := splitOAuthName(c.in)
		if first != c.first || last != c.last {
			t.Errorf("splitOAuthName(%q) = (%q, %q); want (%q, %q)",
				c.in, first, last, c.first, c.last)
		}
	}
}
