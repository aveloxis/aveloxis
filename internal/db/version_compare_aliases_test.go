// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "testing"

// TestCompareVersionishAliases — worklist item 64(b), the two easy fixes the
// operator took (2026-09-28): PEP 440 spells one pre-release several ways
// (`c` is `rc`, `alpha` is `a`, `beta` is `b`, `pre`/`preview` are `rc`), and
// Maven-style `-final`/`-ga`/`-release` name the release itself, not a
// pre-release. Both were ordered by their text.
func TestCompareVersionishAliases(t *testing.T) {
	for _, tc := range []struct{ a, b string }{
		{"1.0rc1", "1.0c1"},
		{"1.0alpha2", "1.0a2"},
		{"1.0beta1", "1.0b1"},
		{"1.0pre1", "1.0rc1"},
		{"1.0preview1", "1.0rc1"},
		{"1.0.0-final", "1.0.0"},
		{"1.0-GA", "1.0"},
		{"2.1.0-release", "2.1.0"},
		{"2.1.0.RELEASE", "2.1.0"},
	} {
		if c := CompareVersionish(tc.a, tc.b); c != 0 {
			t.Errorf("CompareVersionish(%q, %q) = %d; want equal", tc.a, tc.b, c)
		}
	}
	// Still ordered as pre-releases, and still a total order.
	if CompareVersionish("1.0c1", "1.0") >= 0 || CompareVersionish("1.0beta1", "1.0rc1") >= 0 {
		t.Error("aliases must keep their pre-release order (b before rc, before the release)")
	}
	set := []string{"1.0a1", "1.0alpha2", "1.0b1", "1.0beta2", "1.0c1", "1.0rc2", "1.0", "1.0-final", "1.0.post1", "1.0-rc.1", "1.0.1"}
	for i := range set {
		for j := range set {
			if CompareVersionish(set[i], set[j]) != -CompareVersionish(set[j], set[i]) {
				t.Errorf("not antisymmetric: %q vs %q", set[i], set[j])
			}
			for k := range set {
				if CompareVersionish(set[i], set[j]) <= 0 && CompareVersionish(set[j], set[k]) <= 0 && CompareVersionish(set[i], set[k]) > 0 {
					t.Errorf("not transitive: %q <= %q <= %q but %q > %q", set[i], set[j], set[k], set[i], set[k])
				}
			}
		}
	}
}
