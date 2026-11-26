// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import "testing"

func TestClassifyActionRef(t *testing.T) {
	cases := map[string]string{
		"8214744c546c1e5c8f03dde8fab3a7353211988d": Locked,
		"deadbeef1":  Locked, // abbreviated SHA
		"v4.1.2":     Exact,
		"45.0.7":     Exact,
		"v4":         Unpinned,
		"main":       Unpinned,
		"release-v2": Unpinned,
	}
	for ref, want := range cases {
		if got := ClassifyActionRef(ref); got != want {
			t.Errorf("classifyActionRef(%q) = %q, want %q", ref, got, want)
		}
	}
}
