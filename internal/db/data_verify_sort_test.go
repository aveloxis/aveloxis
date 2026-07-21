// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "testing"

// TestSortVerifyResults pins most-severe-first ordering with stable
// order inside each severity — the report contract operators read.
func TestSortVerifyResults(t *testing.T) {
	in := []VerifyResult{
		{Check: "a", Severity: "OK"},
		{Check: "b", Severity: "FAIL"},
		{Check: "c", Severity: "WARN"},
		{Check: "d", Severity: "FAIL"},
		{Check: "e", Severity: "WARN"},
	}
	got := SortVerifyResults(in)
	want := []string{"b", "d", "c", "e", "a"}
	for i, w := range want {
		if got[i].Check != w {
			t.Fatalf("position %d: got %s want %s (full: %+v)", i, got[i].Check, w, got)
		}
	}
}
