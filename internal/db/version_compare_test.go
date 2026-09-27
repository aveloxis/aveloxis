// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"sort"
	"testing"
)

// CompareVersionish is the one semantic-ish version order (the GitHub
// Actions range check and the exposed-repositories version detail).
func TestCompareVersionish(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"1.0.2", "1.0.10", -1}, // numeric, not text: text order puts 1.0.10 first
		{"1.1.9", "1.1.15", -1},
		{"2.0.1", "1.1.15", 1},
		{"4.1", "4.1.0", 0}, // a missing segment reads as 0
		{"v1.2.3", "1.2.3", 0},
		{"1.0.0-rc1", "1.0.0-rc2", -1}, // non-numeric segments compare lexically
		{"1.2.x", "1.2.3", 1},          // a segment without digits sorts after one with
		{"1.0.1b1", "1.0.2", -1},       // digits first, then the rest
		{"1.0.1b1", "1.0.10", -1},
		{"1.0.1b1", "1.0.1", 1},
		{"4.10", "4.9", 1},
		{"007", "7", 0},
		{"99999999999999999999999", "100000000000000000000000", -1}, // no integer overflow
		{"", "", 0},
	} {
		if got := CompareVersionish(c.a, c.b); got != c.want {
			t.Errorf("CompareVersionish(%q, %q) = %d, want %d", c.a, c.b, got, c.want)
		}
		if got := CompareVersionish(c.b, c.a); got != -c.want {
			t.Errorf("CompareVersionish(%q, %q) = %d, want %d (antisymmetric)", c.b, c.a, got, -c.want)
		}
	}
}

// TestCompareVersionishIsASortKey — review of the 2026-09-23 table change:
// the earlier rule compared a mixed segment as text, which made a cycle
// (1.0.2 < 1.0.10 < 1.0.1b1 < 1.0.2) and gave sort an input-order-
// dependent result. Every permutation of a mixed set must sort the same.
func TestCompareVersionishIsASortKey(t *testing.T) {
	want := []string{"1.0.1", "1.0.1b1", "1.0.2", "1.0.10", "1.0.x", "2.0.0-rc.1"}
	perm := func(xs []string) [][]string {
		var out [][]string
		var rec func(int)
		rec = func(k int) {
			if k == len(xs) {
				out = append(out, append([]string(nil), xs...))
				return
			}
			for i := k; i < len(xs); i++ {
				xs[k], xs[i] = xs[i], xs[k]
				rec(k + 1)
				xs[k], xs[i] = xs[i], xs[k]
			}
		}
		rec(0)
		return out
	}
	for _, p := range perm(append([]string(nil), want...)) {
		sort.Slice(p, func(i, j int) bool { return CompareVersionish(p[i], p[j]) < 0 })
		for i := range want {
			if p[i] != want[i] {
				t.Fatalf("sorted %v, want %v (a comparator that is not transitive sorts by input order)", p, want)
			}
		}
	}
}
