package collector

import (
	"testing"
)

// TestMissingPRsFromSet — the reconcile pass compares enumerated PR
// numbers (from phase 2's ListIssuesAndPRs) against the set of PR
// numbers successfully staged, and refetches the diff. The pure set
// operation is unit-testable without DB or network.
func TestMissingPRsFromSet(t *testing.T) {
	tests := []struct {
		name       string
		enumerated []int
		staged     []int
		want       []int
	}{
		{
			name:       "all present",
			enumerated: []int{1, 2, 3, 4, 5},
			staged:     []int{1, 2, 3, 4, 5},
			want:       nil,
		},
		{
			name:       "one missing",
			enumerated: []int{1, 2, 3, 4, 5},
			staged:     []int{1, 2, 4, 5},
			want:       []int{3},
		},
		{
			name:       "many missing",
			enumerated: []int{10, 20, 30, 40, 50, 60},
			staged:     []int{10, 40},
			want:       []int{20, 30, 50, 60},
		},
		{
			name:       "staged has extras (ignored)",
			enumerated: []int{1, 2, 3},
			staged:     []int{1, 2, 3, 99, 100}, // 99/100 showed up after enumeration — not reconcile's job
			want:       nil,
		},
		{
			name:       "empty enumerated",
			enumerated: nil,
			staged:     []int{1, 2},
			want:       nil,
		},
		{
			name:       "empty staged — everything missing",
			enumerated: []int{1, 2, 3},
			staged:     nil,
			want:       []int{1, 2, 3},
		},
		{
			name:       "duplicate numbers in enumerated do not duplicate in missing",
			enumerated: []int{1, 1, 2, 3},
			staged:     []int{1},
			want:       []int{2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := missingPRsFromSet(tt.enumerated, tt.staged)
			if !intSliceEqualAsSet(got, tt.want) {
				t.Errorf("missingPRsFromSet(%v, %v) = %v, want %v",
					tt.enumerated, tt.staged, got, tt.want)
			}
		})
	}
}

// intSliceEqualAsSet treats both slices as sets — order insensitive.
// The reconcile implementation may emit missing PRs in any order since
// Go map iteration is non-deterministic; the semantic contract is
// "same set of numbers".
func intSliceEqualAsSet(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[int]int{}
	for _, v := range a {
		m[v]++
	}
	for _, v := range b {
		m[v]--
		if m[v] < 0 {
			return false
		}
	}
	for _, count := range m {
		if count != 0 {
			return false
		}
	}
	return true
}
