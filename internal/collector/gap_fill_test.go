// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

// TestComputeGaps verifies gap detection between collected and expected numbers.
func TestComputeGaps(t *testing.T) {
	tests := []struct {
		name      string
		collected []int
		expected  []int
		want      []Gap
	}{
		{
			name:      "no gaps",
			collected: []int{1, 2, 3, 4, 5},
			expected:  []int{1, 2, 3, 4, 5},
			want:      nil,
		},
		{
			name:      "single contiguous gap",
			collected: []int{1, 2, 5, 6},
			expected:  []int{1, 2, 3, 4, 5, 6},
			want:      []Gap{{Start: 3, End: 4, Numbers: []int{3, 4}}},
		},
		{
			name:      "multiple distinct gaps",
			collected: []int{1, 2, 5, 6, 10},
			expected:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			want:      []Gap{{Start: 3, End: 4, Numbers: []int{3, 4}}, {Start: 7, End: 9, Numbers: []int{7, 8, 9}}},
		},
		{
			name:      "gap at start",
			collected: []int{5, 6, 7},
			expected:  []int{1, 2, 3, 4, 5, 6, 7},
			want:      []Gap{{Start: 1, End: 4, Numbers: []int{1, 2, 3, 4}}},
		},
		{
			name:      "gap at end",
			collected: []int{1, 2, 3},
			expected:  []int{1, 2, 3, 4, 5, 6},
			want:      []Gap{{Start: 4, End: 6, Numbers: []int{4, 5, 6}}},
		},
		{
			name:      "all missing",
			collected: []int{},
			expected:  []int{1, 2, 3},
			want:      []Gap{{Start: 1, End: 3, Numbers: []int{1, 2, 3}}},
		},
		{
			name:      "scattered single missing",
			collected: []int{1, 3, 5, 7},
			expected:  []int{1, 2, 3, 4, 5, 6, 7},
			want:      []Gap{{Start: 2, End: 2, Numbers: []int{2}}, {Start: 4, End: 4, Numbers: []int{4}}, {Start: 6, End: 6, Numbers: []int{6}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeGaps(tt.collected, tt.expected)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ComputeGaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestExpandGapsWithEdges verifies that gaps are expanded with edge items
// from the collected set for re-verification of associated data.
func TestExpandGapsWithEdges(t *testing.T) {
	tests := []struct {
		name      string
		gaps      []Gap
		collected []int
		edgeCount int
		wantLen   int // exact number of numbers to fetch (union of gaps + edges)
	}{
		{
			name:      "single gap with edges",
			gaps:      []Gap{{Start: 5, End: 8, Numbers: []int{5, 6, 7, 8}}},
			collected: []int{1, 2, 3, 4, 9, 10, 11, 12},
			edgeCount: 2,
			wantLen:   8, // 3,4 + 5,6,7,8 + 9,10
		},
		{
			name:      "gap at start no before-edge",
			gaps:      []Gap{{Start: 1, End: 3, Numbers: []int{1, 2, 3}}},
			collected: []int{4, 5, 6},
			edgeCount: 2,
			wantLen:   5, // 1,2,3 + 4,5
		},
		{
			name:      "multiple distinct gaps",
			gaps:      []Gap{{Start: 3, End: 4, Numbers: []int{3, 4}}, {Start: 8, End: 9, Numbers: []int{8, 9}}},
			collected: []int{1, 2, 5, 6, 7, 10, 11},
			edgeCount: 2,
			wantLen:   11, // (1,2,3,4,5,6) ∪ (6,7,8,9,10,11)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExpandGapsWithEdges(tt.gaps, tt.collected, tt.edgeCount)
			if len(got) != tt.wantLen {
				t.Errorf("ExpandGapsWithEdges() returned %d numbers, want exactly %d: %v", len(got), tt.wantLen, got)
			}
			// Verify all gap numbers are included.
			gotSet := make(map[int]bool)
			for _, n := range got {
				gotSet[n] = true
			}
			for _, g := range tt.gaps {
				for _, n := range g.Numbers {
					if !gotSet[n] {
						t.Errorf("missing gap number %d in result", n)
					}
				}
			}
		})
	}
}

// TestGapFillFileExists verifies gap_fill.go has the expected types and functions.
func TestGapFillFileExists(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{"ComputeGaps", "ExpandGapsWithEdges", "Gap", "GapFiller", "AssessAndFillGaps"} {
		if !strings.Contains(code, fn) {
			t.Errorf("gap_fill.go must contain %s", fn)
		}
	}
}

// TestGapFillDBMethods verifies DB methods exist for querying collected numbers.
func TestGapFillDBMethods(t *testing.T) {
	src, err := os.ReadFile("../db/gap_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	for _, fn := range []string{"GetCollectedIssueNumbers", "GetCollectedPRNumbers"} {
		if !strings.Contains(code, fn) {
			t.Errorf("gap_store.go must contain %s", fn)
		}
	}
}

// TestGapFillPlatformMethods verifies the platform interface has methods for
// fetching individual issues/PRs by number for targeted gap filling.
func TestGapFillPlatformMethods(t *testing.T) {
	src, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "FetchIssueByNumber") {
		t.Error("platform interface must have FetchIssueByNumber for targeted gap filling")
	}
	if !strings.Contains(code, "FetchPRByNumber") {
		t.Error("platform interface must have FetchPRByNumber for targeted gap filling")
	}
}

// TestGapFillThreshold verifies a 5% threshold constant exists.
func TestGapFillThreshold(t *testing.T) {
	src, err := os.ReadFile("gap_fill.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GapThreshold") || !strings.Contains(code, "0.05") {
		t.Error("gap_fill.go must define GapThreshold = 0.05 (5%)")
	}
}

// TestSchedulerCallsGapFill verifies the scheduler runs gap assessment
// after collection completes.
func TestSchedulerCallsGapFill(t *testing.T) {
	src, err := os.ReadFile("../scheduler/scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "AssessAndFillGaps") && !strings.Contains(code, "GapFill") {
		t.Error("scheduler must call gap fill after collection completes")
	}
}

// TestExpandGapsWithEdgesFetchesOnlyListedNumbers — v0.28.15: a gap whose
// bounds span numbers the listing never returned (on GitHub, issue
// numbers inside a PR gap) must NOT fetch those numbers. Pre-fix the
// expansion walked the integer range and produced 22,837 NOT_FOUND
// per-path errors on one production heal run.
func TestExpandGapsWithEdgesFetchesOnlyListedNumbers(t *testing.T) {
	collected := []int{1, 2, 8, 9}
	expected := []int{1, 2, 3, 7, 8, 9} // 4, 5, 6 are not this entity kind
	gaps := ComputeGaps(collected, expected)
	if len(gaps) != 1 || gaps[0].Start != 3 || gaps[0].End != 7 {
		t.Fatalf("ComputeGaps = %+v, want one gap [3,7]", gaps)
	}
	got := ExpandGapsWithEdges(gaps, collected, 2)
	gotSet := map[int]bool{}
	for _, n := range got {
		gotSet[n] = true
	}
	for _, want := range []int{3, 7, 1, 2, 8, 9} {
		if !gotSet[want] {
			t.Errorf("result %v must include listed-missing/edge number %d", got, want)
		}
	}
	for _, unlisted := range []int{4, 5, 6} {
		if gotSet[unlisted] {
			t.Errorf("result %v fetches %d, which the listing never returned — that is the wasted NOT_FOUND alias", got, unlisted)
		}
	}
}
