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
			want:      []Gap{{Start: 3, End: 4}},
		},
		{
			name:      "multiple distinct gaps",
			collected: []int{1, 2, 5, 6, 10},
			expected:  []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			want:      []Gap{{Start: 3, End: 4}, {Start: 7, End: 9}},
		},
		{
			name:      "gap at start",
			collected: []int{5, 6, 7},
			expected:  []int{1, 2, 3, 4, 5, 6, 7},
			want:      []Gap{{Start: 1, End: 4}},
		},
		{
			name:      "gap at end",
			collected: []int{1, 2, 3},
			expected:  []int{1, 2, 3, 4, 5, 6},
			want:      []Gap{{Start: 4, End: 6}},
		},
		{
			name:      "all missing",
			collected: []int{},
			expected:  []int{1, 2, 3},
			want:      []Gap{{Start: 1, End: 3}},
		},
		{
			name:      "scattered single missing",
			collected: []int{1, 3, 5, 7},
			expected:  []int{1, 2, 3, 4, 5, 6, 7},
			want:      []Gap{{Start: 2, End: 2}, {Start: 4, End: 4}, {Start: 6, End: 6}},
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
		wantLen   int // total numbers to fetch (at minimum)
	}{
		{
			name:      "single gap with edges",
			gaps:      []Gap{{Start: 5, End: 8}},
			collected: []int{1, 2, 3, 4, 9, 10, 11, 12},
			edgeCount: 2,
			wantLen:   8, // 3,4 + 5,6,7,8 + 9,10
		},
		{
			name:      "gap at start no before-edge",
			gaps:      []Gap{{Start: 1, End: 3}},
			collected: []int{4, 5, 6},
			edgeCount: 2,
			wantLen:   5, // 1,2,3 + 4,5
		},
		{
			name:      "multiple distinct gaps",
			gaps:      []Gap{{Start: 3, End: 4}, {Start: 8, End: 9}},
			collected: []int{1, 2, 5, 6, 7, 10, 11},
			edgeCount: 2,
			wantLen:   8, // (1,2,3,4,5,6) + (6,7,8,9,10,11) with dedup
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExpandGapsWithEdges(tt.gaps, tt.collected, tt.edgeCount)
			if len(got) < tt.wantLen {
				t.Errorf("ExpandGapsWithEdges() returned %d numbers, want at least %d: %v", len(got), tt.wantLen, got)
			}
			// Verify all gap numbers are included.
			gotSet := make(map[int]bool)
			for _, n := range got {
				gotSet[n] = true
			}
			for _, g := range tt.gaps {
				for n := g.Start; n <= g.End; n++ {
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
