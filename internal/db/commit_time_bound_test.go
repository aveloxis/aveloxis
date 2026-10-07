// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
	"time"
)

// 2026-10-07 (operator, NVIDIA/nova's page): a few commits carry author
// dates far in the future (2080), and every commit window ran up to them —
// the weekly chart's axis reached 2080 with three years of data in its
// first two percent, and the stored last_commit_at (which only ever
// widened) kept the bogus date for good. One rule bounds every reader and
// writer: the latest plausible commit time.
func TestLatestPlausibleCommitTime(t *testing.T) {
	now := time.Date(2026, 10, 7, 15, 4, 5, 0, time.UTC)
	got := LatestPlausibleCommitTime(now)
	// A committer's clock at UTC+14 dates a commit up to one calendar day
	// ahead of UTC; one more day absorbs clock skew. Two UTC days ahead,
	// at midnight, so the bound is itself a day-aligned window edge.
	if want := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !alignedToUTCDay(got) {
		t.Fatal("the bound must be a UTC midnight so the daily commit table can serve a window ending there")
	}
	ny, _ := time.LoadLocation("America/New_York")
	if !LatestPlausibleCommitTime(now.In(ny)).Equal(got) {
		t.Fatal("the bound does not depend on the caller's zone")
	}
}

func TestResolveWindowClampsToThePlausibleBound(t *testing.T) {
	bound := LatestPlausibleCommitTime(time.Now())
	if _, upper := resolveWindow(time.Time{}, time.Time{}); !upper.Equal(bound) {
		t.Errorf("an open upper bound is the plausible bound, got %v want %v", upper, bound)
	}
	far := time.Date(2080, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, upper := resolveWindow(time.Time{}, far); !upper.Equal(bound) {
		t.Errorf("an explicit upper bound beyond it is clamped, got %v", upper)
	}
	past := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, upper := resolveWindow(time.Time{}, past); !upper.Equal(past) {
		t.Errorf("an explicit upper bound within it stands, got %v", upper)
	}
}
