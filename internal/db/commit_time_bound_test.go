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
	// An author timestamp is an absolute instant (git gives its offset), so
	// only a wrong clock can place one ahead of now: a day of skew, rounded
	// up to the next UTC midnight — two UTC days ahead, a day-aligned
	// window edge (24–48 h of margin).
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

// 2026-10-07 (operator): the past end too. A Unix clock cannot produce an
// instant before the epoch, and the epoch itself is what every tool stamps
// when a clock is unset, so the earliest plausible commit time is the first
// UTC midnight after the epoch day. Converted histories from the 1970s stay
// plausible; year-0001 and epoch-zero garbage does not.
func TestEarliestPlausibleCommitTime(t *testing.T) {
	got := EarliestPlausibleCommitTime()
	if want := time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if !alignedToUTCDay(got) {
		t.Fatal("the floor is a UTC midnight: a day-aligned window edge")
	}
}

func TestResolveWindowClampsToThePlausibleFloor(t *testing.T) {
	floor := EarliestPlausibleCommitTime()
	if lower, _ := resolveWindow(time.Time{}, time.Time{}); !lower.Equal(floor) {
		t.Errorf("an open lower bound is the plausible floor, got %v want %v", lower, floor)
	}
	if lower, _ := resolveWindow(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}); !lower.Equal(floor) {
		t.Errorf("an explicit lower bound before the floor is clamped, got %v", lower)
	}
	epoch := time.Unix(0, 0).UTC()
	if lower, _ := resolveWindow(epoch, time.Time{}); !lower.Equal(floor) {
		t.Errorf("the epoch itself is below the floor, got %v", lower)
	}
	within := time.Date(1985, 1, 1, 0, 0, 0, 0, time.UTC)
	if lower, _ := resolveWindow(within, time.Time{}); !lower.Equal(within) {
		t.Errorf("an explicit lower bound within stands, got %v", lower)
	}
}
