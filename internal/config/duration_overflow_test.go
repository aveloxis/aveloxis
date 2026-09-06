// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"testing"
	"time"
)

// TestDurationAccessorsClampOverflow (Copilot round 18 on PR #193):
// an unbounded positive knob overflowed time.Duration(n)*unit into a
// NEGATIVE duration — the sender-backfill ticker panics on a
// non-positive NewTicker, and a negative Jira cadence hot-loops
// resyncs. Both accessors clamp before the multiply.
func TestDurationAccessorsClampOverflow(t *testing.T) {
	huge := 1 << 60 // times an hour/minute overflows int64 nanos

	c := &CollectionConfig{JiraCadenceHours: huge}
	if d := c.JiraCadenceDuration(); d <= 0 {
		t.Fatalf("JiraCadenceDuration() = %v, want a positive clamped value (a negative cadence hot-loops resyncs)", d)
	}
	c2 := &CollectionConfig{MailingListSenderBackfillMinutes: huge}
	if d := c2.MailingListSenderBackfillInterval(); d <= 0 {
		t.Fatalf("MailingListSenderBackfillInterval() = %v, want positive (time.NewTicker panics on <= 0)", d)
	}

	// Normal values are unchanged.
	if d := (&CollectionConfig{JiraCadenceHours: 6}).JiraCadenceDuration(); d != 6*time.Hour {
		t.Fatalf("JiraCadenceDuration(6) = %v, want 6h", d)
	}
	if d := (&CollectionConfig{MailingListSenderBackfillMinutes: 30}).MailingListSenderBackfillInterval(); d != 30*time.Minute {
		t.Fatalf("MailingListSenderBackfillInterval(30) = %v, want 30m", d)
	}
	// Defaults still hold.
	if d := (&CollectionConfig{}).JiraCadenceDuration(); d != 24*time.Hour {
		t.Fatalf("JiraCadenceDuration default = %v, want 24h", d)
	}
}
