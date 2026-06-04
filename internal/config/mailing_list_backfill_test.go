// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"testing"
)

// TestMailingListBackfillFullHistoryReachable pins the v0.25.12 fix: an
// explicit mailing_list_backfill_months of 0 (or negative) must reach the
// worker as-is so it triggers full-history collection. The pre-v0.25.12
// accessor coerced <= 0 to 6, making full history unreachable — lists only
// collected the recent ~6 months (the "only 2026" bug). Absent → 6.
func TestMailingListBackfillFullHistoryReachable(t *testing.T) {
	zero := 0
	neg := -1
	five := 5
	cases := []struct {
		name string
		in   *int
		want int
	}{
		{"absent (nil) → bounded default 6", nil, 6},
		{"explicit 0 → full history (passed through)", &zero, 0},
		{"explicit negative → full history (passed through)", &neg, -1},
		{"explicit positive → that window", &five, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &CollectionConfig{MailingListBackfillMonths: tc.in}
			if got := c.MailingListBackfillMonthsOrDefault(); got != tc.want {
				t.Errorf("MailingListBackfillMonthsOrDefault() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestMailingListBackfillJSONRoundTrip confirms the *int field distinguishes
// "absent" from "explicit 0" through the real load path (DefaultConfig base +
// JSON overlay), which is what makes 0 = full history actually work.
func TestMailingListBackfillJSONRoundTrip(t *testing.T) {
	// Absent: DefaultConfig base leaves it nil → accessor returns 6.
	base := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"collection":{}}`), base); err != nil {
		t.Fatal(err)
	}
	if got := base.Collection.MailingListBackfillMonthsOrDefault(); got != 6 {
		t.Errorf("absent field: got %d, want 6", got)
	}

	// Explicit 0: must survive as 0 (full history), not be coerced to 6.
	full := DefaultConfig()
	if err := json.Unmarshal([]byte(`{"collection":{"mailing_list_backfill_months":0}}`), full); err != nil {
		t.Fatal(err)
	}
	if got := full.Collection.MailingListBackfillMonthsOrDefault(); got != 0 {
		t.Errorf("explicit 0: got %d, want 0 (full history)", got)
	}
}
