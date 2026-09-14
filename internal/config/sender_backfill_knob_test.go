// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// sender_backfill_knob_test.go — Part A's cadence knob. The hardcoded
// hourly const in the scheduler is promoted to
// collection.mailing_list_sender_backfill_interval_minutes so operators
// control how fast email sender identities converge ("hours, not days").
// Named for the ticker it governs — runMailingListSenderBackfill (the
// DB-side identity join), NOT runMailingListSenderResolve (the API-side
// GitHub-search ticker, which keeps its own interval).
package config

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestMailingListSenderBackfillKnobDeclared pins the field + JSON tag.
func TestMailingListSenderBackfillKnobDeclared(t *testing.T) {
	f, ok := reflect.TypeOf(CollectionConfig{}).FieldByName("MailingListSenderBackfillMinutes")
	if !ok {
		t.Fatal("CollectionConfig.MailingListSenderBackfillMinutes missing")
	}
	if tag := strings.Split(f.Tag.Get("json"), ",")[0]; tag != "mailing_list_sender_backfill_interval_minutes" {
		t.Fatalf("json tag = %q, want mailing_list_sender_backfill_interval_minutes", tag)
	}
}

// TestMailingListSenderBackfillIntervalAccessor — SR-10: the accessor is
// the single default layer. Zero/negative → 60 minutes.
func TestMailingListSenderBackfillIntervalAccessor(t *testing.T) {
	var c CollectionConfig
	if got := c.MailingListSenderBackfillInterval(); got != 60*time.Minute {
		t.Fatalf("zero-value default = %v, want 60m", got)
	}
	c.MailingListSenderBackfillMinutes = 15
	if got := c.MailingListSenderBackfillInterval(); got != 15*time.Minute {
		t.Fatalf("explicit 15 = %v, want 15m", got)
	}
	c.MailingListSenderBackfillMinutes = -3
	if got := c.MailingListSenderBackfillInterval(); got != 60*time.Minute {
		t.Fatalf("negative = %v, want the 60m default", got)
	}
}

// TestMailingListSenderBackfillKnobEndToEnd — SR-10's end-to-end half:
// the JSON value must reach the accessor unclamped through the real
// decode path (the mailing_list_backfill_months double-clamp incident is
// why per-layer tests alone don't count).
func TestMailingListSenderBackfillKnobEndToEnd(t *testing.T) {
	raw := []byte(`{"collection": {"mailing_list_sender_backfill_interval_minutes": 7}}`)
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	if got := cfg.Collection.MailingListSenderBackfillInterval(); got != 7*time.Minute {
		t.Fatalf("JSON 7 → %v, want 7m", got)
	}
}
