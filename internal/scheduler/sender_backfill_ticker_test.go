// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// sender_backfill_ticker_test.go — Part A's ticker rewrite: the sender
// backfill walks keyset windows with a cursor that PERSISTS across
// ticks, bounded by floor/ceiling (never rows-affected), at the
// knob-driven interval. The pre-Part-A shape — one LIMIT-5000 batch per
// hourly tick — converged in ~31 days on the production aveloxis DB.
package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func senderBackfillBody(t *testing.T) string {
	t.Helper()
	src := srctest.Read(t, "internal/scheduler/mailinglist_wiring.go")
	return srctest.StripGoComments(srctest.FuncBody(t, src,
		"func (s *Scheduler) runMailingListSenderBackfill("))
}

// TestSenderBackfillTickerUsesKnob — the interval comes from the SR-10
// accessor, and the EFFECTIVE value is logged at startup (the
// log-the-effective-value rule).
func TestSenderBackfillTickerUsesKnob(t *testing.T) {
	body := senderBackfillBody(t)
	if !strings.Contains(body, "MailingListSenderBackfillInterval()") {
		t.Error("ticker must read the interval via the config accessor (SR-10), not a package const")
	}
	if !strings.Contains(body, "interval") || !strings.Contains(body, "Info(") {
		t.Error("ticker must log the effective interval at startup")
	}
}

// TestSenderBackfillTickerWalksKeysetWindows — the loop shape: bounds
// from floor/ceiling, cursor advanced by window, pass-complete reset;
// and the old single-batch call is BANNED (its return would silently
// reintroduce the 31-day convergence).
func TestSenderBackfillTickerWalksKeysetWindows(t *testing.T) {
	body := senderBackfillBody(t)
	for _, needle := range []string{
		"MailingListMsgIDFloor(",
		"MailingListMsgIDCeiling(",
		"BackfillMailingListSenderIDs(",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("ticker body must call %q", needle)
		}
	}
	if strings.Contains(body, "mailingListSenderBackfillBatch") {
		t.Error("the LIMIT-batch const must not return — keyset windows only")
	}
	// The cursor must be declared OUTSIDE the ticker select loop so a
	// per-tick window budget cannot starve the high-msg_id tail (the
	// adversarial-review #4 hole: budget-truncation + restart-from-0
	// never reaches the tail).
	cursorAt := strings.Index(body, "cursor")
	loopAt := strings.Index(body, "for {")
	if cursorAt == -1 || loopAt == -1 || cursorAt > loopAt {
		t.Errorf("pass cursor must be declared before the ticker loop so it persists across ticks (cursor@%d, loop@%d)", cursorAt, loopAt)
	}
}
