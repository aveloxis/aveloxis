// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 28/29 (v0.28.18): `stop serve` inside the weekly rebuild is the
// common shape, not a failure — both halves classify context.Canceled
// FIRST, log at INFO, and RETURN (no ERROR, no "collection will resume"
// line for a process that is exiting). Each arm is checked for its own
// INFO needle and its `return`, and its ERROR needle must sit after the
// classification (a decorative arm that falls through, or an ERROR arm
// moved ahead of it, fails).
func TestWeeklyRebuildTreatsShutdownAsCanceledNotFailed(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) rebuildMatviews("))
	arms := []struct{ info, errorNeedle string }{
		{`"weekly matview rebuild canceled by shutdown — owed again on the next rebuild day"`, `"weekly matview rebuild failed"`},
		{`"dm_ aggregate refresh canceled by shutdown — owed again on the next rebuild day"`, `"dm_ aggregate refresh failed"`},
	}
	const classify = "errors.Is(err, context.Canceled)"
	pos := 0
	for _, arm := range arms {
		i := strings.Index(body[pos:], classify)
		if i < 0 {
			t.Fatalf("missing a %s classification for the %s arm", classify, arm.info)
		}
		i += pos
		end := strings.Index(body[i:], "} else if err != nil")
		if end < 0 {
			t.Fatalf("the canceled arm before %s must be followed by the `else if err != nil` failure arm", arm.info)
		}
		segment := body[i : i+end]
		if !strings.Contains(segment, arm.info) || !strings.Contains(segment, "return") {
			t.Errorf("the canceled arm must log %s at INFO AND return; got: %s", arm.info, strings.TrimSpace(segment))
		}
		if e := strings.Index(body, arm.errorNeedle); e >= 0 && e < i {
			t.Errorf("the ERROR arm %s must come AFTER the context.Canceled classification", arm.errorNeedle)
		}
		pos = i + end
	}
	if n := strings.Count(body, classify); n != 2 {
		t.Errorf("exactly two classifications expected in rebuildMatviews, found %d", n)
	}
}

// The same rule at every ticker task fed Run's ctx: a canceled error is
// shutdown, never a WARN "… failed" (pass 29 class sweep).
// The seven sites below are the per-message pins from pass 24; the set
// itself is enforced structurally by TestEveryTickerTaskClassifiesCancellation
// (ticker_cancel_structural_test.go — Copilot round 7 / pass 34), which
// derives every ticker task from Run, follows the scheduler-side
// helpers they call to a fixpoint, and checks every ctx-bound failure
// log in that set. Delegates in other packages are outside it.
func TestTickerTasksTreatShutdownAsCanceledNotFailed(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	for _, msg := range []string{
		`"staging cleanup failed"`, `"mailing-list staging cleanup failed"`, `"search resolve: failed to get candidates"`,
		`"affiliations population failed"`, `"never-scanned-orgs probe failed"`, `"org link reconciliation failed"`, `"breadth worker failed"`,
	} {
		i := strings.Index(src, msg)
		if i < 0 {
			t.Errorf("log site %s not found", msg)
			continue
		}
		// Anchor on THIS site's own failure arm — the `if err != nil {`
		// immediately before the message — and require the classification
		// right ahead of it (a neighbour's arm 372 chars earlier used to
		// satisfy a plain lookback).
		arm := strings.LastIndex(src[:i], "if err != nil {")
		if arm < 0 || i-arm > 200 {
			t.Errorf("%s must sit inside an `if err != nil {` failure arm (found at distance %d)", msg, i-arm)
			continue
		}
		// Two legitimate shapes: the classification right BEFORE the arm
		// (`if errors.Is(...) { return }` then `if err != nil {`), or as the
		// arm's FIRST statement (`if err != nil { if errors.Is(...) { return }`).
		// Either way it lies between shortly before the arm and the message.
		span := src[max(0, arm-160):i]
		cls := strings.Index(span, "errors.Is(err, context.Canceled)")
		if cls < 0 || !strings.Contains(span[cls:], "return") {
			t.Errorf("%s: the failure arm must classify errors.Is(err, context.Canceled) AND return before it logs (shutdown is not a failure); a classification that falls through is decorative", msg)
		}
	}
}
