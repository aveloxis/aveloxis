// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// breadth_tally_test.go — v0.29.56. Two hours of the 2026-09-17 chaoss.tv
// log carried 184 identical WARN lines, one per contributor, all of them
// "not found: https://api.github.com/users/X/events" — a deleted account,
// which is an ANSWER, not a failure. The house rule is one aggregated line
// per batch (the v0.27.91 flood class), and the two cases must stay
// distinguishable: a gone account is expected, a lookup that failed
// without an answer is the one worth waking up for.

import (
	"errors"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestBreadthErrorTallySplitsAnswersFromFailures(t *testing.T) {
	var tally breadthErrorTally
	for _, login := range []string{"ghost1", "ghost2", "ghost3"} {
		tally.note(login, platform.ErrNotFound)
	}
	tally.note("busy", errors.New("exhausted 10 retries: transient"))
	tally.note("gone", platform.ErrGone)
	// A 403 is NOT a gone account: a suspended account or a narrowed token
	// scope is a systemic problem, and counting it as churn would hide it.
	tally.note("forbidden", platform.ErrForbidden)

	if tally.missing != 4 {
		t.Errorf("missing = %d, want 4 (three 404s and one 410 — the forge answered)", tally.missing)
	}
	if tally.failed != 2 {
		t.Errorf("failed = %d, want 2 (the transient failure and the 403)", tally.failed)
	}
	if tally.sampleLogin != "busy" || tally.sampleErr == nil {
		t.Errorf("sample = %q/%v, want the failure that said nothing about the account", tally.sampleLogin, tally.sampleErr)
	}
	if !tally.any() {
		t.Error("any() must report a non-empty tally")
	}
	var empty breadthErrorTally
	if empty.any() {
		t.Error("an empty tally must report nothing")
	}
}
