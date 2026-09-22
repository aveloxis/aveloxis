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
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

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
	// Copilot on PR #210: a 403 is a definitive answer (permission or
	// visibility), so it is not a failure "without an answer" either — it
	// gets its own count.
	if tally.forbidden != 1 {
		t.Errorf("forbidden = %d, want 1 (the 403)", tally.forbidden)
	}
	if tally.failed != 1 {
		t.Errorf("failed = %d, want 1 (the transient failure alone)", tally.failed)
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
	// A run whose only errors were 403s still reports them.
	var onlyForbidden breadthErrorTally
	onlyForbidden.note("suspended", platform.ErrForbidden)
	if !onlyForbidden.any() {
		t.Error("any() must report a tally holding only 403s")
	}
}

// The run's one WARN line names each kind apart, so an operator reading
// it can tell churn, a permission problem and a failure from one another.
func TestBreadthWarnNamesEachKindApart(t *testing.T) {
	store := &fakeBreadthStore{contributors: breadthFixture(3)}
	store.getNewestErrFor = map[string]error{
		store.contributors[0].ID: fmt.Errorf("events: %w", platform.ErrNotFound),
		store.contributors[1].ID: fmt.Errorf("events: %w", platform.ErrForbidden),
		store.contributors[2].ID: errors.New("events: connection reset"),
	}
	worker, logs := newBreadthLoggingWorker(t, store, http.NotFoundHandler())
	if _, err := worker.WithFetchConcurrency(1).Run(context.Background(), 3, time.Hour); err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	for _, want := range []string{"gone_accounts=1", "forbidden=1", "failed_without_an_answer=1"} {
		if !strings.Contains(out, want) {
			t.Errorf("the run's WARN must carry %s; got:\n%s", want, out)
		}
	}
}
