// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-11 finding 3: the scan-failure arm was `logger.Warn(...);
// return`, which threw away every waiter already accumulated. The log
// line ALREADY said "this cycle's report is incomplete", so reporting
// what we have is what that wording promises — and during a blocked
// migration the accumulated holder PIDs are the whole point of the
// poll.
//
// Constraint: the round-3/4 pin bans both `break` and `continue` from
// this row loop (a keep-going arm is how a failed read became "no
// blocker" in the first place), so the fix is flag-and-fall-through: a
// failed Scan leaves the pgx iterator in an error state, rows.Next()
// ends the loop on its own, and rows.Err() carries the cause.
func TestCheckBlockersReportsPartialResultsOnReadFailure(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func checkBlockersFrom("))

	// The report loop is the fall-through target.
	reportAt := strings.Index(body, "for _, pid := range order {")
	if reportAt < 0 {
		t.Fatal("checkBlockersFrom must still render the accumulated waiters")
	}

	scanAt := strings.Index(body, "migration blocker poll: row read failed")
	if scanAt < 0 {
		t.Fatal("the scan-failure log line is gone from checkBlockersFrom")
	}
	// The scan-failure ARM itself may not abandon the cycle. (The
	// rows.Err() check further down still returns on context.Canceled:
	// during shutdown there is nothing to report and a WARN is noise.)
	// The arm ends at the next `} else` — which may be a plain else or
	// the round-11 finding 9 `else if !present` arm (that one DOES
	// return, deliberately: with no probing-session row every holder
	// would be placed on this host).
	armEnd := strings.Index(body[scanAt:], "\n\t\t} else")
	if armEnd < 0 {
		t.Fatalf("could not delimit the scan-failure arm; it must fall through to the accumulate branch.\nbody:\n%s", body[scanAt:min(len(body), scanAt+400)])
	}
	between := body[scanAt : scanAt+armEnd]
	if strings.Contains(between, "return") {
		t.Errorf("a failed row read must NOT abandon the waiters already accumulated (round-11\n"+
			"finding 3) — the log line promises an INCOMPLETE report, not no report, and the holder\n"+
			"PIDs are what the operator needs during a blocked migration.\nbetween:\n%s", between)
	}

	// The report must SAY it is partial, or the operator reads a
	// truncated holder list as the complete picture.
	if !strings.Contains(body, `"report_incomplete"`) {
		t.Error("the blocked-on-lock WARN must carry a report_incomplete attribute when a read failed —\n" +
			"a silently truncated holder list reads as the complete set of blockers.")
	}

	// The round-3/4 ban still holds: no keep-going arm in the row loop.
	for _, banned := range []string{"continue", "break"} {
		if strings.Contains(body, banned) {
			t.Errorf("checkBlockersFrom must not contain %q — flag-and-fall-through, not a keep-going arm", banned)
		}
	}
}

// Round-11 finding 5: blockerAdvice(local, other, background, hidden,
// unknown []int) took five same-typed positional slices, and `local` is
// the one bucket that emits `pg_terminate_backend` recipes. A
// transposition with `hidden` or `other` reproduces the 2026-09-09
// incident's failure mode — a terminate recipe for a backend that is
// not this host's — and the compiler cannot see it (all five are
// []int), while a consistent swap on both sides of the sole test stays
// green. A named struct makes the transposition unrepresentable.
func TestBlockerAdviceTakesNamedBuckets(t *testing.T) {
	got := blockerAdvice(holderBuckets{thisHost: []int{11}})
	if !strings.Contains(got, "pg_terminate_backend") {
		t.Errorf("the thisHost bucket is the one that carries the terminate recipe; got %q", got)
	}
	for _, c := range []struct {
		name string
		h    holderBuckets
	}{
		{"otherAddr", holderBuckets{otherAddr: []int{99}}},
		{"hidden", holderBuckets{hidden: []int{5}}},
		{"background", holderBuckets{background: []int{4242}}},
		{"unseen", holderBuckets{unseen: []int{7}}},
	} {
		if strings.Contains(blockerAdvice(c.h), "pg_terminate_backend") {
			t.Errorf("%s must never be offered for termination from here", c.name)
		}
	}
	if blockerAdvice(holderBuckets{}) != "" {
		t.Error("an empty bucket set renders nothing")
	}
}
