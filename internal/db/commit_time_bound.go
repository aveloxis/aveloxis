// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "time"

// LatestPlausibleCommitTime is the one rule for "no real commit is dated
// after this" (2026-10-07, operator: NVIDIA/nova carries author dates in
// 2080, and every commit window ran up to them — the weekly chart's axis
// reached 2080 with three years of data in its first two percent, and the
// stored repos.last_commit_at, which only ever widened, kept the date for
// good). Author dates are stamped by the committer's clock in the
// committer's zone: at UTC+14 a local calendar day is up to one day ahead
// of UTC, and one more day absorbs clock skew. So: two UTC days ahead of
// now, at midnight — a day-aligned edge the daily commit table can serve.
// Readers clamp their upper bound to it, the facade's run bounds and the
// stored bounds exclude dates beyond it, and a stored bound beyond it reads
// as unfilled and is repaired on the repository's next walk. The rows
// themselves are kept: they are git's truth about the repository.
func LatestPlausibleCommitTime(now time.Time) time.Time {
	return utcDay(now).AddDate(0, 0, 2)
}

// boundedUpper is the window's effective upper bound: the plausible bound
// when until is open or lies beyond it, until otherwise.
func boundedUpper(until time.Time) time.Time {
	bound := LatestPlausibleCommitTime(time.Now())
	if until.IsZero() || until.After(bound) {
		return bound
	}
	return until
}
