// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import "time"

// LatestPlausibleCommitTime is the one rule for "no real commit is dated
// at or after this" (2026-10-07, operator: NVIDIA/nova carries author
// dates in 2080, and every commit window ran up to them — the weekly
// chart's axis reached 2080 with three years of data in its first two
// percent, and the stored repos.last_commit_at, which only ever widened,
// kept the date for good). An author timestamp is an absolute instant (git
// gives it with its offset, parsed as RFC 3339), so a committer's zone
// cannot put it ahead of now; only a wrong clock can. The margin is a day
// of clock skew, rounded up to the next UTC midnight so the bound is a
// day-aligned window edge the daily commit table can serve: two UTC days
// ahead of now, which is between 24 and 48 hours of skew. Readers clamp
// their (half-open) upper bound to it, the facade's run bounds and the
// stored bounds exclude dates at or beyond it, and a stored bound at or
// beyond it reads as unfilled and is repaired on the repository's next
// walk. The rows themselves are kept: they are git's truth about the
// repository.
func LatestPlausibleCommitTime(now time.Time) time.Time {
	return utcDay(now).AddDate(0, 0, 2)
}

// BoundedUpper is a commit window's effective upper bound: the plausible
// bound when until is open or lies beyond it, until otherwise. Every reader
// of commit timestamps with a caller-supplied window goes through it.
func BoundedUpper(until time.Time) time.Time {
	bound := LatestPlausibleCommitTime(time.Now())
	if until.IsZero() || until.After(bound) {
		return bound
	}
	return until
}

// EarliestPlausibleCommitTime is the floor (2026-10-07, operator: the
// past end too). A Unix clock cannot produce an instant before the epoch,
// and the epoch itself — a timestamp of zero — is what every tool stamps
// when a clock is unset (a year-1 date is an uninitialised time, further
// back still), so no real commit is dated before the first UTC midnight
// after the epoch day. Converted histories from the 1970s on stay
// plausible. Readers clamp their lower bound to it, the facade's run bounds
// and the stored bounds exclude dates before it, and a stored first before
// it reads as unfilled and is repaired on the next walk.
func EarliestPlausibleCommitTime() time.Time {
	return time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC)
}

// BoundedLower is a commit window's effective lower bound: the plausible
// floor when since is open or lies before it, since otherwise.
func BoundedLower(since time.Time) time.Time {
	floor := EarliestPlausibleCommitTime()
	if since.IsZero() || since.Before(floor) {
		return floor
	}
	return since
}
