// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// summary/49 review round 1 F5: the daily commit table serves a window only
// when both bounds are UTC midnights. Every default window a route hands
// TopContributors or GetRepoTimeSeries is one, or that route keeps the
// scattered commits-table plan (118 s on a kernel fork).
func TestDefaultWindowsAreUTCDayAligned(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/api/contributor_elsewhere.go"))
	if !strings.Contains(src, "since := db.UTCDay(time.Now()).AddDate(0, 0, -elsewhereWindowDays)") {
		t.Error("/contributors/elsewhere's default since must be a UTC midnight (db.UTCDay)")
	}
	if strings.Contains(src, "time.Now().AddDate(0, 0, -elsewhereWindowDays)") {
		t.Error("the unaligned default must be gone")
	}
	show := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/generate_showcase.go"))
	if !strings.Contains(show, "since := db.UTCDay(now).AddDate(-1, 0, 0)") {
		t.Error("the showcase generator's one-year window must start at a UTC midnight (db.UTCDay)")
	}
	// Round 2 F3: the upper bound was `now` — not a midnight — so the
	// aligned since was a no-op and the showcase kept the 118 s plan.
	if !strings.Contains(show, "store.TopContributors(ctx, repoID, since, time.Time{}, 5, true)") {
		t.Error("the showcase's top contributors must pass an open (zero) upper bound: `now` is not a UTC midnight")
	}
}
