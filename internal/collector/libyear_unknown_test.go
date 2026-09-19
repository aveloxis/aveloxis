// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.57 — libyear 0 meant two different things, and the difference is
// the whole metric.
//
// calcLibyear returns 0 when EITHER release date is missing or unparseable,
// and nothing set NoLibyear for it, so "we could not work this out" was
// stored as "this dependency is perfectly up to date". Measured on
// chaoss.tv on 2026-09-17: of 3,196,143 rows carrying a libyear number,
// 1,704,902 had a missing date — 53%. Of every row reading libyear = 0,
// 87% meant unknown. The fleet average read 1.335 years where the honest
// figure was 2.861, and the median read 0.
//
// NULL is the existing, already-handled answer for "no timeline" — it is
// what GitHub Actions rows have carried since v0.27.47, and avg() and
// percentile_cont() both skip it. Two classes reach it:
//
//   - a dependency with no pinned version at all (an unpinned manifest
//     entry): no version means no release date, ever;
//   - a pinned version whose release date the registry would not give.
//
// The first can never be computed. The second heals when the registry
// answers — which for Go and Maven is what the rest of v0.29.56 fixed.

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestLibyearIsUnknownWhenADateIsMissing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		row        db.LibyearRow
		wantNoLib  bool
		wantReason string
	}{
		{
			name:      "both dates present: a real zero survives",
			row:       db.LibyearRow{CurrentReleaseDate: "2024-01-01T00:00:00Z", LatestReleaseDate: "2024-01-01T00:00:00Z", Libyear: 0},
			wantNoLib: false,
		},
		{
			name:      "both dates present and stale",
			row:       db.LibyearRow{CurrentReleaseDate: "2022-01-01T00:00:00Z", LatestReleaseDate: "2024-01-01T00:00:00Z", Libyear: 2},
			wantNoLib: false,
		},
		{
			name:      "no current release date",
			row:       db.LibyearRow{LatestReleaseDate: "2024-01-01T00:00:00Z"},
			wantNoLib: true,
		},
		{
			name:      "no latest release date",
			row:       db.LibyearRow{CurrentReleaseDate: "2024-01-01T00:00:00Z"},
			wantNoLib: true,
		},
		{
			name:      "neither date",
			row:       db.LibyearRow{},
			wantNoLib: true,
		},
		{
			// The clamp is load-bearing only when a number is present:
			// without this case the "unknown rows carry no number"
			// assertion below can never fire.
			name:      "a number alongside a missing date is cleared",
			row:       db.LibyearRow{Libyear: 3.4, LatestReleaseDate: "2024-01-01T00:00:00Z"},
			wantNoLib: true,
		},
		{
			name:      "an already-unknown row stays unknown",
			row:       db.LibyearRow{NoLibyear: true, CurrentReleaseDate: "2024-01-01T00:00:00Z", LatestReleaseDate: "2024-01-01T00:00:00Z"},
			wantNoLib: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.row
			markUnknownLibyear(&row)
			if row.NoLibyear != tc.wantNoLib {
				t.Errorf("NoLibyear = %v, want %v", row.NoLibyear, tc.wantNoLib)
			}
			// An unknown row must not also carry a number that would read
			// as a measurement if the NULL were ever dropped.
			if row.NoLibyear && row.Libyear != 0 {
				t.Errorf("Libyear = %v on an unknown row, want 0", row.Libyear)
			}
		})
	}
}

// The rule has to be applied where the rows are COLLECTED, not in each
// resolver: there are a dozen resolvers and every one of them would have to
// remember. This pins that the analysis path calls it.
func TestAnalysisAppliesTheUnknownLibyearRule(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/collector/analysis.go"))
	idx := strings.Index(src, "InsertRepoLibyear(ctx, repoID, lb)")
	if idx < 0 {
		t.Fatal("cannot find the libyear insert")
	}
	if mark := strings.Index(src, "markUnknownLibyear(lb)"); mark < 0 || mark > idx {
		t.Error("markUnknownLibyear must run on every resolved row BEFORE it is stored — applying it per resolver is how a dozen sites drift apart")
	}
}

// TestUnparseableDateIsUnknownNotZero — v0.29.57. The rule has to be asked
// of calcLibyear's OWN parser: a date that is present but unreadable yields
// 0 from calcLibyear exactly as a missing one does, so keying the flag on
// emptiness left that case storing a fabricated 0. Latent (no registry
// currently emits such a date) but it is the same defect one spelling away,
// and the doc comment claimed to cover it.
func TestUnparseableDateIsUnknownNotZero(t *testing.T) {
	const good = "2024-01-01T00:00:00Z"
	// The last three PARSE but are the zero time, which calcLibyear also
	// treats as no date. A shared layout list was not enough: the predicate
	// has to be the same function, or a date that reads as known still
	// stores a fabricated 0 (v0.29.57 round 2). Reachable via
	// fetchRegistryLastModified re-formatting a mirror's Last-Modified.
	for _, bad := range []string{"not-a-date", "0000-00-00", "2024/01/01", "1704067200",
		"0001-01-01T00:00:00Z", "0001-01-01T00:00:00", "0001-01-01 00:00:00"} {
		row := db.LibyearRow{CurrentReleaseDate: bad, LatestReleaseDate: good, Libyear: 0}
		markUnknownLibyear(&row)
		if !row.NoLibyear {
			t.Errorf("current date %q is unparseable but the row was stored as a measurement", bad)
		}
		// And the same date must be unreadable to calcLibyear — otherwise
		// the two spellings disagree, which is what sharing the layout list
		// prevents.
		if got := calcLibyear(bad, good); got != 0 {
			t.Errorf("calcLibyear(%q, good) = %v, want 0 — the predicate and the calculator must agree", bad, got)
		}
	}
	// A date calcLibyear CAN read must not be called unknown.
	for _, ok := range []string{good, "2024-01-01T00:00:00", "2024-01-01 00:00:00"} {
		row := db.LibyearRow{CurrentReleaseDate: ok, LatestReleaseDate: good}
		markUnknownLibyear(&row)
		if row.NoLibyear {
			t.Errorf("date %q is parseable but was called unknown", ok)
		}
	}
}
