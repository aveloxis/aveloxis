// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strconv"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// vulnTile is the repository page's vulnerability tile (0.29.200; summary/70,
// 4.8, N4). Everything it shows is decided before the template: the count or
// a dash, by the coverage view's count_blank; the state and the second line
// in vulnclass's wordings.
type vulnTile struct {
	// Value is the count of current exposure findings, "—" where a 0 would
	// claim a check that did not happen, or "unavailable" when the
	// statistics could not be read.
	Value string
	// Critical marks the value red and is named in the label.
	Critical int
	// VersionNotDetermined counts the findings of dependencies with no
	// declared version, which are not in Value.
	VersionNotDetermined int
	// State is the coverage line ("scanned 2026-11-01"); SecondLine what is
	// shown beside it, "" for none.
	State      string
	SecondLine string
}

// statUnavailable is what a tile shows when its number could not be read. A
// 0 there would be a count nobody made.
const statUnavailable = "unavailable"

// statBlank is what the vulnerability tile shows in place of a count that
// would be a fabricated zero.
const statBlank = "—"

// vulnTileOf builds the tile from the statistics, which carry the coverage
// view. Nil statistics are "unavailable". Statistics with no coverage view
// (a repository that does not exist; none other since 0.29.197) show a
// non-zero count and blank a zero, as "coverage not known".
func vulnTileOf(stats *db.RepoStats) vulnTile {
	if stats == nil {
		return vulnTile{Value: statUnavailable}
	}
	t := vulnTile{
		Value: strconv.Itoa(stats.Vulnerabilities), Critical: stats.CriticalVulns, VersionNotDetermined: stats.VulnsVersionUnknown,
		State: vulnclass.CoverageLine(stats.Coverage), SecondLine: vulnclass.CoverageSecondLineText(stats.Coverage),
	}
	if (stats.Coverage == nil && stats.Vulnerabilities == 0) || (stats.Coverage != nil && stats.Coverage.CountBlank) {
		t.Value = statBlank
	}
	return t
}

// statOrUnavailable is a count tile's value: the number, or "unavailable"
// when the statistics could not be read.
func statOrUnavailable(stats *db.RepoStats, n func(*db.RepoStats) int) string {
	if stats == nil {
		return statUnavailable
	}
	return strconv.Itoa(n(stats))
}
