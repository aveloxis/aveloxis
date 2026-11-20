// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/vulnclass"
)

// The repository page's vulnerability tile (summary/70, 4.8, N4; first
// pinned in v0.29.70 round 6 F2 for the findings counted apart): the count
// or a dash by the coverage view, the findings whose version is not
// determined named beside it, and the coverage state under it.
func TestVulnTile(t *testing.T) {
	at := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	view := func(mut func(*vulnclass.CoverageInputs)) *vulnclass.CoverageView {
		in := vulnclass.CoverageInputs{HasRow: true, ScanStamp: &at}
		in.Coverage.AnalysedAt, in.Coverage.ScanAttemptAt = &at, &at
		in.Coverage.Scan.Outcome = model.ScanSent
		mut(&in)
		v := vulnclass.Coverage(in)
		return &v
	}
	for _, c := range []struct {
		what  string
		stats *db.RepoStats
		want  vulnTile
	}{
		{"the statistics could not be read", nil, vulnTile{Value: "unavailable"}},
		{"scanned, exposure, some version not determined",
			&db.RepoStats{Vulnerabilities: 5, CriticalVulns: 1, VulnsVersionUnknown: 2, Coverage: view(func(in *vulnclass.CoverageInputs) { in.CurrentExposure, in.HasFindings = 5, true })},
			vulnTile{Value: "5", Critical: 1, VersionNotDetermined: 2, State: "scanned 2026-11-01"}},
		{"scanned, nothing found: a real 0",
			&db.RepoStats{Coverage: view(func(*vulnclass.CoverageInputs) {})},
			vulnTile{Value: "0", State: "scanned 2026-11-01"}},
		{"never scanned: a dash, not a 0",
			&db.RepoStats{Coverage: view(func(in *vulnclass.CoverageInputs) { *in = vulnclass.CoverageInputs{} })},
			vulnTile{Value: "—", State: "not yet scanned"}},
		{"nothing could be sent, an older stamp: a dash",
			&db.RepoStats{Coverage: view(func(in *vulnclass.CoverageInputs) { in.Coverage.Scan.Outcome = model.ScanUnsendable })},
			vulnTile{Value: "—", State: "no dependency could be sent for checking; last completed scan 2026-11-01"}},
		{"scan failed, earlier findings: the count stays",
			&db.RepoStats{Vulnerabilities: 3, Coverage: view(func(in *vulnclass.CoverageInputs) {
				in.Coverage.Scan.Outcome, in.CurrentExposure, in.HasFindings = model.ScanFailed, 3, true
			})},
			vulnTile{Value: "3", State: "scan failed; last completed scan 2026-11-01"}},
		{"a failed analysis attempt is on the second line",
			&db.RepoStats{Coverage: view(func(in *vulnclass.CoverageInputs) {
				in.Coverage.AnalysedAt = nil
				in.Coverage.LastAttemptAt, in.Coverage.LastAttemptResult = &at, model.InventoryAttemptRootUnreadable
			})},
			vulnTile{Value: "0", State: "scanned 2026-11-01", SecondLine: "last analysis attempt failed (the clone could not be read)"}},
		{"no coverage view, no findings: not known, and blank",
			&db.RepoStats{}, vulnTile{Value: "—", State: "coverage not known"}},
		{"no coverage view, findings: the count stays",
			&db.RepoStats{Vulnerabilities: 4}, vulnTile{Value: "4", State: "coverage not known"}},
	} {
		if got := vulnTileOf(c.stats); got != c.want {
			t.Errorf("%s:\n got %+v\nwant %+v", c.what, got, c.want)
		}
	}
	if statOrUnavailable(nil, func(s *db.RepoStats) int { return s.GatheredIssues }) != "unavailable" ||
		statOrUnavailable(&db.RepoStats{GatheredIssues: 7}, func(s *db.RepoStats) int { return s.GatheredIssues }) != "7" {
		t.Error("a count tile shows its number, or unavailable when the statistics could not be read")
	}
}

// The tile's short label for the class is "version not determined"
// (summary/70, 8.2). "version unknown" is banned from the page templates
// and from what they render.
func TestTheTileDoesNotSayVersionUnknown(t *testing.T) {
	if strings.Contains(strings.ToLower(allTemplates), "version unknown") {
		t.Error(`a page template says "version unknown"; the label is "version not determined"`)
	}
	if !strings.Contains(allTemplates, "version not determined") {
		t.Error("the tile no longer names the findings whose version is not determined")
	}
}
