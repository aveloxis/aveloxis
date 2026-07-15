// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.16 — contributor_retention metric registration + threshold
// parameter + compare-response Parts plumbing (8Knot port).

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRetentionMetricRegisteredInCatalog: the metric must ride the
// docs-as-data catalog like every other metric (this also arms the
// pre-existing TestMetricsDocCoversCatalog sync tripwire for it).
func TestRetentionMetricRegisteredInCatalog(t *testing.T) {
	m := catalogEntry("contributor_retention")
	if m == nil {
		t.Fatal("contributor_retention must be registered in metricCatalog")
	}
	if m.Kind != "temporal" {
		t.Errorf("kind = %q, want temporal (it is a /compare-able time series)", m.Kind)
	}
	if m.ImprovesOn.ChaossURL == "" || m.ImprovesOn.DeltaNote == "" {
		t.Error("contributor_retention must carry the CHAOSS nominal link AND our delta note")
	}
	if m.Definition == "" || m.Unit == "" {
		t.Error("contributor_retention must carry a definition and unit")
	}
	for _, needle := range []string{"drive-by", "repeat", "FIRST"} {
		if !strings.Contains(m.Definition, needle) {
			t.Errorf("definition must mention %q (the drive-by/repeat split bucketed by first contribution)", needle)
		}
	}
}

// TestRetentionDefaultThresholdMatches8Knot pins the verified 8Knot UI
// default (Contributions Required input, value=4) and that the source
// carries the citation so a future edit can re-verify it.
func TestRetentionDefaultThresholdMatches8Knot(t *testing.T) {
	if defaultRetentionThreshold != 4 {
		t.Errorf("defaultRetentionThreshold = %d, want 4 (8Knot's contrib_drive_repeat.py input default)", defaultRetentionThreshold)
	}
	src := mustReadFile(t, "retention.go")
	if !strings.Contains(src, "8Knot") || !strings.Contains(src, "contrib_drive_repeat") {
		t.Error("retention.go must cite the 8Knot source of the default threshold " +
			"(oss-aspen/8Knot contrib_drive_repeat.py) next to the constant")
	}
}

func TestParseRetentionThreshold(t *testing.T) {
	get := func(q string) (int, error) {
		r := httptest.NewRequest("GET", "/api/v1/compare"+q, nil)
		return parseRetentionThreshold(r)
	}
	if n, err := get(""); err != nil || n != defaultRetentionThreshold {
		t.Errorf("absent param: got (%d, %v), want (%d, nil)", n, err, defaultRetentionThreshold)
	}
	if n, err := get("?retention_threshold=7"); err != nil || n != 7 {
		t.Errorf("explicit 7: got (%d, %v)", n, err)
	}
	if n, err := get("?retention_threshold=1"); err != nil || n != 1 {
		t.Errorf("floor value 1: got (%d, %v)", n, err)
	}
	for _, bad := range []string{"?retention_threshold=0", "?retention_threshold=-3",
		"?retention_threshold=abc", "?retention_threshold=4.5"} {
		if _, err := get(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

// TestCompareCarriesPartsForRetention pins the response-shape
// extension: compareSeries gains Parts (omitted for single-series
// metrics), handleCompare routes through metricSeriesAndParts, and
// the cache key carries the threshold so different thresholds never
// collide in the 60s compare cache.
func TestCompareCarriesPartsForRetention(t *testing.T) {
	analytics := mustReadFile(t, "analytics.go")
	if !strings.Contains(analytics, `Parts map[string][]db.WeeklyPoint`) ||
		!strings.Contains(analytics, `json:"parts,omitempty"`) {
		t.Error("compareSeries must carry Parts map[string][]db.WeeklyPoint `json:\"parts,omitempty\"`")
	}
	if !strings.Contains(analytics, "metricSeriesAndParts(") {
		t.Error("handleCompare must route series computation through metricSeriesAndParts (v0.27.16)")
	}
	if !strings.Contains(analytics, "retentionThreshold") {
		t.Error("handleCompare must parse the retention threshold and fold it into the cache key")
	}
	retention := mustReadFile(t, "retention.go")
	if !strings.Contains(retention, "ContributorRetentionSeries") {
		t.Error("retention.go must call store.ContributorRetentionSeries")
	}
	for _, part := range []string{`"drive_by"`, `"repeat"`} {
		if !strings.Contains(retention, part) {
			t.Errorf("retention.go must name the %s component series", part)
		}
	}
}

// TestRetentionAPINeverTouchesExplorerMatview — same negative
// tripwire as the db side, applied to the API-layer file. Comments
// are stripped first so an explanatory mention can't false-match
// (the v0.21.5 lesson).
func TestRetentionAPINeverTouchesExplorerMatview(t *testing.T) {
	src := mustReadFile(t, "retention.go")
	var code []string
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "explorer_contributor_actions") {
		t.Error("retention.go must never reference explorer_contributor_actions " +
			"(operator decision: compute live from base tables)")
	}
}
