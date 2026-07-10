// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestParseEntities(t *testing.T) {
	es, err := parseEntities("repo:12,org:github.com/chaoss")
	if err != nil || len(es) != 2 {
		t.Fatalf("parse: %v %v", es, err)
	}
	if es[0].Kind != "repo" || es[0].RepoID != 12 {
		t.Errorf("repo entity: %+v", es[0])
	}
	if es[1].Kind != "org" || es[1].Host != "github.com" || es[1].Login != "chaoss" {
		t.Errorf("org entity: %+v", es[1])
	}
	if _, err := parseEntities("repo:1,repo:2,repo:3,repo:4,repo:5,repo:6,repo:7,repo:8"); err == nil {
		t.Error("8 entities must be rejected (≤7 per operator requirement)")
	}
	for _, bad := range []string{"", "repo:x", "org:nohost", "banana:7"} {
		if _, err := parseEntities(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestMetricCatalogCompleteAndBranded(t *testing.T) {
	want := []string{"contributors", "change_requests", "change_requests_merged",
		"issues", "issues_closed", "code_change_commits", "committers",
		"burstiness", "project_velocity", "labor_investment",
		"upstream_dependencies", "license_coverage"}
	have := map[string]MetricDef{}
	for _, m := range metricCatalog {
		have[m.ID] = m
	}
	for _, id := range want {
		m, ok := have[id]
		if !ok {
			t.Errorf("catalog missing metric %q", id)
			continue
		}
		if m.ImprovesOn.ChaossURL == "" || m.ImprovesOn.DeltaNote == "" {
			t.Errorf("%s must carry the CHAOSS nominal link AND our delta note "+
				"(the 'Improvements on CHAOSS metrics' contract)", id)
		}
		if m.OurReferenceURL == "" || m.Definition == "" {
			t.Errorf("%s must carry our reference URL + definition", id)
		}
	}
}

// TestMetricsDocCoversCatalog: docs-as-data sync — every catalog id
// must have a section in docs/guide/metrics.md and vice versa.
func TestMetricsDocCoversCatalog(t *testing.T) {
	doc := mustReadFile(t, "../../docs/guide/metrics.md")
	for _, m := range metricCatalog {
		if !strings.Contains(doc, "## "+m.Name) && !strings.Contains(doc, "{#"+m.ID+"}") {
			t.Errorf("docs/guide/metrics.md missing a section for %q — the catalog and "+
				"the docs must not drift", m.ID)
		}
	}
	if !strings.Contains(doc, "Improvements on CHAOSS metrics") {
		t.Error("metrics.md must carry the operator's branding")
	}
}

func TestBurstinessMath(t *testing.T) {
	mk := func(vals ...float64) []db.WeeklyPoint {
		out := make([]db.WeeklyPoint, len(vals))
		base := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
		for i, v := range vals {
			out[i] = db.WeeklyPoint{Bucket: base.AddDate(0, 0, 7*i), Value: v}
		}
		return out
	}
	// Metronome-regular activity → B approaches −1 (σ=0).
	reg := burstinessSeries(mk(5, 5, 5, 5, 5, 5, 5, 5), 8)
	if got := reg[len(reg)-1].Value; got != -1 {
		t.Errorf("constant activity must give B=-1, got %f", got)
	}
	// One giant spike among silence → B > 0 (bursty).
	burst := burstinessSeries(mk(0, 0, 0, 0, 0, 0, 0, 100), 8)
	if got := burst[len(burst)-1].Value; got <= 0 {
		t.Errorf("spiky activity must give B>0, got %f", got)
	}
	// Silence → clamped to 0, not NaN.
	silent := burstinessSeries(mk(0, 0, 0), 3)
	if got := silent[len(silent)-1].Value; got != 0 || math.IsNaN(got) {
		t.Errorf("silent window must clamp to 0, got %f", got)
	}
}

func TestVelocityMath(t *testing.T) {
	mk := func(vals ...float64) []db.WeeklyPoint {
		out := make([]db.WeeklyPoint, len(vals))
		base := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
		for i, v := range vals {
			out[i] = db.WeeklyPoint{Bucket: base.AddDate(0, 0, 7*i), Value: v}
		}
		return out
	}
	// Rising components → last bucket's z-average must be positive,
	// first negative; constant component contributes 0 (σ=0 guard).
	v := velocitySeries([][]db.WeeklyPoint{mk(1, 2, 3, 4), mk(2, 4, 6, 8), mk(5, 5, 5, 5)})
	if v[0].Value >= 0 || v[len(v)-1].Value <= 0 {
		t.Errorf("velocity z-composite: first=%f last=%f", v[0].Value, v[len(v)-1].Value)
	}
}

func TestFillBucketsAligns(t *testing.T) {
	since := time.Date(2026, 1, 7, 12, 0, 0, 0, time.UTC) // a Wednesday
	until := since.AddDate(0, 0, 28)
	filled := fillBuckets(nil, since, until, "week")
	if len(filled) < 4 {
		t.Fatalf("expected ≥4 weekly buckets, got %d", len(filled))
	}
	if wd := filled[0].Bucket.Weekday(); wd != time.Monday {
		t.Errorf("weekly buckets must start Monday (match Postgres date_trunc), got %v", wd)
	}
}

// TestFillBucketsSurvivesOffsetTimestamps is the 2026-07-10 flat-line
// regression: a series whose buckets carry a non-UTC offset (what
// date_trunc returns under a non-UTC session) must still join onto
// the UTC-generated grid instead of zeroing out.
func TestFillBucketsSurvivesOffsetTimestamps(t *testing.T) {
	chicago := time.FixedZone("CDT", -5*3600)
	pts := []db.WeeklyPoint{
		{Bucket: time.Date(2026, 1, 5, 0, 0, 0, 0, chicago), Value: 42},
	}
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	filled := fillBuckets(pts, since, since.AddDate(0, 0, 21), "week")
	var sum float64
	for _, p := range filled {
		sum += p.Value
	}
	if sum != 42 {
		t.Errorf("offset-timestamped value must survive densification, sum=%f (flat-line bug)", sum)
	}
}

func TestAnalyticsRoutesRegistered(t *testing.T) {
	src := mustReadFile(t, "server.go")
	for _, route := range []string{
		`"GET /api/v1/metrics"`,
		`"GET /api/v1/compare"`,
		`"GET /api/v1/compare/snapshot"`,
		`"GET /api/v1/entities/search"`,
	} {
		if !strings.Contains(src, route) {
			t.Errorf("server.go must register %s", route)
		}
	}
}
