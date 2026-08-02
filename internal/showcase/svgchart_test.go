// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

import (
	"strings"
	"testing"
	"time"
)

func chartWeek(i int) time.Time {
	return time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 7*i)
}

func TestRenderLineChartSVG(t *testing.T) {
	series := []ChartSeries{
		{Label: "Commits", Color: "#2563eb", Points: []ChartPoint{
			{T: chartWeek(0), V: 10}, {T: chartWeek(1), V: 40}, {T: chartWeek(2), V: 25},
		}},
		{Label: "Issues", Color: "#d97706", Points: []ChartPoint{
			{T: chartWeek(0), V: 5}, {T: chartWeek(1), V: 0}, {T: chartWeek(2), V: 8},
		}},
	}
	svg := RenderLineChartSVG(series, 720, 220)
	for _, needle := range []string{
		"<svg", "viewBox=", "</svg>",
		`stroke="#2563eb"`, `stroke="#d97706"`, // one polyline per series
		"<polyline",
	} {
		if !strings.Contains(svg, needle) {
			t.Errorf("chart missing %q", needle)
		}
	}
	if strings.Count(svg, "<polyline") != 2 {
		t.Errorf("want exactly 2 series polylines, got %d", strings.Count(svg, "<polyline"))
	}
	// Y axis scales to a nice bound at/above the max value (40 → 40 or 50).
	if !strings.Contains(svg, ">40<") && !strings.Contains(svg, ">50<") {
		t.Error("y-axis must label a nice max at/above the series max of 40")
	}
	// X axis carries date labels.
	if !strings.Contains(svg, "Jan") {
		t.Error("x-axis must carry date tick labels")
	}
}

func TestRenderLineChartSVGDegenerate(t *testing.T) {
	// Under two points nothing can be drawn — return "" so templates
	// render the honest empty state instead of a broken chart.
	if svg := RenderLineChartSVG(nil, 720, 220); svg != "" {
		t.Error("no series must render nothing")
	}
	one := []ChartSeries{{Label: "x", Color: "#000", Points: []ChartPoint{{T: chartWeek(0), V: 1}}}}
	if svg := RenderLineChartSVG(one, 720, 220); svg != "" {
		t.Error("a single point cannot be drawn as a line — return empty")
	}
	// All-zero series still render (a flat line is honest data).
	flat := []ChartSeries{{Label: "x", Color: "#2563eb", Points: []ChartPoint{
		{T: chartWeek(0), V: 0}, {T: chartWeek(1), V: 0},
	}}}
	if svg := RenderLineChartSVG(flat, 720, 220); !strings.Contains(svg, "<polyline") {
		t.Error("an all-zero series is honest data and must still draw")
	}
}

func TestDensifyWeekly(t *testing.T) {
	since := chartWeek(0)
	until := chartWeek(4) // 4 weekly buckets: 0,1,2,3
	sparse := []ChartPoint{{T: chartWeek(1), V: 7}, {T: chartWeek(3), V: 2}}
	out := DensifyWeekly(sparse, since, until)
	if len(out) != 4 {
		t.Fatalf("want 4 buckets, got %d", len(out))
	}
	want := []float64{0, 7, 0, 2}
	for i, w := range want {
		if out[i].V != w {
			t.Errorf("bucket %d = %v, want %v", i, out[i].V, w)
		}
	}
	// Join on date strings — the 2026-07-10 flat-line lesson: a point
	// carrying a non-UTC offset for the same Monday must still land.
	offset := []ChartPoint{{T: chartWeek(1).In(time.FixedZone("CST", -6*3600)), V: 9}}
	out2 := DensifyWeekly(offset, since, until)
	if out2[1].V != 9 {
		t.Errorf("offset-timezone point must land in its UTC bucket, got %v", out2[1].V)
	}
}
