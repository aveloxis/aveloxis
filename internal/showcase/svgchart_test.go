// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

import (
	"fmt"
	"math"
	"regexp"
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

// TestRenderLineChartSVGNegativeDomain: burstiness lives in [-1, 1]
// and project velocity averages z-scores — both go NEGATIVE. The
// first shipped renderer assumed a 0-based axis and drew pytorch's
// burstiness line entirely OUTSIDE the plot (y 267-338 against a
// plot bottom of 174, measured on the 2026-08-02 preview). Negative
// values must stay inside the plot area.
func TestRenderLineChartSVGNegativeDomain(t *testing.T) {
	series := []ChartSeries{{Label: "Burstiness", Color: "#2563eb", Points: []ChartPoint{
		{T: chartWeek(0), V: -0.9}, {T: chartWeek(1), V: -0.4}, {T: chartWeek(2), V: 0.3},
	}}}
	const width, height = 720, 200
	svg := RenderLineChartSVG(series, width, height)
	if svg == "" {
		t.Fatal("negative-domain series must render")
	}
	m := regexpMustFind(t, svg, `<polyline points="([^"]+)"`)
	maxY := 0.0
	for _, p := range strings.Fields(m) {
		var y float64
		if _, err := fmt.Sscanf(p[strings.Index(p, ",")+1:], "%f", &y); err != nil {
			t.Fatal(err)
		}
		if y > maxY {
			maxY = y
		}
	}
	if maxY > float64(height) {
		t.Errorf("polyline reaches y=%.1f — outside the %dpx viewBox (negative values off-plot)", maxY, height)
	}
	// The axis labels the negative floor.
	if !strings.Contains(svg, ">-1<") && !strings.Contains(svg, ">-0.9<") {
		t.Error("negative domain must label its floor on the y-axis")
	}
}

func regexpMustFind(t *testing.T, s, pattern string) string {
	t.Helper()
	m := regexp.MustCompile(pattern).FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("pattern %q not found", pattern)
	}
	return m[1]
}

// TestChartFrameDensityAndLabels pins the live-parity axis frame
// (operator report 2026-08-02: static charts were "missing the
// Y-Axis labels, the x and y axis grids, and more frequent y-values
// along the grid"). Live reference: a 0..405 series ticks every 50
// (0, 50, …, 450) with a vertical gridline per date tick and the
// unit rotated on the left.
func TestChartFrameDensityAndLabels(t *testing.T) {
	var series ChartSeries
	series.Label, series.Color = "Contributors", "#2563eb"
	vals := []float64{22, 210, 240, 200, 240, 210, 205, 250, 275, 175, 210, 215,
		195, 150, 128, 245, 240, 255, 270, 280, 250, 245, 280, 295, 210, 240,
		235, 220, 295, 300, 378, 362, 380, 345, 280, 220, 225, 270, 405, 262, 220, 330}
	for i, v := range vals {
		series.Points = append(series.Points, ChartPoint{T: chartWeek(i), V: v})
	}
	svg := RenderLineChart([]ChartSeries{series}, LineChartOpts{
		Width: 720, Height: 220, YLabel: "people",
	})
	// Step-based y ticks: 0..450 by 50 — ten labels, not three.
	for _, tick := range []string{">0<", ">50<", ">200<", ">450<"} {
		if !strings.Contains(svg, tick) {
			t.Errorf("y-axis missing tick %s (want 0..450 by 50)", tick)
		}
	}
	if got := strings.Count(svg, `text-anchor="end"`); got < 8 {
		t.Errorf("want ≥8 y tick labels, got %d", got)
	}
	// Vertical gridlines ride every x tick; horizontal every y tick.
	if got := strings.Count(svg, "<line"); got < 17 {
		t.Errorf("want a full x+y grid (≥17 gridlines), got %d", got)
	}
	// ~9 date labels like the live axis, not 5.
	if got := strings.Count(svg, `text-anchor="middle"`); got < 8 {
		t.Errorf("want ≥8 x date labels, got %d", got)
	}
	// The unit label rides rotated on the left.
	if !strings.Contains(svg, "rotate(-90") || !strings.Contains(svg, ">people</text>") {
		t.Error("y-axis unit label must render rotated on the left")
	}
	// Stacked charts share the same frame.
	stacked := RenderStackedBarChart([]ChartSeries{series}, nil, LineChartOpts{
		Width: 720, Height: 220, YLabel: "count",
	})
	if !strings.Contains(stacked, "rotate(-90") || !strings.Contains(stacked, ">count</text>") {
		t.Error("stacked charts must carry the y-axis unit label too")
	}
	if got := strings.Count(stacked, `text-anchor="end"`); got < 8 {
		t.Errorf("stacked chart wants the dense y ticks too, got %d", got)
	}
}

// TestFitTrend pins the trend.js math port — same formulas, same
// edge behavior, so static charts can never disagree with the live
// trend component.
func TestFitTrend(t *testing.T) {
	pts := func(vs ...float64) []ChartPoint {
		out := make([]ChartPoint, len(vs))
		for i, v := range vs {
			out[i] = ChartPoint{T: chartWeek(i), V: v}
		}
		return out
	}
	// Perfect line y = 2x: slope 2, σ 0, R² 1.
	f := FitTrend(pts(0, 2, 4, 6))
	if f == nil || math.Abs(f.Slope-2) > 1e-9 || f.Sigma != 0 || math.Abs(f.R2-1) > 1e-9 {
		t.Errorf("perfect line: got %+v, want slope 2, sigma 0, R² 1", f)
	}
	// Constant series: SS_tot = 0 → R² = 0 (trend.js contract).
	f = FitTrend(pts(5, 5, 5, 5))
	if f == nil || f.Slope != 0 || f.R2 != 0 {
		t.Errorf("constant series: got %+v, want slope 0, R² 0", f)
	}
	// Under three points → no fit.
	if FitTrend(pts(1, 2)) != nil {
		t.Error("n < 3 must not fit")
	}
	// Leading zeros are the static stand-in for the live first-activity
	// window clamp (v0.27.24): they must NOT bias the fit. A young
	// repo's ramp preceded by dead zeros fits the ramp only.
	padded := pts(0, 0, 0, 0, 0, 0, 10, 12, 14, 16)
	f = FitTrend(padded)
	if f == nil || f.Start != 6 {
		t.Fatalf("fit must start at the first active bucket, got %+v", f)
	}
	if math.Abs(f.Slope-2) > 1e-9 {
		t.Errorf("leading zeros biased the fit: slope %v, want 2", f.Slope)
	}
}

func TestTrendChipText(t *testing.T) {
	weak := &TrendFit{R2: 0.03, Slope: 1.5}
	if got := weak.ChipText(); got != "no meaningful trend · R² 0.03" {
		t.Errorf("weak fit chip = %q", got)
	}
	up := &TrendFit{R2: 0.42, Slope: 1.238}
	if got := up.ChipText(); got != "↑ +1.24/week · R² 0.42" {
		t.Errorf("up chip = %q", got)
	}
	down := &TrendFit{R2: 0.55, Slope: -12.34}
	if got := down.ChipText(); got != "↓ -12.3/week · R² 0.55" {
		t.Errorf("down chip = %q", got)
	}
	var none *TrendFit
	if none.ChipText() != "" {
		t.Error("nil fit must produce an empty chip")
	}
}

// TestRenderLineChartWithTrend pins the full live-grammar overlay:
// dashed green trend, dashed amber tube boundaries + translucent
// fill, raw-point markers, and a larger red dot on the breaching
// outlier.
func TestRenderLineChartWithTrend(t *testing.T) {
	// A noisy-but-flat series with one wild outlier — the outlier
	// breaches the ±2σ tube.
	vals := []float64{10, 12, 9, 11, 10, 12, 9, 11, 10, 12, 9, 11, 60, 11, 10, 12}
	series := []ChartSeries{{Label: "Change Requests", Color: "#2563eb"}}
	for i, v := range vals {
		series[0].Points = append(series[0].Points, ChartPoint{T: chartWeek(i), V: v})
	}
	svg := RenderLineChart(series, LineChartOpts{Width: 720, Height: 200, Trend: true})
	for _, needle := range []string{
		`stroke="#15803d"`,           // dashed green trend line
		`stroke="#b45309"`,           // dashed amber tube boundaries
		`fill="rgba(180,83,9,0.10)"`, // translucent tube fill polygon
		`stroke-dasharray`,
		"<polygon",
		`r="2"`,                // raw point markers
		`r="4" fill="#dc2626"`, // the breaching outlier's red dot
	} {
		if !strings.Contains(svg, needle) {
			t.Errorf("trend chart missing %q", needle)
		}
	}
	if got := strings.Count(svg, `r="4"`); got != 1 {
		t.Errorf("exactly the one outlier breaches the tube, got %d red dots", got)
	}
	// Multi-series (the compare grammar): overlays take the SERIES
	// color — green/amber ×N is unreadable (v0.27.16 compare rule).
	two := append(series, ChartSeries{Label: "b", Color: "#d97706", Points: series[0].Points})
	svg = RenderLineChart(two, LineChartOpts{Width: 720, Height: 200, Trend: true})
	if strings.Contains(svg, `stroke="#15803d"`) {
		t.Error("multi-series trend overlays must use series colors, not the green/amber pair")
	}
	if !strings.Contains(svg, `fill="rgba(37,99,235,0.08)"`) {
		t.Error("multi-series tube fill must be the series color at low alpha")
	}
}

func TestRenderStackedBarChartSVG(t *testing.T) {
	bars := []ChartSeries{
		{Label: "Commits", Color: "#2563eb", Points: []ChartPoint{
			{T: chartWeek(0), V: 10}, {T: chartWeek(1), V: 20}, {T: chartWeek(2), V: 5},
		}},
		{Label: "Issues", Color: "#d97706", Points: []ChartPoint{
			{T: chartWeek(0), V: 5}, {T: chartWeek(1), V: 15}, {T: chartWeek(2), V: 0},
		}},
	}
	overlay := &ChartSeries{Label: "PRs merged", Color: "#7c3aed", Points: []ChartPoint{
		{T: chartWeek(0), V: 3}, {T: chartWeek(1), V: 8}, {T: chartWeek(2), V: 1},
	}}
	svg := RenderStackedBarChartSVG(bars, overlay, 720, 220)
	for _, needle := range []string{
		"<svg", "</svg>", "<rect", `fill="#2563eb"`, `fill="#d97706"`,
		"<polyline", `stroke="#7c3aed"`, "Jan",
	} {
		if !strings.Contains(svg, needle) {
			t.Errorf("stacked chart missing %q", needle)
		}
	}
	// One rect per non-zero segment: commits 3 + issues 2 (week 2 is 0).
	if got := strings.Count(svg, "<rect"); got != 5 {
		t.Errorf("want 5 bar segments (zero-height skipped), got %d", got)
	}
	// Y axis scales to the stacked MAX (20+15=35 → a tick at/above
	// 35), never the tallest single series (20).
	if !strings.Contains(svg, ">35<") && !strings.Contains(svg, ">40<") && !strings.Contains(svg, ">50<") {
		t.Error("y-axis must cover the stacked sum (35), not the tallest single series")
	}

	// Degenerate: under two buckets → "".
	if svg := RenderStackedBarChartSVG(nil, nil, 720, 220); svg != "" {
		t.Error("no bars must render nothing")
	}
	// All-zero bars still render axes (honest flat state).
	flat := []ChartSeries{{Label: "x", Color: "#2563eb", Points: []ChartPoint{
		{T: chartWeek(0), V: 0}, {T: chartWeek(1), V: 0},
	}}}
	if svg := RenderStackedBarChartSVG(flat, nil, 720, 220); !strings.Contains(svg, "<svg") {
		t.Error("all-zero stacks are honest data and must still render the frame")
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

// TestChartFrameAllNegativeDomainTopsAtZero (v0.27.87, Copilot round
// on PR #173): an all-negative series — burstiness of a perfectly
// steady repo sits near -1 for every bucket — must top its axis at 0,
// not at a phantom positive step that misrepresents the domain. The
// all-zero degenerate keeps yMax = step so a flat line still has a
// visible axis.
func TestChartFrameAllNegativeDomainTopsAtZero(t *testing.T) {
	opts := LineChartOpts{Width: 640, Height: 260}
	f := newChartFrame(-1.2, -0.8, time.Now().Add(-24*time.Hour), time.Now(), opts)
	if f.yMax != 0 {
		t.Errorf("all-negative domain must top the axis at 0, got yMax=%v", f.yMax)
	}
	if f.yMin >= 0 {
		t.Errorf("all-negative domain must keep a negative floor, got yMin=%v", f.yMin)
	}
	// All-zero degenerate: keep the existing visible-axis behavior.
	z := newChartFrame(0, 0, time.Now().Add(-24*time.Hour), time.Now(), opts)
	if z.yMax <= 0 {
		t.Errorf("all-zero series must keep a positive yMax for a visible axis, got %v", z.yMax)
	}
}
