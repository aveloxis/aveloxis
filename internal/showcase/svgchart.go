// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// svgchart.go — a tiny hand-rolled SVG line-chart renderer for the
// public showcase's STATIC charts (v0.27.80). No JS, no external
// libraries, no data endpoints: the chart is baked into the page at
// generation time, which keeps the auth surface untouched (the whole
// showcase design premise) and makes the pages fast + SEO-safe.
// Hand-rolled on the per-IP-rate-limiter precedent: the need is a
// polyline with axes, not a charting framework.

package showcase

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// ChartPoint is one bucketed observation.
type ChartPoint struct {
	T time.Time
	V float64
}

// ChartSeries is one line on a chart.
type ChartSeries struct {
	Label  string
	Color  string // hex, from ChartPalette
	Points []ChartPoint
}

// ChartPalette is the series color order — the house light-grammar
// hues (blue, amber, green, violet) used by the GUI charts.
var ChartPalette = []string{"#2563eb", "#d97706", "#059669", "#7c3aed"}

// DensifyWeekly zero-fills a sparse weekly series onto the Monday grid
// covering [since, until). Buckets join on UTC DATE STRINGS, never
// time.Time equality — the 2026-07-10 flat-line lesson (an offset
// timestamp for the same Monday must land in its bucket).
func DensifyWeekly(points []ChartPoint, since, until time.Time) []ChartPoint {
	byDay := map[string]float64{}
	for _, p := range points {
		byDay[p.T.UTC().Format("2006-01-02")] += p.V
	}
	var out []ChartPoint
	for t := since.UTC(); t.Before(until); t = t.AddDate(0, 0, 7) {
		out = append(out, ChartPoint{T: t, V: byDay[t.Format("2006-01-02")]})
	}
	return out
}

// niceCeil rounds v up to a 1/2/5×10^k "nice" axis bound.
func niceCeil(v float64) float64 {
	if v <= 0 {
		return 1
	}
	mag := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 5, 10} {
		if v <= m*mag {
			return m * mag
		}
	}
	return 10 * mag
}

// RenderLineChartSVG draws the series as a self-contained SVG. Returns
// "" when nothing can be drawn (no series with at least two points) so
// templates render an honest empty state. All-zero series still draw —
// a flat line is real data. Legends are the caller's job (rendered in
// HTML next to the chart from the same series labels).
func RenderLineChartSVG(series []ChartSeries, width, height int) string {
	maxLen := 0
	maxV := 0.0
	var t0, t1 time.Time
	for _, s := range series {
		if len(s.Points) > maxLen {
			maxLen = len(s.Points)
		}
		for _, p := range s.Points {
			if p.V > maxV {
				maxV = p.V
			}
			if t0.IsZero() || p.T.Before(t0) {
				t0 = p.T
			}
			if p.T.After(t1) {
				t1 = p.T
			}
		}
	}
	if maxLen < 2 || !t1.After(t0) {
		return ""
	}
	yMax := niceCeil(maxV)

	const marginL, marginR, marginT, marginB = 46.0, 12.0, 10.0, 26.0
	plotW := float64(width) - marginL - marginR
	plotH := float64(height) - marginT - marginB
	xOf := func(t time.Time) float64 {
		return marginL + plotW*float64(t.Sub(t0))/float64(t1.Sub(t0))
	}
	yOf := func(v float64) float64 {
		return marginT + plotH*(1-v/yMax)
	}

	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %d %d" width="100%%" role="img" xmlns="http://www.w3.org/2000/svg">`, width, height)
	// Horizontal gridlines + y tick labels at 0, ½, max.
	for _, frac := range []float64{0, 0.5, 1} {
		v := yMax * frac
		y := yOf(v)
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="rgba(37,99,235,0.12)" stroke-width="1"/>`,
			marginL, y, marginL+plotW, y)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="end" font-size="10" fill="#66739a">%s</text>`,
			marginL-6, y+3.5, formatTick(v))
	}
	// X tick labels — five evenly spaced dates.
	for i := 0; i <= 4; i++ {
		t := t0.Add(time.Duration(i) * t1.Sub(t0) / 4)
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" text-anchor="middle" font-size="10" fill="#66739a">%s</text>`,
			xOf(t), marginT+plotH+16, t.UTC().Format("Jan 06"))
	}
	// One polyline per drawable series.
	for _, s := range series {
		if len(s.Points) < 2 {
			continue
		}
		var pts []string
		for _, p := range s.Points {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", xOf(p.T), yOf(p.V)))
		}
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`,
			strings.Join(pts, " "), s.Color)
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// formatTick renders an axis value compactly (1200 → "1.2k").
func formatTick(v float64) string {
	switch {
	case v >= 1_000_000:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", v/1_000_000), ".0") + "M"
	case v >= 1_000:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", v/1_000), ".0") + "k"
	default:
		return strings.TrimSuffix(fmt.Sprintf("%.1f", v), ".0")
	}
}
