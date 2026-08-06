// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// svgchart.go — a tiny hand-rolled SVG chart renderer for the public
// showcase's STATIC charts (v0.27.80). No JS, no external libraries,
// no data endpoints: the chart is baked into the page at generation
// time, which keeps the auth surface untouched (the whole showcase
// design premise) and makes the pages fast + SEO-safe. Hand-rolled on
// the per-IP-rate-limiter precedent: polylines with axes, not a
// charting framework.
//
// The axis frame mirrors the live Chart.js rendering (operator
// report 2026-08-02): step-based y ticks (~9 labels, e.g. 0..450 by
// 50), BOTH horizontal and vertical gridlines, ~9 date labels, and
// the metric's unit rotated on the left — shared by the line and
// stacked renderers so the two chart families can't drift.

package showcase

import (
	"fmt"
	"html/template"
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

// LineChartOpts parameterizes RenderLineChart / RenderStackedBarChart.
type LineChartOpts struct {
	Width, Height int
	// Trend draws the live-site overlay per series: dashed OLS trend
	// line, dashed ±2σ residual tube with translucent fill, point
	// markers on the raw series, and larger red dots on tube-breaching
	// points (the lib/trend.js grammar, v0.27.16). A single series
	// uses the house green/amber colors; multi-series charts color the
	// overlays per series (the v0.27.16 compare rule — green/amber ×N
	// is unreadable).
	Trend bool
	// YLabel is the metric's unit (catalog `unit`, e.g. "people"),
	// rendered rotated along the y-axis like the live charts.
	YLabel string
}

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

// niceStep rounds a rough step up to 1/2/5×10^k.
func niceStep(rough float64) float64 {
	if rough <= 0 {
		return 1
	}
	mag := math.Pow(10, math.Floor(math.Log10(rough)))
	for _, m := range []float64{1, 2, 5, 10} {
		if rough <= m*mag {
			return m * mag
		}
	}
	return 10 * mag
}

// chartYTicksTarget sizes the y step so ~9-10 labels render — the
// live Chart.js density (a 0..405 series ticks 0..450 by 50, which
// needs the rough step 405/10 ≈ 40 to snap to 50, not 100).
const chartYTicksTarget = 10

// chartFrame is the shared axis frame: scales, grid, tick labels,
// and the rotated unit label. Both renderers draw inside one.
type chartFrame struct {
	width, height                      int
	marginL, marginR, marginT, marginB float64
	plotW, plotH                       float64
	yMin, yMax, step                   float64
	t0, t1                             time.Time
}

func newChartFrame(minV, maxV float64, t0, t1 time.Time, opts LineChartOpts) chartFrame {
	if minV > 0 {
		minV = 0
	}
	span := maxV - minV
	if span <= 0 {
		span = math.Max(math.Abs(maxV), 1)
	}
	step := niceStep(span / chartYTicksTarget)
	yMax := math.Ceil(maxV/step-1e-9) * step
	if yMax <= 0 {
		if minV < 0 {
			// All-negative domain (burstiness of a perfectly steady
			// repo ≈ -1 everywhere): top the axis at zero instead of a
			// phantom positive step (v0.27.87, Copilot round PR #173).
			yMax = 0
		} else {
			// All-zero degenerate: keep a visible axis.
			yMax = step
		}
	}
	yMin := 0.0
	if minV < 0 {
		yMin = math.Floor(minV/step+1e-9) * step
	}
	f := chartFrame{
		width: opts.Width, height: opts.Height,
		marginL: 46, marginR: 12, marginT: 10, marginB: 26,
		yMin: yMin, yMax: yMax, step: step, t0: t0, t1: t1,
	}
	if opts.YLabel != "" {
		f.marginL += 16
	}
	f.plotW = float64(opts.Width) - f.marginL - f.marginR
	f.plotH = float64(opts.Height) - f.marginT - f.marginB
	return f
}

func (f chartFrame) xOf(t time.Time) float64 {
	return f.marginL + f.plotW*float64(t.Sub(f.t0))/float64(f.t1.Sub(f.t0))
}

func (f chartFrame) yOf(v float64) float64 {
	return f.marginT + f.plotH*(f.yMax-v)/(f.yMax-f.yMin)
}

// renderOpen emits the opening <svg> tag, the full x+y grid with tick
// labels, and the rotated unit label.
func (f chartFrame) renderOpen(b *strings.Builder, yLabel string) {
	fmt.Fprintf(b, `<svg viewBox="0 0 %d %d" width="100%%" role="img" xmlns="http://www.w3.org/2000/svg">`, f.width, f.height)
	// Horizontal gridline + label per y step; the zero baseline stands
	// out when the domain crosses it (burstiness, velocity).
	for i := 0; ; i++ {
		v := f.yMin + f.step*float64(i)
		if v > f.yMax+f.step*0.001 {
			break
		}
		y := f.yOf(v)
		stroke := "rgba(37,99,235,0.10)"
		if f.yMin < 0 && math.Abs(v) < f.step*1e-6 {
			stroke = "rgba(70,85,122,0.35)"
		}
		fmt.Fprintf(b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>`,
			f.marginL, y, f.marginL+f.plotW, y, stroke)
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" text-anchor="end" font-size="10" fill="#66739a">%s</text>`,
			f.marginL-6, y+3.5, formatTick(v))
	}
	// Vertical gridline + date label at nine x ticks (live density).
	for i := 0; i <= 8; i++ {
		t := f.t0.Add(time.Duration(i) * f.t1.Sub(f.t0) / 8)
		x := f.xOf(t)
		fmt.Fprintf(b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="rgba(37,99,235,0.07)" stroke-width="1"/>`,
			x, f.marginT, x, f.marginT+f.plotH)
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" text-anchor="middle" font-size="10" fill="#66739a">%s</text>`,
			x, f.marginT+f.plotH+16, t.UTC().Format("Jan 2, 06"))
	}
	if yLabel != "" {
		midY := f.marginT + f.plotH/2
		fmt.Fprintf(b, `<text transform="rotate(-90 12 %.1f)" x="12" y="%.1f" text-anchor="middle" font-size="10" fill="#66739a">%s</text>`,
			midY, midY+3.5, template.HTMLEscapeString(yLabel))
	}
}

// RenderLineChartSVG draws the series as a plain line chart —
// RenderLineChart without overlays (kept for callers/tests that
// predate the trend grammar).
func RenderLineChartSVG(series []ChartSeries, width, height int) string {
	return RenderLineChart(series, LineChartOpts{Width: width, Height: height})
}

// RenderLineChart draws the series as a self-contained SVG. Returns
// "" when nothing can be drawn (no series with at least two points) so
// templates render an honest empty state. All-zero series still draw —
// a flat line is honest data. Legends are the caller's job (rendered
// in HTML next to the chart from the same series labels).
func RenderLineChart(series []ChartSeries, opts LineChartOpts) string {
	maxLen := 0
	maxV, minV := 0.0, 0.0
	var t0, t1 time.Time
	for _, s := range series {
		if len(s.Points) > maxLen {
			maxLen = len(s.Points)
		}
		for _, p := range s.Points {
			if p.V > maxV {
				maxV = p.V
			}
			if p.V < minV {
				minV = p.V
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
	// Trend fits (index-aligned with series). The tube can extend past
	// the data, so its extremes join the y-domain — the live charts do
	// the same via Chart.js auto-scaling.
	fits := make([]*TrendFit, len(series))
	if opts.Trend {
		for i, s := range series {
			f := FitTrend(s.Points)
			fits[i] = f
			if f == nil {
				continue
			}
			for j := f.Start; j < len(s.Points); j++ {
				y := f.Intercept + f.Slope*float64(j)
				maxV = math.Max(maxV, y+2*f.Sigma)
				minV = math.Min(minV, y-2*f.Sigma)
			}
		}
	}
	fr := newChartFrame(minV, maxV, t0, t1, opts)

	var b strings.Builder
	fr.renderOpen(&b, opts.YLabel)
	// Trend overlays draw BENEATH the raw series (trend.js order=1).
	if opts.Trend {
		for i, s := range series {
			f := fits[i]
			if f == nil || len(s.Points) < 2 {
				continue
			}
			trendColor, tubeColor, tubeFill := trendLineColor, trendTubeColor, trendTubeFill
			if len(series) > 1 {
				trendColor, tubeColor, tubeFill = s.Color, s.Color, hexToRGBA(s.Color, 0.08)
			}
			var trendPts, upperPts, lowerPts []string
			for j := f.Start; j < len(s.Points); j++ {
				x := fr.xOf(s.Points[j].T)
				y := f.Intercept + f.Slope*float64(j)
				trendPts = append(trendPts, fmt.Sprintf("%.1f,%.1f", x, fr.yOf(y)))
				upperPts = append(upperPts, fmt.Sprintf("%.1f,%.1f", x, fr.yOf(y+2*f.Sigma)))
				lowerPts = append(lowerPts, fmt.Sprintf("%.1f,%.1f", x, fr.yOf(y-2*f.Sigma)))
			}
			// Tube fill: upper boundary out, lower boundary back.
			rev := append([]string(nil), lowerPts...)
			for l, r := 0, len(rev)-1; l < r; l, r = l+1, r-1 {
				rev[l], rev[r] = rev[r], rev[l]
			}
			fmt.Fprintf(&b, `<polygon points="%s %s" fill="%s" stroke="none"/>`,
				strings.Join(upperPts, " "), strings.Join(rev, " "), tubeFill)
			for _, boundary := range [][]string{upperPts, lowerPts} {
				fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.2" stroke-dasharray="4 3"/>`,
					strings.Join(boundary, " "), tubeColor)
			}
			fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.6" stroke-dasharray="6 4"/>`,
				strings.Join(trendPts, " "), trendColor)
		}
	}
	// One polyline per drawable series.
	for _, s := range series {
		if len(s.Points) < 2 {
			continue
		}
		var pts []string
		for _, p := range s.Points {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", fr.xOf(p.T), fr.yOf(p.V)))
		}
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`,
			strings.Join(pts, " "), s.Color)
	}
	// Point markers + red breach dots ride on top (trend mode only).
	if opts.Trend {
		for i, s := range series {
			f := fits[i]
			for j, p := range s.Points {
				fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="2" fill="%s"/>`,
					fr.xOf(p.T), fr.yOf(p.V), s.Color)
				if f == nil || j < f.Start || f.Sigma == 0 {
					continue // σ = 0 (perfect fit) never flags breaches
				}
				if math.Abs(p.V-(f.Intercept+f.Slope*float64(j))) > 2*f.Sigma {
					fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="4" fill="%s"/>`,
						fr.xOf(p.T), fr.yOf(p.V), trendBreachFill)
				}
			}
		}
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// RenderStackedBarChartSVG is RenderStackedBarChart with default
// options (kept for callers/tests that predate LineChartOpts).
func RenderStackedBarChartSVG(bars []ChartSeries, overlay *ChartSeries, width, height int) string {
	return RenderStackedBarChart(bars, overlay, LineChartOpts{Width: width, Height: height})
}

// RenderStackedBarChart draws bar series STACKED per bucket with an
// optional line overlay — the signed-in activity-chart grammar
// (v0.27.16: commits + issues + PRs-opened stack; PRs-merged rides as
// a line because it is a subset of PRs-opened and stacking it would
// double-count). All bar series must share the caller-densified
// bucket grid. Returns "" when under two buckets exist; all-zero
// stacks still render the frame (honest flat state).
func RenderStackedBarChart(bars []ChartSeries, overlay *ChartSeries, opts LineChartOpts) string {
	buckets := 0
	var t0, t1 time.Time
	for _, s := range bars {
		if len(s.Points) > buckets {
			buckets = len(s.Points)
		}
		for _, p := range s.Points {
			if t0.IsZero() || p.T.Before(t0) {
				t0 = p.T
			}
			if p.T.After(t1) {
				t1 = p.T
			}
		}
	}
	if buckets < 2 || !t1.After(t0) {
		return ""
	}
	// Y max = tallest STACK (plus the overlay's own max).
	maxV := 0.0
	for i := 0; i < buckets; i++ {
		sum := 0.0
		for _, s := range bars {
			if i < len(s.Points) {
				sum += s.Points[i].V
			}
		}
		if sum > maxV {
			maxV = sum
		}
	}
	if overlay != nil {
		for _, p := range overlay.Points {
			if p.V > maxV {
				maxV = p.V
			}
		}
	}
	fr := newChartFrame(0, maxV, t0, t1, opts)
	slotW := fr.plotW / float64(buckets)
	barW := slotW * 0.8

	var b strings.Builder
	fr.renderOpen(&b, opts.YLabel)
	// Stacked segments, bottom-up in series order; zero heights skipped.
	for i := 0; i < buckets; i++ {
		base := 0.0
		for _, s := range bars {
			if i >= len(s.Points) || s.Points[i].V <= 0 {
				continue
			}
			v := s.Points[i].V
			x := fr.marginL + slotW*float64(i) + (slotW-barW)/2
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s"/>`,
				x, fr.yOf(base+v), barW, fr.yOf(base)-fr.yOf(base+v), s.Color)
			base += v
		}
	}
	if overlay != nil && len(overlay.Points) >= 2 {
		var pts []string
		for _, p := range overlay.Points {
			pts = append(pts, fmt.Sprintf("%.1f,%.1f", fr.xOf(p.T), fr.yOf(p.V)))
		}
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="%s" stroke-width="1.8" stroke-linejoin="round"/>`,
			strings.Join(pts, " "), overlay.Color)
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// ── Trend + ±2σ residual tube (v0.27.80) ──────────────────────────
//
// A Go PORT of aveloxis-gui/lib/trend.js (v0.27.16) — same formulas,
// same thresholds, same chip wording, so the static showcase charts
// can never disagree with the live ones. Math (from the JS header):
//   x = bucket index 0..n−1; x̄ = Σx/n; ȳ = Σy/n
//   slope b = Σ((x−x̄)(y−ȳ)) / Σ((x−x̄)²);  intercept a = ȳ − b·x̄
//   residual eᵢ = yᵢ − (a + b·xᵢ);  σ = sqrt(Σeᵢ²/n)  (population)
//   R² = 1 − Σeᵢ²/Σ((y−ȳ)²)   (SS_tot = 0 → R² = 0, clamped ≥ 0)
// Tube = trend ± 2σ; a point breaches when |eᵢ| > 2σ (σ = 0 never
// flags). n < 3 or a single distinct x → no fit.

// TrendR2Meaningful mirrors trend.js's R2_MEANINGFUL: below this the
// chip reads "no meaningful trend" — a presentation choice, not a
// statistical test.
const TrendR2Meaningful = 0.10

// Trend overlay colors — trend.js's F3 (-600/700) shades.
const (
	trendLineColor  = "#15803d"             // dashed green trend
	trendTubeColor  = "#b45309"             // dashed amber boundaries
	trendTubeFill   = "rgba(180,83,9,0.10)" // translucent amber fill
	trendBreachFill = "#dc2626"             // larger red anomaly dots
)

// TrendFit is trend.js's fit() result.
type TrendFit struct {
	N         int
	Slope     float64
	Intercept float64
	R2        float64
	Sigma     float64
	Mean      float64
	// Start is the index the fit begins at (see FirstActiveIndex —
	// the static stand-in for the live per-entity window clamp).
	Start int
}

// FirstActiveIndex returns the index of the first non-zero bucket.
// The live charts clamp the window to the entity's first activity so
// leading padding never biases the fit (the v0.27.24 lesson: padded
// buckets re-entering as fabricated zeros). Densified static series
// zero-fill the whole window, so the fit skips LEADING zeros the
// same way; mid-series zeros are real data and stay in. An all-zero
// series returns 0 (fit everything — flat is honest).
func FirstActiveIndex(points []ChartPoint) int {
	for i, p := range points {
		if p.V != 0 {
			return i
		}
	}
	return 0
}

// FitTrend ports trend.js fit() over points[start:] with x = global
// bucket index. Returns nil when fewer than 3 points or a single
// distinct x remain.
func FitTrend(points []ChartPoint) *TrendFit {
	start := FirstActiveIndex(points)
	pts := points[start:]
	n := len(pts)
	if n < 3 {
		return nil
	}
	var sx, sy float64
	for i, p := range pts {
		sx += float64(start + i)
		sy += p.V
	}
	mx, my := sx/float64(n), sy/float64(n)
	var sxx, sxy, syy float64
	for i, p := range pts {
		dx := float64(start+i) - mx
		sxx += dx * dx
		sxy += dx * (p.V - my)
		syy += (p.V - my) * (p.V - my)
	}
	if sxx == 0 {
		return nil
	}
	slope := sxy / sxx
	intercept := my - slope*mx
	var sse float64
	for i, p := range pts {
		e := p.V - (intercept + slope*float64(start+i))
		sse += e * e
	}
	sigma := math.Sqrt(sse / float64(n))
	r2 := 0.0
	if syy != 0 {
		r2 = math.Max(0, 1-sse/syy)
	}
	return &TrendFit{N: n, Slope: slope, Intercept: intercept, R2: r2, Sigma: sigma, Mean: my, Start: start}
}

// formatSlope mirrors trend.js's adaptive precision.
func formatSlope(v float64) string {
	a := math.Abs(v)
	switch {
	case a >= 100:
		return fmt.Sprintf("%.0f", v)
	case a >= 10:
		return fmt.Sprintf("%.1f", v)
	case a >= 1:
		return fmt.Sprintf("%.2f", v)
	default:
		return fmt.Sprintf("%.3f", v)
	}
}

// ChipText mirrors trend.js chipHTML's wording: the signed slope per
// week + R², or the honest "no meaningful trend" under the threshold.
func (f *TrendFit) ChipText() string {
	if f == nil {
		return ""
	}
	r2 := fmt.Sprintf("R² %.2f", f.R2)
	if f.R2 < TrendR2Meaningful {
		return "no meaningful trend · " + r2
	}
	if f.Slope >= 0 {
		return "↑ +" + formatSlope(f.Slope) + "/week · " + r2
	}
	return "↓ " + formatSlope(f.Slope) + "/week · " + r2
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

// hexToRGBA converts "#rrggbb" to an rgba() string (trend.js alpha()).
func hexToRGBA(hex string, a float64) string {
	if len(hex) != 7 || hex[0] != '#' {
		return hex
	}
	var r, g, bl int
	if _, err := fmt.Sscanf(hex[1:], "%02x%02x%02x", &r, &g, &bl); err != nil {
		return hex
	}
	return fmt.Sprintf("rgba(%d,%d,%d,%.2f)", r, g, bl, a)
}
