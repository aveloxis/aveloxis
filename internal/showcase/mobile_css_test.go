// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package showcase

// v0.27.90 — mobile retrofit for the generated public pages
// (summary/mobile-gui-plan-2026-08-05.md, Phase 1). The showcase
// templates carry a self-contained inline style block that had ZERO
// media queries, and the server-rendered SVG charts (viewBox 720×220,
// width=100%, font-size 10) scaled to ~4.6px text at phone width.
//
// Contract: charts SCROLL IN PLACE, they do not shrink — each SVG
// embed is wrapped in .chart-scroll (overflow-x auto) whose svg keeps
// a legible min-width. svgchart.go fonts/tick counts are deliberately
// untouched (the v0.27.80 frame-density tests pin them to the live
// Chart.js reference, and desktop must not move).

import (
	"os"
	"strings"
	"testing"
)

func templatesSrc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestShowcaseStyleHasPhoneBlock(t *testing.T) {
	src := templatesSrc(t)
	at := strings.Index(src, "@media (max-width: 640px)")
	if at < 0 {
		t.Fatal("the shared shell-head style must carry a 640px phone block — the generated pages had zero media queries")
	}
	block := src[at:]
	for _, needle := range []string{".wrap", ".top", "th, td"} {
		if !strings.Contains(block[:strings.Index(block, "</style>")], needle) {
			t.Errorf("the phone block must tighten %s (padding/wrap for 375px viewports)", needle)
		}
	}
}

func TestShowcaseChartsScrollInPlace(t *testing.T) {
	src := templatesSrc(t)
	// CSS: the scroll container + a legible floor on the SVG's width.
	if !strings.Contains(src, ".chart-scroll { overflow-x: auto") {
		t.Error("charts need the .chart-scroll overflow container — body scroll is not available")
	}
	if !strings.Contains(src, ".chart-scroll svg { min-width: 560px") {
		t.Error(".chart-scroll svg needs min-width: 560px — width=100% alone shrinks 10px chart text to ~4.6px at phone width")
	}
	// Every SVG embed site is wrapped. Three sites: the repo page's
	// activity chart, its metric charts, and the compare demo.
	for _, embed := range []string{
		`<div class="chart-scroll">{{.ActivityChart.SVG}}</div>`,
	} {
		if !strings.Contains(src, embed) {
			t.Errorf("chart embed must be wrapped in .chart-scroll: missing %s", embed)
		}
	}
	if got := strings.Count(src, `<div class="chart-scroll">{{.SVG}}</div>`); got != 2 {
		t.Errorf("both {{.SVG}} embed sites (metric charts + compare demo) must be wrapped in .chart-scroll — found %d of 2", got)
	}
}
