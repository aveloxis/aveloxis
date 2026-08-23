// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.2 — the showcase round (PDF items 1a/1b/1c/1e + 4).

// Item 1a: chart windows CLAMP to last_collected. The old
// now-anchored window let DensifyWeekly zero-fill every week between
// the last collection and generation time — a fabricated cliff that
// also dragged FitTrend's slope (trailing zeros count as data).
func TestClampedChartWindow(t *testing.T) {
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC) // a Sunday
	// Last collected five weeks ago → until clamps to THAT week's end.
	lc := now.AddDate(0, 0, -35)
	since, until := clampedChartWindow(now, lc)
	wantUntil := weekStartUTC(lc).AddDate(0, 0, 7)
	if !until.Equal(wantUntil) {
		t.Errorf("until = %v, want the last-collected week's boundary %v", until, wantUntil)
	}
	if !since.Equal(until.AddDate(0, 0, -52*7)) {
		t.Error("since must stay 52 buckets before the clamped until (full window of real data)")
	}
	// Bucket-boundary contract: the clamp must land on a Monday so
	// DensifyWeekly's date-string join still matches (2026-07-10
	// flat-line lesson).
	if until.Weekday() != time.Monday {
		t.Errorf("clamped until must be a Monday bucket boundary, got %v", until.Weekday())
	}
	// A fresh collection (this week) doesn't clamp below now's window.
	_, untilFresh := clampedChartWindow(now, now.AddDate(0, 0, -1))
	if !untilFresh.Equal(weekStartUTC(now).AddDate(0, 0, 7)) {
		t.Error("a current-week collection must keep the now-anchored window")
	}
	// Zero lastCollected (legacy) degrades to the unclamped window.
	_, untilZero := clampedChartWindow(now, time.Time{})
	if !untilZero.Equal(weekStartUTC(now).AddDate(0, 0, 7)) {
		t.Error("zero lastCollected must degrade to the now-anchored window")
	}
}

func TestShowcaseChartCallersUseClampedWindow(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/generate_showcase.go")
	if strings.Contains(src, "func showcaseChartWindow(") {
		t.Error("the now-anchored showcaseChartWindow must be gone — clampedChartWindow supersedes it (remove-don't-deprecate)")
	}
	if strings.Count(src, "clampedChartWindow(now,") < 3 {
		t.Error("activity + metric charts + compare demo must all use the clamped window")
	}
	// Captions must name the window's actual end, not claim
	// "trailing 12 months" over a clamped range.
	if !strings.Contains(src, "windowEndLabel(until)") {
		t.Error("captions must name the window end via windowEndLabel")
	}
}

// Item 1b: static SBOMs for featured repos — per-repo non-fatal, and
// the prune covers the .json files (the .html-only prune would have
// leaked stale SBOMs forever).
func TestShowcaseGeneratesStaticSBOMs(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/generate_showcase.go")
	for _, needle := range []string{
		"collector.GenerateSBOM(ctx, store, t.repoID, f.format)",
		`".cyclonedx.json"`,
		`".spdx.json"`,
		"showcase SBOM generation failed — download button omitted",
		`strings.HasSuffix(e.Name(), ".cyclonedx.json")`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate_showcase.go missing %q", needle)
		}
	}
	// SBOM JSON must NOT be sitemapped — only page slugs reach
	// BuildSitemap.
	if strings.Contains(src, "sbom") && strings.Contains(src, "BuildSitemap") {
		i := strings.Index(src, "BuildSitemap(")
		line := src[i : strings.Index(src[i:], "\n")+i]
		if strings.Contains(line, "json") {
			t.Error("SBOM files must not be sitemapped")
		}
	}
}

// Item 1e: the redaction boundary lives in the BUILDER — the real
// login must never flow into the page data (the template can't leak
// what the builder never emits).
func TestShowcaseContributorBuilderDropsIdentity(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/generate_showcase.go")
	i := strings.Index(src, "func buildShowcaseContributors(")
	if i < 0 {
		t.Fatal("buildShowcaseContributors missing")
	}
	body := src[i:]
	if end := strings.Index(body, "\n}"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, `fmt.Sprintf("Contributor #%d"`) {
		t.Error("the placeholder must be a deterministic fake name")
	}
	if strings.Contains(body, "c.Login") || strings.Contains(body, ".FullName") {
		t.Error("the builder must NEVER read the contributor's login/full name into page data — server-side redaction is the contract")
	}
	if !strings.Contains(body, "excludeBots") && !strings.Contains(body, ", true)") {
		t.Error("the section must exclude bots (excludeBots=true)")
	}
}

// Items 1c+4: the six-tile top line with metadata sub-lines.
func TestShowcaseRepoPageReadsMetadataCounts(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/generate_showcase.go")
	if !strings.Contains(src, "store.GetRepoStats(ctx, t.repoID)") {
		t.Error("buildRepoPage must read the metadata counts via GetRepoStats")
	}
	tpl := srctest.Read(t, "internal/showcase/templates.go")
	for _, needle := range []string{
		"metadata {{commaInt .MetaCommits}}",
		"metadata {{commaInt .MetaIssues}}",
		"metadata {{commaInt .MetaPRs}}",
		`<div class="k">Vulnerabilities</div>`,
		"analysis pending",
	} {
		if !strings.Contains(tpl, needle) {
			t.Errorf("repo template missing %q", needle)
		}
	}
}
