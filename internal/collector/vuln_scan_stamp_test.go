// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.1 (A4) — the completed-vuln-scan stamp. Unlike scancode and
// distribution, the vuln scanner recorded NO completion time, so the
// GUI could not distinguish "scanned, clean" from "never scanned"
// (per-finding last_seen_at cannot: a clean scan touches zero rows).
// These pins hold the stamp to its contract (revised v0.28.5): only
// where OSV was actually consulted or the universe was genuinely
// empty — exactly two stamp sites — never the error paths, never the
// no-query degenerate exits.

func vulnScanSrc(t *testing.T) string {
	t.Helper()
	return srctest.Read(t, "internal/collector/vulnerability.go")
}

// The stamp semantics, revised in v0.28.5 (Copilot round): it fires
// only where OSV was actually consulted OR the scan universe was
// GENUINELY empty — exactly TWO call sites:
//  1. the dep-less early return, GATED on blankPurlSkipped == 0
//     (deps that exist but carry no purl were never queried; stamping
//     would make the API read "scanned, clean" for a repo nothing was
//     checked on);
//  2. the final success return after the real OSV batch.
//
// The all-malformed early return deliberately does NOT stamp: zero
// queries ran, and the repo heals on its next analysis → real scan.
func TestVulnScanStampSites(t *testing.T) {
	src := vulnScanSrc(t)
	// Count CALL sites only (exclude the definition + comments by
	// anchoring on the exact call shape).
	calls := strings.Count(src, "stampVulnScanComplete(ctx, store, repoID, logger)")
	if calls != 2 {
		t.Errorf("stampVulnScanComplete must be called at exactly the 2 honest completion exits, found %d call sites", calls)
	}
	// The dep-less exit's stamp is gated on the blank-purl counter.
	i := strings.Index(src, "blank_purl_skipped")
	if i < 0 {
		t.Fatal("the zero-purl exit must log blank_purl_skipped")
	}
	gate := strings.Index(src, "if blankPurlSkipped == 0 {")
	if gate < 0 {
		t.Error("the dep-less exit must stamp ONLY when zero deps were skipped for blank purls (genuinely empty universe)")
	}
	// v0.28.6 (Copilot round 2): the STALE-RESOLUTION shares the gate
	// — with unscannable deps skipped, resolving every current finding
	// claims "fixed" with no evidence, and blank-purl deps never
	// rescan, so the false-clean would be permanent. Pin the dep-less
	// exit's resolve call INSIDE the gated arm (after the gate, before
	// the stamp call that closes the arm).
	if gate >= 0 {
		depless := strings.Index(src, "if len(purls) == 0 {")
		region := src[depless:]
		if end := strings.Index(region, "// v0.27.73: the wire gate"); end > 0 {
			region = region[:end]
		}
		resolvePos := strings.Index(region, "MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, nil)")
		gatePos := strings.Index(region, "if blankPurlSkipped == 0 {")
		if resolvePos < 0 || gatePos < 0 || resolvePos < gatePos {
			t.Errorf("the dep-less exit's MarkStaleVulnerabilitiesResolved must sit INSIDE the blankPurlSkipped==0 arm (resolve=%d gate=%d) — resolving findings nothing scanned is the false-clean class", resolvePos, gatePos)
		}
	}
	// Contrast pin: the all-malformed exit's resolution is DELIBERATE
	// (the zephyr garbage-born-findings heal, v0.27.71-73) and must
	// stay — malformed purls rewrite on the next analysis, so a real
	// finding re-detects within one cycle.
	j2 := strings.Index(src, "all-malformed repo")
	if j2 < 0 {
		t.Error("the all-malformed exit must keep its deliberate stale-resolution (the v0.27.73 heal) — see the DELIBERATE comment at the site")
	}
	// The all-malformed exit must NOT stamp — pin the region between
	// its log line and its return.
	j := strings.Index(src, "all scan targets malformed")
	if j < 0 {
		t.Fatal("all-malformed exit missing")
	}
	region := src[j:]
	if end := strings.Index(region, "return &VulnerabilityResult"); end > 0 {
		region = region[:end]
	}
	if strings.Contains(region, "stampVulnScanComplete(ctx,") {
		t.Error("the all-malformed exit must NOT stamp — no OSV query ran on that path")
	}
}

// The stamp is non-fatal (a failed stamp must never fail a
// successful scan) and routes through the store's single writer.
func TestVulnScanStampIsNonFatal(t *testing.T) {
	src := vulnScanSrc(t)
	i := strings.Index(src, "func stampVulnScanComplete(")
	if i < 0 {
		t.Fatal("stampVulnScanComplete helper missing from vulnerability.go")
	}
	body := src[i:]
	if end := strings.Index(body, "\n}"); end > 0 {
		body = body[:end]
	}
	if !strings.Contains(body, "SetVulnScanLastRun(") {
		t.Error("the stamp must route through store.SetVulnScanLastRun (the single writer)")
	}
	if !strings.Contains(body, "logger.Warn") || strings.Contains(body, "return err") {
		t.Error("the stamp must WARN on failure and never propagate — a failed stamp must not fail a successful scan")
	}
}

// v0.28.8 (Copilot round 4) — MIXED scans must not adjudicate what
// they never sent: blank-purl deps' names ride every stale-resolution
// call that can touch their findings (the final success exit AND the
// all-malformed exit — both are reachable with purl-less deps
// present). "Absent from the seen-set" is not evidence for a dep that
// was never queried, and purl-less deps never rescan, so a wrong
// resolution is PERMANENT false-clean.
func TestStaleResolutionPreservesBlankPurlDeps(t *testing.T) {
	src := vulnScanSrc(t)
	if !strings.Contains(src, "blankPurlNames = append(blankPurlNames, dep.Name)") {
		t.Fatal("the construction loop must collect blank-purl dep NAMES for the preserve list")
	}
	// v0.28.11 (Copilot round 10): SELF-EXCLUSION FIRST — a purl-less
	// SELF dep is a deliberate exclusion, not an unscannable
	// dependency; blank-first misclassified it and blocked the stamp
	// on a genuinely self-excluded universe. Pin the loop order.
	selfPos := strings.Index(src, "if isSelfDependency(dep.Name, selfSet) {")
	blankPos := strings.Index(src, `if dep.Purl == "" {`)
	if selfPos < 0 || blankPos < 0 || selfPos > blankPos {
		t.Errorf("the self-dependency check must precede the blank-purl check (self=%d blank=%d)", selfPos, blankPos)
	}
	if !strings.Contains(src, "MarkStaleVulnerabilitiesResolved(ctx, repoID, seen, blankPurlNames)") {
		t.Error("the final-exit resolution must preserve blank-purl deps' findings")
	}
	if !strings.Contains(src, "MarkStaleVulnerabilitiesResolved(ctx, repoID, nil, blankPurlNames)") {
		t.Error("the all-malformed exit's (deliberate) resolution must still preserve blank-purl deps' findings")
	}
}
