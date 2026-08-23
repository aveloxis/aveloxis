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
// These pins hold the stamp to its contract: exactly the three
// completed-scan exits, never the error paths.

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
	if !strings.Contains(src, "if blankPurlSkipped == 0 {") {
		t.Error("the dep-less exit must stamp ONLY when zero deps were skipped for blank purls (genuinely empty universe)")
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
