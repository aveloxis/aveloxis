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

// The rules these pins held (mark only where OSV was consulted or there was
// genuinely nothing to scan; close a finding only on evidence; preserve the
// findings of a dependency that could not be sent; a failed mark never fails a
// scan) are internal/linking's since 0.29.155, and are tested there by
// behaviour against a store that records every call: one test per exit, the
// error exit, and each tolerated failure.
//
// What is left to pin here is that the scan cannot go around them.

// TestScanLinksThroughLinkingAlone: the scan writes findings, closes them and
// marks the repository scanned ONLY through internal/linking. A direct call to
// one of the three store writes from the collector would be a fifth exit with
// rules of its own (SR-18: the owning layer enforces).
func TestScanLinksThroughLinkingAlone(t *testing.T) {
	code := srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 50))
	for _, write := range []string{".InsertVulnerabilityBatch(", ".MarkStaleVulnerabilitiesResolved(", ".SetVulnScanLastRun("} {
		if n := strings.Count(code, write); n != 0 {
			t.Errorf("internal/collector calls %s %d time(s); findings are written, closed and marked through internal/linking only", write, n)
		}
	}
	scan := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ScanVulnerabilities("))
	for _, needle := range []string{
		"linking.WithoutAnswers(ctx, store, repoID, universe)",
		"linking.WithAnswers(ctx, store, repoID, universe, vulnRows)",
	} {
		if strings.Count(scan, needle) != 1 {
			t.Errorf("ScanVulnerabilities must call %s exactly once", needle)
		}
	}
}

// TestScanHandsLinkingTheUnsendableNames: the names of the dependencies that
// have no purl reach linking, which is what keeps their findings open.
func TestScanHandsLinkingTheUnsendableNames(t *testing.T) {
	scan := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ScanVulnerabilities("))
	if !strings.Contains(scan, "blankPurlSkipped, blankPurlNames := set.BlankPurlSkipped, set.BlankPurlNames") ||
		!strings.Contains(scan, "linking.Universe{Candidates: set.Candidates, Sendable: len(set.Purls), UnsendableNames: blankPurlNames,") {
		t.Error("the scan must build linking's Universe from scantarget's set, unsendable names included")
	}
}

// TestScanReturnsAWriteFailureWithItsCounts: the error exit returns the result
// so far beside the error, as it always has.
func TestScanReturnsAWriteFailureWithItsCounts(t *testing.T) {
	scan := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func ScanVulnerabilities("))
	at := strings.Index(scan, "linking.WithAnswers(")
	if at < 0 || !strings.Contains(scan[at:min(at+200, len(scan))], "return result, err") {
		t.Error("a failed write must return (result, err) straight after linking.WithAnswers")
	}
}
