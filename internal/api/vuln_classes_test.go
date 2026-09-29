// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.70: an unpinned dependency's findings are every advisory OSV has for
// the package (it was asked without a version), so exposure is unknown. The
// payload's counts leave them out of current/critical/direct/transitive/
// dev/runtime and count them apart; the SBOMs keep them marked in triage.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func classRows() []*db.VulnerabilityRow {
	resolved := time.Now()
	r := func(id, res, kind, scope, sev string) *db.VulnerabilityRow {
		return &db.VulnerabilityRow{VulnID: id, PackageName: "p" + id, PackagePurl: "pkg:pypi/p" + id,
			Severity: sev, CVSSScore: map[string]float64{"CRITICAL": 9.8}[sev], Source: "osv.dev",
			VersionResolution: res, DependencyKind: kind, DependencyScope: scope}
	}
	gone := r("gone", "locked", "direct", "runtime", "LOW")
	gone.ResolvedAt = &resolved
	return []*db.VulnerabilityRow{
		r("lock", "locked", "direct", "runtime", "CRITICAL"),
		r("floor", "range-floor", "transitive", "dev", "HIGH"),
		r("unp1", "unpinned", "direct", "runtime", "CRITICAL"),
		r("unp2", "unpinned", "transitive", "build", "HIGH"),
		r("self", "", "self", "", "CRITICAL"),
		// Review round 6 F1: a floating Actions ref's advisory was matched
		// against the ref — exposure, not unknown version.
		func() *db.VulnerabilityRow {
			a := r("actn", "unpinned", "direct", "runtime", "HIGH")
			a.Ecosystem = "githubactions"
			return a
		}(),
		gone,
	}
}

func TestVulnCountsKeepUnknownVersionApart(t *testing.T) {
	got := vulnCounts(classRows())
	want := map[string]int{
		"current": 3, "critical": 1, "direct": 2, "transitive": 1, "dev": 1, "runtime": 2,
		"unknown_version": 2, "unknown_version_critical": 1, "self": 1, "resolved": 1,
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("counts[%q] = %d, want %d (all: %v)", k, got[k], w, got)
		}
	}
}

func TestCycloneDXMarksUnknownVersionInTriage(t *testing.T) {
	sbom := []byte(`{"bomFormat":"CycloneDX","specVersion":"1.7","components":[]}`)
	out, err := annotateCycloneDXWithVulns(sbom, classRows())
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vulnerabilities []struct {
			ID       string `json:"id"`
			Analysis *struct {
				State  string `json:"state"`
				Detail string `json:"detail"`
			} `json:"analysis"`
		} `json:"vulnerabilities"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, v := range doc.Vulnerabilities {
		seen[v.ID] = true
		unknown := strings.HasPrefix(v.ID, "unp") // not "actn": a floating Actions ref is exposure
		if unknown && (v.Analysis == nil || v.Analysis.State != "in_triage" || !strings.Contains(v.Analysis.Detail, "no version")) {
			t.Errorf("%s: an unknown-version finding must carry analysis.state in_triage with its reason, got %+v", v.ID, v.Analysis)
		}
		if !unknown && v.Analysis != nil {
			t.Errorf("%s: a known-version finding must carry no analysis, got %+v", v.ID, v.Analysis)
		}
	}
	for _, id := range []string{"lock", "floor", "unp1", "unp2", "actn"} {
		if !seen[id] {
			t.Errorf("%s missing from the SBOM annotation", id)
		}
	}
	if seen["self"] || seen["gone"] {
		t.Error("self and resolved findings stay out of the SBOM annotation")
	}
}

func TestSPDXCommentsUnknownVersionAdvisories(t *testing.T) {
	sbom := []byte(`{"spdxVersion":"SPDX-2.3","packages":[
		{"SPDXID":"SPDXRef-a","name":"punp1","externalRefs":[{"referenceCategory":"PACKAGE-MANAGER","referenceType":"purl","referenceLocator":"pkg:pypi/punp1"}]},
		{"SPDXID":"SPDXRef-b","name":"plock","externalRefs":[{"referenceCategory":"PACKAGE-MANAGER","referenceType":"purl","referenceLocator":"pkg:pypi/plock"}]}]}`)
	out, err := annotateSPDXWithVulns(sbom, classRows())
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	// The JSON object holding a locator (keys are sorted: comment comes
	// before referenceLocator).
	refAround := func(locator string) string {
		t.Helper()
		k := strings.Index(s, locator)
		if k < 0 {
			t.Fatalf("advisory %s not attached:\n%s", locator, s)
		}
		start := strings.LastIndex(s[:k], "{")
		end := k + strings.Index(s[k:], "}")
		return s[start:end]
	}
	if !strings.Contains(refAround("osv.dev/vulnerability/unp1"), "no version") {
		t.Error("the unknown-version advisory ref must carry a comment saying exposure is unknown")
	}
	if strings.Contains(refAround("osv.dev/vulnerability/lock"), "comment") {
		t.Error("a known-version advisory ref must carry no comment")
	}
}
