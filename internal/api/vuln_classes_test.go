// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.29.70: an unpinned dependency's findings are every advisory OSV has for
// the package (it was asked without a version): the version is not determined. The
// payload's counts leave them out of current/critical/direct/transitive/
// dev/runtime and count them apart; the SBOMs keep them marked in triage.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/model"
)

func classRows() []*model.VulnerabilityRow {
	resolved := time.Now()
	r := func(id, res, kind, scope, sev string) *model.VulnerabilityRow {
		return &model.VulnerabilityRow{VulnID: id, PackageName: "p" + id, PackagePurl: "pkg:pypi/p" + id,
			Severity: sev, CVSSScore: map[string]float64{"CRITICAL": 9.8}[sev], Source: "osv.dev",
			VersionResolution: res, DependencyKind: kind, DependencyScope: scope}
	}
	gone := r("gone", "locked", "direct", "runtime", "LOW")
	gone.ResolvedAt = &resolved
	return []*model.VulnerabilityRow{
		r("lock", "locked", "direct", "runtime", "CRITICAL"),
		r("floor", "range-floor", "transitive", "dev", "HIGH"),
		r("unp1", "unpinned", "direct", "runtime", "CRITICAL"),
		r("unp2", "unpinned", "transitive", "build", "HIGH"),
		r("self", "", "self", "", "CRITICAL"),
		// Review round 6 F1: a floating Actions ref's advisory was matched
		// against the ref — exposure, not unknown version.
		func() *model.VulnerabilityRow {
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

// "Resolved" means resolved (summary/70, 4.8): a closure with a reason is
// not a fix, so the resolved count is split, and the parts sum to it.
func TestVulnCountsSplitResolvedByReason(t *testing.T) {
	at := time.Now()
	closed := func(reason string) *model.VulnerabilityRow {
		return &model.VulnerabilityRow{VulnID: "c-" + reason, PackagePurl: "pkg:npm/x@1", VersionResolution: "locked", DependencyKind: "direct", ResolvedAt: &at, ClosedReason: reason}
	}
	rows := append(classRows(), closed(""), closed(model.ClosedSuperseded), closed(model.ClosedSuperseded), closed(model.ClosedVersionLost),
		closed(model.ClosedNeverApplied), closed("a-reason-from-a-later-release"))
	got := vulnCounts(rows)
	want := map[string]int{
		"resolved": 7, "resolved_unlabelled": 2, // classRows' one, and the one above
		"resolved_superseded": 2, "resolved_version_lost": 1, "resolved_never_applied": 1,
		"resolved_version_determined": 0, "resolved_version_changed": 0, "resolved_unsendable": 0,
		"resolved_other_reason": 1,
	}
	for k, w := range want {
		if v, ok := got[k]; !ok || v != w {
			t.Errorf("counts[%q] = %d (present %v), want %d", k, v, ok, w)
		}
	}
	sum := 0
	for k, v := range got {
		if strings.HasPrefix(k, "resolved_") {
			sum += v
		}
	}
	if sum != got["resolved"] {
		t.Errorf("the resolved parts sum to %d, and resolved is %d", sum, got["resolved"])
	}
}

// Each finding is sent with its class, an exposure finding with its
// evidence grade, and a closed one with its reason.
func TestVulnJSONCarriesClassEvidenceAndReason(t *testing.T) {
	at := time.Now()
	rows := classRows()
	rows = append(rows, &model.VulnerabilityRow{VulnID: "lost", PackagePurl: "pkg:npm/x@1", VersionResolution: "locked", DependencyKind: "direct", ResolvedAt: &at, ClosedReason: model.ClosedVersionLost})
	want := map[string][3]string{ // class, evidence, closed_reason
		"lock":  {"exposure", "in_use", ""},
		"floor": {"exposure", "lowest_allowed", ""},
		"unp1":  {"version_not_determined", "", ""},
		"self":  {"self", "", ""},
		"actn":  {"exposure", "floating_ref", ""},
		"gone":  {"closed", "", ""},
		"lost":  {"closed", "", "version_lost"},
	}
	for _, v := range rows {
		w, ok := want[v.VulnID]
		if !ok {
			continue
		}
		raw, err := json.Marshal(vulnJSONOf(v))
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		_ = json.Unmarshal(raw, &got)
		str := func(k string) string { s, _ := got[k].(string); return s }
		if str("class") != w[0] || str("evidence") != w[1] || str("closed_reason") != w[2] {
			t.Errorf("%s: class %q evidence %q closed_reason %q; want %v", v.VulnID, str("class"), str("evidence"), str("closed_reason"), w)
		}
		if _, has := got["evidence"]; has != (w[1] != "") {
			t.Errorf("%s: evidence present = %v", v.VulnID, has)
		}
	}
}

func TestCycloneDXMarksUnknownVersionInTriage(t *testing.T) {
	sbom := []byte(`{"bomFormat":"CycloneDX","specVersion":"1.7","components":[]}`)
	out, err := collector.AnnotateCycloneDXWithVulns(sbom, classRows())
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
	out, err := collector.AnnotateSPDXWithVulns(sbom, classRows())
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
		t.Error("the advisory ref of a finding whose version is not determined must carry a comment saying so")
	}
	if strings.Contains(refAround("osv.dev/vulnerability/lock"), "comment") {
		t.Error("a known-version advisory ref must carry no comment")
	}
}
