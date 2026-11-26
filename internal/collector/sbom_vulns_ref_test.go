// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// A vulnerability in a CycloneDX document points at a component of that
// document (review round 1 of the vulnerability program's first part). A
// package of an ecosystem asked by name (Bioconductor) has no purl, so its
// component carries an aveloxis: reference, while its finding's identity is
// pkg:bioconductor/...: written as the reference, it pointed at nothing.
func TestAVulnerabilityPointsAtItsComponent(t *testing.T) {
	byNameRef := purlLessBOMRef(sbomGraphKey("bioconductor", "limma"), "3.56.2")
	sbom, _ := json.Marshal(map[string]any{
		"bomFormat": "CycloneDX",
		"components": []map[string]any{
			{"bom-ref": "pkg:npm/express@4.18.0", "name": "express"},
			{"bom-ref": byNameRef, "name": "limma"},
		},
	})
	vulns := []*model.VulnerabilityRow{
		{VulnID: "GHSA-1", PackageName: "express", PackagePurl: "pkg:npm/express@4.18.0", Ecosystem: "npm", VersionResolution: "locked"},
		{VulnID: "BIOC-1", PackageName: "limma", PackagePurl: pkgid.FindingPurlByName("bioconductor", "limma", "3.56.2"), Ecosystem: "bioconductor", VersionResolution: "locked"},
		// One whose package is at another version than any component: the
		// identity is kept as written (nothing better is known).
		{VulnID: "BIOC-2", PackageName: "edgeR", PackagePurl: pkgid.FindingPurlByName("bioconductor", "edgeR", "1.0"), Ecosystem: "Bioconductor", VersionResolution: "locked"},
	}
	out, err := AnnotateCycloneDXWithVulns(sbom, vulns)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Vulnerabilities []struct {
			ID      string `json:"id"`
			Affects []struct {
				Ref string `json:"ref"`
			} `json:"affects"`
		} `json:"vulnerabilities"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, v := range doc.Vulnerabilities {
		got[v.ID] = v.Affects[0].Ref
	}
	for id, want := range map[string]string{"GHSA-1": "pkg:npm/express@4.18.0", "BIOC-1": byNameRef, "BIOC-2": "pkg:bioconductor/edgeR@1.0"} {
		if got[id] != want {
			t.Errorf("%s affects %q, want %q", id, got[id], want)
		}
	}
}
