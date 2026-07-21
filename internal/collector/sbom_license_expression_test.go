// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.29 — multi-license emission semantics (the audit's " AND "
// finding). Ground truth: SPDX license-expression grammar (Annex D —
// licenseDeclared must be a valid expression, NOASSERTION, or NONE)
// and CycloneDX 1.5's licenseChoice oneOf (license object XOR
// expression).

import (
	"encoding/json"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestRegistryLicenseListEmitsAsChoicesNotConjunction(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	deps := []db.SBOMDep{{Name: "dualpkg", CurrentVersion: "1.0",
		Purl: "pkg:npm/dualpkg@1.0", License: "MIT AND Apache-2.0"}}

	// CycloneDX: a stored registry list is a genuine SPDX compound
	// here (both ids valid) → the expression field, machine-readable.
	data, err := generateCycloneDX(repo, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatal(err)
	}
	lic := bom.Components[0].Licenses[0]
	if lic.Expression == "" || lic.License != nil {
		t.Errorf("valid compound must use the expression field (got %+v) — free-text name is invisible to policy engines", lic)
	}

	// SPDX: dual-licensing renders as an OR choice, never the
	// AND-conjunction inversion.
	sdata, err := generateSPDX(repo, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(sdata, &doc); err != nil {
		t.Fatal(err)
	}
	var depDeclared string
	for _, p := range doc.Packages {
		if p.Name == "dualpkg" {
			depDeclared = p.LicenseDeclared
		}
	}
	if depDeclared != "(MIT OR Apache-2.0)" {
		t.Errorf("licenseDeclared = %q, want (MIT OR Apache-2.0) — registry lists mean dual-licensing alternatives", depDeclared)
	}
}

func TestUnmappableLicenseGoesNoAssertionNeverFreeText(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	deps := []db.SBOMDep{{Name: "oddpkg", CurrentVersion: "1.0",
		Purl: "pkg:npm/oddpkg@1.0", License: "MIT AND Custom Corporate License"}}
	sdata, err := generateSPDX(repo, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc spdxDoc
	_ = json.Unmarshal(sdata, &doc)
	for _, p := range doc.Packages {
		if p.Name == "oddpkg" && p.LicenseDeclared != "NOASSERTION" {
			t.Errorf("licenseDeclared = %q, want NOASSERTION — SPDX requires a parseable expression; free text is grammar-invalid", p.LicenseDeclared)
		}
	}
}

func TestSynonymNormalizesToSPDXID(t *testing.T) {
	// "Apache 2.0" (registry spelling) must promote to the SPDX id via
	// NormalizeLicenseToSPDX instead of demoting to license.name.
	l := makeCDXLicense("Apache 2.0")
	if l.License == nil || l.License.ID != "Apache-2.0" {
		t.Errorf("makeCDXLicense(\"Apache 2.0\") = %+v, want id Apache-2.0 (synonym promotion)", l)
	}
}

func TestBomRefsAreUnique(t *testing.T) {
	// The audit's 1d finding: CycloneDX requires unique bom-refs; two
	// manifests declaring the same package+version must merge.
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	deps := []db.SBOMDep{
		{Name: "lodash", CurrentVersion: "4.17.21", Purl: "pkg:npm/lodash@4.17.21", License: "MIT"},
		{Name: "lodash", CurrentVersion: "4.17.21", Purl: "pkg:npm/lodash@4.17.21", License: "MIT"},
	}
	data, err := generateCycloneDX(repo, deps, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bom cycloneDX
	_ = json.Unmarshal(data, &bom)
	seen := map[string]int{}
	for _, c := range bom.Components {
		seen[c.BOMRef]++
	}
	for ref, n := range seen {
		if n > 1 {
			t.Errorf("bom-ref %q appears %d times — the spec requires uniqueness", ref, n)
		}
	}
}
