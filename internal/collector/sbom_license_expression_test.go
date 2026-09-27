// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.29 — multi-license emission semantics (the audit's " AND "
// finding). Ground truth: SPDX license-expression grammar (Annex D —
// licenseDeclared must be a valid expression, NOASSERTION, or NONE)
// and CycloneDX's licenseChoice oneOf (license object XOR expression).
// v0.29.67 (worklist 53): the registry-list reading moved to the WRITER
// (joinRegistryLicenseList stores RubyGems/Packagist/Hex lists as OR), so the
// exporters now carry a stored expression through as it is.

import (
	"encoding/json"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestStoredConjunctionIsNeverInverted replaces v0.27.29's
// TestRegistryLicenseListEmitsAsChoicesNotConjunction, which pinned the
// inversion itself: it fed an npm dependency (npm stores the registry's own
// SPDX expression, where AND means "comply with both") and demanded OR. A
// registry LIST is now stored as OR at write time
// (TestRegistryLicenseListsAreStoredAsOR); a stored AND is a real
// conjunction and both exporters keep it.
func TestStoredConjunctionIsNeverInverted(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	deps := []db.SBOMDep{{Name: "dualpkg", CurrentVersion: "1.0", PackageManager: "npm",
		Purl: "pkg:npm/dualpkg@1.0", License: "MIT AND Apache-2.0"}}
	data, err := generateCycloneDX(repo, deps, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatal(err)
	}
	if lic := bom.Components[0].Licenses[0]; lic.Expression != "MIT AND Apache-2.0" || lic.License != nil {
		t.Errorf("CycloneDX: %+v, want the expression MIT AND Apache-2.0", lic)
	}
	sdata, err := generateSPDX(repo, deps, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(sdata, &doc); err != nil {
		t.Fatal(err)
	}
	for _, p := range doc.Packages {
		if p.Name == "dualpkg" && p.LicenseDeclared != "MIT AND Apache-2.0" {
			t.Errorf("SPDX licenseDeclared = %q, want MIT AND Apache-2.0 (never inverted to OR)", p.LicenseDeclared)
		}
	}
}

func TestUnmappableLicenseGoesNoAssertionNeverFreeText(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "app", Owner: "org", GitURL: "https://github.com/org/app"}
	deps := []db.SBOMDep{{Name: "oddpkg", CurrentVersion: "1.0",
		Purl: "pkg:npm/oddpkg@1.0", License: "MIT AND Custom Corporate License"}}
	sdata, err := generateSPDX(repo, deps, nil, nil)
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
	l := cdxLicenseFor("Apache 2.0", "declared", inCDXLicenseEnum)
	if l.License == nil || l.License.ID != "Apache-2.0" {
		t.Errorf("cdxLicenseFor(\"Apache 2.0\") = %+v, want id Apache-2.0 (synonym promotion)", l)
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
	data, err := generateCycloneDX(repo, deps, nil, nil)
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
