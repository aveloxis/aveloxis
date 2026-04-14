package collector

import (
	"encoding/json"
	"testing"

	"github.com/augurlabs/aveloxis/internal/db"
)

// ============================================================
// CycloneDX generation edge cases
// ============================================================

func TestGenerateCycloneDX_NoDeps(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	data, err := generateCycloneDX(repo, nil, nil)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if bom.BOMFormat != "CycloneDX" {
		t.Errorf("BOMFormat = %q, want CycloneDX", bom.BOMFormat)
	}
	if bom.SpecVersion != "1.5" {
		t.Errorf("SpecVersion = %q, want 1.5", bom.SpecVersion)
	}
	if len(bom.Components) != 0 {
		t.Errorf("expected 0 components, got %d", len(bom.Components))
	}
	if bom.Metadata.Component == nil {
		t.Error("root component should be present")
	}
	if bom.Metadata.Component.Name != "myapp" {
		t.Errorf("root name = %q, want myapp", bom.Metadata.Component.Name)
	}
}

func TestGenerateCycloneDX_WithDeps(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0.0", Purl: "pkg:pypi/flask@2.0.0", Type: "runtime", License: "BSD-3-Clause"},
		{Name: "pytest", CurrentVersion: "7.0", Purl: "pkg:pypi/pytest@7.0", Type: "dev", License: "MIT"},
	}
	data, err := generateCycloneDX(repo, deps, nil)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(bom.Components) != 2 {
		t.Fatalf("expected 2 components, got %d", len(bom.Components))
	}

	// Runtime dep should have scope=required.
	if bom.Components[0].Scope != "required" {
		t.Errorf("runtime dep scope = %q, want required", bom.Components[0].Scope)
	}
	// Dev dep should have scope=excluded (runtime inclusion semantics, not manifest section).
	if bom.Components[1].Scope != "excluded" {
		t.Errorf("dev dep scope = %q, want excluded", bom.Components[1].Scope)
	}
	// License should be set — BSD-3-Clause is a valid SPDX ID, so it goes in the id field.
	if len(bom.Components[0].Licenses) != 1 {
		t.Fatalf("expected 1 license on flask, got %d", len(bom.Components[0].Licenses))
	}
	if bom.Components[0].Licenses[0].License.ID != "BSD-3-Clause" {
		t.Errorf("license ID = %q, want BSD-3-Clause", bom.Components[0].Licenses[0].License.ID)
	}
}

func TestGenerateCycloneDX_WithScancode(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	scanData := &db.ScancodeForSBOM{
		ConcludedLicenseSPDX: "Apache-2.0",
		Copyrights:           []string{"Copyright 2024 ACME Corp", "Copyright 2023 Other Inc"},
	}
	data, err := generateCycloneDX(repo, nil, scanData)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	root := bom.Metadata.Component
	if root == nil {
		t.Fatal("root component missing")
	}
	if root.Evidence == nil {
		t.Fatal("evidence should be present with scancode data")
	}
	if len(root.Evidence.Licenses) != 1 {
		t.Fatalf("expected 1 evidence license, got %d", len(root.Evidence.Licenses))
	}
	// Apache-2.0 is a valid SPDX ID, so it goes in the id field.
	if root.Evidence.Licenses[0].License.ID != "Apache-2.0" {
		t.Errorf("evidence license ID = %q, want Apache-2.0", root.Evidence.Licenses[0].License.ID)
	}
	if len(root.Evidence.Copyright) != 2 {
		t.Fatalf("expected 2 copyright entries, got %d", len(root.Evidence.Copyright))
	}
	// Top-level copyright should include count.
	if root.Copyright == "" {
		t.Error("copyright field should be set")
	}
}

func TestGenerateCycloneDX_NoLicense(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org"}
	deps := []db.SBOMDep{
		{Name: "unknown-lib", CurrentVersion: "1.0", Purl: "pkg:pypi/unknown-lib@1.0", Type: "runtime"},
	}
	data, err := generateCycloneDX(repo, deps, nil)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	// Component without license should have empty licenses array.
	if len(bom.Components[0].Licenses) != 0 {
		t.Errorf("no-license dep should have 0 licenses, got %d", len(bom.Components[0].Licenses))
	}
}

// ============================================================
// SPDX generation edge cases
// ============================================================

func TestGenerateSPDX_NoDeps(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp", License: "MIT"}
	data, err := generateSPDX(repo, nil, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if doc.SPDXVersion != "SPDX-2.3" {
		t.Errorf("version = %q, want SPDX-2.3", doc.SPDXVersion)
	}
	if doc.DataLicense != "CC0-1.0" {
		t.Errorf("data license = %q, want CC0-1.0", doc.DataLicense)
	}
	// Should have root package only.
	if len(doc.Packages) != 1 {
		t.Fatalf("expected 1 package (root only), got %d", len(doc.Packages))
	}
	if doc.Packages[0].LicenseDeclared != "MIT" {
		t.Errorf("declared license = %q, want MIT", doc.Packages[0].LicenseDeclared)
	}
	// Root copyright should be NOASSERTION without scancode.
	if doc.Packages[0].CopyrightText != "NOASSERTION" {
		t.Errorf("copyright = %q, want NOASSERTION", doc.Packages[0].CopyrightText)
	}
	// Should have DESCRIBES relationship.
	found := false
	for _, r := range doc.Relationships {
		if r.RelationshipType == "DESCRIBES" {
			found = true
		}
	}
	if !found {
		t.Error("missing DESCRIBES relationship")
	}
}

func TestGenerateSPDX_WithDeps(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	deps := []db.SBOMDep{
		{Name: "flask", CurrentVersion: "2.0", Purl: "pkg:pypi/flask@2.0", License: "BSD-3-Clause"},
		{Name: "requests", CurrentVersion: "2.28", License: ""}, // no purl, no license
	}
	data, err := generateSPDX(repo, deps, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// 1 root + 2 deps = 3 packages.
	if len(doc.Packages) != 3 {
		t.Fatalf("expected 3 packages, got %d", len(doc.Packages))
	}

	// Flask should have purl external ref.
	flask := doc.Packages[1]
	if len(flask.ExternalRefs) != 1 {
		t.Fatalf("flask external refs = %d, want 1", len(flask.ExternalRefs))
	}
	if flask.ExternalRefs[0].ReferenceLocator != "pkg:pypi/flask@2.0" {
		t.Errorf("purl = %q", flask.ExternalRefs[0].ReferenceLocator)
	}
	// LicenseConcluded for deps should be NOASSERTION (no per-dep source analysis).
	// LicenseDeclared carries the registry-declared license.
	if flask.LicenseConcluded != "NOASSERTION" {
		t.Errorf("dep LicenseConcluded = %q, want NOASSERTION (no source analysis per-dep)", flask.LicenseConcluded)
	}
	if flask.LicenseDeclared != "BSD-3-Clause" {
		t.Errorf("dep LicenseDeclared = %q, want BSD-3-Clause", flask.LicenseDeclared)
	}

	// Requests (no license) should use NOASSERTION.
	requests := doc.Packages[2]
	if requests.LicenseConcluded != "NOASSERTION" {
		t.Errorf("no-license should use NOASSERTION, got %q", requests.LicenseConcluded)
	}
	// No purl = no external refs.
	if len(requests.ExternalRefs) != 0 {
		t.Errorf("no-purl dep should have 0 external refs, got %d", len(requests.ExternalRefs))
	}

	// DEPENDS_ON relationships: one per dep.
	dependsOnCount := 0
	for _, r := range doc.Relationships {
		if r.RelationshipType == "DEPENDS_ON" {
			dependsOnCount++
		}
	}
	if dependsOnCount != 2 {
		t.Errorf("expected 2 DEPENDS_ON, got %d", dependsOnCount)
	}
}

func TestGenerateSPDX_WithScancode(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp", License: "MIT"}
	scanData := &db.ScancodeForSBOM{
		ConcludedLicenseSPDX: "Apache-2.0 AND MIT",
		Copyrights:           []string{"Copyright 2024 ACME"},
	}
	data, err := generateSPDX(repo, nil, scanData)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	root := doc.Packages[0]
	// LicenseConcluded should come from scancode, not the declared license.
	if root.LicenseConcluded != "Apache-2.0 AND MIT" {
		t.Errorf("concluded license = %q, want scancode value", root.LicenseConcluded)
	}
	// LicenseDeclared should still be the registry/API value.
	if root.LicenseDeclared != "MIT" {
		t.Errorf("declared license = %q, want MIT", root.LicenseDeclared)
	}
	if root.CopyrightText != "Copyright 2024 ACME" {
		t.Errorf("copyright = %q", root.CopyrightText)
	}
}

func TestGenerateSPDX_NoLicense(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	data, err := generateSPDX(repo, nil, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if doc.Packages[0].LicenseDeclared != "NOASSERTION" {
		t.Errorf("no license should use NOASSERTION, got %q", doc.Packages[0].LicenseDeclared)
	}
}

// ============================================================
// orNoAssertion edge cases
// ============================================================

func TestOrNoAssertion_Empty(t *testing.T) {
	if v := orNoAssertion(""); v != "NOASSERTION" {
		t.Errorf("empty = %q, want NOASSERTION", v)
	}
}

func TestOrNoAssertion_NonEmpty(t *testing.T) {
	if v := orNoAssertion("MIT"); v != "MIT" {
		t.Errorf("MIT = %q, want MIT", v)
	}
}

// ============================================================
// SBOMFormat edge cases
// ============================================================

func TestSBOMFormat_Constants(t *testing.T) {
	if FormatCycloneDX != "cyclonedx" {
		t.Errorf("FormatCycloneDX = %q", FormatCycloneDX)
	}
	if FormatSPDX != "spdx" {
		t.Errorf("FormatSPDX = %q", FormatSPDX)
	}
}
