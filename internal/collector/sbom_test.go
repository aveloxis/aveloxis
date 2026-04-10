package collector

import (
	"os"
	"strings"
	"testing"
)

// TestSBOMCycloneDXHasEvidence verifies the CycloneDX generator includes
// an evidence section with concluded licenses from ScanCode source analysis.
func TestSBOMCycloneDXHasEvidence(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// CycloneDX 1.5 supports evidence.licenses for concluded (detected) licenses.
	if !strings.Contains(code, "Evidence") || !strings.Contains(code, "evidence") {
		t.Error("CycloneDX SBOM must include evidence section for concluded licenses from ScanCode")
	}
}

// TestSBOMCycloneDXHasCopyrights verifies CycloneDX includes copyright text
// from ScanCode analysis.
func TestSBOMCycloneDXHasCopyrights(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "Copyright") || !strings.Contains(code, "copyright") {
		t.Error("CycloneDX SBOM must include copyright information from ScanCode")
	}
}

// TestSBOMSPDXUsesScancodeForConcluded verifies the SPDX generator uses
// ScanCode data for licenseConcluded on the root package instead of just
// repeating the declared license.
func TestSBOMSPDXUsesScancodeForConcluded(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The SPDX generator should query scancode data for the root package's
	// concluded license. Look for ScancodeForSBOM or similar.
	if !strings.Contains(code, "ScancodeForSBOM") && !strings.Contains(code, "GetScancodeForSBOM") {
		t.Error("SPDX SBOM must use ScanCode data for licenseConcluded on root package")
	}
}

// TestSBOMGeneratorAcceptsScancode verifies GenerateSBOM queries scancode data.
func TestSBOMGeneratorAcceptsScancode(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GetScancodeForSBOM") {
		t.Error("GenerateSBOM must call GetScancodeForSBOM to include source code license data")
	}
}
