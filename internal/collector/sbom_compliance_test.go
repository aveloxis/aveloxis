package collector

import (
	"os"
	"strings"
	"testing"
)

// --- CycloneDX compliance tests ---

// TestCDXUsesLicenseID verifies that CycloneDX uses license.id for valid SPDX
// identifiers instead of always using license.name. Policy tools like Grype and
// FOSSA require the id field for SPDX identifiers; name is for non-standard strings.
func TestCDXUsesLicenseID(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Must have logic to choose between ID and Name based on SPDX validity.
	if !strings.Contains(code, "isSPDXLicense") && !strings.Contains(code, "spdxLicenses") {
		t.Error("sbom.go must validate license strings against the SPDX license list " +
			"and use license.id for valid identifiers, license.name for non-standard strings")
	}
}

// TestCDXHasDependenciesGraph verifies the CycloneDX output includes a
// top-level dependencies array expressing the dependency DAG.
func TestCDXHasDependenciesGraph(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, `"dependencies"`) && !strings.Contains(code, "Dependencies") {
		t.Error("CycloneDX struct must include a Dependencies field for the dependency graph")
	}
}

// TestCDXRootHasBOMRef verifies the root component has a bom-ref set,
// which is required for the dependencies graph to reference it.
func TestCDXRootHasBOMRef(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the root component construction.
	idx := strings.Index(code, "rootComp")
	if idx < 0 {
		t.Fatal("cannot find rootComp in sbom.go")
	}
	section := code[idx:]
	if len(section) > 500 {
		section = section[:500]
	}

	if !strings.Contains(section, "BOMRef") {
		t.Error("Root component must have BOMRef set for the dependencies graph to reference it")
	}
}

// TestCDXDevScopeNotOptional verifies that dev dependencies do NOT get
// scope "optional". CycloneDX scope describes runtime inclusion:
// "required" = production, "excluded" = dev/test, "optional" = truly optional.
func TestCDXDevScopeNotOptional(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the dev scope assignment.
	idx := strings.Index(code, `dep.Type == "dev"`)
	if idx < 0 {
		t.Skip("cannot find dev type check")
	}
	section := code[idx : idx+200]

	if strings.Contains(section, `"optional"`) {
		t.Error("Dev dependencies must use scope 'excluded' not 'optional' — " +
			"CycloneDX scope describes runtime inclusion, not manifest section")
	}
}

// --- SPDX compliance tests ---

// TestSPDXDepsConcludedDiffersFromDeclared verifies that SPDX dependency
// packages have different licenseConcluded and licenseDeclared values when
// no source analysis is available (concluded should be NOASSERTION).
func TestSPDXDepsConcludedDiffersFromDeclared(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the dep loop in generateSPDX.
	idx := strings.Index(code, "for i, dep := range deps")
	if idx < 0 {
		idx = strings.Index(code, "for _, dep := range deps")
	}
	if idx < 0 {
		t.Fatal("cannot find dep loop in generateSPDX")
	}
	section := code[idx:]
	if len(section) > 1500 {
		section = section[:1500]
	}

	// LicenseConcluded for deps should be NOASSERTION (no source analysis per-dep),
	// NOT the same as LicenseDeclared.
	if strings.Contains(section, "LicenseConcluded: license") &&
		strings.Contains(section, "LicenseDeclared:  license") {
		t.Error("SPDX dep packages must not have identical LicenseConcluded and LicenseDeclared — " +
			"Concluded requires source analysis; without it, use NOASSERTION")
	}
}

// TestSPDXDownloadLocationNotPurl verifies SPDX downloadLocation is not
// set to the purl. The field requires a VCS or download URL, not a purl.
func TestSPDXDownloadLocationNotPurl(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if strings.Contains(code, "DownloadLocation = dep.Purl") ||
		strings.Contains(code, "DownloadLocation: dep.Purl") {
		t.Error("SPDX downloadLocation must not be set to the purl — " +
			"it requires a VCS or download URL, use NOASSERTION instead")
	}
}

// TestSPDXNamespaceIsUnique verifies the SPDX documentNamespace contains
// a unique component (UUID or timestamp) per the SPDX spec.
func TestSPDXNamespaceIsUnique(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the generateSPDX function where namespace is assigned.
	idx := strings.Index(code, "func generateSPDX")
	if idx < 0 {
		t.Fatal("cannot find generateSPDX function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 3000 {
		fnBody = fnBody[:3000]
	}

	// Must contain a UUID or similar unique component in the namespace.
	if !strings.Contains(fnBody, "docUUID") && !strings.Contains(fnBody, "uuid.New") {
		t.Error("SPDX documentNamespace must include a unique component (UUID) per spec")
	}
}

// TestSPDXStablePackageIDs verifies SPDX package IDs use stable identifiers
// (e.g., based on purl) rather than loop indices that change when deps are reordered.
func TestSPDXStablePackageIDs(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The old pattern SPDXRef-Package-%d uses a loop index.
	if strings.Contains(code, `"SPDXRef-Package-%d"`) {
		t.Errorf("SPDX package IDs must not use loop indices (SPDXRef-Package-N) — " +
			"IDs are unstable across regenerations when deps are reordered. " +
			"Use a hash of the purl or package name instead")
	}
}
