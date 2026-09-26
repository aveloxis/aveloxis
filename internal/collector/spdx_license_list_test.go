// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — the SPDX identifier allowlist is now the official list,
// embedded via go:embed, replacing a hand-maintained ~70-entry map
// that silently drifted. These tests pin the floor, the promotion of
// previously-demoted ids, and the continued demotion of non-SPDX
// strings.

import (
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestSPDXListPromotesPreviouslyDemotedIDs — a sample of valid SPDX
// identifiers the old hand map did NOT contain. Before v0.27.23 these
// emitted as license.name (unmatchable by policy engines); they must
// now be recognized.
func TestSPDXListPromotesPreviouslyDemotedIDs(t *testing.T) {
	for _, id := range []string{
		"EPL-2.0", // Eclipse — common in Java
		"BSD-2-Clause-Patent",
		"CC-BY-3.0",
		"Python-2.0", // ubiquitous on PyPI
		"Ruby",       // ubiquitous on RubyGems
		"WTFPL",
		"Beerware",
		"Zend-2.0",
	} {
		if !isSPDXLicense(id) {
			t.Errorf("%s is a valid SPDX identifier and must be recognized (was demoted to license.name pre-v0.27.23)", id)
		}
	}
	// Deprecated ids stay recognized — registries still emit them.
	for _, id := range []string{"GPL-2.0", "LGPL-3.0", "AGPL-3.0"} {
		if !isSPDXLicense(id) {
			t.Errorf("deprecated id %s must remain recognized", id)
		}
	}
}

// TestSPDXListStillDemotesNonSPDXStrings — genuinely non-SPDX strings
// keep falling through to license.name.
func TestSPDXListStillDemotesNonSPDXStrings(t *testing.T) {
	for _, s := range []string{"", "Proprietary", "SEE LICENSE IN LICENSE.txt", "Apache 2.0", "mit"} {
		if isSPDXLicense(s) {
			t.Errorf("%q is not an SPDX identifier and must not be treated as one (matching is exact and case-sensitive)", s)
		}
	}
}

// TestSPDXListSourceContract — the collector keeps NO SPDX list of its
// own: internal/spdx is the one list (worklist 53, SR-17), generated and
// pinned there (internal/spdx/data_test.go). The v0.27.23 embedded copy
// (spdx_license_ids.txt) was retired in v0.29.67. The CycloneDX enum file
// is a different list (the schema's frozen enum) and stays here.
func TestSPDXListSourceContract(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if strings.Contains(code, `"MIT": true`) {
		t.Error("sbom.go declares a literal license map again; hand-maintained lists drift")
	}
	if strings.Contains(code, "spdx_license_ids.txt") {
		t.Error("sbom.go embeds its own SPDX list again; internal/spdx is the one list")
	}
	if !strings.Contains(code, "spdx.IsLicenseID(license)") {
		t.Error("isSPDXLicense must delegate to internal/spdx")
	}
	if _, err := os.Stat("spdx_license_ids.txt"); err == nil {
		t.Error("spdx_license_ids.txt is back; internal/spdx/spdx_data.tsv replaced it")
	}
	data, err := os.ReadFile("cdx_license_ids_1_7.txt")
	if err != nil {
		t.Fatalf("cdx_license_ids_1_7.txt missing: %v", err)
	}
	if !strings.Contains(string(data), "# Refresh") {
		t.Error("cdx_license_ids_1_7.txt must carry its refresh procedure in the header")
	}
	if len(cdxLicenseIDs) < 800 {
		t.Errorf("CycloneDX 1.7 enum has %d IDs; the file looks truncated", len(cdxLicenseIDs))
	}
}

// TestSPDXListFileIsNotGitignored — the v0.27.11 lesson: the repo-wide
// *.json ignore rule silently excluded committed fixtures and CI failed on
// missing testdata. The embedded CycloneDX enum must be trackable.
func TestSPDXListFileIsNotGitignored(t *testing.T) {
	cmd := exec.Command("git", "check-ignore", "-q", "cdx_license_ids_1_7.txt")
	cmd.Dir = "."
	if err := cmd.Run(); err == nil {
		t.Error("cdx_license_ids_1_7.txt is gitignored; the binary would embed a file CI checkouts don't have")
	}
}

// TestCDXLicenseEnumMatchesItsSchema — v0.29.67 review round 1: the
// embedded enum says it is generated from the committed CycloneDX 1.7
// fixture, but nothing compared them. An ID added by hand outside the
// schema's enum would be emitted as license.id and fail the schema, caught
// only if a seed used that ID. The two must be the same set.
func TestCDXLicenseEnumMatchesItsSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/sbom_schemas/cyclonedx-1.7-spdx.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Enum []string `json:"enum"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, id := range schema.Enum {
		want[id] = true
	}
	if len(want) == 0 {
		t.Fatal("the fixture's enum is empty; the extraction path is wrong")
	}
	for id := range cdxLicenseIDs {
		if !want[id] {
			t.Errorf("cdx_license_ids_1_7.txt has %q, which the CycloneDX 1.7 schema's enum lacks", id)
		}
	}
	for id := range want {
		if !cdxLicenseIDs[id] {
			t.Errorf("cdx_license_ids_1_7.txt lacks %q from the schema's enum", id)
		}
	}
}
