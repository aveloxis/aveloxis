// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — the SPDX identifier allowlist is now the official list,
// embedded via go:embed, replacing a hand-maintained ~70-entry map
// that silently drifted. These tests pin the floor, the promotion of
// previously-demoted ids, and the continued demotion of non-SPDX
// strings.

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestSPDXLicenseListFloor guards against a truncated or corrupted
// refresh: the official list has 733 identifiers as of 2026-07-20 and
// only ever grows. 700 is the tripwire floor.
func TestSPDXLicenseListFloor(t *testing.T) {
	if got := len(spdxLicenses); got < 700 {
		t.Errorf("embedded SPDX license set has %d entries, want >= 700 — spdx_license_ids.txt looks truncated (refresh procedure is in its header)", got)
	}
}

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

// TestSPDXListSourceContract — the hand-maintained literal map must
// not return, and the generated file must exist with its refresh
// header.
func TestSPDXListSourceContract(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), `"MIT": true`) {
		t.Error("sbom.go declares a literal license map again — v0.27.23 embeds the official list (spdx_license_ids.txt); hand-maintained lists drift")
	}
	if !strings.Contains(string(src), "go:embed spdx_license_ids.txt") {
		t.Error("sbom.go must embed spdx_license_ids.txt")
	}
	data, err := os.ReadFile("spdx_license_ids.txt")
	if err != nil {
		t.Fatalf("spdx_license_ids.txt missing: %v", err)
	}
	if !strings.Contains(string(data), "# Refresh:") {
		t.Error("spdx_license_ids.txt must carry its refresh procedure in the header")
	}
}

// TestSPDXListFileIsNotGitignored — the v0.27.11 lesson: the repo-wide
// *.json ignore rule silently excluded committed lockfile fixtures and
// CI failed on missing testdata. Verify the embedded list is actually
// trackable by git.
func TestSPDXListFileIsNotGitignored(t *testing.T) {
	cmd := exec.Command("git", "check-ignore", "-q", "spdx_license_ids.txt")
	cmd.Dir = "."
	if err := cmd.Run(); err == nil {
		t.Error("spdx_license_ids.txt is gitignored — the binary would embed a file CI checkouts don't have; add a negation rule like the lockfile fixtures'")
	}
}
