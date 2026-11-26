// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package pkgid

import "testing"

// Bioconductor (0.29.206). The purl specification has no type for it and
// OSV maps no purl to it: a Bioconductor package is asked of OSV by name and
// ecosystem. A finding still needs an identity, and takes one of this
// program's own, as a GitHub Action's does: pkg:bioconductor/<name>@<version>.
// It is built where a scan target is, and is NOT a purl of the package: the
// functions that build a package's purl for an SBOM or for OSV give none.
func TestBioconductorHasNoPackagePurl(t *testing.T) {
	if got := PurlForPackage("bioconductor", "", "limma", "3.56.2"); got != "" {
		t.Errorf("PurlForPackage(bioconductor) = %q; the specification has no such type, and an SBOM must not carry one", got)
	}
	if typ, ok := PurlTypeOfManager("bioconductor"); ok {
		t.Errorf("PurlTypeOfManager(bioconductor) = %q", typ)
	}
	if got := SelfPurl("bioconductor", "limma"); got != "" {
		t.Errorf("SelfPurl(bioconductor) = %q", got)
	}
}

// The identity is read back to the package's key, so that the closing rule
// can ask "is this the same package at another version?".
func TestBioconductorFindingIdentity(t *testing.T) {
	if got := FindingPurlByName("bioconductor", "limma", "3.56.2"); got != "pkg:bioconductor/limma@3.56.2" {
		t.Errorf("FindingPurlByName = %q", got)
	}
	if got := FindingPurlByName("bioconductor", "limma", ""); got != "pkg:bioconductor/limma" {
		t.Errorf("FindingPurlByName with no version = %q", got)
	}
	// Only for an ecosystem that is asked by name.
	if got := FindingPurlByName("npm", "x", "1"); got != "" {
		t.Errorf("FindingPurlByName(npm) = %q; npm packages have a real purl", got)
	}
	key, version, ok := KeyOfPurl("pkg:bioconductor/limma@3.56.2")
	if !ok || key != Key("bioconductor", "limma") || version != "3.56.2" {
		t.Errorf("KeyOfPurl = (%q, %q, %v)", key, version, ok)
	}
	if eco, ok := OSVEcosystemByName("bioconductor"); !ok || eco != "Bioconductor" {
		t.Errorf("OSVEcosystemByName(bioconductor) = (%q, %v)", eco, ok)
	}
	if _, ok := OSVEcosystemByName("cran"); ok {
		t.Error("cran is asked by purl")
	}
}
