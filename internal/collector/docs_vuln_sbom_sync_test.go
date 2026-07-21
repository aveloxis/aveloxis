// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — sync tripwires between the subsystem reference doc
// (docs/architecture/vulnerability-and-sbom.md) and the code it
// describes, following the metrics-catalog precedent: a spec bump or
// roster change that doesn't update the doc fails the build.

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
)

const vulnSBOMDocPath = "../../docs/architecture/vulnerability-and-sbom.md"

func TestVulnSBOMDocExists(t *testing.T) {
	if _, err := os.Stat(vulnSBOMDocPath); err != nil {
		t.Fatalf("%s missing — it is the operator-facing reference for this subsystem (created v0.27.22-era, wired into the RTD toctree); restore it before shipping doc-affecting changes", vulnSBOMDocPath)
	}
}

// TestVulnSBOMDocSpecVersionsMatchCode pins the doc's stated CycloneDX
// and SPDX versions to the literals sbom.go actually emits.
func TestVulnSBOMDocSpecVersionsMatchCode(t *testing.T) {
	src, err := os.ReadFile("sbom.go")
	if err != nil {
		t.Fatal(err)
	}
	cdx := regexp.MustCompile(`SpecVersion:\s*"([\d.]+)"`).FindSubmatch(src)
	spdx := regexp.MustCompile(`SPDXVersion:\s*"SPDX-([\d.]+)"`).FindSubmatch(src)
	if cdx == nil || spdx == nil {
		t.Fatal("cannot extract spec-version literals from sbom.go")
	}
	doc, err := os.ReadFile(vulnSBOMDocPath)
	if err != nil {
		t.Fatal(err)
	}
	wantCDX := fmt.Sprintf("| CycloneDX | **%s** |", cdx[1])
	wantSPDX := fmt.Sprintf("| SPDX | **%s** |", spdx[1])
	for _, want := range []string{wantCDX, wantSPDX} {
		if !strings.Contains(string(doc), want) {
			t.Errorf("doc's format table missing %q — sbom.go's spec version changed without a doc update", want)
		}
	}
}

// TestVulnSBOMDocLockfileCountMatchesRoster pins the doc's
// "Lockfile parsing — N formats" heading to len(lockfileKinds).
func TestVulnSBOMDocLockfileCountMatchesRoster(t *testing.T) {
	doc, err := os.ReadFile(vulnSBOMDocPath)
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`Lockfile parsing — (\d+) formats`).FindSubmatch(doc)
	if m == nil {
		t.Fatal("doc missing the 'Lockfile parsing — N formats' heading")
	}
	want := fmt.Sprintf("Lockfile parsing — %d formats", len(lockfileKinds))
	if string(m[0]) != want {
		t.Errorf("doc says %q but lockfileKinds has %d entries — update the doc heading and its table", m[0], len(lockfileKinds))
	}
}

// TestVulnSBOMDocInToctree — the doc must stay reachable from the
// Sphinx toctree; an orphaned page is not "easily findable".
func TestVulnSBOMDocInToctree(t *testing.T) {
	idx, err := os.ReadFile("../../docs/index.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(idx), "architecture/vulnerability-and-sbom") {
		t.Error("docs/index.md toctree missing architecture/vulnerability-and-sbom — the page would build orphaned")
	}
}
