// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.27.23 — SBOM tool provenance. The external analysis tools are
// installed unpinned and auto-updated monthly (tools.go
// ToolUpdateInterval), so two SBOMs of the same commit can carry
// different license evidence. The document must therefore say which
// ScanCode version produced its evidence fields. These tests pin the
// present/absent contract on both formats.

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestCycloneDXRecordsScancodeVersionWhenEvidencePresent(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	scan := &db.ScancodeForSBOM{
		ConcludedLicenseSPDX: "MIT",
		ScancodeVersion:      "32.4.0",
	}
	data, err := generateCycloneDX(repo, nil, scan, nil)
	if err != nil {
		t.Fatalf("generateCycloneDX: %v", err)
	}
	var bom cycloneDX
	if err := json.Unmarshal(data, &bom); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	var found bool
	for _, tc := range bom.Metadata.Tools.Components {
		if tc.Name == "scancode-toolkit-mini" {
			found = true
			if tc.Version != "32.4.0" {
				t.Errorf("scancode tool version = %q, want 32.4.0", tc.Version)
			}
		}
	}
	if !found {
		t.Error("CycloneDX metadata.tools.components missing scancode-toolkit-mini despite scan evidence with a version")
	}
	// aveloxis itself must always remain the first tool.
	if len(bom.Metadata.Tools.Components) == 0 || bom.Metadata.Tools.Components[0].Name != "aveloxis" {
		t.Error("aveloxis must remain the first tool component")
	}
}

func TestCycloneDXOmitsScancodeToolWithoutEvidence(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	for name, scan := range map[string]*db.ScancodeForSBOM{
		"nil scanData":   nil,
		"version absent": {ConcludedLicenseSPDX: "MIT"}, // pre-version-column scan
	} {
		data, err := generateCycloneDX(repo, nil, scan, nil)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var bom cycloneDX
		if err := json.Unmarshal(data, &bom); err != nil {
			t.Fatalf("%s: invalid JSON: %v", name, err)
		}
		for _, tc := range bom.Metadata.Tools.Components {
			if tc.Name == "scancode-toolkit-mini" {
				t.Errorf("%s: scancode tool component present without a recorded version — provenance must never be guessed", name)
			}
		}
	}
}

func TestSPDXRecordsScancodeCreatorWhenEvidencePresent(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	scan := &db.ScancodeForSBOM{ConcludedLicenseSPDX: "MIT", ScancodeVersion: "32.4.0"}
	data, err := generateSPDX(repo, nil, scan, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	var found bool
	for _, c := range doc.CreationInfo.Creators {
		if c == "Tool: scancode-toolkit-mini-32.4.0" {
			found = true
		}
	}
	if !found {
		t.Errorf("SPDX creators = %v, want a scancode-toolkit-mini entry", doc.CreationInfo.Creators)
	}
}

func TestSPDXOmitsScancodeCreatorWithoutEvidence(t *testing.T) {
	repo := &db.RepoForSBOM{Name: "myapp", Owner: "org", GitURL: "https://github.com/org/myapp"}
	data, err := generateSPDX(repo, nil, nil, nil)
	if err != nil {
		t.Fatalf("generateSPDX: %v", err)
	}
	var doc spdxDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	for _, c := range doc.CreationInfo.Creators {
		if strings.Contains(c, "scancode") {
			t.Errorf("SPDX creator %q present without scan evidence", c)
		}
	}
}

// TestGetScancodeForSBOMSelectsVersion pins the store side: the
// aggregation query must fetch scancode_version from the latest scan.
func TestGetScancodeForSBOMSelectsVersion(t *testing.T) {
	raw, err := os.ReadFile("../db/scancode_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(raw)
	body := src[strings.Index(src, "func (s *PostgresStore) GetScancodeForSBOM"):]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	for _, needle := range []string{"scancode_version", "ScancodeVersion", "ORDER BY data_collection_date DESC"} {
		if !strings.Contains(body, needle) {
			t.Errorf("GetScancodeForSBOM missing %q — SBOM provenance needs the latest scan's version", needle)
		}
	}
}
