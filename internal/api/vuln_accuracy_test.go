// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.11 — the vulnerabilities endpoint gains per-finding
// declared_requirement + version_resolution and a repo-level
// lockfile_certainty envelope field.

import (
	"os"
	"strings"
	"testing"
)

func TestVulnJSONCarriesResolutionFields(t *testing.T) {
	src, err := os.ReadFile("vulnerabilities.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		`json:"declared_requirement,omitempty"`,
		`json:"version_resolution,omitempty"`,
		"DeclaredRequirement: v.DeclaredRequirement",
		"VersionResolution:   v.VersionResolution",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("vulnerabilities.go must carry %q — the GUI renders \"≥X declared — floor shown\" from these", needle)
		}
	}
}

func TestVulnResponseCarriesLockfileCertainty(t *testing.T) {
	src, err := os.ReadFile("vulnerabilities.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, `"lockfile_certainty": certainty`) {
		t.Error("the vulnerabilities envelope must include lockfile_certainty")
	}
	if !strings.Contains(s, "GetRepoLockfileCertainty(") {
		t.Error("lockfile_certainty must be derived at read time via GetRepoLockfileCertainty")
	}
	// Degrade, don't 500: a certainty lookup failure must not take
	// down the finding list.
	if !strings.Contains(s, `Overall: "none"`) {
		t.Error("certainty lookup errors must degrade to overall=none, not fail the endpoint")
	}
}
