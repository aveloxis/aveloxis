// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.14 — the vulnerabilities table shows WHICH version was
// scanned. The version is derived server-side from the package purl
// (everything after the LAST '@' — npm-scope-safe because a scope's
// '@' always precedes the final '/', while the version separator
// always follows it).

import (
	"strings"
	"testing"
)

func TestScannedVersionFromPurl(t *testing.T) {
	cases := []struct {
		purl string
		want string
	}{
		{"pkg:npm/lodash@4.17.21", "4.17.21"},
		// npm scope: the scope '@' precedes a '/', the version '@' does not.
		{"pkg:npm/%40babel/traverse@7.23.2", "7.23.2"},
		{"pkg:npm/@babel/traverse@7.23.2", "7.23.2"},
		// Scoped package WITHOUT a version — the scope '@' must NOT be
		// mistaken for a version separator.
		{"pkg:npm/@babel/traverse", ""},
		{"pkg:pypi/django@4.2.1", "4.2.1"},
		{"pkg:golang/github.com/gin-gonic/gin@v1.9.0", "v1.9.0"},
		// Qualifiers / subpath after the version must be stripped.
		{"pkg:maven/org.apache.logging.log4j/log4j-core@2.14.1?type=jar", "2.14.1"},
		{"pkg:npm/lodash@4.17.21#lib/utils", "4.17.21"},
		// No version at all.
		{"pkg:cargo/serde", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := scannedVersionFromPurl(c.purl); got != c.want {
			t.Errorf("scannedVersionFromPurl(%q) = %q, want %q", c.purl, got, c.want)
		}
	}
}

// TestRepoVulnerabilitiesCarryScannedVersion pins the wiring: the
// response row struct declares scanned_version and the handler
// derives it from the purl. (The full-response shape executes against
// the real schema in the endpoint smoke harness.)
func TestRepoVulnerabilitiesCarryScannedVersion(t *testing.T) {
	src := mustReadFile(t, "vulnerabilities.go")
	if !strings.Contains(src, `json:"scanned_version`) {
		t.Error("vulnJSON must declare a scanned_version field — the GUI's Version column reads it")
	}
	if !strings.Contains(src, "scannedVersionFromPurl(") {
		t.Error("handleRepoVulnerabilities must derive scanned_version via scannedVersionFromPurl")
	}
	docs := mustReadFile(t, "../../docs/guide/api.md")
	if !strings.Contains(docs, "scanned_version") {
		t.Error("docs/guide/api.md must document the scanned_version field (same-commit docs rule)")
	}
}
