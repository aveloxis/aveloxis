package web

import (
	"strings"
	"testing"
)

// TestRepoDetailHasSourceCodeLicenses verifies the repo detail page includes
// a "Source Code Licenses" section showing ScanCode analysis results.
func TestRepoDetailHasSourceCodeLicenses(t *testing.T) {
	if !strings.Contains(allTemplates, "Source Code Licenses") {
		t.Error("repo detail template must include 'Source Code Licenses' section for ScanCode data")
	}
}

// TestRepoDetailHasScancodeTable verifies the template has a table for
// displaying per-file license detections from ScanCode.
func TestRepoDetailHasScancodeTable(t *testing.T) {
	if !strings.Contains(allTemplates, "scancode-license-table") {
		t.Error("repo detail template must have a scancode-license-table element")
	}
}

// TestRepoDetailHasCopyrightSection verifies the template shows copyright
// holders from ScanCode analysis.
func TestRepoDetailHasCopyrightSection(t *testing.T) {
	if !strings.Contains(allTemplates, "copyright") || !strings.Contains(allTemplates, "Copyright") {
		t.Error("repo detail template must show copyright holders from ScanCode")
	}
}

// TestRepoDetailFetchesScancodeLicenses verifies the JS fetches the scancode
// license endpoint from the API.
func TestRepoDetailFetchesScancodeLicenses(t *testing.T) {
	if !strings.Contains(allTemplates, "scancode-licenses") {
		t.Error("repo detail template must fetch scancode-licenses API endpoint")
	}
}
