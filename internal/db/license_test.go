package db

import (
	"os"
	"strings"
	"testing"
)

// TestGetRepoLicensesQueryNormalizesEmptyToUnknown verifies the SQL groups
// dependencies with empty, whitespace-only, or sentinel-value licenses under
// "Unknown" rather than showing blank rows or cryptic registry values.
func TestGetRepoLicensesQueryNormalizesEmptyToUnknown(t *testing.T) {
	data, err := os.ReadFile("timeseries.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find GetRepoLicenses function.
	idx := strings.Index(src, "func (s *PostgresStore) GetRepoLicenses")
	if idx < 0 {
		t.Fatal("cannot find GetRepoLicenses function")
	}
	fn := src[idx : idx+600]

	// Must handle whitespace-only licenses (TRIM), not just exact empty string.
	if !strings.Contains(fn, "TRIM") {
		t.Error("GetRepoLicenses should TRIM whitespace from license before checking for empty (some registries return ' ')")
	}

	// Must map empty/whitespace to 'Unknown'.
	if !strings.Contains(fn, "'Unknown'") {
		t.Error("GetRepoLicenses should map empty licenses to 'Unknown'")
	}
}

// TestNormalizeLicenseFunction verifies the Go-side license normalizer
// that catches common "no license" sentinel values from package registries.
func TestNormalizeLicenseFunction(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"", "Unknown"},
		{"  ", "Unknown"},
		{"NOASSERTION", "Unknown"},
		{"NONE", "Unknown"},
		{"N/A", "Unknown"},
		{"none", "Unknown"},
		{"(none)", "Unknown"},
		{"MIT", "MIT"},
		{"Apache-2.0", "Apache-2.0"},
		{"  MIT  ", "MIT"},
	}
	for _, tt := range tests {
		got := normalizeLicense(tt.input)
		if got != tt.want {
			t.Errorf("normalizeLicense(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestUnknownLicenseIsNotOSI verifies that "Unknown" licenses are never
// marked as OSI-compliant.
func TestUnknownLicenseIsNotOSI(t *testing.T) {
	if isOSILicense("Unknown") {
		t.Error("'Unknown' should not be considered an OSI license")
	}
	if isOSILicense("NOASSERTION") {
		t.Error("'NOASSERTION' should not be considered an OSI license")
	}
}

// TestLicensePageShowsUnknownDistinctly verifies the frontend renders
// "Unknown" license rows with a visual indicator (italic or color) so
// they stand out from named licenses.
func TestLicensePageShowsUnknownDistinctly(t *testing.T) {
	data, err := os.ReadFile("../web/templates.go")
	if err != nil {
		t.Fatal(err)
	}
	tmpl := string(data)
	// The license rendering JavaScript should check for "Unknown" and
	// style it distinctly (e.g., italic, color, or a class).
	if !strings.Contains(tmpl, "Unknown") || !strings.Contains(tmpl, "italic") {
		t.Error("license table should render 'Unknown' licenses with distinct styling (e.g., italic)")
	}
}
