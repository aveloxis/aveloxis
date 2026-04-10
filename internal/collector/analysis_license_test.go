package collector

import (
	"os"
	"strings"
	"testing"
)

// TestNormalizeSemanticVersion verifies version normalization for crates.io matching.
// "1.0" should normalize to "1.0.0" to match crates.io's 3-part versions.
func TestNormalizeSemanticVersion(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"1.0", "1.0.0"},
		{"1.0.0", "1.0.0"},
		{"2.3", "2.3.0"},
		{"1.2.3", "1.2.3"},
		{"", ""},
		{"1", "1.0.0"},
		{"0.1.0-beta", "0.1.0-beta"},
	}
	for _, tt := range tests {
		got := normalizeSemanticVersion(tt.input)
		if got != tt.want {
			t.Errorf("normalizeSemanticVersion(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestParsePyPIClassifierLicense verifies extraction of license from PyPI classifiers.
// Many Python packages declare license via classifiers instead of info.license.
func TestParsePyPIClassifierLicense(t *testing.T) {
	tests := []struct {
		name        string
		classifiers []string
		want        string
	}{
		{
			name:        "MIT from classifier",
			classifiers: []string{"Programming Language :: Python :: 3", "License :: OSI Approved :: MIT License"},
			want:        "MIT",
		},
		{
			name:        "Apache from classifier",
			classifiers: []string{"License :: OSI Approved :: Apache Software License"},
			want:        "Apache-2.0",
		},
		{
			name:        "BSD from classifier",
			classifiers: []string{"License :: OSI Approved :: BSD License"},
			want:        "BSD",
		},
		{
			name:        "GPL from classifier",
			classifiers: []string{"License :: OSI Approved :: GNU General Public License v3 (GPLv3)"},
			want:        "GPL-3.0",
		},
		{
			name:        "no license classifier",
			classifiers: []string{"Programming Language :: Python :: 3"},
			want:        "",
		},
		{
			name:        "empty classifiers",
			classifiers: nil,
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parsePyPIClassifierLicense(tt.classifiers)
			if got != tt.want {
				t.Errorf("parsePyPIClassifierLicense() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestGoLibyearHasLicenseFallback verifies the Go resolver fetches license from
// the GitHub API when the Go proxy doesn't provide one.
func TestGoLibyearHasLicenseFallback(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func resolveGoLibyear(")
	if idx < 0 {
		t.Fatal("cannot find resolveGoLibyear")
	}
	fnBody := code[idx:]
	// Find end of function (next func declaration or reasonable boundary)
	endIdx := strings.Index(fnBody[100:], "\nfunc ")
	if endIdx > 0 {
		fnBody = fnBody[:endIdx+100]
	}

	// Must have a license fallback — not just the empty string comment.
	if strings.Contains(fnBody, `License:            ""`) && !strings.Contains(fnBody, "fetchGoModuleLicense") {
		t.Error("resolveGoLibyear must not hardcode empty license — needs a fallback (fetchGoModuleLicense)")
	}
}

// TestCargoVersionNormalization verifies cargo resolver normalizes versions
// before matching. "1.0" in Cargo.toml should match "1.0.0" on crates.io.
func TestCargoVersionNormalization(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func resolveCargoLibyear(")
	if idx < 0 {
		t.Fatal("cannot find resolveCargoLibyear")
	}
	fnBody := code[idx : idx+1200]

	if !strings.Contains(fnBody, "normalizeSemanticVersion") {
		t.Error("resolveCargoLibyear must normalize version strings to match crates.io (e.g., '1.0' → '1.0.0')")
	}
}

// TestPyPIClassifierFallback verifies PyPI resolver falls back to classifiers
// when info.license is empty.
func TestPyPIClassifierFallback(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func resolvePyPILibyear(")
	if idx < 0 {
		t.Fatal("cannot find resolvePyPILibyear")
	}
	fnBody := code[idx : idx+1500]

	if !strings.Contains(fnBody, "Classifiers") || !strings.Contains(fnBody, "parsePyPIClassifierLicense") {
		t.Error("resolvePyPILibyear must fall back to classifier-based license when info.license is empty")
	}
}

// TestRubyGemsLatestFallback verifies RubyGems resolver falls back to the
// latest version's license when the specific version lacks one.
func TestRubyGemsLatestFallback(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func resolveRubyGemsLibyear(")
	if idx < 0 {
		t.Fatal("cannot find resolveRubyGemsLibyear")
	}
	fnBody := code[idx : idx+800]

	// Should check latest version's license as fallback
	if !strings.Contains(fnBody, "latestLicense") && !strings.Contains(fnBody, "versions[0]") {
		t.Error("resolveRubyGemsLibyear must fall back to latest version's license when specific version lacks one")
	}
}
