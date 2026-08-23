// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
			// v0.28.1 (A2): the trove classifier reads "GNU Library
			// or Lesser…" — the old Contains("GNU Lesser General
			// Public License") arm missed it and the default arm
			// returned the 50-char string verbatim, which nothing
			// downstream normalized (the operator's LGPL-shown-as-
			// not-OSI report).
			name:        "LGPL library-or-lesser trove wording",
			classifiers: []string{"License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)"},
			want:        "LGPL",
		},
		{
			// v0.28.4 (review-lens finding): version-carrying
			// classifiers keep their version — a v3-only declaration
			// must not collapse to bare LGPL (which downstream
			// canonicalizes to LGPL-2.0-or-later, granting a 2.0/2.1
			// choice the declaration never made).
			name:        "LGPL modern lesser wording keeps v3",
			classifiers: []string{"License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)"},
			want:        "LGPL-3.0-only",
		},
		{
			name:        "LGPL v3-or-later keeps the or-later form",
			classifiers: []string{"License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)"},
			want:        "LGPL-3.0-or-later",
		},
		{
			name:        "LGPL v2.1 keeps its version",
			classifiers: []string{"License :: OSI Approved :: GNU Lesser General Public License v2.1 (LGPLv2.1)"},
			want:        "LGPL-2.1-only",
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
	// Window widened v0.27.30: the PEP 639 license_expression support
	// (and its doc comment) grew the function past the old 1500 chars.
	fnBody := code[idx:min(idx+3000, len(code))]

	if !strings.Contains(fnBody, "Classifiers") || !strings.Contains(fnBody, "parsePyPIClassifierLicense") {
		t.Error("resolvePyPILibyear must fall back to classifier-based license when info.license is empty")
	}
	if !strings.Contains(fnBody, "LicenseExpression") {
		t.Error("resolvePyPILibyear must read PEP 639 license_expression FIRST — modern PyPI packages (flask 3.x) leave both the legacy field and classifiers empty")
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
