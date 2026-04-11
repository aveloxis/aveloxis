package db

import (
	"testing"
)

// ============================================================
// NormalizeLicenseToSPDX — maps common synonyms to canonical SPDX IDs
// ============================================================

func TestNormalizeLicense_MIT_Synonyms(t *testing.T) {
	synonyms := []string{"MIT", "MIT License", "mit", "The MIT License", "MIT license", "The MIT License (MIT)"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "MIT" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want MIT", s, got)
		}
	}
}

func TestNormalizeLicense_Apache2_Synonyms(t *testing.T) {
	synonyms := []string{
		"Apache-2.0", "Apache 2.0", "Apache License 2.0", "Apache License, Version 2.0",
		"apache-2.0", "Apache Software License 2.0", "Apache License v2.0",
		"Apache License Version 2.0", "Apache2", "Apache 2",
	}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "Apache-2.0" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want Apache-2.0", s, got)
		}
	}
}

func TestNormalizeLicense_BSD3_Synonyms(t *testing.T) {
	synonyms := []string{
		"BSD-3-Clause", "BSD 3-Clause", "3-Clause BSD License",
		"BSD 3-Clause License", "New BSD License", "Modified BSD License",
		"BSD-3-Clause License", "BSD 3 Clause",
	}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "BSD-3-Clause" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want BSD-3-Clause", s, got)
		}
	}
}

func TestNormalizeLicense_BSD2_Synonyms(t *testing.T) {
	synonyms := []string{
		"BSD-2-Clause", "BSD 2-Clause", "Simplified BSD License",
		"FreeBSD License", "BSD-2-Clause License",
	}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "BSD-2-Clause" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want BSD-2-Clause", s, got)
		}
	}
}

func TestNormalizeLicense_BSD_Bare(t *testing.T) {
	// Bare "BSD" without clause number → BSD-3-Clause (most common interpretation).
	if got := NormalizeLicenseToSPDX("BSD"); got != "BSD-3-Clause" {
		t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want BSD-3-Clause", "BSD", got)
	}
}

func TestNormalizeLicense_GPL2_Synonyms(t *testing.T) {
	synonyms := []string{"GPL-2.0", "GPL-2.0-only", "GNU General Public License v2.0", "GPLv2"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "GPL-2.0-only" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want GPL-2.0-only", s, got)
		}
	}
}

func TestNormalizeLicense_GPL3_Synonyms(t *testing.T) {
	synonyms := []string{"GPL-3.0", "GPL-3.0-only", "GNU General Public License v3.0", "GPLv3"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "GPL-3.0-only" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want GPL-3.0-only", s, got)
		}
	}
}

func TestNormalizeLicense_ISC_Synonyms(t *testing.T) {
	synonyms := []string{"ISC", "ISC License", "ISC license"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "ISC" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want ISC", s, got)
		}
	}
}

func TestNormalizeLicense_MPL_Synonyms(t *testing.T) {
	synonyms := []string{"MPL-2.0", "Mozilla Public License 2.0", "MPL 2.0"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "MPL-2.0" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want MPL-2.0", s, got)
		}
	}
}

func TestNormalizeLicense_Unlicense(t *testing.T) {
	synonyms := []string{"Unlicense", "The Unlicense", "UNLICENSE"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "Unlicense" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want Unlicense", s, got)
		}
	}
}

func TestNormalizeLicense_CC0(t *testing.T) {
	synonyms := []string{"CC0-1.0", "CC0 1.0", "CC0", "CC0 1.0 Universal"}
	for _, s := range synonyms {
		if got := NormalizeLicenseToSPDX(s); got != "CC0-1.0" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want CC0-1.0", s, got)
		}
	}
}

func TestNormalizeLicense_Unknown(t *testing.T) {
	unknowns := []string{"", "NOASSERTION", "NONE", "N/A", "Unknown", "(none)"}
	for _, s := range unknowns {
		if got := NormalizeLicenseToSPDX(s); got != "Unknown" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want Unknown", s, got)
		}
	}
}

func TestNormalizeLicense_PassthroughUnrecognized(t *testing.T) {
	// Licenses we don't recognize should pass through unchanged (trimmed).
	exotic := "Custom Enterprise License v3"
	if got := NormalizeLicenseToSPDX(exotic); got != exotic {
		t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want unchanged", exotic, got)
	}
}

func TestNormalizeLicense_CaseInsensitive(t *testing.T) {
	if got := NormalizeLicenseToSPDX("apache license 2.0"); got != "Apache-2.0" {
		t.Errorf("lowercase: got %q, want Apache-2.0", got)
	}
	if got := NormalizeLicenseToSPDX("APACHE-2.0"); got != "Apache-2.0" {
		t.Errorf("uppercase: got %q, want Apache-2.0", got)
	}
}

func TestNormalizeLicense_Whitespace(t *testing.T) {
	if got := NormalizeLicenseToSPDX("  MIT  "); got != "MIT" {
		t.Errorf("whitespace: got %q, want MIT", got)
	}
}

// ============================================================
// isOSILicense — should recognize normalized forms
// ============================================================

func TestIsOSILicense_NormalizedForms(t *testing.T) {
	// After normalization, all these should be recognized as OSI.
	licenses := []string{"MIT", "Apache-2.0", "BSD-3-Clause", "BSD-2-Clause", "GPL-2.0-only", "ISC", "MPL-2.0"}
	for _, l := range licenses {
		if !isOSILicense(l) {
			t.Errorf("isOSILicense(%q) = false, want true", l)
		}
	}
}

func TestIsOSILicense_Unknown(t *testing.T) {
	if isOSILicense("Unknown") {
		t.Error("Unknown should not be OSI")
	}
}
