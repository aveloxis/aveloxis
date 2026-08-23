// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

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
	// v0.27.46: the SQL moved into GetRepoLicensesScoped (GetRepoLicenses
	// is a thin shim); the TRIM/Unknown contract rides with it.
	idx := strings.Index(src, "func (s *PostgresStore) GetRepoLicensesScoped")
	if idx < 0 {
		t.Fatal("cannot find GetRepoLicensesScoped function")
	}
	fn := src[idx : idx+1200]

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

// v0.28.1 (A2) — the operator's LGPL report: "GNU Library or Lesser
// General Public License (LGPL)" (the PyPI trove wording — "Library
// OR Lesser") normalized to nothing and rendered "not OSI" even
// though every LGPL version is OSI-approved. Unversioned family
// labels map to their own canonical bucket (never fabricate a
// version) and the OSI set covers them: all released versions of
// LGPL / EPL / Artistic are OSI-approved, so the unversioned label
// is safely approved too.
func TestUnversionedLicenseFamiliesAreOSI(t *testing.T) {
	// Version-unspecified LGPL translates to the exact SPDX
	// expression for "some LGPL version": the classifier's wording
	// spans "Library" (2.0) and "Lesser" (2.1+/3.0), i.e.
	// LGPL-2.0-or-later — a REAL SPDX id (the synonym-canonical
	// groundtruth tripwire bans invented labels).
	if got := NormalizeLicenseToSPDX("GNU Library or Lesser General Public License (LGPL)"); got != "LGPL-2.0-or-later" {
		t.Errorf("trove LGPL wording normalized to %q, want LGPL-2.0-or-later", got)
	}
	if got := NormalizeLicenseToSPDX("LGPL"); got != "LGPL-2.0-or-later" {
		t.Errorf("bare LGPL normalized to %q, want LGPL-2.0-or-later", got)
	}
	// EPL/Artistic have no SPDX any-version expression (or-later is
	// GNU-only) — the bare family labels stay and are OSI-approved
	// (every released version of each family is).
	for _, lic := range []string{"LGPL-2.0-or-later", "EPL", "Artistic"} {
		if !isOSILicense(lic) {
			t.Errorf("%s must be OSI-approved", lic)
		}
	}
	// -or-later SPDX ids are expressions over approved licenses.
	for _, lic := range []string{
		"LGPL-2.1-or-later", "LGPL-3.0-or-later",
		"GPL-2.0-or-later", "GPL-3.0-or-later", "AGPL-3.0-or-later",
	} {
		if !isOSILicense(lic) {
			t.Errorf("%s must be OSI-approved", lic)
		}
	}
}

// v0.28.1 (A2) — compound expressions: the dep tables store
// multi-license declarations joined with " AND " (the v0.27.29
// storage decision), so the OSI check must evaluate the PARTS —
// approved iff every part is approved. Pre-fix, "MPL-2.0 AND MIT"
// was looked up as one literal map key and always rendered
// "not OSI".
func TestCompoundLicenseExpressionsOSI(t *testing.T) {
	if !isOSILicense("MPL-2.0 AND MIT") {
		t.Error("MPL-2.0 AND MIT: both parts OSI-approved — compound must be approved")
	}
	// Parts re-normalize individually (synonym forms inside a
	// compound still resolve).
	if !isOSILicense("MIT License AND Apache 2.0") {
		t.Error("compound parts must be normalized before lookup")
	}
	if isOSILicense("MIT AND Unknown") {
		t.Error("a compound with any non-approved part must NOT be approved")
	}
	if isOSILicense("MIT AND ") {
		t.Error("an empty part must never count as approved")
	}
	if isOSILicense(" AND ") {
		t.Error("all-empty compound must not be approved")
	}
}
