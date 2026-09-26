// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"strings"
	"testing"
)

// identity is a term normalizer that changes nothing: the expression layer
// must work with any caller-supplied synonym map, including none.
func identity(s string) string { return strings.TrimSpace(s) }

// synonyms stands in for db.NormalizeLicenseToSPDX's term map in these
// tests: a few registry spellings, and the three family labels.
func synonyms(s string) string {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "mit license", "the mit license":
		return "MIT"
	case "apache 2.0", "apache license 2.0":
		return "Apache-2.0"
	case "gnu library or lesser general public license (lgpl)":
		return "LGPL"
	case "zlib/libpng":
		return "Zlib"
	}
	return s
}

func TestDataFile(t *testing.T) {
	if v := ListVersion(); v == "" || !strings.Contains(v, ".") {
		t.Errorf("ListVersion() = %q; the data file header must record the SPDX list version", v)
	}
	for _, id := range []string{"MIT", "Apache-2.0", "Unicode-3.0", "MIT-0", "GPL-2.0", "BSD-ask-to-endorse"} {
		if !IsLicenseID(id) {
			t.Errorf("%s must be an SPDX license ID", id)
		}
	}
	for _, id := range []string{"mit", "Apache 2.0", "BSD", "LGPL", "Unknown", ""} {
		if IsLicenseID(id) {
			t.Errorf("IsLicenseID(%q) must be false (exact, canonical case)", id)
		}
	}
	if got, ok := CanonicalLicenseID("apache-2.0"); !ok || got != "Apache-2.0" {
		t.Errorf("CanonicalLicenseID(apache-2.0) = %q, %v; IDs match case-insensitively (spec Annex D)", got, ok)
	}
	if !IsExceptionID("Classpath-exception-2.0") || IsExceptionID("MIT") {
		t.Error("exception IDs are a separate set")
	}
	// SPDX 3.29.0's isOsiApproved, the ground truth the hand list got wrong.
	for id, want := range map[string]bool{
		"MIT": true, "Unicode-3.0": true, "MIT-0": true, "BSL-1.0": true,
		"CC0-1.0": false, "PSF-2.0": false, "SSPL-1.0": false, "BUSL-1.1": false,
	} {
		if got := IsOSIApprovedID(id); got != want {
			t.Errorf("IsOSIApprovedID(%s) = %v, want %v", id, got, want)
		}
	}
}

func TestNormalizeExpression(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// Single terms keep the caller's term behaviour exactly.
		{"MIT", "MIT"},
		{"MIT License", "MIT"},
		{"Custom Corp License", "Custom Corp License"},
		{"GNU Library or Lesser General Public License (LGPL)", "LGPL"}, // prose with "or": the synonym wins
		// Both halves are SPDX IDs (Zlib, Libpng), so only the
		// whole-string-first rule keeps the synonym's single answer.
		{"zlib/libpng", "Zlib"},
		// Expressions.
		{"MIT OR Apache-2.0", "MIT OR Apache-2.0"},
		{"mit or apache-2.0", "MIT OR Apache-2.0"},
		{"MIT/Apache-2.0", "MIT OR Apache-2.0"},
		{"Unlicense/MIT", "Unlicense OR MIT"},
		{"MIT License AND Apache 2.0", "MIT AND Apache-2.0"},
		{"(Apache-2.0 OR MIT) AND BSD-3-Clause", "(Apache-2.0 OR MIT) AND BSD-3-Clause"},
		{"Apache-2.0 OR MIT AND BSD-3-Clause", "Apache-2.0 OR MIT AND BSD-3-Clause"},
		{"GPL-2.0-only with classpath-exception-2.0", "GPL-2.0-only WITH Classpath-exception-2.0"},
		{"GPL-2.0+", "GPL-2.0+"},
		{"gpl-2.0+ OR MIT", "GPL-2.0+ OR MIT"},
		{"MIT OR LicenseRef-Corp", "MIT OR LicenseRef-Corp"},
		{"((MIT))", "MIT"},
		{"MIT OR (Apache-2.0 OR ISC)", "MIT OR Apache-2.0 OR ISC"},
		{"LGPL AND MIT", "LGPL AND MIT"}, // family labels are house canonical forms
		// Not expressions: returned through the term normalizer unchanged.
		{"MIT OR", "MIT OR"},
		{"(MIT", "(MIT"},
		{"MIT OR Some Corp License", "MIT OR Some Corp License"},
		{"GPL-2.0-only WITH Not-An-Exception", "GPL-2.0-only WITH Not-An-Exception"},
		{"MIT WITH Apache-2.0", "MIT WITH Apache-2.0"},
	} {
		if got := NormalizeExpression(tc.in, synonyms); got != tc.want {
			t.Errorf("NormalizeExpression(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := NormalizeExpression("MIT/Apache-2.0", identity); got != "MIT OR Apache-2.0" {
		t.Errorf("with no synonym map, NormalizeExpression = %q", got)
	}
}

func TestValidate(t *testing.T) {
	for _, ok := range []string{
		"MIT", "MIT OR Apache-2.0", "(Apache-2.0 OR MIT) AND BSD-3-Clause",
		"GPL-2.0-only WITH Classpath-exception-2.0", "GPL-2.0+", "LicenseRef-Corp",
		"Unicode-3.0", "BSD-ask-to-endorse OR MIT", // newer than go-spdx's bundled list
		"GPL-2.0", // deprecated but valid
	} {
		if err := Validate(ok); err != nil {
			t.Errorf("Validate(%q) = %v, want valid", ok, err)
		}
	}
	for _, bad := range []string{
		"", "MIT/Apache-2.0", "MIT or Apache-2.0", "Apache 2.0", "BSD", "LGPL", "(MIT", "MIT OR",
		"MIT WITH Apache-2.0", "GPL-2.0-only WITH No-Such-exception", "Not-A-License",
	} {
		if err := Validate(bad); err == nil {
			t.Errorf("Validate(%q) = nil, want an error", bad)
		}
	}
}

func TestOSIApproved(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want bool
	}{
		// The operator's screenshot (after normalization).
		{"MIT OR Apache-2.0", true},
		{"Apache-2.0 OR MIT", true},
		{"CC0-1.0 OR MIT-0 OR Apache-2.0", true},
		{"Unlicense OR MIT", true},
		{"Apache-2.0 OR ISC OR MIT", true},
		{"Apache-2.0 OR BSL-1.0 OR MIT", true},
		{"(Apache-2.0 OR MIT) AND BSD-3-Clause", true},
		{"Unicode-3.0", true},
		{"CC0-1.0", false},
		// OR: any option; AND: every term.
		{"CC0-1.0 OR SSPL-1.0", false},
		{"MIT OR CC0-1.0", true},
		{"MIT AND CC0-1.0", false},
		{"MIT AND Apache-2.0", true},
		{"MIT OR LicenseRef-Corp", true},
		{"LicenseRef-Corp", false},
		// WITH follows its base license (decision 3).
		{"GPL-2.0-only WITH Classpath-exception-2.0", true},
		{"Apache-2.0 WITH LLVM-exception", true},
		{"SSPL-1.0 WITH Classpath-exception-2.0", false},
		// + and deprecated IDs.
		{"GPL-2.0+", true},
		{"GPL-2.0", true},
		// An OSI-approved ID go-spdx does not know yet (the list lag).
		{"BSD-ask-to-endorse", true},
		{"BSD-ask-to-endorse AND CC0-1.0", false},
		// The house family labels (v0.28.1 / v0.28.8 decisions).
		{"LGPL", true},
		{"EPL", true},
		{"Artistic", true},
		{"LGPL AND MIT", true},
		// Never approved.
		{"Unknown", false},
		{"NOASSERTION", false},
		{"", false},
		{"MIT/Apache-2.0", false}, // un-normalized input is not an expression
		{"(MIT", false},
		{"Custom Corp License", false},
	} {
		if got := OSIApproved(tc.expr); got != tc.want {
			t.Errorf("OSIApproved(%q) = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// TestDisplayKey — decision 4: the license table groups by a canonical
// operand order, so "MIT OR Apache-2.0" and "Apache-2.0 OR MIT" are one row.
func TestDisplayKey(t *testing.T) {
	for _, group := range [][]string{
		{"MIT OR Apache-2.0", "Apache-2.0 OR MIT"},
		{"(Apache-2.0 OR MIT) AND BSD-3-Clause", "BSD-3-Clause AND (MIT OR Apache-2.0)"},
		{"Apache-2.0 OR ISC OR MIT", "MIT OR ISC OR Apache-2.0", "ISC OR (MIT OR Apache-2.0)"},
	} {
		want := DisplayKey(group[0])
		for _, e := range group[1:] {
			if got := DisplayKey(e); got != want {
				t.Errorf("DisplayKey(%q) = %q, want %q (same license choice)", e, got, want)
			}
		}
	}
	if DisplayKey("MIT AND Apache-2.0") == DisplayKey("MIT OR Apache-2.0") {
		t.Error("AND and OR must never share a row")
	}
	if got := DisplayKey("GPL-2.0-only WITH Classpath-exception-2.0"); got != "GPL-2.0-only WITH Classpath-exception-2.0" {
		t.Errorf("WITH is not reordered: %q", got)
	}
	for _, s := range []string{"Custom Corp License", "Unknown", "(MIT"} {
		if got := DisplayKey(s); got != s {
			t.Errorf("DisplayKey(%q) = %q; a non-expression passes through", s, got)
		}
	}
}
