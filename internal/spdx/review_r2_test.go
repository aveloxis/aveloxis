// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"strings"
	"testing"
)

// v0.29.67 review round 2.

// phraseSynonyms stands in for db's synonym map, with its operator-bearing
// keys passed as phrases.
func phraseSynonyms(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "zlib/libpng", "zlib/libpng license":
		return "Zlib"
	case "common development and distribution license":
		return "CDDL-1.0"
	case "gnu library or lesser general public license (lgpl)":
		return "LGPL"
	}
	return strings.TrimSpace(s)
}

var testPhrases = []string{"zlib/libpng", "zlib/libpng license", "common development and distribution license",
	"gnu library or lesser general public license (lgpl)"}

// TestOperatorBearingSynonymsStayWhole — the whole-string-first rule held
// only at the top level: inside a larger expression (a joined list, a
// stored " AND " row, a parenthesized part) the parser split the synonym on
// its own "and", "or" or slash.
func TestOperatorBearingSynonymsStayWhole(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"(zlib/libpng) OR MIT", "Zlib OR MIT"},
		{"MIT OR zlib/libpng", "MIT OR Zlib"},
		{"(zlib/libpng License) OR MIT", "Zlib OR MIT"},
		{"Common Development and Distribution License AND MIT", "CDDL-1.0 AND MIT"},
		{"(Common Development and Distribution License) OR MIT", "CDDL-1.0 OR MIT"},
		{"(GNU Library or Lesser General Public License (LGPL)) OR MIT", "LGPL OR MIT"},
		{"GNU Library or Lesser General Public License (LGPL) AND MIT", "LGPL AND MIT"},
		// A phrase only matches whole: "zlib/libpngX" is not the synonym.
		{"MIT OR zlib/libpngX", "MIT OR zlib/libpngX"},
		// The top-level rule is unchanged.
		{"zlib/libpng", "Zlib"},
		{"MIT/Apache-2.0", "MIT OR Apache-2.0"},
	} {
		if got := NormalizeExpression(tc.in, phraseSynonyms, testPhrases...); got != tc.want {
			t.Errorf("NormalizeExpression(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestJoinExpressionsUsesTheParsersOperators — JoinExpressions looked for
// " and " / " or " padded with spaces, while the tokenizer also splits on
// tabs and newlines: "MIT\nor Apache-2.0" joined unparenthesized and
// regrouped (it then read OSI-approved next to CC0-1.0).
func TestJoinExpressionsUsesTheParsersOperators(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		want  string
	}{
		{[]string{"MIT\nor Apache-2.0", "CC0-1.0"}, "(MIT\nor Apache-2.0) AND CC0-1.0"},
		{[]string{"MIT\tOR Apache-2.0", "CC0-1.0"}, "(MIT\tOR Apache-2.0) AND CC0-1.0"},
		{[]string{"LicenseRef-or-later", "MIT"}, "LicenseRef-or-later AND MIT"},
		{[]string{"GPL-2.0-only WITH Classpath-exception-2.0", "MIT"}, "GPL-2.0-only WITH Classpath-exception-2.0 AND MIT"},
	} {
		if got := JoinExpressions("AND", tc.parts); got != tc.want {
			t.Errorf("JoinExpressions(%q) = %q, want %q", tc.parts, got, tc.want)
		}
	}
	if OSIApproved(NormalizeExpression(JoinExpressions("AND", []string{"MIT\nor Apache-2.0", "CC0-1.0"}), identity)) {
		t.Error("(MIT OR Apache-2.0) AND CC0-1.0 must not be approved")
	}
}

// TestDisplayKeyIsNotQuadratic — sortTree re-rendered each subtree on every
// comparison; keys are now rendered once per node.
func TestDisplayKeyIsNotQuadratic(t *testing.T) {
	var b strings.Builder
	depth := 1500
	for i := 0; i < depth; i++ {
		if i%2 == 0 {
			b.WriteString("(MIT AND ")
		} else {
			b.WriteString("(ISC OR ")
		}
	}
	b.WriteString("Apache-2.0")
	b.WriteString(strings.Repeat(")", depth))
	expr := NormalizeExpression(b.String(), identity)
	counted := 0
	sortKeyRenders = func() { counted++ }
	defer func() { sortKeyRenders = func() {} }()
	_ = DisplayKey(expr)
	if counted > 4*depth {
		t.Errorf("DisplayKey rendered %d subtree keys for a %d-deep expression; want O(nodes)", counted, depth)
	}
}

// TestPlaceholderLookalikesAreNotPhrases — v0.29.67 review round 3: the
// phrase placeholders are private-use runes, and registry text that already
// contained one was read as the substituted phrase ("0 OR
// zlib/libpng" became "Zlib OR Zlib"). Input containing the placeholder rune
// is never substituted, so the look-alike is just an unknown operand.
func TestPlaceholderLookalikesAreNotPhrases(t *testing.T) {
	in := "0 OR zlib/libpng"
	if got := NormalizeExpression(in, phraseSynonyms, testPhrases...); Valid(got) {
		t.Errorf("NormalizeExpression(%q) = %q, a valid expression; the look-alike must not become a license", in, got)
	}
}

// TestOperandTexts — the name-list split (review rounds 4-8): "and"/"or"
// words only; parentheses and slashes stay inside; an and/or opening a
// version range ("or later", "or (at your option) any later version", "and
// above") is not a separator, but an and/or before a bare "any" is; "with"
// never is.
func TestOperandTexts(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"GPL-3.0-only AND Apache License 2.0 AND Proprietary", []string{"GPL-3.0-only", "Apache License 2.0", "Proprietary"}},
		{"ISC License (ISC) or MIT/X11", []string{"ISC License (ISC)", "MIT/X11"}},
		{"GPL-2.0 or later; see COPYING", []string{"GPL-2.0 or later; see COPYING"}},
		{"GPL v2 OR ANY later version", []string{"GPL v2 OR ANY later version"}},
		{"GPLv2 or (at your option) any later version", []string{"GPLv2 or (at your option) any later version"}},
		{"GPLv3 and later; see COPYING", []string{"GPLv3 and later; see COPYING"}},
		{"GPL-2.0 or (at your option) MIT", []string{"GPL-2.0", "(at your option) MIT"}},
		{"GPLv2 or (unclosed", []string{"GPLv2", "(unclosed"}},
		{"GPL-3.0-only and any code under MPL-2.0", []string{"GPL-3.0-only", "any code under MPL-2.0"}},
		{"Apache License 2.0 with LLVM Exceptions", []string{"Apache License 2.0 with LLVM Exceptions"}},
		{"MIT and", []string{"MIT"}},
		{"", nil},
	} {
		got := OperandTexts(tc.in)
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("OperandTexts(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestLicenseRefPrefixIsCanonicalized — mcp-gopls review (A5): a lowercase
// "licenseref-foo" parsed but kept its spelling, so "licenseref-foo OR MIT"
// failed Validate and the SBOM wrote NOASSERTION for a valid expression.
// The prefix is canonicalized; the idstring keeps its own spelling.
func TestLicenseRefPrefixIsCanonicalized(t *testing.T) {
	got := NormalizeExpression("licenseref-Foo OR MIT", identity)
	if got != "LicenseRef-Foo OR MIT" {
		t.Errorf("NormalizeExpression = %q, want LicenseRef-Foo OR MIT", got)
	}
	if !Valid(got) {
		t.Errorf("%q must validate", got)
	}
	if got := NormalizeExpression("LICENSEREF-x", identity); got != "LicenseRef-x" {
		t.Errorf("single ref = %q", got)
	}
	// Review round 15 C4: the prefix is matched on its own bytes, not by
	// lowercasing the whole leaf and slicing at the ASCII length. U+0130
	// lowercases from two bytes to one, which shifted the slice and minted
	// "LicenseRef--x". A non-ASCII prefix is not the SPDX prefix at all.
	if got := NormalizeExpression("L\u0130CENSEREF-x", identity); strings.HasPrefix(got, "LicenseRef-") {
		t.Errorf("a non-ASCII look-alike prefix = %q, want it left unresolved", got)
	}
}
