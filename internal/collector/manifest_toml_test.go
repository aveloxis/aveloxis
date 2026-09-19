// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "testing"

// TestSplitTOMLDottedKeyHandlesQuotedNames — v0.29.57 (Copilot review on
// PR #210, raised in both tiers). A quoted key may still carry a dotted
// SUBKEY: `"zope.interface".version = "6"` is one quoted name plus the
// `version` subkey. The shortcut returned the whole string unquoted, so the
// name became `zope.interface".version` — a package no registry has — and
// the real dependency's version was never applied. Quoted names with dots
// are ordinary in Python (`zope.interface`, `ruamel.yaml`) and legal in
// Cargo.
func TestSplitTOMLDottedKeyHandlesQuotedNames(t *testing.T) {
	for _, tc := range []struct{ in, name, sub string }{
		{`"zope.interface".version`, "zope.interface", "version"},
		{`"zope.interface"`, "zope.interface", ""},
		{`'ruamel.yaml'.features`, "ruamel.yaml", "features"},
		{`"plain-quoted"`, "plain-quoted", ""},
		{`serde.version`, "serde", "version"},
		{`serde`, "serde", ""},
		{`"has space".workspace`, "has space", "workspace"},
		// An unterminated quote names nothing usable; it must not invent a
		// name carrying the quote character.
		{`"unterminated`, "unterminated", ""},
	} {
		name, sub := splitTOMLDottedKey(tc.in)
		if name != tc.name || sub != tc.sub {
			t.Errorf("splitTOMLDottedKey(%s) = (%q, %q), want (%q, %q)", tc.in, name, sub, tc.name, tc.sub)
		}
	}
}

// The end-to-end shape: a quoted dotted dependency is inventoried under its
// real name with its real version.
func TestScanTOMLDepTablesQuotedDottedKey(t *testing.T) {
	got := scanTOMLDepTables("[dependencies]\n\"zope.interface\".version = \"6\"\n",
		map[string]bool{"[dependencies]": true})
	if len(got) != 1 {
		t.Fatalf("got %d entries, want 1: %+v", len(got), got)
	}
	if got[0].Name != "zope.interface" || got[0].Version != "6" {
		t.Errorf("entry = %q@%q, want zope.interface@6", got[0].Name, got[0].Version)
	}
}
