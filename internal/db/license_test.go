// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/spdx"
	"github.com/aveloxis/aveloxis/internal/srctest"
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
	// The bare-LGPL family bucket (v0.28.6 — the -or-later id would
	// invent a choose-later-versions grant; see the synonym-canonical
	// groundtruth tripwire bans invented labels).
	if got := NormalizeLicenseToSPDX("GNU Library or Lesser General Public License (LGPL)"); got != "LGPL" {
		t.Errorf("trove LGPL wording normalized to %q, want the LGPL family bucket", got)
	}
	if got := NormalizeLicenseToSPDX("LGPL"); got != "LGPL" {
		t.Errorf("bare LGPL normalized to %q, want the LGPL family bucket (never a version-specific or -or-later id)", got)
	}
	// EPL/Artistic have no SPDX any-version expression (or-later is
	// GNU-only) — the bare family labels stay and are OSI-approved
	// (every released version of each family is).
	// v0.28.8 (operator correction, SPDX-verified): the ENTIRE LGPL
	// family is OSI-approved — spdx.org/licenses marks every
	// LGPL-2.0/2.1/3.0 -only and -or-later id isOsiApproved=true, so
	// the bucket AND every versioned id the classifier can emit must
	// read approved (LGPL-2.0-only was missing, so LGPLv2-exact
	// packages wrongly read "not OSI").
	for _, lic := range []string{
		"LGPL",
		"LGPL-2.0-only", "LGPL-2.1-only", "LGPL-3.0-only",
		"LGPL-2.0-or-later", "LGPL-2.1-or-later", "LGPL-3.0-or-later",
		"EPL", "Artistic",
	} {
		if !isOSILicense(lic) {
			t.Errorf("%s must be OSI-approved (SPDX ground truth: the whole LGPL family is)", lic)
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

// TestOSIBadgeReadsSPDXExpressions — worklist 53 (summary/39): the badge
// understood only " AND ", so every OR, slash, parenthesized and WITH
// license read "not OSI", and the hand-kept allowlist disagreed with the
// SPDX list (CC0-1.0 and PSF-2.0 approved, Unicode-3.0 and MIT-0 missing).
// The raw spellings below are the operator's screenshot, through the real
// synonym map.
func TestOSIBadgeReadsSPDXExpressions(t *testing.T) {
	for raw, want := range map[string]bool{
		"MIT OR Apache-2.0":                         true,
		"Apache-2.0 OR MIT":                         true,
		"MIT/Apache-2.0":                            true,
		"Unlicense/MIT":                             true,
		"mit or apache-2.0":                         true,
		"CC0-1.0 OR MIT-0 OR Apache-2.0":            true,
		"Unlicense OR MIT":                          true,
		"Apache-2.0 OR ISC OR MIT":                  true,
		"Apache-2.0 OR BSL-1.0 OR MIT":              true,
		"(Apache-2.0 OR MIT) AND BSD-3-Clause":      true,
		"Unicode-3.0":                               true,
		"MIT-0":                                     true,
		"GPL-2.0-only WITH Classpath-exception-2.0": true,
		"MIT License OR Apache 2.0":                 true,
		"CC0-1.0":                                   false,
		"PSF-2.0":                                   false,
		"MIT AND CC0-1.0":                           false,
	} {
		if got := isOSILicense(raw); got != want {
			t.Errorf("isOSILicense(%q) = %v, want %v", raw, got, want)
		}
	}
}

// TestLicenseTableGroupsOneChoiceAsOneRow — decision 4: the table's key
// (normalizeLicense, shared by the aggregate and the drill-down) puts every
// spelling of one license choice in one row.
func TestLicenseTableGroupsOneChoiceAsOneRow(t *testing.T) {
	want := normalizeLicense("MIT OR Apache-2.0")
	for _, raw := range []string{"Apache-2.0 OR MIT", "MIT/Apache-2.0", "Apache-2.0/MIT", "mit or apache-2.0", "MIT License OR Apache 2.0"} {
		if got := normalizeLicense(raw); got != want {
			t.Errorf("normalizeLicense(%q) = %q, want %q", raw, got, want)
		}
	}
	if normalizeLicense("MIT AND Apache-2.0") == want {
		t.Error("AND and OR must never share a row")
	}
	// The real synonym map's one operator-bearing key keeps its synonym.
	if got := NormalizeLicenseToSPDX("zlib/libpng License"); strings.Contains(got, " OR ") {
		t.Errorf("zlib/libpng License normalized to %q; the synonym, not an expression", got)
	}
}

// TestNoHandKeptOSIList — the SPDX list's isOsiApproved is the only OSI
// source (internal/spdx); a literal allowlist here drifted both ways.
func TestNoHandKeptOSIList(t *testing.T) {
	src := srctest.StripGoComments(readSourceFile(t, "timeseries.go"))
	for _, banned := range []string{`"MIT": true`, "osiLicenses", `" AND ")`} {
		if strings.Contains(src, banned) {
			t.Errorf("timeseries.go contains %q: the OSI answer comes from internal/spdx only", banned)
		}
	}
}

// TestZlibLibpngSpellingsAreOneLicense — v0.29.67 review round 1: the
// slash rule read "libpng/zlib" (the zlib/libpng license, spelled the other
// way round) as the choice "Libpng OR Zlib". Every spelling of the one
// license is a synonym, which wins over the expression reading.
func TestZlibLibpngSpellingsAreOneLicense(t *testing.T) {
	for _, s := range []string{"zlib/libpng", "zlib/libpng License", "libpng/zlib", "libpng/zlib License"} {
		if got := NormalizeLicenseToSPDX(s); got != "Zlib" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want Zlib", s, got)
		}
	}
}

// TestSynonymsWithOperatorWordsInsideExpressions — v0.29.67 review round 2:
// a synonym containing "and", "or", a slash or parentheses was split by the
// expression reader whenever it was one operand of a larger expression (a
// joined registry list, a pre-0.29.67 " AND " row). The old " AND " splitter
// normalized each part, so "Common Development and Distribution License AND
// MIT" read OSI-approved before this release; it must again.
func TestSynonymsWithOperatorWordsInsideExpressions(t *testing.T) {
	for raw, want := range map[string]string{
		"Common Development and Distribution License AND MIT":         "CDDL-1.0 AND MIT",
		"(zlib/libpng) OR MIT":                                        "Zlib OR MIT",
		"(Common Development and Distribution License) OR MIT":        "CDDL-1.0 OR MIT",
		"GNU Library or Lesser General Public License (LGPL) AND MIT": "LGPL AND MIT",
		"The MIT License (MIT) OR Apache 2.0":                         "MIT OR Apache-2.0",
	} {
		if got := NormalizeLicenseToSPDX(raw); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want %q", raw, got, want)
		}
	}
	if !isOSILicense("Common Development and Distribution License AND MIT") {
		t.Error("a pre-0.29.67 row of two approved licenses must read approved (it did before this release)")
	}
}

// TestLongExpressionsAreNotFingerprinted — v0.29.67 review round 3: over 80
// characters the synonym rules also fingerprint full license texts
// ("apache license" + "2.0" = Apache-2.0). The whole-string-first rule ran
// that before the parser, so a long real expression collapsed to one
// license: a GPL obligation or a proprietary conjunct vanished from the SBOM
// and the table. A long string is read as an expression first; the
// fingerprint applies only when it is not one.
func TestLongExpressionsAreNotFingerprinted(t *testing.T) {
	for raw, want := range map[string]string{
		"GPL-3.0-only OR Apache License 2.0 OR Mozilla Public License, Version 2.0 OR BSD-3-Clause": "GPL-3.0-only OR Apache-2.0 OR MPL-2.0 OR BSD-3-Clause",
		"Apache License 2.0 AND LicenseRef-commercial-proprietary-terms AND MIT AND BSD-3-Clause":   "Apache-2.0 AND LicenseRef-commercial-proprietary-terms AND MIT AND BSD-3-Clause",
		"GPL-3.0-only AND Apache License 2.0 AND MIT AND BSD-3-Clause AND ISC AND Zlib AND 0BSD":    "GPL-3.0-only AND Apache-2.0 AND MIT AND BSD-3-Clause AND ISC AND Zlib AND 0BSD",
	} {
		if got := NormalizeLicenseToSPDX(raw); got != want {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want %q", raw, got, want)
		}
	}
	if isOSILicense("Apache License 2.0 AND LicenseRef-commercial-proprietary-terms AND MIT AND BSD-3-Clause") {
		t.Error("a proprietary conjunct must keep the expression from reading OSI-approved")
	}
	// A real full license text still fingerprints (the >80 path it exists for).
	text := "Apache License, Version 2.0, January 2004. Licensed under the Apache License, Version 2.0 (the License); you may not use this file except in compliance"
	if got := NormalizeLicenseToSPDX(text); got != "Apache-2.0" {
		t.Errorf("a full Apache text normalized to %q, want Apache-2.0", got)
	}
}

// TestLongNameListsWithALicenseNameStayWhole — v0.29.67 review round 4: the
// round-3 fix read a long string as an expression first, but only succeeded
// when every operand resolved. With one free-text operand the string went
// back to the full-text fingerprint and collapsed to one license: an 83-byte
// "GPL-3.0-only AND Apache License 2.0 AND Proprietary license of Example
// Corp AND MIT" exported as Apache-2.0 (a false SPDX assertion, read
// OSI-approved), while its 58-byte twin correctly gave no license. A string
// whose "and"/"or" split it into name-length operands, at least one of them a
// license, is a list of names and stays whole.
//
// A long list in which NO operand is a license by the synonym rules ("Apache
// License Version 2 or GNU General Public License version 3 or ...") still
// fingerprints, as before this release: with no recognized name there is
// nothing to tell it from a license text (review round 5, declined).
func TestLongNameListsWithALicenseNameStayWhole(t *testing.T) {
	for _, raw := range []string{
		"GPL-3.0-only AND Apache License 2.0 AND Proprietary license of Example Corp AND MIT",
		"Apache License, Version 2.0 OR Ruby's License OR Commercial license, see COMMERCIAL.txt",
		"MIT License and Apache License, Version 2.0 and BSD 3-Clause License and ISC License too",
	} {
		got := NormalizeLicenseToSPDX(raw)
		if got == "Apache-2.0" || got == "MIT" {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q: a list of names collapsed to one license", raw, got)
		}
		if isOSILicense(raw) {
			t.Errorf("isOSILicense(%q) = true; an expression with a free-text operand is never approved", raw)
		}
	}
	// The short twin already behaved; both lengths must agree.
	short := "Apache License 2.0 AND Proprietary license of Example Corp"
	if got := NormalizeLicenseToSPDX(short); got == "Apache-2.0" {
		t.Errorf("the short twin normalized to %q", got)
	}
}

// TestLicenseNoticesStillFingerprint — v0.29.67 review round 5: the round-4
// name-list check split at parentheses and slashes, so a URL, a "(c)" or a
// parenthesized short name cut a real license notice into short pieces, and
// the notice's own title counted as "a license name": notices HEAD
// recognized came back as raw text. A name list is split only at operator
// WORDS; parentheses and slashes belong to the names and notices.
func TestLicenseNoticesStillFingerprint(t *testing.T) {
	isc := "ISC License (ISC)\n\nCopyright (c) 2004-2010 by Internet Systems Consortium, Inc. (\"ISC\")\n\n" +
		"Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is " +
		"hereby granted, provided that the above copyright notice and this permission notice appear in all copies."
	for raw, want := range map[string]string{
		isc: "ISC",
		"Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0) - see LICENSE file":                                    "Apache-2.0",
		"Apache License, Version 2.0 (see http://www.apache.org/licenses/LICENSE-2.0 for details)":                                       "Apache-2.0",
		"Mozilla Public License 2.0 (MPL 2.0) - see https://www.mozilla.org/en-US/MPL/2.0/ for details":                                  "MPL-2.0",
		"CC0 1.0 Universal (CC0 1.0) Public Domain Dedication, Creative Commons, https://creativecommons.org/publicdomain/zero/1.0/":     "CC0-1.0",
		"This is free and unencumbered software released into the public domain. See the Unlicense (Unlicense) at https://unlicense.org": "Unlicense",
		"Apache License 2.0 (Apache-2.0); Copyright 2020 The Authors; see http://www.apache.org/licenses/":                               "Apache-2.0",
	} {
		// The pre-0.29.67 answer is normalizeLicenseTerm's; each notice must keep it.
		if got, head := NormalizeLicenseToSPDX(raw), normalizeLicenseTerm(raw); got != want || head != want {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q (HEAD rules: %q), want %q", raw, got, head, want)
		}
	}
}

// TestOneLicenseNoticesAreNotNameLists — v0.29.67 review round 6: the
// name-list split treated the "or" of "or later" and the "with" before an
// exception's prose as list separators, so a notice for ONE license lost it
// ("GPL-2.0 or later; see COPYING ..." came back as raw text). And the long
// expression reader resolved a prose operand through the full-text
// fingerprint, which duplicated the first operand's license ("GPL-2.0-only
// OR GPL-2.0-only"), a row HEAD never produced. Each keeps its pre-0.29.67
// answer (normalizeLicenseTerm's).
func TestOneLicenseNoticesAreNotNameLists(t *testing.T) {
	resolved := 0
	for _, raw := range []string{
		"Apache License, Version 2.0 or later; see http://www.apache.org/licenses/ for the text",
		"Apache License 2.0 with LLVM Exceptions, see https://llvm.org/LICENSE.txt for details",
		"GPLv2 with the Classpath exception; see the GNU General Public License version 2 in LICENSE",
		"GPL-2.0 or later; see COPYING for the terms of the GNU General Public License version 2",
		"GPLv2 or later, see the file COPYING in the source distribution of the GNU General Public License version 2",
		"PSF or later, Python Software Foundation License Version 2, see LICENSE for the text of the license",
		"MPL-2.0 or\nThis Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.",
		// Round 7: the FSF wording and the other version-range idioms, with
		// prose tails of 80 bytes or less (a longer tail never split).
		"GPLv2 or (at your option) any later version of the GNU General Public License version 2",
		"Apache License 2.0 or (at your option) any later version of the Apache License, see LICENSE",
		"GPL-2.0 or (at your option) later versions of the GNU General Public License version 2",
		"GPLv2 or above; see COPYING for the terms of the GNU General Public License version 2",
		"GPLv3 or newer - see COPYING for the terms of the GNU General Public License version 3",
		"GPLv3 or higher - see COPYING for the terms of the GNU General Public License version 3",
		"GPLv2 or greater - see COPYING for the terms of the GNU General Public License version 2",
		"GPLv3 and later; see COPYING for the terms of the GNU General Public License version 3",
		"GPLv3 and any later version; see COPYING for the GNU General Public License version 3",
	} {
		// The contract: the notice is not split into a name list, so the long
		// path gives the fingerprint's own answer, license or text (a split
		// returns the whole trimmed string, which the fingerprint never
		// does). Since worklist 60 the fingerprint answers only what a notice
		// states exactly: "X WITH exception" or "X+" when it says so, and text
		// when its mentions disagree ("GPL-2.0 or later; see ... version 2",
		// review round 21).
		head := normalizeLicenseTerm(raw)
		if spdx.Valid(head) {
			resolved++
		}
		if got := NormalizeLicenseToSPDX(raw); got != head {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q, want the fingerprint's %q", raw, got, head)
		}
	}
	// Some fixtures must still resolve, or the table tests only text.
	if resolved == 0 {
		t.Error("no fixture resolves to a license; the table no longer tests that a one-license notice keeps its license")
	}
}

// TestShortProseTailAfterAndOrStaysText pins both sides of the round-7 class
// decision. First fixture: a one-license notice whose short prose tail
// follows "and"/"or" (not a version range) cannot be told from a name list
// with a free-text operand, so it stays text (NOASSERTION in the SBOM) rather
// than asserting its license. Second fixture: the real name list that the
// opposite lean would collapse into one license, a false assertion (round 4).
// For the first fixture HEAD's answer was the license, and losing it is the
// accepted cost (summary/changelog/v0.29.md); for the second, HEAD's answer
// was the round-4 defect itself.
func TestShortProseTailAfterAndOrStaysText(t *testing.T) {
	for _, raw := range []string{
		"Apache License 2.0 and the additional terms in NOTICE file included with this distribution",
		"GPL-3.0-only AND Apache License 2.0 AND Proprietary license of Example Corp AND MIT",
	} {
		if got := NormalizeLicenseToSPDX(raw); got != raw {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q; the decided behaviour keeps it as text", raw, got)
		}
		if isOSILicense(raw) {
			t.Errorf("isOSILicense(%q) = true", raw)
		}
	}
}

// TestAnyIsNotAVersionRangeByItself — v0.29.67 review round 8: "any" counted
// as a version-range word on its own, so "GPL-3.0-only and any code under
// the Mozilla Public License 2.0 ..." was one operand, fingerprinted to
// MPL-2.0 and read OSI-approved: a real two-license list collapsed, the
// dangerous direction. "any" only leads into a range word ("any later").
func TestAnyIsNotAVersionRangeByItself(t *testing.T) {
	for _, raw := range []string{
		"GPL-3.0-only and any code under the Mozilla Public License 2.0 in the vendor directory",
		"LGPL-2.1-or-later and (for the bundled parts) any files under the Apache License 2.0 now",
		"GPL-3.0-only or any file under the Apache License 2.0 as marked in the file headers",
	} {
		if got := NormalizeLicenseToSPDX(raw); got != raw {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q; a list with a named license stays whole", raw, got)
		}
		if isOSILicense(raw) {
			t.Errorf("isOSILicense(%q) = true", raw)
		}
	}
}

// TestNamedListWithALongNoticeStaysText — worklist 58, closed by the round-7
// class decision (never assert a license falsely; keep text when unsure): a
// list whose "and"/"or" split it into parts, one of them a recognized license
// name, is a list however long its other parts are. Before, one part over 80
// bytes sent the whole string to the full-text fingerprint, and "GPL-3.0-only
// AND Apache License, Version 2.0 (see LICENSE-APACHE, ...)" read Apache-2.0
// and OSI-approved, dropping the GPL conjunct (also the pre-0.29.67
// behaviour).
func TestNamedListWithALongNoticeStaysText(t *testing.T) {
	notice := "Apache License, Version 2.0 (see LICENSE-APACHE, http://www.apache.org/licenses/LICENSE-2.0)"
	for _, raw := range []string{
		"GPL-3.0-only AND " + notice,
		"MIT OR " + notice,
	} {
		if got := NormalizeLicenseToSPDX(raw); got != raw {
			t.Errorf("NormalizeLicenseToSPDX(%q) = %q; a named list stays text", raw, got)
		}
		if isOSILicense(raw) {
			t.Errorf("isOSILicense(%q) = true", raw)
		}
	}
	// The boundary: a list with NO part that is a recognized license name
	// (notice + " AND Proprietary") still fingerprints, as a license text's
	// prose pieces never name a license either (round 5, declined S1).
	// The notice alone is still one license.
	if got := NormalizeLicenseToSPDX(notice); got != "Apache-2.0" {
		t.Errorf("the notice alone normalized to %q, want Apache-2.0", got)
	}
}

// TestNameListWhosePartsAgreeIsThatLicense — worklist 58's other side: a
// list whose parts all resolve to ONE license (a short name, and a long
// notice through the full-text fingerprint) asserts nothing false, so it is
// that license. Parts that disagree keep the text.
func TestNameListWhosePartsAgreeIsThatLicense(t *testing.T) {
	gpl3 := "This program is free software: you can redistribute it under the terms of the GNU General Public License version 3."
	if got := NormalizeLicenseToSPDX("GPL-3.0-only AND " + gpl3); got != "GPL-3.0-only" {
		t.Errorf("agreeing parts normalized to %q, want GPL-3.0-only", got)
	}
	disagree := "MIT AND " + gpl3
	if got := NormalizeLicenseToSPDX(disagree); got != disagree {
		t.Errorf("disagreeing parts normalized to %q, want the text", got)
	}
}

// TestLicenseRowBadgeJudgesItsKey — mcp-gopls review (A1): the license table
// and the scancode table built the badge with isOSILicense(key), which
// normalizes the ALREADY-normalized row key again. A license text the
// normalizer could not identify is keyed by its truncated first line ("MIT
// License"), and the second pass read that as the MIT license: the badge said
// approved while the row said unidentified. The badge judges the key itself.
func TestLicenseRowBadgeJudgesItsKey(t *testing.T) {
	// The three unidentified texts the mcp-gopls reviewer probed: each key is
	// a truncated first line that re-normalizing read as a license.
	for _, custom := range []string{
		"MIT License\nCustom terms: redistribution requires written consent of Example Corp and a signed agreement with the maintainers, see TERMS.",
		"Apache License\nThis software is distributed under the terms of a custom agreement with Example Corp; redistribution requires written consent.",
		"Permission to use, copy, modify, and/or distribute this software for any purpose is granted to members of Example Corp only; redistribution to third parties is prohibited.",
	} {
		key := normalizeLicense(custom)
		// Premise: the normalizer does not identify it (it returns a
		// truncated label, not an SPDX expression).
		if n := NormalizeLicenseToSPDX(custom); spdx.Valid(n) || spdx.FamilyLabels[n] {
			t.Fatalf("fixture premise: %q must stay unidentified, got %q", custom, n)
		}
		if licenseKeyIsOSI(key) {
			t.Errorf("row key %q (an unidentified text) reads OSI-approved", key)
		}
	}
	for k, want := range map[string]bool{
		normalizeLicense("MIT OR Apache-2.0"): true,
		normalizeLicense("CC0-1.0"):           false,
		normalizeLicense("LGPL"):              true,
		"Unknown":                             false,
	} {
		if got := licenseKeyIsOSI(k); got != want {
			t.Errorf("licenseKeyIsOSI(%q) = %v, want %v", k, got, want)
		}
	}
	// Both table builders use it, never isOSILicense on a key.
	for _, f := range []string{"timeseries.go", "scancode_store.go"} {
		src := srctest.StripGoComments(readSourceFile(t, f))
		if !strings.Contains(src, "IsOSI:   licenseKeyIsOSI(lic)") && !strings.Contains(src, "IsOSI:     licenseKeyIsOSI(lic)") {
			t.Errorf("%s must set IsOSI with licenseKeyIsOSI(lic)", f)
		}
		if strings.Contains(src, "isOSILicense(lic)") {
			t.Errorf("%s still re-normalizes the row key with isOSILicense(lic)", f)
		}
	}
}
