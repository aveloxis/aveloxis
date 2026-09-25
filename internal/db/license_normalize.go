// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — license_normalize.go maps common license name synonyms,
// misspellings, and verbose expressions to canonical SPDX identifiers.
//
// Problem: Package registries return license names inconsistently —
// "MIT", "MIT License", "The MIT License (MIT)" are all the same license
// but appear as separate rows. Same for "Apache 2.0", "Apache-2.0",
// "Apache License, Version 2.0", etc.
//
// Approach: A two-level lookup:
//  1. Exact match on the trimmed input against a synonym map (case-insensitive).
//  2. If no match, return the input as-is (don't guess — an unrecognized license
//     like "Custom Enterprise License v3" should stay unchanged).
//
// The synonym map covers the most common licenses seen in npm, PyPI, crates.io,
// RubyGems, Maven, Go modules, and NuGet registries. It is intentionally
// conservative — only clear synonyms are mapped, not fuzzy matches.
package db

import (
	"regexp"
	"sort"
	"strings"

	"github.com/aveloxis/aveloxis/internal/spdx"
)

// NormalizeLicenseToSPDX maps a license string to its canonical SPDX form:
// one identifier, or (worklist 53) a canonical license expression. It is the
// one normalizer every reader uses: the license table, the scancode tables
// and both SBOM exporters. A single license name goes through the synonym
// rules below (normalizeLicenseTerm); a string that reads as an expression
// ("MIT/Apache-2.0", "mit or apache-2.0", "MIT License AND Apache 2.0")
// comes back with canonical IDs and operators when every operand resolves
// (internal/spdx.NormalizeExpression), and through the synonym rules
// otherwise.
func NormalizeLicenseToSPDX(license string) string {
	// Over fullTextMinLen the term rules also fingerprint full license TEXTS
	// ("apache license" + "2.0" = Apache-2.0). An expression of that length
	// ("GPL-3.0-only OR Apache License 2.0 OR ...") must be read as an
	// expression first, or the fingerprint collapses it to one license and
	// drops the others from the SBOM and the table (v0.29.67 review round 3).
	if trimmed := strings.TrimSpace(license); len(trimmed) > fullTextMinLen {
		if expr, ok := spdx.ParseExpression(trimmed, nameTerm, operatorBearingSynonyms...); ok {
			return expr
		}
		// A list of license NAMES with a free-text operand is not an
		// expression, but it is not a license text either: fingerprinting it
		// collapsed "GPL-3.0-only AND Apache License 2.0 AND Proprietary ...
		// AND MIT" to Apache-2.0 (review round 4). A name list stays the free
		// text it is, unless all its parts agree on ONE license.
		if one, isList := nameListVerdict(trimmed); isList {
			if one != "" {
				return one
			}
			return trimmed
		}
	}
	// Long text that is neither an expression nor a name list is read only
	// as a whole: splitting it on "/" or "or" fingerprinted prose operands
	// into a false expression ("... version 2. Files in vendor/MIT" read
	// "GPL-2.0-only OR MIT", review round 22; the round-6 class that nameTerm
	// guards in the parse above).
	if len(strings.TrimSpace(license)) > fullTextMinLen {
		return normalizeLicenseTerm(license)
	}
	return spdx.NormalizeExpression(license, normalizeLicenseTerm, operatorBearingSynonyms...)
}

// nameTerm is normalizeLicenseTerm for an operand that is a license NAME: an
// operand over fullTextMinLen is prose and stays unresolved, so it cannot
// fingerprint to the license its neighbour already names ("GPLv2 or later,
// see the file COPYING ... version 2" read "GPL-2.0-only OR GPL-2.0-only",
// review round 6).
func nameTerm(op string) string {
	if len(strings.TrimSpace(op)) > fullTextMinLen {
		return strings.TrimSpace(op)
	}
	return normalizeLicenseTerm(op)
}

// nameListVerdict decides a string over fullTextMinLen that did not parse as
// an expression. isList is true when its "and"/"or" split it into two or more
// parts (spdx.OperandTexts) and at least one part of name length (at most
// fullTextMinLen bytes) is a license by the synonym rules; the other parts may
// be anything, including a long notice. For a list, one is the single license
// every part resolves to (a short name, or a long notice through the
// full-text fingerprint), and "" when the parts disagree or any part is free
// text.
//
// The class decision behind it (v0.29.67 review round 7, applied to
// worklist 58): never assert a license falsely; keep the text when unsure. A
// list whose parts all name the same license asserts nothing its parts'
// fingerprints do not already assert ("MPL-2.0 or <the MPL's own header
// notice>" is MPL-2.0). The fingerprint's own limits carry through: it drops
// "or later" and WITH exceptions from a notice (worklist 60), so a part it
// misreads agrees with a name it should not. One whose parts name
// different licenses, or include free text, keeps the text ("GPL-3.0-only AND
// <an Apache notice>" used to fingerprint to Apache-2.0 and drop the GPL
// term). A license text's short pieces never name a license on their own
// (TestNormalizeLicense_FullApache2Text / _FullISCText and the notice table
// hold that line), and version ranges and "with" never split (OperandTexts).
func nameListVerdict(s string) (one string, isList bool) {
	ops := spdx.OperandTexts(s)
	if len(ops) < 2 {
		return "", false
	}
	named, allResolved := false, true
	licenses := map[string]bool{}
	for _, op := range ops {
		t := normalizeLicenseTerm(op)
		if !spdx.Valid(t) && !spdx.FamilyLabels[t] {
			allResolved = false
			continue
		}
		licenses[t] = true
		if len(op) <= fullTextMinLen {
			named = true
		}
	}
	if !named {
		return "", false
	}
	if allResolved && len(licenses) == 1 {
		for l := range licenses {
			return l, true
		}
	}
	return "", true
}

// fullTextMinLen is the length above which a license string may be a full
// license TEXT rather than a name: normalizeLicenseTerm fingerprints such
// strings (detectFullLicenseText) and truncates the unrecognized ones, and
// NormalizeLicenseToSPDX reads them as an expression first. One constant, so
// the two cannot drift.
const fullTextMinLen = 80

// operatorBearingSynonyms are the synonym keys that contain an operator word,
// a slash or parentheses ("zlib/libpng", "common development and distribution
// license", "gnu library or lesser general public license (lgpl)"). The
// expression reader keeps each one whole wherever it appears (v0.29.67 review
// round 2), so a synonym is never split into the licenses its words spell.
var operatorBearingSynonyms = func() []string {
	var out []string
	for key := range licenseSynonyms {
		padded := " " + key + " "
		if strings.Contains(padded, " and ") || strings.Contains(padded, " or ") || strings.Contains(padded, " with ") ||
			strings.ContainsAny(key, "/()") {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}()

// normalizeLicenseTerm maps ONE license name to its canonical SPDX identifier.
// Returns "Unknown" for empty/sentinel values, the canonical form for known
// synonyms, or the trimmed input for unrecognized licenses.
//
// For long strings (>80 chars), it also detects full license texts — some Python
// packages embed the entire BSD/MIT/Apache license body in the "license" field.
// These are identified by content fingerprints (e.g., "Permission is hereby
// granted" = MIT, "Redistribution and use in source and binary forms" = BSD).
func normalizeLicenseTerm(license string) string {
	trimmed := strings.TrimSpace(license)

	// Check for "no license" sentinels first.
	upper := strings.ToUpper(trimmed)
	switch upper {
	case "", "NOASSERTION", "NONE", "N/A", "(NONE)", "UNKNOWN":
		return "Unknown"
	}

	// Case-insensitive lookup in the synonym map.
	lower := strings.ToLower(trimmed)
	if canonical, ok := licenseSynonyms[lower]; ok {
		return canonical
	}

	// For long strings, try to identify full license texts by content fingerprints.
	// Python packages (via PyPI/pip) frequently store the entire license body in
	// the "license" metadata field instead of a short SPDX identifier.
	if len(trimmed) > fullTextMinLen {
		// An SPDX-License-Identifier line states the license exactly
		// (worklist 60, review round 19; license_fulltext.go).
		if id := spdxIdentifierLine(trimmed); id != "" {
			return id
		}
		if id := detectFullLicenseText(lower); id != "" {
			return id
		}
		// Unrecognized long text — truncate to keep the UI usable.
		return truncateLicenseText(trimmed, 80)
	}

	// No match — return trimmed input unchanged.
	return trimmed
}

// detectFullLicenseText identifies full license text bodies by looking for
// distinctive phrases. Checked in priority order — first match wins.
// Only called for strings > 80 chars (short strings go through the synonym map).
func detectFullLicenseText(lower string) string {
	// Order matters: check most specific patterns first. BSD is checked before
	// Apache because Python packages often concatenate multiple license texts,
	// and BSD (the more specific match) should win when both appear.

	// MIT: "permission is hereby granted, free of charge" + "as is" warranty
	if strings.Contains(lower, "permission is hereby granted, free of charge") &&
		strings.Contains(lower, "the software is provided \"as is\"") {
		return "MIT"
	}
	// ISC: "permission to use, copy, modify, and/or distribute" (no redistribution clause)
	if strings.Contains(lower, "permission to use, copy, modify, and/or distribute") &&
		!strings.Contains(lower, "redistribution") {
		return "ISC"
	}
	// PSF: "python software foundation license" (check before BSD — PSF texts include BSD clauses)
	if strings.Contains(lower, "python software foundation license") {
		return "PSF-2.0"
	}
	// BSD 3-Clause: "redistribution and use" + "neither the name" (more specific than BSD-2)
	if strings.Contains(lower, "redistribution and use in source and binary forms") &&
		strings.Contains(lower, "neither the name") {
		return "BSD-3-Clause"
	}
	// BSD 2-Clause: "redistribution and use" without the third clause
	if strings.Contains(lower, "redistribution and use in source and binary forms") &&
		!strings.Contains(lower, "neither the name") {
		return "BSD-2-Clause"
	}
	// Apache 2.0 and the GPL: read by detectApacheText / detectGPLText
	// (license_fulltext.go, worklist 60). Only a definite answer ends the
	// search: a GPL notice may name Apache, and the MPL-2.0 body names the
	// GNU GPL in its "Secondary License" definition and is still MPL-2.0.
	// The gate only pre-filters; detectApacheText decides ("Apache 2.0
	// license" names it too, review round 19).
	if strings.Contains(lower, "apache") && strings.Contains(lower, "2.0") {
		if id := detectApacheText(lower); id != "" {
			return id
		}
	}
	if strings.Contains(lower, "gnu general public license") {
		if id := detectGPLText(lower); id != "" {
			return id
		}
	}
	// MPL 2.0: "mozilla public license" + "2.0"
	// The version must follow the name ("Mozilla Public License, v. 2.0", the
	// body's "Mozilla Public License Version 2.0"): "2.0" anywhere read the
	// MPL 1.1 tri-license block ("MPL 1.1/GPL 2.0/LGPL 2.1") as MPL-2.0 once
	// the GPL reader stopped answering it (review round 29).
	if mpl2Re.MatchString(lower) {
		return "MPL-2.0"
	}
	// Unlicense: "this is free and unencumbered software"
	if strings.Contains(lower, "this is free and unencumbered software") {
		return "Unlicense"
	}
	// CC0: "creative commons" + "cc0" or "public domain"
	if strings.Contains(lower, "creative commons") && (strings.Contains(lower, "cc0") || strings.Contains(lower, "public domain")) {
		return "CC0-1.0"
	}
	return ""
}

// mpl2Re is the MPL 2.0 named with its version.
// The words are matched on one line on purpose: round 30's \s+ between
// them extended this fingerprint's known false answers (an MPL name inside
// dual-license prose) to wrapped text (round 31); worklist 61 is the MPL
// notice reader that would read those exactly.
var mpl2Re = regexp.MustCompile(`mozilla public license,?\s+(?:v\.?\s*|version\s+)?2\.0`)

// truncateLicenseText truncates a long license string for display purposes.
// Tries to find the first recognizable license name in the first line, otherwise
// cuts at maxLen and appends an indicator.
func truncateLicenseText(s string, maxLen int) string {
	// Try to extract a meaningful first line (up to the first period or newline).
	firstLine := s
	for _, sep := range []string{"\n", ". ", " Copyright"} {
		if idx := strings.Index(s, sep); idx > 0 && idx < maxLen {
			firstLine = s[:idx]
			break
		}
	}
	if len(firstLine) <= maxLen {
		return firstLine
	}
	return s[:maxLen] + "..."
}

// licenseSynonyms maps lowercased license strings to canonical SPDX identifiers.
// Only clear, unambiguous synonyms are included.
var licenseSynonyms = func() map[string]string {
	m := map[string]string{}

	// Helper: add all synonyms for a canonical ID.
	add := func(canonical string, synonyms ...string) {
		for _, s := range synonyms {
			m[strings.ToLower(s)] = canonical
		}
		// Also add the canonical form itself (lowercased).
		m[strings.ToLower(canonical)] = canonical
	}

	// --- MIT ---
	add("MIT",
		"MIT", "MIT License", "The MIT License", "The MIT License (MIT)",
		"mit license", "Expat", "Expat License",
	)

	// --- Apache 2.0 ---
	add("Apache-2.0",
		"Apache-2.0", "Apache 2.0", "Apache License 2.0", "Apache License, Version 2.0",
		"Apache Software License 2.0", "Apache License v2.0", "Apache License Version 2.0",
		"Apache2", "Apache 2", "ASL 2.0", "Apache Software License",
		"Apache License", // bare "Apache License" almost always means 2.0
	)

	// --- BSD 3-Clause ---
	add("BSD-3-Clause",
		"BSD-3-Clause", "BSD 3-Clause", "BSD 3-Clause License", "BSD-3-Clause License",
		"3-Clause BSD License", "New BSD License", "Modified BSD License",
		"BSD 3 Clause", "BSD", // bare "BSD" is most commonly BSD-3-Clause
		"BSD License",
	)

	// --- BSD 2-Clause ---
	add("BSD-2-Clause",
		"BSD-2-Clause", "BSD 2-Clause", "BSD 2-Clause License", "BSD-2-Clause License",
		"Simplified BSD License", "FreeBSD License", "BSD 2 Clause",
	)

	// --- GPL 2.0 ---
	add("GPL-2.0-only",
		"GPL-2.0", "GPL-2.0-only", "GPLv2", "GNU General Public License v2.0",
		"GNU GPL v2", "GPL 2.0", "GPL2",
	)

	// --- GPL 3.0 ---
	add("GPL-3.0-only",
		"GPL-3.0", "GPL-3.0-only", "GPLv3", "GNU General Public License v3.0",
		"GNU GPL v3", "GPL 3.0", "GPL3",
	)

	// --- LGPL ---
	add("LGPL-2.1-only",
		"LGPL-2.1", "LGPL-2.1-only", "GNU Lesser General Public License v2.1", "LGPLv2.1",
	)
	add("LGPL-3.0-only",
		"LGPL-3.0", "LGPL-3.0-only", "GNU Lesser General Public License v3.0", "LGPLv3",
	)
	// v0.28.6 (Copilot round 2, revising v0.28.1): version-unspecified
	// LGPL normalizes to the bare "LGPL" FAMILY BUCKET — the same
	// treatment the classifier gives EPL/Artistic — NOT to
	// LGPL-2.0-or-later. The "-or-later" SPDX id specifically asserts
	// that recipients may choose later versions; version-unspecified
	// metadata may describe a version-only license, so mapping it to
	// -or-later invents a permission the source never granted. "LGPL"
	// is not an SPDX id (the groundtruth tripwire carries a reviewed
	// family-bucket exemption for it); explicit "or later" wordings
	// keep their exact -or-later ids via the classifier arms.
	add("LGPL",
		"GNU Library or Lesser General Public License (LGPL)",
		"GNU Lesser General Public License",
	)

	// --- AGPL ---
	add("AGPL-3.0-only",
		"AGPL-3.0", "AGPL-3.0-only", "GNU Affero General Public License v3.0", "AGPLv3",
	)

	// --- ISC ---
	add("ISC",
		"ISC", "ISC License", "ISC license",
	)

	// --- MPL ---
	add("MPL-2.0",
		"MPL-2.0", "MPL 2.0", "Mozilla Public License 2.0", "Mozilla Public License, Version 2.0",
	)

	// --- EPL ---
	add("EPL-1.0", "EPL-1.0", "Eclipse Public License 1.0")
	add("EPL-2.0", "EPL-2.0", "Eclipse Public License 2.0", "Eclipse Public License v2.0")

	// --- CDDL ---
	add("CDDL-1.0", "CDDL-1.0", "CDDL 1.0", "Common Development and Distribution License")

	// --- Artistic ---
	add("Artistic-2.0", "Artistic-2.0", "Artistic License 2.0", "Perl Artistic License 2.0")

	// --- Unlicense ---
	add("Unlicense", "Unlicense", "The Unlicense", "UNLICENSE", "Unlicence")

	// --- CC0 ---
	add("CC0-1.0",
		"CC0-1.0", "CC0 1.0", "CC0", "CC0 1.0 Universal",
		"Creative Commons Zero v1.0 Universal",
	)

	// --- 0BSD ---
	add("0BSD", "0BSD", "Zero-Clause BSD", "Free Public License 1.0.0")

	// --- Zlib ---
	// v0.29.67 (review round 1): every spelling of the one zlib/libpng
	// license is a synonym, so the expression reader never splits it into
	// the choice "Libpng OR Zlib".
	add("Zlib", "Zlib", "zlib License", "zlib/libpng License", "zlib/libpng", "libpng/zlib", "libpng/zlib License")

	// --- BSL ---
	add("BSL-1.0", "BSL-1.0", "Boost Software License 1.0", "BSL 1.0")

	// --- PostgreSQL ---
	add("PostgreSQL", "PostgreSQL", "PostgreSQL License")

	// --- Python ---
	add("PSF-2.0", "PSF-2.0", "Python Software Foundation License", "PSF", "PSFL")

	return m
}()
