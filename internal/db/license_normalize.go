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

import "strings"

// NormalizeLicenseToSPDX maps a license string to its canonical SPDX identifier.
// Returns "Unknown" for empty/sentinel values, the canonical form for known
// synonyms, or the trimmed input for unrecognized licenses.
func NormalizeLicenseToSPDX(license string) string {
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

	// No match — return trimmed input unchanged.
	return trimmed
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
	add("Zlib", "Zlib", "zlib License", "zlib/libpng License")

	// --- BSL ---
	add("BSL-1.0", "BSL-1.0", "Boost Software License 1.0", "BSL 1.0")

	// --- PostgreSQL ---
	add("PostgreSQL", "PostgreSQL", "PostgreSQL License")

	// --- Python ---
	add("PSF-2.0", "PSF-2.0", "Python Software Foundation License", "PSF", "PSFL")

	return m
}()
