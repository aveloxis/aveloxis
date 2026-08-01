// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — version_normalize.go: the central version-
// hygiene choke point (v0.27.71).
//
// Background (the 2026-08-01 zephyr incident): pip's hash-pinned
// requirements format ends pin lines with a backslash continuation
// ("pyyaml==6.0.3 \"). The parser kept the backslash, the purl became
// pkg:pypi/pyyaml@6.0.3 \, and OSV — unable to parse the version —
// degraded to package-level matching and returned the package's
// ENTIRE advisory history. zephyr rendered "6.0.3, fixed in 5.4"
// CRITICALs. A fleet survey found the same class in five more shapes
// across ecosystems (cargo inline tables, maven/msbuild property
// interpolation, Gemfile/mix option capture, pub block sub-keys, npm
// workspace protocols).
//
// The manifest parsers were fixed at the source, but 14 ecosystems ×
// N syntax corners is exactly the surface where slop recurs — so this
// normalizer runs over EVERY parsed dep in the analysis walk, after
// parsing and before resolution/storage/purl construction. A version
// that can't survive normalization becomes "" — the established
// 'unpinned' pathway (honest classification, versionless purl) —
// instead of a garbage string that silently defeats OSV version
// matching AND the registry libyear lookups.
//
// The Requirement field is never touched: it stays the raw manifest
// truth (the v0.27.11 display philosophy). Only Version — our
// RESOLUTION of the requirement — is normalized.
package collector

import "strings"

// nonVersionProtocols are value prefixes that mark a dependency
// source, not a registry version: npm/pnpm monorepo protocols
// (workspace:, catalog:, file:, link:, portal:, npm: aliases) and
// the git/path/url forms that leak out of option-style manifests.
var nonVersionProtocols = []string{
	"workspace:", "catalog:", "file:", "link:", "portal:", "npm:",
	"git:", "github:", "path:", "http://", "https://",
}

// normalizeParsedVersion sanitizes one parser-produced version string.
// Returns the cleaned version, or "" when the input carries no usable
// registry version (property interpolation, monorepo protocols,
// wildcards, structural garbage).
//
// githubactions is exempt wholesale: its "versions" are git refs
// ("releases/v1", 40-hex SHAs) evaluated client-side against advisory
// ranges (actionRefAffected, v0.27.47) — ref syntax must pass through
// verbatim.
func normalizeParsedVersion(manager, version string) string {
	if manager == "githubactions" {
		return version
	}
	v := strings.TrimSpace(version)
	// Line-continuation backslash (the zephyr shape). Belt — the
	// requirements.txt parser also strips it — but this layer is what
	// guarantees no OTHER parser can ever leak one again.
	v = strings.TrimSpace(strings.TrimSuffix(v, "\\"))
	v = strings.Trim(v, "\"'")
	if v == "" {
		return ""
	}
	// Property interpolation (maven ${spring.version}, msbuild
	// $(MauiVersion)), template placeholders ({{latest-pypi-version}}),
	// and inline-table fragments ({version = "*") all carry braces or
	// parens — never valid in any registry's version grammar.
	if strings.ContainsAny(v, "{}()") {
		return ""
	}
	lower := strings.ToLower(v)
	for _, proto := range nonVersionProtocols {
		if strings.HasPrefix(lower, proto) {
			return ""
		}
	}
	// Packagist stability suffix: "2.1.0@beta" → "2.1.0"; a bare
	// "@dev" collapses to "".
	if at := strings.LastIndex(v, "@"); at >= 0 {
		switch suffix := strings.ToLower(v[at+1:]); {
		case suffix == "dev" || suffix == "stable" || suffix == "beta" || suffix == "alpha" || strings.HasPrefix(suffix, "rc"):
			v = v[:at]
		}
	}
	// Operator strip + compound-range floor (commas, pipes, spaces).
	v = cleanVersion(v)
	if v == "" || strings.Contains(v, "*") {
		// Wildcards mean "any version" — that's unpinned, not a pin.
		return ""
	}
	if !versionGateOK(v) {
		return ""
	}
	return v
}

// versionGateOK is the final validity gate: a usable registry version
// starts with an alphanumeric, contains at least one digit, and uses
// only the characters real version grammars use (semver, PEP 440
// epochs, maven qualifiers, Go pseudo-versions). Everything else —
// quotes, colons, slashes, equals, brackets — is structural garbage
// from a manifest corner some parser mis-captured.
func versionGateOK(v string) bool {
	hasDigit := false
	for i, r := range v {
		switch {
		case r >= '0' && r <= '9':
			hasDigit = true
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case r == '.' || r == '+' || r == '_' || r == '~' || r == '!' || r == '-':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return hasDigit
}
