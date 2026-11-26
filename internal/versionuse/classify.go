// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package versionuse

import "strings"

// classifyActionRef maps a pinned ref onto the version_resolution
// vocabulary: full commit SHAs are 'locked' (immutable), dotted
// version tags are 'exact', and floating refs (major-only tags,
// branch names) are 'unpinned'.
func ClassifyActionRef(ref string) string {
	if IsHexSHA(ref) {
		return Locked
	}
	if strings.Contains(ref, ".") {
		return Exact
	}
	return Unpinned
}

// isHexSHA reports whether ref looks like a git commit SHA
// (7–40 lowercase hex chars — the abbreviated-through-full range).
func IsHexSHA(ref string) bool {
	if len(ref) < 7 || len(ref) > 40 {
		return false
	}
	for _, c := range ref {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Version-resolution classes, in the precedence order applied at purl
// construction. resolutionLocked is assigned by the scan itself (a
// lockfile resolved the package, or the ecosystem is Go — go.mod
// versions are exact under MVS, locked by construction); the pure
// classifier below hands out the other four from the raw requirement
// string.
const (
	Locked       = "locked"
	Exact        = "exact"
	BoundedRange = "bounded-range"
	RangeFloor   = "range-floor"
	Unpinned     = "unpinned"
	// Managed (0.29.178): the declaration gives no version and Maven's
	// dependencyManagement does (the same pom, a parent, an imported BOM).
	// Not one of the classifier's: Of hands it out from the row's source.
	Managed = "managed"
)

// ClassifyRequirement maps a raw manifest requirement string (plus the
// floor version the parser extracted) to a resolution class:
//
//	exact         — ==X, =X, or a bare version: the declaration names
//	                one version and the scan used it.
//	bounded-range — the requirement has an upper bound (~= PEP 440
//	                compatible release, ^ caret, ~ tilde / ~> ruby
//	                pessimistic, npm x-ranges / hyphen ranges /
//	                wildcard pins, or a compound containing < / <=).
//	                The purl stays at the floor.
//	range-floor   — lower bound only (>=, >). The purl stays at the
//	                floor: the worst case the declaration permits.
//	unpinned      — no version at all. Such a dependency keeps a
//	                versionless purl, OSV answers with every advisory
//	                its package has had, and those findings are
//	                "version not determined", not exposure. Of decides
//	                this from the version alone, before this function
//	                is asked.
//
// Requirement strings vary wildly by ecosystem — some parsers record
// just the spec ("^4.17.1"), others the whole manifest line
// ("gem 'rails', '~> 5.0'") — so classification is by operator
// presence in the string, which survives both shapes.
func ClassifyRequirement(requirement, version string) string {
	req := strings.TrimSpace(requirement)

	// No version material anywhere: nothing was pinned or bounded.
	if !containsDigit(req) && !containsDigit(version) {
		return Unpinned
	}

	// == pins name exactly one version (spec precedence: exact beats
	// bounded-range when both markers appear, e.g. "==1.4,<2.0" — the
	// pin makes any co-declared bound redundant). Wildcard "pins"
	// (==1.*) admit a range and fall through to bounded-range below.
	if strings.Contains(req, "==") && !strings.Contains(req, "*") {
		return Exact
	}

	// Any upper-bound marker → bounded-range.
	if strings.ContainsAny(req, "<~^") || // <, <= compounds; ~=, ~, ~>; ^
		strings.Contains(req, " - ") || // npm hyphen range "1.2.3 - 2.3.4"
		strings.Contains(req, ".x") || // npm x-range "1.2.x"
		strings.Contains(req, ".*") { // pypi wildcard "==1.*"
		return BoundedRange
	}

	// Lower bound only.
	if strings.Contains(req, ">") {
		return RangeFloor
	}

	// Bare version (or single "=" pin).
	return Exact
}

func containsDigit(s string) bool {
	for _, r := range s {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}
