// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — depclass.go classifies HOW a dependency's
// scanned version relates to what the manifest actually declared
// (v0.27.11 vulnerability-accuracy work).
//
// The operator explicitly REJECTED resolving `>=X` requirements to
// latest-satisfying: actual resolution depends on the full dependency
// graph (co-dependencies can pin the resolver anywhere in the admitted
// range), so the floor — the worst case the declaration permits —
// stays the scanned version. What changed in v0.27.11 is honesty:
// every finding carries the raw requirement and one of these classes
// so the UI can say "≥2.20 declared — floor shown".
package collector

import "strings"

// Version-resolution classes, in the precedence order applied at purl
// construction. resolutionLocked is assigned by the scan itself (a
// lockfile resolved the package, or the ecosystem is Go — go.mod
// versions are exact under MVS, locked by construction); the pure
// classifier below hands out the other four from the raw requirement
// string.
const (
	resolutionLocked       = "locked"
	resolutionExact        = "exact"
	resolutionBoundedRange = "bounded-range"
	resolutionRangeFloor   = "range-floor"
	resolutionUnpinned     = "unpinned"
)

// classifyRequirement maps a raw manifest requirement string (plus the
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
//	unpinned      — no version at all. These produce no purls (and so
//	                no findings) today; the class exists so the
//	                classifier handles the input honestly instead of
//	                panicking or mislabeling.
//
// Requirement strings vary wildly by ecosystem — some parsers record
// just the spec ("^4.17.1"), others the whole manifest line
// ("gem 'rails', '~> 5.0'") — so classification is by operator
// presence in the string, which survives both shapes.
func classifyRequirement(requirement, version string) string {
	req := strings.TrimSpace(requirement)

	// No version material anywhere: nothing was pinned or bounded.
	if !containsDigit(req) && !containsDigit(version) {
		return resolutionUnpinned
	}

	// == pins name exactly one version (spec precedence: exact beats
	// bounded-range when both markers appear, e.g. "==1.4,<2.0" — the
	// pin makes any co-declared bound redundant). Wildcard "pins"
	// (==1.*) admit a range and fall through to bounded-range below.
	if strings.Contains(req, "==") && !strings.Contains(req, "*") {
		return resolutionExact
	}

	// Any upper-bound marker → bounded-range.
	if strings.ContainsAny(req, "<~^") || // <, <= compounds; ~=, ~, ~>; ^
		strings.Contains(req, " - ") || // npm hyphen range "1.2.3 - 2.3.4"
		strings.Contains(req, ".x") || // npm x-range "1.2.x"
		strings.Contains(req, ".*") { // pypi wildcard "==1.*"
		return resolutionBoundedRange
	}

	// Lower bound only.
	if strings.Contains(req, ">") {
		return resolutionRangeFloor
	}

	// Bare version (or single "=" pin).
	return resolutionExact
}

func containsDigit(s string) bool {
	for _, r := range s {
		if r >= '0' && r <= '9' {
			return true
		}
	}
	return false
}
