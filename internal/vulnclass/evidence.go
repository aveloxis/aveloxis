// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package vulnclass

import "github.com/aveloxis/aveloxis/internal/model"

// The evidence grade of an exposure finding (v0.29.196; summary/70, 8.2):
// how sure the version the advisory was matched against is. It is shown
// beside a finding and is not part of its class.
const (
	// EvidenceInUse: the version in use, from a lockfile, an exact
	// declaration or the build's own management.
	EvidenceInUse = "in_use"
	// EvidenceLowestAllowed: the lowest version a declared range allows.
	// The version in use may be a later one the advisory does not affect.
	EvidenceLowestAllowed = "lowest_allowed"
	// EvidenceFloatingRef: a GitHub Actions ref that moves (@v4, @main),
	// matched as written.
	EvidenceFloatingRef = "floating_ref"
	// EvidenceNotClassified: a row written before the resolution was
	// stored, or a resolution this release does not know.
	EvidenceNotClassified = "not_classified"
)

// The stored resolutions the grade reads (written by the scan).
const (
	resolutionLocked       = "locked"
	resolutionExact        = "exact"
	resolutionManaged      = "managed"
	resolutionRangeFloor   = "range-floor"
	resolutionBoundedRange = "bounded-range"
)

// Evidence is the grade of a finding, and "" for a finding that is not
// exposure: only exposure is a statement about a version.
func Evidence(v *model.VulnerabilityRow) string {
	if Of(v) != Exposure {
		return ""
	}
	switch v.VersionResolution {
	case resolutionLocked, resolutionExact, resolutionManaged:
		return EvidenceInUse
	case resolutionRangeFloor, resolutionBoundedRange:
		return EvidenceLowestAllowed
	case resolutionUnpinned:
		// Exposure and unpinned: a GitHub Actions ref (Of's exception).
		return EvidenceFloatingRef
	}
	return EvidenceNotClassified
}

// ClosedReasons are the reasons a finding closes that are not a fix, in the
// order the API lists their counts.
var ClosedReasons = []string{
	model.ClosedSuperseded, model.ClosedVersionDetermined, model.ClosedVersionLost,
	model.ClosedVersionChanged, model.ClosedNeverApplied, model.ClosedUnsendable,
}

// closedReasonWordings is the one wording of each reason, on every surface.
// None says "resolved": none of these is a fix.
var closedReasonWordings = map[string]string{
	model.ClosedSuperseded:        "now tracked at a known version",
	model.ClosedVersionDetermined: "does not affect the version in use",
	model.ClosedVersionLost:       "version no longer known",
	model.ClosedVersionChanged:    "now tracked at another version",
	model.ClosedNeverApplied:      "never applied",
	model.ClosedUnsendable:        "closed: its package name could not be sent",
}

// closedReasonUnknownWording is shown for a reason this release does not
// know (a later release wrote it). It must never read as a fix.
const closedReasonUnknownWording = "closed for a reason this version does not name"

// ClosedReasonWording is the wording of a closing reason; "" for no reason
// (a closure with no reason is a fix or a removed dependency, which each
// surface words as resolved).
//
// No surface in this repository words a closed finding today (the built-in
// pages list current ones; the API sends the reason's identifier and
// aveloxis-gui words it from a copy of the map above, held equal by its
// tests). This function is the rule that copy follows, the fallback for an
// unknown reason included, and is kept as its one statement in Go.
func ClosedReasonWording(reason string) string {
	if reason == "" {
		return ""
	}
	if w, ok := closedReasonWordings[reason]; ok {
		return w
	}
	return closedReasonUnknownWording
}

// ResolvedUnlabelledKey counts the closed findings with no reason: the ones
// that are resolved in the ordinary sense.
const ResolvedUnlabelledKey = "resolved_unlabelled"

// ResolvedOtherReasonKey counts the closed findings whose reason this
// release does not know (a later release wrote it). Never a fix. It was
// resolved_unlabelled_other until 0.29.208, before any host ran it: that
// name invited a client to add everything named resolved_unlabelled* into
// its count of fixes.
const ResolvedOtherReasonKey = "resolved_other_reason"

// ResolvedCountKey is the counts key a closed finding is counted under:
// resolved_<reason>, resolved_unlabelled for no reason, and
// resolved_other_reason for a reason this release does not know, so that
// the keys always sum to "resolved" and an unknown reason is never counted
// as a fix.
func ResolvedCountKey(reason string) string {
	if reason == "" {
		return ResolvedUnlabelledKey
	}
	if _, ok := closedReasonWordings[reason]; ok {
		return "resolved_" + reason
	}
	return ResolvedOtherReasonKey
}
