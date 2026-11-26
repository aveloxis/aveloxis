// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package vulnclass is the one place that says what kind of thing a stored
// vulnerability finding is: whether it is still reported, whether it is about
// a dependency or about the repository's own package, and whether Aveloxis
// knows the version the advisory was matched against.
//
// Every surface that counts or labels findings asks here (the API's counts,
// both SBOM annotators), so that a repository never reads one way on its page
// and another in its export. The same rule exists once more in SQL, for the
// counts the database computes (internal/db/vuln_exposure.go); a
// database-tier test there holds the two equal over every combination of the
// columns the rule reads.
//
// It imports internal/model and nothing else of Aveloxis (summary/70,
// section 3).
package vulnclass

import "github.com/aveloxis/aveloxis/internal/model"

// Class is the one class every finding has.
type Class string

// The classes, in the order the rule tries them: the first that applies wins.
const (
	// Closed: no longer reported. Not live.
	Closed Class = "closed"
	// Self: an advisory against a package the repository itself publishes.
	// Listed apart from the repository's dependency exposure.
	Self Class = "self"
	// VersionNotDetermined: the dependency declares no version, so OSV was
	// asked without one and answered with every advisory ever published for
	// the package. Whether the version in use is affected is not known. Live,
	// and counted apart from exposure.
	VersionNotDetermined Class = "version_not_determined"
	// Exposure: affected at a version in use, at the lowest version a
	// declared range allows, or at a GitHub Actions ref matched as such.
	Exposure Class = "exposure"
)

// The stored values the rule reads. They are written by the scan
// (dependency_kind, version_resolution) and by OSV's vocabulary (ecosystem).
const (
	resolutionUnpinned  = "unpinned"
	ecosystemGitHubActs = "githubactions"
	severityCritical    = "CRITICAL"
	criticalCVSSFloor   = 9.0
)

// Of is the class of a finding.
//
// A GitHub Actions finding is never VersionNotDetermined: a floating ref
// (@v4, @main) is stored 'unpinned' as well, but its advisories were matched
// against that ref, so they are exposure.
func Of(v *model.VulnerabilityRow) Class {
	switch {
	case v.ResolvedAt != nil:
		return Closed
	case v.DependencyKind == model.DependencyKindSelf:
		return Self
	case hasNoDeterminedVersion(v):
		return VersionNotDetermined
	default:
		return Exposure
	}
}

// hasNoDeterminedVersion is the version half of the rule, for a finding
// already known to be about a dependency.
func hasNoDeterminedVersion(v *model.VulnerabilityRow) bool {
	return v.VersionResolution == resolutionUnpinned && v.Ecosystem != ecosystemGitHubActs
}

// IsVersionNotDetermined reports whether a finding's dependency declares no
// version, whether or not the finding is still reported: it is the class a
// CLOSED finding had while it was live. It is false for a finding about the
// repository's own package and for a GitHub Actions ref.
func IsVersionNotDetermined(v *model.VulnerabilityRow) bool {
	return v.DependencyKind != model.DependencyKindSelf && hasNoDeterminedVersion(v)
}

// IsCritical is the one "critical" rule the counts share: the advisory's
// severity label says so, or its CVSS score is 9.0 or higher.
func IsCritical(v *model.VulnerabilityRow) bool {
	return v.Severity == severityCritical || v.CVSSScore >= criticalCVSSFloor
}

// VersionNotDeterminedDetail is the one sentence every surface uses for a
// finding of class VersionNotDetermined (the CycloneDX analysis detail, the
// SPDX reference comment; the GUIs say the same in their own words).
const VersionNotDeterminedDetail = "The dependency declares no version, so OSV was queried without one and returned every advisory published for the package; whether the version in use is affected is unknown."
