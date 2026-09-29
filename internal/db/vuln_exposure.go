// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// exposurePredicateSQL (and its "v."-aliased twin) is the ONE definition of "this finding is dependency
// exposure" for every count and supply-chain read (SR-17), appended to a
// WHERE clause over repo_deps_vulnerabilities. Two
// classes are not exposure:
//
//   - kind 'self' — advisories against the repository's OWN published
//     releases, versionless (v0.27.29);
//   - version_resolution 'unpinned' — the dependency declares no version,
//     so OSV was queried without one and answered with every advisory ever
//     published for the package (v0.29.70). Whether the repository is
//     affected is unknown: the VEX under_investigation class. GitHub's
//     dependency graph and Trivy's default mode report nothing here.
//     Except GitHub Actions (ecosystem 'githubactions', review round 6):
//     a floating ref (@v4, @main) is stored 'unpinned' too, but its
//     advisories were MATCHED against the ref (actionRefAffected) — they
//     are exposure.
//
// Range floors ('range-floor', 'bounded-range') stay: the lowest version the
// declaration allows is affected, a conservative lower bound the page labels.
const (
	exposurePredicateSQL  = ` AND COALESCE(dependency_kind, '') <> 'self' AND NOT (COALESCE(version_resolution, '') = 'unpinned' AND COALESCE(ecosystem, '') <> 'githubactions')`
	exposurePredicateSQLv = ` AND COALESCE(v.dependency_kind, '') <> 'self' AND NOT (COALESCE(v.version_resolution, '') = 'unpinned' AND COALESCE(v.ecosystem, '') <> 'githubactions')`
)

// criticalSQL is the one "critical" rule the counts share.
const criticalSQL = `(severity = 'CRITICAL' OR cvss_score >= 9.0)`

// VulnClassCounts are a repository's CURRENT findings by class.
type VulnClassCounts struct {
	Exposure, Critical                     int // exposurePredicate
	UnknownVersion, UnknownVersionCritical int // unpinned dependency findings
}

// CountRepoVulnerabilityClasses counts a repository's current findings:
// exposure (with its critical subset) and, apart, the unknown-version
// advisories of unpinned dependencies. One statement.
func (s *PostgresStore) CountRepoVulnerabilityClasses(ctx context.Context, repoID int64) (VulnClassCounts, error) {
	var c VulnClassCounts
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE TRUE`+exposurePredicateSQL+`),
		       COUNT(*) FILTER (WHERE `+criticalSQL+exposurePredicateSQL+`),
		       COUNT(*) FILTER (WHERE `+unknownVersionSQL+`),
		       COUNT(*) FILTER (WHERE `+criticalSQL+` AND `+unknownVersionSQL+`)
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND resolved_at IS NULL`, repoID).
		Scan(&c.Exposure, &c.Critical, &c.UnknownVersion, &c.UnknownVersionCritical)
	if err != nil {
		return VulnClassCounts{}, fmt.Errorf("counting vulnerabilities: %w", err)
	}
	return c, nil
}

// unknownVersionSQL selects the unpinned dependency findings (not 'self').
const unknownVersionSQL = `COALESCE(version_resolution, '') = 'unpinned' AND COALESCE(ecosystem, '') <> 'githubactions' AND COALESCE(dependency_kind, '') <> 'self'`

// UnknownVersionDetail is the one sentence every surface uses for an
// unpinned dependency's advisories (the SBOMs' in_triage detail and
// comment; the GUI says the same in its own words).
const UnknownVersionDetail = "The dependency declares no version, so OSV was queried without one and returned every advisory published for the package; whether the version in use is affected is unknown."

// IsUnknownVersionFinding is exposurePredicateSQL's unpinned class, in Go:
// a dependency finding (not 'self') whose dependency declares no version —
// not a floating GitHub Actions ref, whose advisories were matched against
// the ref.
func IsUnknownVersionFinding(v *VulnerabilityRow) bool {
	return v.DependencyKind != "self" && v.VersionResolution == "unpinned" && v.Ecosystem != "githubactions"
}

// IsExposureFinding is exposurePredicateSQL in Go, for current rows:
// neither a 'self' advisory nor an unknown-version one.
func IsExposureFinding(v *VulnerabilityRow) bool {
	return v.ResolvedAt == nil && v.DependencyKind != "self" && !IsUnknownVersionFinding(v)
}
