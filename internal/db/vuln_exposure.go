// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// exposurePredicateSQL (and its "v."-aliased twin) is the ONE SQL definition of "this finding is dependency
// exposure" for every count and supply-chain read (SR-17), appended to a
// WHERE clause over repo_deps_vulnerabilities. Its Go form is
// internal/vulnclass (Of), and TestVulnClassAgreesWithTheSQLPredicates holds
// the two equal over every combination of the columns they read. Two
// classes are not exposure:
//
//   - kind 'self' — advisories against the repository's OWN published
//     releases, versionless (v0.27.29);
//   - version_resolution 'unpinned' — the dependency declares no version,
//     so OSV was queried without one and answered with every advisory ever
//     published for the package (v0.29.70). The version in use is not
//     determined, so whether it is affected is not known. GitHub's
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

// exposureExprSQL is the predicate itself, over the three operands it reads.
// The two constants above stay constants (other constants are built from
// them); TestExposurePredicateHasOneSpelling holds them equal to this.
// The upsert builds the class a row HAD and the class it is ABOUT TO HAVE
// from it (entered_exposure_at, 0.29.166), so those cannot drift from the
// reads.
func exposureExprSQL(kind, resolution, ecosystem string) string {
	return `COALESCE(` + kind + `, '') <> 'self' AND NOT (COALESCE(` + resolution + `, '') = 'unpinned' AND COALESCE(` + ecosystem + `, '') <> 'githubactions')`
}

// criticalSQL is the one "critical" rule the counts share
// (vulnclass.IsCritical, in SQL).
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

// unknownVersionSQL selects the unpinned dependency findings (not 'self'):
// vulnclass.IsVersionNotDetermined, in SQL.
const unknownVersionSQL = `COALESCE(version_resolution, '') = 'unpinned' AND COALESCE(ecosystem, '') <> 'githubactions' AND COALESCE(dependency_kind, '') <> 'self'`
