// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — vuln_targets.go: pure per-dependency decisions
// for the v0.27.11 vulnerability-scan accuracy work. Applied at purl
// construction, in this order:
//
//  1. Self-dependency exclusion (isSelfDependency) — a publisher
//     monorepo's manifests declaring supported ranges of the repo's
//     OWN packages are support-matrix declarations, not exposure.
//     Excluded deps produce NO purls. Vulnerability scan ONLY —
//     libyear's handling of the same deps is completely untouched.
//  2. Version selection (vulnScanTargets) — lockfile-resolved versions
//     win ('locked', one purl per distinct locked version); Go is
//     'locked' by construction (go.mod is exact under MVS); otherwise
//     the purl stays at the manifest floor and the requirement string
//     classifies it (classifyRequirement in depclass.go).
package collector

// sliceContains reports whether list contains s (tiny slices only —
// locked-version lists and log samples).
func sliceContains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
