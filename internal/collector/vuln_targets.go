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

import (
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
)

// vulnScanTarget is one purl to scan, with the classification the
// finding rows will carry.
type vulnScanTarget struct {
	Purl        string
	Dep         db.VulnScanDep
	Requirement string
	Resolution  string
}

// isSelfDependency reports whether a declared dependency names one of
// the repo's own packages (the self-set from
// db.GetRepoSelfPackageNames — lowercased, exact matches only). The
// dep-name side is also checked with '_'→'-' folded, mirroring the
// self-set's own name-heuristic variants.
func isSelfDependency(name string, selfSet map[string]bool) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return selfSet[n] || selfSet[strings.ReplaceAll(n, "_", "-")]
}

// vulnScanTargets computes the purl(s) to scan for one dependency.
// locked maps lockfileMatchKey(ecosystem, name) → distinct resolved
// versions from the repo's committed lockfiles.
//
// Precedence (the v0.27.11 class order):
//   - Go dependency          → 'locked' at the go.mod version (exact
//     under MVS — no lockfile needed).
//   - lockfile resolution(s) → 'locked', one purl per distinct
//     resolved version (two apps in a monorepo may legitimately pin
//     two versions).
//   - otherwise              → the stored floor purl, classified from
//     the raw requirement string.
func vulnScanTargets(dep db.VulnScanDep, locked map[string][]string) []vulnScanTarget {
	if dep.Purl == "" {
		return nil
	}
	if dep.PackageManager == "go" {
		return []vulnScanTarget{{
			Purl: dep.Purl, Dep: dep,
			Requirement: dep.Requirement,
			Resolution:  resolutionLocked,
		}}
	}
	if versions := locked[lockfileMatchKey(dep.PackageManager, dep.Name)]; len(versions) > 0 {
		targets := make([]vulnScanTarget, 0, len(versions))
		for _, v := range versions {
			targets = append(targets, vulnScanTarget{
				Purl: purlWithVersion(dep.Purl, v), Dep: dep,
				Requirement: dep.Requirement,
				Resolution:  resolutionLocked,
			})
		}
		return targets
	}
	return []vulnScanTarget{{
		Purl: dep.Purl, Dep: dep,
		Requirement: dep.Requirement,
		Resolution:  classifyRequirement(dep.Requirement, dep.CurrentVersion),
	}}
}

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

// purlWithVersion swaps the version suffix of a purl
// ("pkg:npm/express@4.18.0" → "pkg:npm/express@4.19.2"). The version
// separator is the LAST '@' — npm scoped names ("pkg:npm/@scope/name@1.0")
// carry an earlier '@' that must not be touched; an '@' followed by a
// '/' is a scope marker, not a version separator.
func purlWithVersion(purl, version string) string {
	if at := strings.LastIndex(purl, "@"); at > 0 && !strings.Contains(purl[at:], "/") {
		return purl[:at+1] + version
	}
	return purl + "@" + version
}
