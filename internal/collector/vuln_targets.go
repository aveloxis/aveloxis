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
	"github.com/aveloxis/aveloxis/internal/model"
)

// vulnScanTarget is one purl to scan, with the classification the
// finding rows will carry.
type vulnScanTarget struct {
	Purl        string
	Dep         db.VulnScanDep
	Requirement string
	Resolution  string
	// Kind (v0.27.21 C1) is 'direct' or 'transitive'; Scope is
	// 'dev' / 'runtime' / '' (unknown) carried from the lockfile
	// row for transitive targets.
	Kind  string
	Scope string
	// OSVQueryName/OSVQueryEcosystem (v0.27.47, summary/19 P4):
	// when set, the batch query for this target uses the
	// {name, ecosystem} form instead of the purl — OSV's "GitHub
	// Actions" advisories carry no purl, and its API has no version
	// comparator for the ecosystem, so the query is VERSIONLESS and
	// the pinned ref is evaluated client-side (actionRefAffected).
	OSVQueryName      string
	OSVQueryEcosystem string
}

// dependency_kind values carried onto findings (v0.27.21 C1;
// 'self' added v0.27.29 — advisories against the repo's OWN
// published packages, version-unconstrained).
const (
	dependencyKindDirect     = "direct"
	dependencyKindTransitive = "transitive"
	dependencyKindSelf       = "self"
)

// selfAdvisoryPurlTypes maps the DISTRIBUTION subsystem's ecosystem
// strings (deps.dev/ecosyste.ms/manifest flavors — a different
// vocabulary than the libyear package managers in purlEcosystemTypes)
// to purl types. Unmapped ecosystems (conda, cran, julia, cpp…) are
// honestly omitted, same posture as purlForPackage.
var selfAdvisoryPurlTypes = map[string]string{
	"npm":      "npm",
	"pypi":     "pypi",
	"go":       "golang",
	"cargo":    "cargo",
	"rubygems": "gem",
	"gem":      "gem",
	"maven":    "maven",
	"composer": "composer",
	"elixir":   "hex",
	"hex":      "hex",
	"nuget":    "nuget",
	"dart":     "pub",
	"pub":      "pub",
	"swift":    "swift",
	"haskell":  "hackage",
}

// selfAdvisoryPurl builds a VERSIONLESS purl for self-advisory
// scanning — OSV returns every advisory for the package (verified
// live 2026-07-21: pkg:pypi/numpy → 16 stubs from querybatch).
func selfAdvisoryPurl(ecosystem, name string) string {
	typ, ok := selfAdvisoryPurlTypes[strings.ToLower(ecosystem)]
	if !ok || name == "" {
		return ""
	}
	if typ == "maven" {
		name = strings.Replace(name, ":", "/", 1)
	}
	return buildPurl(typ, name, "") // v0.27.29: spec-canonical, versionless
}

// purlEcosystemTypes maps our package-manager strings to purl types
// for TRANSITIVE lockfile targets (direct deps carry purls built by
// the libyear writers; this mapping deliberately mirrors those
// writers' formats so cross-kind dedup works). maven names arrive as
// "group:artifact" and become pkg:maven/group/artifact@v.
var purlEcosystemTypes = map[string]string{
	"npm":      "npm",
	"pypi":     "pypi",
	"go":       "golang",
	"cargo":    "cargo",
	"gem":      "gem",
	"maven":    "maven",
	"composer": "composer",
	"hex":      "hex",
	"nuget":    "nuget",
	"pub":      "pub",
	"swift":    "swift",
	"haskell":  "hackage",
	// v0.27.133 (C2 exploration find): the LOCKFILE roster emits these
	// ecosystem strings — without the aliases, every rubygems/packagist/
	// swiftpm/hackage transitive produced "" and was SILENTLY dropped
	// from the vuln scan.
	"rubygems":  "gem",
	"packagist": "composer",
	"swiftpm":   "swift",
	"hackage":   "hackage",
}

// purlForPackage builds a purl for a transitive lockfile resolution.
// Returns "" for unmapped ecosystems (the target is skipped — honest
// omission beats a malformed purl OSV can't match).
func purlForPackage(ecosystem, name, version string) string {
	typ, ok := purlEcosystemTypes[ecosystem]
	if !ok || name == "" || version == "" {
		return ""
	}
	if typ == "maven" {
		name = strings.Replace(name, ":", "/", 1)
	}
	return buildPurl(typ, name, version) // v0.27.29: spec-canonical
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
	// v0.27.46 (summary/19 P3): direct targets carry the dep's scope
	// so findings' dependency_scope works for direct deps too.
	// StoredScope keeps the '' = runtime column convention.
	scope := model.StoredScope(dep.Type)
	if dep.PackageManager == "githubactions" {
		// v0.27.47 (summary/19 P4): versionless {name, ecosystem}
		// query + client-side ref evaluation. Resolution classifies
		// the PIN (SHA=locked, dotted tag=exact, floating=unpinned).
		return []vulnScanTarget{{
			Purl: dep.Purl, Dep: dep,
			Requirement:       dep.Requirement,
			Resolution:        classifyActionRef(dep.CurrentVersion),
			Kind:              dependencyKindDirect,
			Scope:             scope,
			OSVQueryName:      dep.Name,
			OSVQueryEcosystem: "GitHub Actions",
		}}
	}
	if dep.PackageManager == "go" {
		return []vulnScanTarget{{
			Purl: dep.Purl, Dep: dep,
			Requirement: dep.Requirement,
			Resolution:  resolutionLocked,
			Kind:        dependencyKindDirect,
			Scope:       scope,
		}}
	}
	if versions := locked[lockfileMatchKey(dep.PackageManager, dep.Name)]; len(versions) > 0 {
		targets := make([]vulnScanTarget, 0, len(versions))
		for _, v := range versions {
			targets = append(targets, vulnScanTarget{
				Purl: purlWithVersion(dep.Purl, v), Dep: dep,
				Requirement: dep.Requirement,
				Resolution:  resolutionLocked,
				Kind:        dependencyKindDirect,
				Scope:       scope,
			})
		}
		return targets
	}
	return []vulnScanTarget{{
		Purl: dep.Purl, Dep: dep,
		Requirement: dep.Requirement,
		Resolution:  classifyRequirement(dep.Requirement, dep.CurrentVersion),
		Kind:        dependencyKindDirect,
		Scope:       scope,
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

// wireValidPurl (v0.27.73) is the last-line syntactic gate before a
// purl goes to OSV. The 2026-08-01 heal run failed 81 repos with 400s
// (`invalid URL escape "%i["`, `"purl is missing name"`) because
// legacy dep rows carried RAW pre-v0.27.29 purls — built by string
// concatenation before the canonical escaper — and ONE malformed
// query 400s the repo's ENTIRE batch. OSV's error names only "query
// at index N", so the offending purl is invisible without production
// forensics. Anything failing this gate is dropped and NAMED in the
// log instead of sinking the repo.
//
// Rules (syntactic only — semantic hygiene is normalizeParsedVersion's
// job upstream): "pkg:type/…" shape; no whitespace or control bytes;
// every '%' begins a valid two-hex escape; the name region (after the
// type, before any version separator) must be non-blank once %20s are
// discounted.
func wireValidPurl(p string) bool {
	if !strings.HasPrefix(p, "pkg:") {
		return false
	}
	rest := p[len("pkg:"):]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 {
		return false
	}
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == ' ' || c == '\t' || c < 0x20 {
			return false
		}
		if c == '%' {
			if i+2 >= len(p) || !isHexByte(p[i+1]) || !isHexByte(p[i+2]) {
				return false
			}
		}
	}
	nameRegion := rest[slash+1:]
	if at := strings.LastIndex(nameRegion, "@"); at >= 0 && !strings.Contains(nameRegion[at:], "/") {
		nameRegion = nameRegion[:at]
	}
	if strings.Trim(strings.ReplaceAll(nameRegion, "%20", " "), " /") == "" {
		return false
	}
	return true
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// purlReplaceVersion (v0.27.72) swaps a purl's version segment for a
// NORMALIZED one, or strips it entirely when version is "" (the
// unpinned pathway). This is the scan-side half of the v0.27.71
// version-hygiene fix: stored dep rows keep pre-fix garbage versions
// until each repo's analysis re-runs, so the scan rebuilds purls from
// normalizeParsedVersion output at READ time — which is what lets
// `aveloxis heal-vulnerabilities` clean up the malformed-purl false
// positives immediately after deploy. Same scope-marker rule as
// purlWithVersion.
func purlReplaceVersion(purl, version string) string {
	if purl == "" {
		return ""
	}
	if at := strings.LastIndex(purl, "@"); at > 0 && !strings.Contains(purl[at:], "/") {
		purl = purl[:at]
	}
	if version == "" {
		return purl
	}
	return purl + "@" + purlEscapeSegment(version)
}
