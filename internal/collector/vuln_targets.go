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
	if purlNamespaceRequired[typ] && !purlNameHasNamespace(name) {
		return "" // v0.29.58: the third minting site shares the rule (review round 1)
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
// omission beats a malformed purl OSV can't match). namespace is the
// stored purl namespace (repo_lockfile_packages.purl_namespace, v0.29.59:
// a SwiftPM pin's host/owner); when present it is prefixed to the name.
func purlForPackage(ecosystem, namespace, name, version string) string {
	typ, ok := purlEcosystemTypes[ecosystem]
	if !ok || name == "" || version == "" {
		return ""
	}
	if typ == "maven" {
		name = strings.Replace(name, ":", "/", 1)
	}
	if namespace != "" {
		name = strings.Trim(namespace, "/") + "/" + name
	}
	if purlNamespaceRequired[typ] && !purlNameHasNamespace(name) {
		// v0.29.58: a Package.resolved pin without a repository URL keeps
		// its identity alone ("alamofire"; since v0.29.59 a hosted pin
		// carries its namespace), a composer platform package has no
		// vendor ("php"), a Gradle shorthand can drop the group — none is
		// a purl OSV accepts, and
		// one of them 400s the repository's whole batch. Minted nowhere
		// rather than dropped at the wire (both gates share this rule).
		return ""
	}
	return buildPurl(typ, name, version) // v0.27.29: spec-canonical
}

// purlNamespaceRequired lists the purl types whose spec makes the
// namespace mandatory — maven (groupId), swift (the package's host and
// owner path) and composer (vendor). OSV enforces it: "namespace is
// required" rejected four repositories' entire scans in the 2026-09-22
// log. npm, golang, pypi, cargo, gem, nuget, hex, pub and hackage leave
// it optional and are untouched by this rule.
var purlNamespaceRequired = map[string]bool{
	"maven":    true,
	"swift":    true,
	"composer": true,
}

// purlNameHasNamespace reports whether a purl name (the part after the
// type, before any version) carries a non-empty namespace segment ahead
// of a non-empty name segment.
func purlNameHasNamespace(name string) bool {
	ns, rest, ok := strings.Cut(name, "/")
	return ok && ns != "" && rest != ""
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

// TWO purl version rules live in this file, and they answer different
// questions (v0.29.56 — a review asked whether they could be one):
//
//   - purlSplitVersion — "where would OSV split this?" The receiver's
//     rule: the LAST '@', whatever follows it. Used by wireValidPurl,
//     because what matters at the gate is what the RECEIVER will parse.
//     This is what catches a name region that OSV reads as empty
//     ("purl is missing name", the 2026-09-17 batch failure).
//   - purlScopeAwareBase — "where do I replace the version?" Our rule:
//     an '@' followed by a '/' is a scope marker, not a version
//     separator, so "pkg:npm/@scope/name" keeps its name. Used by the
//     two rebuilders; OSV's rule would cut that purl to "pkg:npm/".
//
// They cannot be merged without breaking one of the two.
//
// Known, accepted consequence: a legacy row whose stored purl is already
// mis-split ("pkg:npm/registry.npmjs.org/@jest/transform/26.0.1", the shape
// v0.29.56's lockfile fix stops producing) is REJECTED by the gate as it
// stands, but once a lockfile supplies a resolved version the rebuilt purl
// ends in a real version and passes. It then queries OSV under a name no
// package has, which matches nothing — a wasted query, not a wrong finding
// — and the row heals on the repo's next analysis. Guarding the rebuilders
// on the gate was tried and reverted: purlReplaceVersion exists to clean
// exactly these malformed stored purls at read time (v0.27.72, the
// heal-vulnerabilities path), so refusing to rebuild them defeats the heal.
func purlSplitVersion(purl string) (base, version string) {
	cut := purl
	if i := strings.LastIndexByte(cut, '#'); i >= 0 {
		cut = cut[:i]
	}
	if i := strings.IndexByte(cut, '?'); i >= 0 {
		cut = cut[:i]
	}
	// Both halves come out of `cut`, not the original (v0.29.57): slicing
	// the original handed the qualifiers and subpath back as part of the
	// version ("1.0?repository_url=x#src"), and as part of the base when
	// there was no version at all — the opposite of what stripping them
	// was for.
	at := strings.LastIndexByte(cut, '@')
	if at < 0 {
		return cut, ""
	}
	return cut[:at], cut[at+1:]
}

// purlScopeAwareBase returns a purl without its version, treating an '@'
// followed by a '/' as a scope marker. See the note on purlSplitVersion.
func purlScopeAwareBase(purl string) string {
	if at := strings.LastIndex(purl, "@"); at > 0 && !strings.Contains(purl[at:], "/") {
		return purl[:at]
	}
	return purl
}

// purlWithVersion swaps the version suffix of a purl
// ("pkg:npm/express@4.18.0" → "pkg:npm/express@4.19.2").
func purlWithVersion(purl, version string) string {
	return purlScopeAwareBase(purl) + "@" + version
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
	// Parse like the purl spec (and OSV): drop "#subpath" and "?qualifiers",
	// then split the version at the LAST '@'. What remains must be a
	// non-blank name with no empty path segment. v0.29.56: the gate kept an
	// '@' followed by '/' in the name, so a mis-split pnpm lockfile key
	// ("pkg:npm/registry.npmjs.org/@jest/transform/26.0.1") passed here and
	// OSV rejected the repository's whole batch: "purl is missing name".
	nameRegion, _ := purlSplitVersion(rest[slash+1:])
	if i := strings.LastIndexByte(nameRegion, '#'); i >= 0 {
		nameRegion = nameRegion[:i]
	}
	if i := strings.IndexByte(nameRegion, '?'); i >= 0 {
		nameRegion = nameRegion[:i]
	}
	if strings.Trim(strings.ReplaceAll(nameRegion, "%20", " "), " /") == "" {
		return false
	}
	if strings.HasSuffix(nameRegion, "/") || strings.Contains(nameRegion, "//") {
		return false
	}
	// v0.29.58: the namespace rule at the wire too — legacy dep rows
	// minted before purlForPackage/analysis applied it still reach here.
	if purlNamespaceRequired[rest[:slash]] && !purlNameHasNamespace(nameRegion) {
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
// positives immediately after deploy. Uses the scope-aware base — see the
// note on purlSplitVersion for why the gate and the rebuilders split
// differently.
func purlReplaceVersion(purl, version string) string {
	if purl == "" {
		return ""
	}
	base := purlScopeAwareBase(purl)
	if version == "" {
		return base
	}
	return base + "@" + purlEscapeSegment(version)
}
