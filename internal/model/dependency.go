// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// The types in this file are the ones the dependency and vulnerability units
// share (summary/70, 2.4). They were defined in internal/db and
// internal/collector; they live here so that a unit can use them without
// importing either. None has struct tags, and every one is scanned by hand.

// LibyearRow is a row for the repo_deps_libyear table.
// Also used as input for SBOM generation.
type LibyearRow struct {
	Name               string
	Requirement        string
	Type               string // "runtime", "dev"
	PackageManager     string // "npm", "pypi", "go", "cargo", "rubygems"
	CurrentVersion     string
	LatestVersion      string
	CurrentReleaseDate string
	LatestReleaseDate  string
	Libyear            float64
	License            string // SPDX license identifier (e.g., "MIT", "Apache-2.0")
	Purl               string // Package URL (e.g., "pkg:npm/express@4.18.0")
	// NoLibyear (v0.27.47, summary/19 P4) stores libyear as NULL —
	// for ecosystems with no registry timeline (GitHub Actions). A
	// fabricated 0.0 would read as "perfectly fresh" and leak into
	// the upstream-dependencies snapshot, which filters libyear IS
	// NOT NULL.
	NoLibyear bool
	// ManifestPath is the manifest that declared the dependency, relative to
	// the clone's root with forward slashes ("svc/package.json"). It is what
	// a failed manifest's rows are carried by (summary/70, 4.7). Empty on a
	// row written before 0.29.160, which therefore cannot be carried.
	ManifestPath string
	// Status is why the row has a libyear or lacks one: one of the Libyear*
	// constants, set by the collector's settleLibyear. Not stored yet; the
	// column arrives with the rows it is needed for (summary/70, 4.2).
	Status string
	// VersionFound is what the registry lookup established about the
	// declared version, apart from its date. Not stored yet (chunk 3 reads
	// it for versions that were never published).
	VersionFound VersionFound
	// VersionSource is repo_deps_libyear.version_source (0.29.177): where
	// CurrentVersion came from when it is not the declaration's own
	// (versionuse.SourceLocked, SourceLockedSeveral). Empty: declared.
	VersionSource string
	// VersionReason is repo_deps_libyear.version_reason (0.29.181).
	VersionReason string
	// VersionHosts is repo_deps_libyear.version_hosts (0.29.185).
	VersionHosts []string
}

// VulnScanDep is one dependency as the vulnerability scan consumes it:
// the libyear table's floor version + purl plus the RAW requirement
// string the manifest declared ("apache-airflow>=3.0.0"), which drives
// the v0.27.11 version_resolution classification.
type VulnScanDep struct {
	Name           string
	CurrentVersion string
	PackageManager string
	Purl           string
	Requirement    string
	// Type is the dependency scope from repo_deps_libyear.type
	// (v0.27.46, summary/19 P3) — stamped onto direct findings'
	// dependency_scope so runtime/dev splits work for direct deps,
	// not just transitives.
	Type string
	// VersionSource is repo_deps_libyear.version_source (0.29.177): a
	// version whose source is "locked" came from a lockfile as written and
	// is not put through the manifest version hygiene.
	VersionSource string
}

// RepoLockfilePackage is one stored resolution: a lockfile-resolved
// version of a package that matches one of the repo's DIRECT declared
// dependencies.
type RepoLockfilePackage struct {
	Ecosystem       string
	PackageName     string
	ResolvedVersion string
	LockfilePath    string
	// Namespace is repo_lockfile_packages.purl_namespace (v0.29.59): the
	// purl namespace the lockfile supplied (a SwiftPM pin's host/owner),
	// "" where the format has none.
	Namespace string
	// Direct (v0.27.21 C1) — TRUE for resolutions of the repo's
	// declared direct dependencies (the only rows written pre-C1 and
	// with vuln_scan_transitive off); FALSE for transitive entries.
	Direct bool
	// Scope — 'dev' / 'runtime' / '' (unknown), carried from formats
	// that flag it (package-lock v2/3 dev, poetry category).
	Scope string
}

// SelfAdvisoryPackage is one of the repo's OWN published packages,
// eligible for version-unconstrained advisory scanning (v0.27.29).
type SelfAdvisoryPackage struct {
	Ecosystem string // repo_distribution.ecosystem / manifest_type flavor
	// PackageName is lower-cased, except for an ecosystem whose own-package
	// name is case-sensitive where it is asked (pkgid.SelfNameKeepsCase,
	// which lists them), which keeps the stored spelling.
	PackageName string
}

// VulnerabilityRow represents a known vulnerability affecting a dependency.
type VulnerabilityRow struct {
	VulnID            string   // e.g., "GHSA-abc-123" or "PYSEC-2024-001"
	CVEID             string   // e.g., "CVE-2024-12345" (extracted from aliases)
	PackageName       string   // e.g., "express"
	PackagePurl       string   // e.g., "pkg:npm/express@4.18.0"
	Ecosystem         string   // e.g., "npm", "PyPI", "Go"
	Severity          string   // e.g., "CRITICAL", "HIGH", "MEDIUM", "LOW"
	CVSSScore         float64  // e.g., 9.8
	CVSSVector        string   // e.g., "CVSS:3.1/AV:N/AC:L/..."
	Summary           string   // short description
	Details           string   // full description
	FixedVersion      string   // version that fixes the vulnerability
	IntroducedVersion string   // version that introduced the vulnerability
	Source            string   // "osv.dev", "github-advisory"
	Aliases           []string // cross-references (CVE, GHSA, etc.)
	ReferencesJSON    []byte   // JSON array of reference URLs

	// v0.27.4 lifecycle fields. A row is CURRENT while ResolvedAt is
	// nil; when a complete scan no longer reports the (vuln_id, purl)
	// pair, ResolvedAt is stamped and the row becomes historical
	// record (dependency upgraded past the vuln, or dropped). A vuln
	// that reappears is un-resolved by the upsert.
	FirstDetectedAt time.Time
	LastSeenAt      time.Time
	ResolvedAt      *time.Time
	// ClosedReason (read side, 0.29.196) is why a closed finding closed when
	// that was not a fix (one of the Closed* reasons); "" for a current
	// finding and for an ordinary resolution. The store writes it when it
	// closes a finding; a reader fills it from the row.
	ClosedReason string

	// v0.29.166: when the finding entered exposure in this repository. The
	// store sets it when the row's class becomes exposure. A writer sets
	// these two only for a successor of a finding that was itself exposure
	// when it closed: EnteredExposureCarried says "use the predecessor's
	// time, not now", and EnteredExposureAt is that time, nil when the
	// predecessor had none (it was written before 0.29.166).
	EnteredExposureCarried bool
	EnteredExposureAt      *time.Time

	// v0.29.171, the decision guards (summary/70, 4.2 and 5.4). AskedAt is
	// when the answer this row comes from was asked of OSV; TargetsReadAt
	// when the scan writing it read its targets. Both database times. The
	// store applies the row only if neither is older than what the stored
	// finding was last decided by. Zero means "not tracked": the row is
	// applied and the stored times are kept.
	AskedAt       time.Time
	TargetsReadAt time.Time

	// v0.27.11 version-resolution accuracy. DeclaredRequirement is the
	// raw manifest requirement string ("apache-airflow>=3.0.0");
	// VersionResolution is how the scanned version was chosen:
	// 'locked' / 'exact' / 'bounded-range' / 'range-floor' /
	// 'unpinned'. '' = pre-v0.27.11 row (heals on next scan). Both are
	// ALWAYS refreshed on upsert — NOT prefer-nonempty — because the
	// classification must track the current manifest.
	DeclaredRequirement string
	VersionResolution   string
	// v0.27.21 C1: 'direct' | 'transitive' ('' = pre-C1 row, heals on
	// next scan) + 'dev'/'runtime'/'' scope from the lockfile. ALWAYS
	// refreshed on upsert, like version_resolution.
	DependencyKind  string
	DependencyScope string
}

// LibyearDep is one declared dependency as a manifest parser reports it.
type LibyearDep struct {
	Name        string
	Version     string
	Requirement string
	Type        string // "runtime", "dev"
	Manager     string // "npm", "pypi"
	// NonRegistry (v0.29.56) marks a dependency the manifest sources from
	// somewhere other than its ecosystem's registry: a local path or
	// workspace package, a git or URL reference. The registry knows nothing
	// about it — looking it up only drew 404s — and it is NOT the registry
	// package of the same name (SR-6), so scanLibyear skips it: no lookup
	// and no repo_deps_libyear row, counted in one INFO line per repo. The
	// dependency scan still inventories it in repo_dependencies.
	//
	// Storing a purl-less row instead was tried and reverted: a blank purl
	// counts as an UNSCANNABLE dependency in the vulnerability scan, which
	// would stop a repo whose deps are all local from being stamped
	// scan-complete (the v0.28.5 contract).
	NonRegistry bool
	// ManifestPath is the manifest that declared it, relative to the clone's
	// root with forward slashes. Set by the walk, not by the parsers.
	ManifestPath string
	// VersionSource (0.29.177) says where Version came from when it is not
	// the declaration's own: versionuse.SourceLocked (the nearest lockfile
	// resolves the dependency to exactly this version, which replaced the
	// declared one) or SourceLockedSeveral (it resolves to several; Version
	// is still the declared one). Empty: the declaration's.
	VersionSource string
	// VersionReason (0.29.181) is why the dependency has no version when a
	// pom that might have given it one could not be obtained (the
	// VersionReasonInherited* values). Empty otherwise.
	VersionReason string
	// VersionHosts (0.29.185) are the package repository hosts the fetched
	// poms came from that this dependency's version was worked out
	// through; nil when it came from the repository's own files.
	VersionHosts []string
	// ManagedEntry (0.29.183) marks a Maven dependencyManagement entry: not
	// a dependency, but the version one would get. BOMImport marks the
	// entry that imports a BOM. The analysis decides, per tree, whether
	// such an entry is still inventoried (manifest.SettleMavenManaged).
	ManagedEntry bool
	BOMImport    bool
}

// VulnScanTarget is one purl to scan, with the classification the
// finding rows will carry.
type VulnScanTarget struct {
	Purl        string
	Dep         VulnScanDep
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
	// OSVQueryVersion (0.29.206), when set, is sent with the name and the
	// ecosystem, and OSV matches it: the form for a package of an
	// ecosystem OSV has a version order for and no purl (Bioconductor).
	// Empty for a GitHub Action, as described above.
	OSVQueryVersion string
}

// VersionSourceManaged is repo_deps_libyear.version_source for a Maven
// dependency whose version comes from dependencyManagement (0.29.178). The
// lockfile sources are internal/versionuse's.
const VersionSourceManaged = "managed"

// Why a Maven dependency has no version when its pom gives it none and the
// pom that manages it could not be obtained (repo_deps_libyear.version_reason,
// 0.29.181).
const (
	// VersionReasonInheritedNotFound: a parent or BOM is on no repository
	// that is read; or it was never asked for because it is not one pom
	// that can be asked for: a snapshot, a version range, or a coordinate
	// that is not text a request can be built from (0.29.210).
	VersionReasonInheritedNotFound = "inherited_not_found"
	// VersionReasonInheritedLookupFailed: it is not known whether a
	// repository has it. A repository did not answer and nothing is stored
	// for the coordinate; or declared repositories were left unconsulted
	// because the analysis had waited on them as long as it may, or
	// because the build declares more of them than one lookup consults
	// (0.29.209). The first two can change at the next analysis; the last
	// is the same every time, and the log says so.
	VersionReasonInheritedLookupFailed = "inherited_lookup_failed"
	// VersionReasonInheritedRepositoryRefused: the repository that holds it
	// may not be fetched from.
	VersionReasonInheritedRepositoryRefused = "inherited_repository_refused"
	// VersionReasonProfileManaged (0.29.215): a Maven profile of the build
	// manages the dependency (or imports a BOM, or declares it with a
	// version), and this reader does not know which profiles a build
	// activates. A version that the pom, its parents or its BOMs would give
	// when no profile is active is withheld.
	VersionReasonProfileManaged = "profile_managed"
	// VersionReasonPlatformsDisagree (0.29.215): Gradle. More than one thing
	// gives the dependency a version and they do not agree in a way this
	// reader can settle: two platforms recommend versions that Gradle's
	// version ordering, as this reader implements it, does not order; or a
	// platform and Spring's dependency-management plugin each manage it, at
	// different versions.
	VersionReasonPlatformsDisagree = "platforms_disagree"
	// VersionReasonValueNotRead (0.29.215): Gradle. The version is written
	// as a value this reader does not read whole: one expression of more
	// tokens than it follows (a map or list of thousands of entries, a
	// string built from thousands of parts).
	VersionReasonValueNotRead = "value_not_read"
)

// VersionReasons are the values repo_deps_libyear.version_reason takes, the
// empty one apart: the one list a reader of rows it did not make holds them
// to (the manifest reader's parent, internal/collector).
func VersionReasons() []string {
	return []string{VersionReasonInheritedNotFound, VersionReasonInheritedLookupFailed, VersionReasonInheritedRepositoryRefused,
		VersionReasonProfileManaged, VersionReasonPlatformsDisagree, VersionReasonValueNotRead}
}
