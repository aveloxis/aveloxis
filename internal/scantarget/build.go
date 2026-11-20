// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package scantarget builds a repository's vulnerability scan targets from
// its stored rows, at scan time: what will be asked of OSV, and under which
// classification each answer will be stored.
//
// It is the target-building half of the scan, which was the first 250 lines
// of collector.ScanVulnerabilities (summary/70, 4.1.6). The scan, the
// heal-vulnerabilities command and, from chunk 2, the advisory ticker all get
// their targets here and hand them on. It decides nothing about findings:
// what is written, kept or closed is the linking unit's.
//
// In order, Build:
//
//  1. reads the declared dependencies and normalises their stored versions
//     (rows written before the parser fixes of v0.27.71 hold garbage versions
//     until the next analysis rewrites them);
//  2. leaves out a dependency that names one of the repository's own
//     packages (a support-matrix declaration, not exposure);
//  3. counts and names a dependency with no purl (it cannot be sent, and its
//     existing findings must not be closed for being absent from the answer);
//  4. adds one target per version in use of each remaining dependency
//     (versionuse), folding scope when one purl is declared twice;
//  5. adds the transitive lockfile packages, when asked, and the repository's
//     own published packages, each only where no earlier target has its purl;
//  6. applies the wire gate: a target OSV would reject is dropped and named,
//     because one malformed query costs the repository its whole batch.
//
// It reads through an interface it declares, and imports internal/model,
// internal/pkgid, internal/manifest and internal/versionuse.
package scantarget

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// Store is what Build reads: reads of one repository's stored rows.
// *db.PostgresStore satisfies it.
type Store interface {
	// GetRepoDepsForVulnScan returns the declared dependencies with their
	// purls and raw requirement strings.
	GetRepoDepsForVulnScan(ctx context.Context, repoID int64) ([]model.VulnScanDep, error)
	// GetRepoSelfPackageNames returns the lower-cased names of the packages
	// the repository itself publishes (the broad set, for exclusion).
	GetRepoSelfPackageNames(ctx context.Context, repoID int64) (map[string]bool, error)
	// GetRepoLockedVersions returns the lockfile resolutions of the
	// repository's direct dependencies.
	GetRepoLockedVersions(ctx context.Context, repoID int64) ([]model.RepoLockfilePackage, error)
	// GetRepoTransitivePackages returns the lockfile resolutions that are not
	// direct dependencies.
	GetRepoTransitivePackages(ctx context.Context, repoID int64) ([]model.RepoLockfilePackage, error)
	// GetRepoSelfAdvisoryPackages returns the packages attributed to the
	// repository precisely enough to scan for advisories against itself.
	GetRepoSelfAdvisoryPackages(ctx context.Context, repoID int64) ([]model.SelfAdvisoryPackage, error)
	// GetRepoInventoryEvidence returns what is stored about the repository's
	// dependencies and earlier scans.
	GetRepoInventoryEvidence(ctx context.Context, repoID int64) (model.InventoryEvidence, error)
}

// selfSampleLimit is how many self-excluded dependency names are kept for the
// scan's log line.
const selfSampleLimit = 5

// Dropped is a target the wire gate refused.
type Dropped struct {
	Purl    string
	DepName string
	Kind    string
}

// Set is a repository's scan targets and what was left out on the way.
type Set struct {
	// Purls are the targets that can be sent, in the order they were built.
	Purls []string
	// Targets holds each sendable purl's target.
	Targets map[string]model.VulnScanTarget

	// Candidates is the number of targets before the wire gate. Zero means
	// the repository had nothing to scan at all; Candidates > 0 with no Purls
	// means every target was malformed. The scan treats the two differently.
	Candidates int
	// Dropped are the targets the wire gate refused, in order.
	Dropped []Dropped

	// SelfExcludedDirect counts declared dependencies left out for naming one
	// of the repository's own packages, with up to selfSampleLimit distinct
	// names; SelfExcluded adds the transitive packages left out the same way.
	SelfExcludedDirect int
	SelfExcluded       int
	SelfSamples        []string

	// BlankPurlSkipped counts declared dependencies that have no purl, and
	// BlankPurlNames names each one (a name may repeat).
	BlankPurlSkipped int
	BlankPurlNames   []string

	// Transitive and Self count the transitive and own-package targets
	// added, before the wire gate.
	Transitive int
	Self       int

	// Evidence is what was stored about the repository's dependencies and
	// earlier scans when the targets were read: the closing rule's
	// precondition (Evidence.DependenciesUnknown) and whether this can be the
	// repository's first scan. Read last of the six reads, and before the
	// scan writes anything.
	Evidence model.InventoryEvidence
}

// Build builds the scan targets of one repository. transitive adds the
// lockfile packages that are not direct dependencies.
//
// Every read fails the build: scanning without the self set or the locked
// versions would add self-dependency noise or scan a locked dependency at its
// floor, and the findings would then flip between closed and open from one
// scan to the next.
func Build(ctx context.Context, store Store, repoID int64, transitive bool) (*Set, error) {
	deps, err := store.GetRepoDepsForVulnScan(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading dependencies: %w", err)
	}

	// v0.27.72: normalise STORED versions at read time and rebuild the purl
	// when that changed anything. A garbage version ("6.0.3 \", "workspace =
	// true", "${project.version}") defeats OSV's version matching: OSV falls
	// back to the package and returns its whole advisory history (the zephyr
	// incident). Analysis rewrites the rows on the repository's next cycle,
	// but neither the scan nor heal-vulnerabilities may depend on that order.
	for i := range deps {
		// A version a lockfile supplied is the lockfile's field as written
		// and is not put through this hygiene (0.29.177): it would blank
		// some (a Composer "dev-master" has no digit) and re-key the
		// finding to a versionless purl.
		if deps[i].VersionSource == versionuse.SourceLocked {
			continue
		}
		if norm := manifest.NormalizeParsedVersion(deps[i].PackageManager, deps[i].CurrentVersion); norm != deps[i].CurrentVersion {
			deps[i].CurrentVersion = norm
			deps[i].Purl = pkgid.ReplaceVersion(deps[i].Purl, norm)
		}
	}

	selfSet, err := store.GetRepoSelfPackageNames(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading self-package set: %w", err)
	}
	lockedRows, err := store.GetRepoLockedVersions(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading locked versions: %w", err)
	}
	locked := make(map[string][]string, len(lockedRows))
	for _, lr := range lockedRows {
		key := pkgid.MatchKey(lr.Ecosystem, lr.PackageName)
		if !slices.Contains(locked[key], lr.ResolvedVersion) {
			locked[key] = append(locked[key], lr.ResolvedVersion)
		}
	}

	set := &Set{Targets: map[string]model.VulnScanTarget{}}
	var purls []string
	for _, dep := range deps {
		// Self-exclusion FIRST (v0.28.11): a purl-less dependency that names
		// the repository's own package is a deliberate exclusion, not an
		// unsendable dependency. Counted the other way round it blocked the
		// completed-scan stamp on a repository whose every dependency was
		// its own package.
		if IsSelfDependency(dep.Name, selfSet) {
			set.SelfExcludedDirect++
			if len(set.SelfSamples) < selfSampleLimit && !slices.Contains(set.SelfSamples, dep.Name) {
				set.SelfSamples = append(set.SelfSamples, dep.Name)
			}
			continue
		}
		if dep.Purl == "" {
			// No purl mapping for its ecosystem: it cannot be sent. Counted
			// (v0.28.5) and named (v0.28.8), because a dependency that is
			// never sent never rescans, and closing its findings for being
			// absent from the answer would be a permanent false "clean".
			set.BlankPurlSkipped++
			set.BlankPurlNames = append(set.BlankPurlNames, dep.Name)
			continue
		}
		for _, tgt := range DirectTargets(dep, locked) {
			purls = MergeDirectTarget(set.Targets, purls, tgt)
		}
	}
	set.SelfExcluded = set.SelfExcludedDirect

	// v0.27.21 C1: transitive lockfile targets, read from the table (not a
	// fresh parse) so heal-vulnerabilities has them too. A direct target wins
	// on a purl collision, and the self set applies as it does above.
	if transitive {
		rows, err := store.GetRepoTransitivePackages(ctx, repoID)
		if err != nil {
			return nil, fmt.Errorf("loading transitive lockfile packages: %w", err)
		}
		for _, tr := range rows {
			if IsSelfDependency(tr.PackageName, selfSet) {
				set.SelfExcluded++
				continue
			}
			purl := pkgid.PurlForPackage(tr.Ecosystem, tr.Namespace, tr.PackageName, tr.ResolvedVersion)
			tgt := model.VulnScanTarget{
				Dep:        model.VulnScanDep{Name: tr.PackageName, PackageManager: tr.Ecosystem},
				Resolution: versionuse.Locked,
				Kind:       model.DependencyKindTransitive,
				Scope:      model.StoredScope(tr.Scope),
			}
			// 0.29.206: a package of an ecosystem that has no purl
			// (Bioconductor) is asked by name, ecosystem and version, and
			// its finding takes this program's own identity.
			if osvEcosystem, byName := pkgid.OSVEcosystemByName(tr.Ecosystem); byName && strings.TrimSpace(tr.PackageName) != "" {
				purl = pkgid.FindingPurlByName(tr.Ecosystem, tr.PackageName, tr.ResolvedVersion)
				tgt.Dep.CurrentVersion = tr.ResolvedVersion
				tgt.OSVQueryName, tgt.OSVQueryEcosystem, tgt.OSVQueryVersion = tr.PackageName, osvEcosystem, tr.ResolvedVersion
			}
			if purl == "" {
				continue
			}
			if _, dup := set.Targets[purl]; dup {
				continue
			}
			tgt.Purl = purl
			set.Targets[purl] = tgt
			purls = append(purls, purl)
			set.Transitive++
		}
	}

	// v0.27.29: the repository's OWN published packages, versionless, one per
	// precisely attributed package (numpy/numpy has no dependencies, so a
	// dependency-only scan showed nothing while OSV carried 16 advisories for
	// pkg:pypi/numpy). For a repository with no dependencies these ARE the
	// scan, which is why they are added before anything looks at the count.
	selfPkgs, err := store.GetRepoSelfAdvisoryPackages(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading self-advisory packages: %w", err)
	}
	for _, sp := range selfPkgs {
		purl := pkgid.SelfPurl(sp.Ecosystem, sp.PackageName)
		tgt := model.VulnScanTarget{
			Dep:  model.VulnScanDep{Name: sp.PackageName, PackageManager: sp.Ecosystem},
			Kind: model.DependencyKindSelf,
		}
		// 0.29.206: a package of an ecosystem that has no purl
		// (Bioconductor) is asked by name and ecosystem, with no version,
		// as an own package is asked everywhere: every advisory it has.
		if osvEcosystem, byName := pkgid.OSVEcosystemByName(sp.Ecosystem); byName && strings.TrimSpace(sp.PackageName) != "" {
			purl = pkgid.FindingPurlByName(sp.Ecosystem, sp.PackageName, "")
			tgt.OSVQueryName, tgt.OSVQueryEcosystem = sp.PackageName, osvEcosystem
		}
		if purl == "" {
			continue
		}
		if _, dup := set.Targets[purl]; dup {
			continue
		}
		tgt.Purl = purl
		set.Targets[purl] = tgt
		purls = append(purls, purl)
		set.Self++
	}

	set.Evidence, err = store.GetRepoInventoryEvidence(ctx, repoID)
	if err != nil {
		return nil, fmt.Errorf("loading inventory evidence: %w", err)
	}

	// v0.27.73: the wire gate. Legacy rows can still carry purls built by raw
	// concatenation, and one malformed query gets the repository's ENTIRE
	// batch refused (81 repositories on the 2026-08-01 fleet heal); OSV's own
	// error ("query at index N") names nothing. A target queried by name and
	// ecosystem (a GitHub Action) is not a purl query and must instead carry
	// a name that is not blank.
	set.Candidates = len(purls)
	set.Purls = make([]string, 0, len(purls))
	for _, p := range purls {
		t := set.Targets[p]
		ok := pkgid.WireValid(p)
		if t.OSVQueryEcosystem != "" {
			ok = strings.TrimSpace(t.OSVQueryName) != ""
		}
		if !ok {
			set.Dropped = append(set.Dropped, Dropped{Purl: p, DepName: t.Dep.Name, Kind: t.Kind})
			delete(set.Targets, p)
			continue
		}
		set.Purls = append(set.Purls, p)
	}
	return set, nil
}
