// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scantarget

import (
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// IsSelfDependency reports whether a declared dependency names one of
// the repo's own packages (the self-set from
// db.GetRepoSelfPackageNames — lowercased, exact matches only). The
// dep-name side is also checked with '_'→'-' folded, mirroring the
// self-set's own name-heuristic variants.
func IsSelfDependency(name string, selfSet map[string]bool) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	return selfSet[n] || selfSet[strings.ReplaceAll(n, "_", "-")]
}

// githubActions is the package manager a workflow's `uses:` reference is
// stored under, and osvGitHubActions the ecosystem OSV files its advisories
// under. OSV's GitHub Actions advisories carry no purl and its API has no
// version semantics for them, so such a target is queried by name and
// ecosystem, and the ref is evaluated on this side.
const (
	githubActions    = "githubactions"
	osvGitHubActions = "GitHub Actions"
)

// DirectTargets computes the purl or purls to scan for one declared
// dependency: one per version in use (versionuse.Of, which owns the order of
// evidence). locked maps pkgid.MatchKey(ecosystem, name) to the distinct
// versions the repository's lockfiles resolved. A dependency with no purl has
// no target.
func DirectTargets(dep model.VulnScanDep, locked map[string][]string) []model.VulnScanTarget {
	if dep.Purl == "" {
		return nil
	}
	// v0.27.46 (summary/19 P3): direct targets carry the dep's scope
	// so findings' dependency_scope works for direct deps too.
	// StoredScope keeps the '' = runtime column convention.
	scope := model.StoredScope(dep.Type)
	uses := versionuse.Of(dep, locked)
	targets := make([]model.VulnScanTarget, 0, len(uses))
	for _, u := range uses {
		tgt := model.VulnScanTarget{
			Purl: dep.Purl, Dep: dep,
			Requirement: dep.Requirement,
			Resolution:  u.Resolution,
			Kind:        model.DependencyKindDirect,
			Scope:       scope,
		}
		if u.FromLockfile {
			tgt.Purl = pkgid.WithVersion(dep.Purl, u.Version)
		}
		if dep.PackageManager == githubActions {
			tgt.OSVQueryName = dep.Name
			tgt.OSVQueryEcosystem = osvGitHubActions
		}
		targets = append(targets, tgt)
	}
	return targets
}

// MergeDirectTarget inserts a direct scan target, folding scope on
// purl collision: the SAME name@version can be declared in BOTH a
// runtime and a non-runtime section (the local-canary catch, 2026-07-21:
// pipenv declares setuptools 67 in [build-system].requires AND
// [project].dependencies — the build-scoped row iterated first, won
// the dedup, and re-stamped a previously-runtime finding as build,
// dropping it from the headline and the digest). A purl reachable
// from any runtime declaration is runtime exposure (empty — the stored
// convention), mirroring the lockfile fold's any-runtime-wins rule.
func MergeDirectTarget(m map[string]model.VulnScanTarget, purls []string, tgt model.VulnScanTarget) []string {
	if existing, dup := m[tgt.Purl]; dup {
		// v0.27.51: the fold speaks the contract (IsRuntimeScope),
		// not literal values — runtime now stores as the word, and
		// legacy '' must fold identically.
		if !model.IsRuntimeScope(existing.Scope) && model.IsRuntimeScope(tgt.Scope) {
			existing.Scope = tgt.Scope
			m[tgt.Purl] = existing
		}
		return purls
	}
	m[tgt.Purl] = tgt
	return append(purls, tgt.Purl)
}
