// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package versionuse answers one question about a declared dependency: which
// version of it is in use, and how is that known?
//
// It is a pure function over what is already stored (the declaration, and the
// versions the repository's lockfiles resolved it to); it reads nothing and
// fetches nothing. The answer is a list, because two applications in one
// repository may lock one package at two versions.
//
// The order of evidence is the one the scan has used since v0.27.11
// (summary/70, 4.1.6 moved it here unchanged; 4.3 to 4.6 add managed sources
// to it, each as its own step):
//
//  1. A GitHub Action is known by its ref: a full commit SHA is locked, a
//     dotted tag is exact, a floating ref (@v4, @main) is unpinned.
//  2. A Go dependency is locked at the version go.mod states: minimal version
//     selection makes that version exact, with no lockfile needed.
//  3. A lockfile resolution is locked, once per distinct resolved version.
//  4. Otherwise the version is the lowest the declaration allows (the floor),
//     and the requirement's own text says how firm that is: exact, a bounded
//     range, a range with only a floor, or no version at all. Taking the
//     newest version that satisfies the requirement was considered and
//     rejected by the operator: what is really installed depends on the whole
//     dependency graph.
//
// It imports internal/model, internal/pkgid and internal/manifest.
package versionuse

import (
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
)

// githubActions is the package manager a workflow's `uses:` reference is
// stored under.
const githubActions = "githubactions"

// Use is one version of a dependency that is in use, and how that is known.
type Use struct {
	// Version is the version. For a Use that is not FromLockfile it is the
	// declaration's own stored version (which may be empty).
	Version string
	// Resolution is how the version is known: Locked, Exact, BoundedRange,
	// RangeFloor or Unpinned.
	Resolution string
	// FromLockfile is true when a lockfile supplied the version, so it
	// replaces whatever version the declaration's purl carries.
	FromLockfile bool
}

// Of returns the version or versions of dep in use. locked maps
// pkgid.MatchKey(package manager, name) to the distinct versions the
// repository's lockfiles resolved; nil is allowed. The result is never empty.
func Of(dep model.VulnScanDep, locked map[string][]string) []Use {
	if dep.PackageManager == githubActions {
		return []Use{{Version: dep.CurrentVersion, Resolution: ClassifyActionRef(dep.CurrentVersion)}}
	}
	if dep.PackageManager == "go" && dep.CurrentVersion != "" {
		// go.mod under minimal version selection is exact. With no
		// version it is not (the rule below).
		return []Use{{Version: dep.CurrentVersion, Resolution: Locked}}
	}
	if versions := locked[pkgid.MatchKey(dep.PackageManager, dep.Name)]; len(versions) > 0 {
		uses := make([]Use, 0, len(versions))
		for _, v := range versions {
			uses = append(uses, Use{Version: v, Resolution: Locked, FromLockfile: true})
		}
		return uses
	}
	if dep.VersionSource == model.VersionSourceManaged && dep.CurrentVersion != "" {
		// v0.29.178: the declaration gives no version; dependencyManagement
		// (the pom's own, a parent's, an imported BOM's) does. The version
		// is known, so this is exposure, and the label says how it is
		// known: the requirement's text, which has no version, would call
		// it unpinned.
		return []Use{{Version: dep.CurrentVersion, Resolution: Managed}}
	}
	if dep.CurrentVersion == "" {
		// No version: the purl has none, OSV answers for the package's
		// whole history, and that is "version not determined" whatever the
		// requirement's text looks like. The text is often the manifest
		// line with the package's name in it (`boto3` has a digit), or an
		// upper bound alone (`jinja2<3.1`), and ClassifyRequirement, which
		// reads digits and operators, called both a known version (review
		// round 1; the behaviour predates this package).
		return []Use{{Resolution: Unpinned}}
	}
	return []Use{{
		Version:    dep.CurrentVersion,
		Resolution: ClassifyRequirement(manifest.ClassificationText(dep.PackageManager, dep.Requirement), dep.CurrentVersion),
	}}
}
