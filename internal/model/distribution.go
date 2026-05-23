// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// v0.24.0 — types for the DistributionWorker subsystem.
//
// PackageDistribution captures one "this repo is published at this
// (ecosystem, package_name) per source X" observation. The same
// (ecosystem, package_name) may be reported by multiple sources
// (deps.dev knows about npm + PyPI + Maven + 4 others; ecosyste.ms
// adds Conda + Homebrew + CRAN + the long tail; GitHub Packages
// reports GitHub-hosted artifacts), so the row is keyed on
// (repo_id, ecosystem, package_name, source) at the database level.

// PackageDistribution is one (ecosystem, package_name) tuple for a
// single repo, as reported by one of the registry sources.
//
// Source values: "deps.dev" | "ecosyste.ms" | "github_packages" |
// "github_release_asset". The first two are the registry-side reverse
// lookups. github_packages is GitHub's own registry (distinct from
// the public ones). github_release_asset is a derived signal — when
// a repo's release assets include files with package-like extensions
// (.whl, .gem, .jar, .nupkg, .deb, .rpm, etc.), that's evidence the
// project distributes via GitHub Releases even if no registry knows
// about it.
type PackageDistribution struct {
	Ecosystem         string
	PackageName       string
	VersionCount      int
	FirstPublishedAt  time.Time
	LatestPublishedAt time.Time
	Source            string
	// Extra captures source-specific fields we don't promote to
	// dedicated columns. Stored in repo_distribution.extra (JSONB).
	// Kept small — the columns above are the queryable surface.
	Extra map[string]any
}

// DistributionManifest is one well-known manifest file observed in a
// repo's tree, optionally with its declared package name parsed out.
//
// ManifestType values parallel the PackageDistribution.Ecosystem
// values where possible: "npm", "pypi", "maven", "cargo", "go",
// "rubygems", "nuget", "conda", "homebrew", "composer", "elixir",
// "swift", "dart", "haskell", "cpp". This lets the headline analysis
// query JOIN manifest evidence directly against registry evidence:
//
//	WHERE m.manifest_type = d.ecosystem AND d.distribution_id IS NULL
//
// PackageNameDeclared is best-effort: package.json/Cargo.toml/
// pyproject.toml are trivial to parse; setup.py and Gemfile are
// harder. When the parser can't reliably extract a name, the field
// stays empty — the row still records the *intent* even without
// the declared name.
type DistributionManifest struct {
	ManifestPath        string
	ManifestType        string
	PackageNameDeclared string
}
