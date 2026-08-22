// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — lockfile_store.go persists the v0.27.11 lockfile
// inventory (repo_lockfiles) and direct-dependency resolutions
// (repo_lockfile_packages), and serves the vulnerability scan's
// version-accuracy reads:
//
//   - GetRepoDepsForVulnScan — the scan's dependency source (name,
//     floor version, purl, RAW requirement string). Deliberately a
//     separate method from GetRepoLibyearDeps so the libyear/SBOM read
//     path stays byte-identical (HARD RULE: libyear is untouched).
//   - GetRepoSelfPackageNames — the "self-set" used to exclude a
//     publisher monorepo's own packages from vuln purl generation.
//   - GetRepoLockedVersions — lockfile-resolved versions consumed at
//     purl construction (repo_lockfile_packages, so
//     `aveloxis heal-vulnerabilities` — which scans WITHOUT a fresh
//     analysis pass — benefits too).
//   - GetRepoLockfileCertainty — the API's repo-level
//     "lockfile certainty" summary, derived at read time.
package db

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
)

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
}

// GetRepoDepsForVulnScan returns the repo's dependencies with the raw
// requirement string, for vulnerability purl construction. Reads
// repo_deps_libyear (the same source the scan always used) but through
// its own SELECT so GetRepoLibyearDeps — the libyear/SBOM contract —
// is not modified.
func (s *PostgresStore) GetRepoDepsForVulnScan(ctx context.Context, repoID int64) ([]VulnScanDep, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT name, current_version, package_manager,
			COALESCE(purl, ''), COALESCE(requirement, ''), COALESCE(type, '')
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1
		ORDER BY name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deps []VulnScanDep
	for rows.Next() {
		var d VulnScanDep
		if err := rows.Scan(&d.Name, &d.CurrentVersion, &d.PackageManager, &d.Purl, &d.Requirement, &d.Type); err != nil {
			return nil, err
		}
		deps = append(deps, d)
	}
	return deps, rows.Err()
}

// GetRepoSelfPackageNames builds the repo's "self-set": lowercased
// package names that ARE the repo's own published packages, so a
// manifest declaring a supported range of them is a support-matrix
// declaration, not exposure (the apache/airflow provider-manifest
// case). Union of three signals:
//
//	(a) packages the repo PUBLISHES (repo_distribution),
//	(b) packages the repo DECLARES in its own manifests
//	    (repo_distribution_manifest.package_name_declared),
//	(c) a name heuristic — always available even where distribution
//	    tracking never ran: repo_name, repo_name with '_'→'-',
//	    owner-name, owner-name with '_'→'-' (all lowercased).
//
// Exact matches only — NO prefix rules. The distribution tables cover
// provider-style subpackages (apache-airflow-providers-*) where they
// exist; a prefix rule would over-exclude forks and lookalikes.
func (s *PostgresStore) GetRepoSelfPackageNames(ctx context.Context, repoID int64) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT LOWER(package_name) FROM aveloxis_data.repo_distribution
		WHERE repo_id = $1 AND package_name <> ''
		UNION
		SELECT LOWER(package_name_declared) FROM aveloxis_data.repo_distribution_manifest
		WHERE repo_id = $1 AND package_name_declared <> ''
		UNION
		SELECT unnest(ARRAY[
			LOWER(r.repo_name),
			LOWER(REPLACE(r.repo_name, '_', '-')),
			LOWER(r.repo_owner || '-' || r.repo_name),
			LOWER(REPLACE(r.repo_owner || '-' || r.repo_name, '_', '-'))
		]) FROM aveloxis_data.repos r WHERE r.repo_id = $1`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name != "" {
			out[name] = true
		}
	}
	return out, rows.Err()
}

// RepoLockfileInfo is one inventory row for repo_lockfiles: a lockfile
// the analysis walk found, with Phase-C sizing counters (EntryCount =
// every package the lockfile resolves, transitives included;
// DirectCount = direct entries when the format distinguishes them,
// else the entries matched to declared direct deps).
type RepoLockfileInfo struct {
	Ecosystem    string // package-manager string: npm/pypi/cargo/…
	LockfilePath string // repo-relative path
	LockfileKind string // e.g. "package-lock.json", "poetry.lock"
	EntryCount   int
	DirectCount  int
}

// RepoLockfilePackage is one stored resolution: a lockfile-resolved
// version of a package that matches one of the repo's DIRECT declared
// dependencies.
type RepoLockfilePackage struct {
	Ecosystem       string
	PackageName     string
	ResolvedVersion string
	LockfilePath    string
	// Direct (v0.27.21 C1) — TRUE for resolutions of the repo's
	// declared direct dependencies (the only rows written pre-C1 and
	// with vuln_scan_transitive off); FALSE for transitive entries.
	Direct bool
	// Scope — 'dev' / 'runtime' / '' (unknown), carried from formats
	// that flag it (package-lock v2/3 dev, poetry category).
	Scope string
}

// RepoLockfileEdge is one stored parent→child dependency edge
// (v0.27.133 C2 — the parent-chain attribution substrate). Child edges
// are name-level; ChildConstraint is the raw declared range.
type RepoLockfileEdge struct {
	Ecosystem       string
	LockfilePath    string
	ParentName      string
	ParentVersion   string
	ChildName       string
	ChildConstraint string
}

// ReplaceRepoLockfileSnapshot atomically replaces a repo's lockfile
// inventory + direct-dep resolutions + dependency edges: DELETE the
// tables' rows for the repo, then INSERT the fresh ones, all in ONE
// transaction (the
// v0.27.7 ReplaceRepoLaborSnapshot shape). No history tables exist by
// design — lockfile state is derivable from git history and has no
// time-series consumer — so the rotation half is a plain DELETE; the
// single transaction still guarantees a failed analysis pass can never
// leave the tables empty or half-written (previous snapshot stays
// current on rollback).
//
// An empty snapshot (no lockfiles found) still deletes: the tables are
// a snapshot of the last successful analysis walk. Callers must NOT
// invoke this when the walk itself failed.
func (s *PostgresStore) ReplaceRepoLockfileSnapshot(ctx context.Context, repoID int64,
	inventory []*RepoLockfileInfo, packages []*RepoLockfilePackage, edges []*RepoLockfileEdge) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfile_edges WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfile_packages WHERE repo_id = $1`, repoID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM aveloxis_data.repo_lockfiles WHERE repo_id = $1`, repoID); err != nil {
			return err
		}

		batch := &pgx.Batch{}
		for _, inv := range inventory {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_lockfiles
					(repo_id, ecosystem, lockfile_path, lockfile_kind,
					 entry_count, direct_count, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path) DO UPDATE SET
					ecosystem = EXCLUDED.ecosystem,
					lockfile_kind = EXCLUDED.lockfile_kind,
					entry_count = EXCLUDED.entry_count,
					direct_count = EXCLUDED.direct_count,
					data_collection_date = NOW()`,
				repoID, inv.Ecosystem, inv.LockfilePath, inv.LockfileKind,
				inv.EntryCount, inv.DirectCount)
		}
		for _, p := range packages {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_lockfile_packages
					(repo_id, ecosystem, package_name, resolved_version,
					 lockfile_path, direct, dependency_scope, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path, package_name, resolved_version) DO NOTHING`,
				repoID, p.Ecosystem, p.PackageName, p.ResolvedVersion, p.LockfilePath,
				p.Direct, p.Scope)
		}
		for _, e := range edges {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_lockfile_edges
					(repo_id, ecosystem, lockfile_path, parent_name,
					 parent_version, child_name, child_constraint, data_source, data_collection_date)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'analysis', NOW())
				ON CONFLICT (repo_id, lockfile_path, parent_name, parent_version, child_name) DO NOTHING`,
				repoID, e.Ecosystem, e.LockfilePath, e.ParentName,
				e.ParentVersion, e.ChildName, e.ChildConstraint)
		}
		if batch.Len() > 0 {
			if err := tx.SendBatch(ctx, batch).Close(); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	})
}

// GetRepoLockfileEdges returns every stored dependency edge for a repo
// (v0.27.133 C2). Consumers walk child→parents to attribute a
// vulnerable transitive to the direct roots that pull it in.
func (s *PostgresStore) GetRepoLockfileEdges(ctx context.Context, repoID int64) ([]RepoLockfileEdge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, lockfile_path, parent_name, parent_version,
		       child_name, COALESCE(child_constraint, '')
		FROM aveloxis_data.repo_lockfile_edges
		WHERE repo_id = $1
		ORDER BY lockfile_path, parent_name, child_name`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoLockfileEdge
	for rows.Next() {
		var e RepoLockfileEdge
		if err := rows.Scan(&e.Ecosystem, &e.LockfilePath, &e.ParentName,
			&e.ParentVersion, &e.ChildName, &e.ChildConstraint); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetRepoLockedVersions returns every stored lockfile resolution for a
// repo. The vulnerability scan consumes this at purl construction —
// reading the TABLE (not a fresh parse) is what lets
// `aveloxis heal-vulnerabilities`, which runs scans without an
// analysis pass, benefit from lockfile accuracy too.
// v0.27.21: filters to direct=TRUE — with transitive storage on, a
// transitive entry must never reclassify a DECLARED dep as 'locked'.
func (s *PostgresStore) GetRepoLockedVersions(ctx context.Context, repoID int64) ([]RepoLockfilePackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version, lockfile_path
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND COALESCE(direct, TRUE)
		ORDER BY package_name, resolved_version`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoLockfilePackage
	for rows.Next() {
		var p RepoLockfilePackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.LockfilePath); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// GetRepoTransitivePackages returns the repo's stored TRANSITIVE
// lockfile resolutions (v0.27.21 C1) — the rows the vulnerability
// scan turns into dependency_kind='transitive' purls when
// collection.vuln_scan_transitive is on. Distinct per (ecosystem,
// package, version): a package appearing in several lockfiles scans
// once, and the scope keeps any non-dev observation ("" or 'runtime'
// beats 'dev' — a package pulled in by BOTH a dev tool and a runtime
// dependency is runtime exposure).
func (s *PostgresStore) GetRepoTransitivePackages(ctx context.Context, repoID int64) ([]RepoLockfilePackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ecosystem, package_name, resolved_version,
		       MIN(CASE WHEN COALESCE(dependency_scope, '') IN ('dev','test','build','optional','peer') THEN dependency_scope ELSE '' END)
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND NOT COALESCE(direct, TRUE)
		GROUP BY ecosystem, package_name, resolved_version
		ORDER BY package_name, resolved_version`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RepoLockfilePackage
	for rows.Next() {
		var p RepoLockfilePackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName, &p.ResolvedVersion, &p.Scope); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// LockfileEcosystemCertainty is one ecosystem's slice of the API's
// lockfile_certainty summary.
type LockfileEcosystemCertainty struct {
	Ecosystem      string `json:"ecosystem"`
	LockfileKind   string `json:"lockfile_kind"`
	LockedPackages int    `json:"locked_packages"`
}

// LockfileCertainty is the repo-level summary served by
// GET /api/v1/repos/{id}/vulnerabilities. Overall is "full" when every
// ecosystem that has dependencies also has a lockfile (Go counts as
// locked by construction — go.mod versions are exact under MVS),
// "partial" when some do, "none" otherwise (including the
// no-dependencies case: with nothing declared there is nothing a
// lockfile could make certain).
type LockfileCertainty struct {
	Overall    string                       `json:"overall"` // "full" | "partial" | "none"
	Ecosystems []LockfileEcosystemCertainty `json:"ecosystems"`
}

// GetRepoLockfileCertainty derives the lockfile-certainty summary at
// read time: dependency ecosystems come from repo_deps_libyear,
// lockfile coverage from repo_lockfiles/repo_lockfile_packages, and Go
// is intrinsically covered (synthesized as lockfile_kind "go.mod").
func (s *PostgresStore) GetRepoLockfileCertainty(ctx context.Context, repoID int64) (*LockfileCertainty, error) {
	// Ecosystems that have dependencies, with per-ecosystem dep counts
	// (the Go count doubles as its locked_packages figure).
	depRows, err := s.pool.Query(ctx, `
		SELECT package_manager, COUNT(*)
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1 AND package_manager <> ''
		GROUP BY package_manager`, repoID)
	if err != nil {
		return nil, err
	}
	depCounts := map[string]int{}
	for depRows.Next() {
		var eco string
		var n int
		if err := depRows.Scan(&eco, &n); err != nil {
			depRows.Close()
			return nil, err
		}
		depCounts[eco] = n
	}
	depRows.Close()
	if err := depRows.Err(); err != nil {
		return nil, err
	}

	// Lockfile coverage per (ecosystem, kind), with the count of
	// distinct stored (direct-matched) packages resolved by that kind's
	// lockfiles.
	lockRows, err := s.pool.Query(ctx, `
		SELECT l.ecosystem, l.lockfile_kind, COUNT(DISTINCT p.package_name)
		FROM aveloxis_data.repo_lockfiles l
		LEFT JOIN aveloxis_data.repo_lockfile_packages p
			ON p.repo_id = l.repo_id AND p.lockfile_path = l.lockfile_path
		WHERE l.repo_id = $1
		GROUP BY l.ecosystem, l.lockfile_kind
		ORDER BY l.ecosystem, l.lockfile_kind`, repoID)
	if err != nil {
		return nil, err
	}
	defer lockRows.Close()

	out := &LockfileCertainty{Ecosystems: []LockfileEcosystemCertainty{}}
	coveredEcos := map[string]bool{}
	for lockRows.Next() {
		var e LockfileEcosystemCertainty
		if err := lockRows.Scan(&e.Ecosystem, &e.LockfileKind, &e.LockedPackages); err != nil {
			return nil, err
		}
		out.Ecosystems = append(out.Ecosystems, e)
		coveredEcos[e.Ecosystem] = true
	}
	if err := lockRows.Err(); err != nil {
		return nil, err
	}

	// Go needs no lockfile: go.mod versions are exact under MVS, so
	// the ecosystem is covered by construction.
	if n, ok := depCounts["go"]; ok && !coveredEcos["go"] {
		out.Ecosystems = append(out.Ecosystems, LockfileEcosystemCertainty{
			Ecosystem: "go", LockfileKind: "go.mod", LockedPackages: n,
		})
		coveredEcos["go"] = true
	}

	covered := 0
	for eco := range depCounts {
		if coveredEcos[eco] {
			covered++
		}
	}
	switch {
	case len(depCounts) > 0 && covered == len(depCounts):
		out.Overall = "full"
	case covered > 0:
		out.Overall = "partial"
	default:
		out.Overall = "none"
	}
	return out, nil
}

// SelfAdvisoryPackage is one of the repo's OWN published packages,
// eligible for version-unconstrained advisory scanning (v0.27.29).
type SelfAdvisoryPackage struct {
	Ecosystem   string // repo_distribution.ecosystem / manifest_type flavor
	PackageName string // lowercased
}

// GetRepoSelfAdvisoryPackages returns the repo's own published
// packages for SELF-ADVISORY scanning (v0.27.29 — the numpy/numpy
// face-validity fix: a repo with no dependencies showed zero
// vulnerabilities even though OSV carries 16 advisories for the
// package it publishes, because the scan covered dependencies only).
//
// DELIBERATELY NARROWER than GetRepoSelfPackageNames (the exclusion
// set above): deps.dev's reverse lookup returns every package that
// merely CLAIMS the repo URL — numpy's rows include intel-numpy,
// mmwave, and numpydoc — and attaching those packages' advisories to
// this repo would be wrong. Broad is correct for EXCLUDING deps from
// the scan (over-exclusion is conservative); precise is required for
// ATTRIBUTING advisories (over-attribution is misinformation). Two
// precise sources only:
//
//  1. The repo's OWN manifests' declared package names (authoritative
//     — the repo says what it publishes), with the manifest's
//     ecosystem type.
//  2. Registry evidence whose package name exactly matches the repo's
//     name variants (the v0.27.11 4-variant heuristic).
func (s *PostgresStore) GetRepoSelfAdvisoryPackages(ctx context.Context, repoID int64) ([]SelfAdvisoryPackage, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT m.manifest_type, LOWER(m.package_name_declared)
		FROM aveloxis_data.repo_distribution_manifest m
		WHERE m.repo_id = $1 AND m.package_name_declared <> ''
		UNION
		SELECT DISTINCT d.ecosystem, LOWER(d.package_name)
		FROM aveloxis_data.repo_distribution d
		JOIN aveloxis_data.repos r ON r.repo_id = d.repo_id
		WHERE d.repo_id = $1 AND d.package_name <> ''
		  AND LOWER(d.package_name) IN (
			LOWER(r.repo_name),
			LOWER(REPLACE(r.repo_name, '_', '-')),
			LOWER(r.repo_owner || '-' || r.repo_name),
			LOWER(REPLACE(r.repo_owner || '-' || r.repo_name, '_', '-')))`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SelfAdvisoryPackage
	for rows.Next() {
		var p SelfAdvisoryPackage
		if err := rows.Scan(&p.Ecosystem, &p.PackageName); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// lockfileEcoFold collapses the ecosystem-vocabulary split between
// the libyear writers' package_manager strings and the lockfile
// roster's ecosystem strings (round 22 — previously duplicated as the
// SBOM side's private fold while the chain walk had NONE, silently
// dropping rubygems/packagist/swiftpm chains).
func lockfileEcoFold(eco string) string {
	switch strings.ToLower(eco) {
	case "rubygems":
		return "gem"
	case "packagist":
		return "composer"
	case "swiftpm":
		return "swift"
	case "elixir":
		return "hex"
	case "dart":
		return "pub"
	case "golang":
		return "go"
	case "haskell":
		return "hackage"
	}
	return strings.ToLower(eco)
}

// LockfileGraphKey is THE name-level resolution key for lockfile-graph
// endpoints (chain attribution, SBOM dependency graphs, direct-root
// sets): folded ecosystem + lowercased name, with PyPI's
// underscore/dot folding (PEP 503 treats them as equivalent). Every
// producer and consumer of graph keys must use this ONE function —
// mismatched folding between sides silently drops valid edges.
func LockfileGraphKey(eco, name string) string {
	e := lockfileEcoFold(eco)
	n := strings.ToLower(strings.TrimSpace(name))
	if e == "pypi" {
		n = strings.NewReplacer("_", "-", ".", "-").Replace(n)
	}
	return e + "|" + n
}

// DirectPackageSets is the chain walk's root set with PROVENANCE
// (round 20 — v0.27.137), keyed "ecosystem|lowercase(name)".
type DirectPackageSets struct {
	ByLockfile map[string]map[string]bool // lockfile_path -> ecosystem|lower(name)
	Declared   map[string]bool            // repo-wide manifest-declared fallback
}

// GetRepoDirectPackageSets returns the repo's DIRECT dependency set
// (v0.27.133 C2 — the chain walk's root set): direct lockfile
// resolutions keyed by their lockfile_path, plus the repo-wide
// declared manifest set as a separate fallback. The split matters in
// monorepos: a package direct in apps/b but transitive in apps/a must
// NOT terminate apps/a's chain walk — it is not an actionable root in
// that lockfile's graph. Declared manifest deps (repo_deps_libyear)
// carry no path column, so they stay an explicitly repo-wide fallback
// (this also covers Go — no lockfile by design — and every
// lockfile-less ecosystem).
func (s *PostgresStore) GetRepoDirectPackageSets(ctx context.Context, repoID int64) (DirectPackageSets, error) {
	out := DirectPackageSets{ByLockfile: map[string]map[string]bool{}, Declared: map[string]bool{}}
	rows, err := s.pool.Query(ctx, `
		SELECT 'lockfile', lockfile_path, ecosystem, package_name
		FROM aveloxis_data.repo_lockfile_packages
		WHERE repo_id = $1 AND COALESCE(direct, TRUE)
		UNION
		SELECT 'declared', '', package_manager, name
		FROM aveloxis_data.repo_deps_libyear
		WHERE repo_id = $1`, repoID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var source, path, eco, name string
		if err := rows.Scan(&source, &path, &eco, &name); err != nil {
			return out, err
		}
		key := LockfileGraphKey(eco, name)
		if source == "declared" {
			out.Declared[key] = true
			continue
		}
		if out.ByLockfile[path] == nil {
			out.ByLockfile[path] = map[string]bool{}
		}
		out.ByLockfile[path][key] = true
	}
	return out, rows.Err()
}
