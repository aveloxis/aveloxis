// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package collector — lockfile_scan.go: the analysis-phase lockfile
// walk (v0.27.11). Runs right after the libyear phase (which refreshes
// repo_deps_libyear — the declared-direct-deps set the storage filter
// matches against) and snapshot-replaces the repo's lockfile inventory
// + direct-dep resolutions. The vulnerability scan then consumes the
// TABLES, not a fresh parse, so `aveloxis heal-vulnerabilities`
// benefits without an analysis pass.
package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/pkgid"
	"github.com/aveloxis/aveloxis/internal/versionuse"
)

// parsedLockfile pairs a repo-relative path with its parse result.
type parsedLockfile struct {
	Path   string
	Result *LockfileResult
}

// collectLockfiles walks workDir for roster lockfiles and parses each.
// Best-effort throughout: an unreadable or malformed lockfile logs a
// WARN and is skipped — the walk itself never fails. Same traversal
// rules as the manifest walks in analysis.go: symlinks rejected,
// vendor/node_modules/.git skipped.
func collectLockfiles(workDir string, logger *slog.Logger) lockfileSet {
	var out []parsedLockfile
	var failed []model.ManifestFailure
	// v0.27.36: a root-stat failure means zero lockfiles with no signal.
	walkErr := filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if path == workDir {
				// The root itself: nothing is known about this
				// repository's lockfiles (0.29.176). Swallowed, this read
				// as "it has none" and the snapshot was replaced empty.
				return err
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil
		}
		if info.IsDir() {
			base := filepath.Base(path)
			if base == "vendor" || base == "node_modules" || base == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		base := filepath.Base(path)
		spec, ok := lockfileKinds[base]
		if !ok {
			return nil
		}
		rel, relErr := filepath.Rel(workDir, path)
		if relErr != nil {
			rel = base
		}
		if spec.binaryOnly {
			// bun.lockb: binary — DETECT for the inventory (kind
			// marker, zero entries), never parse.
			res, _ := ParseLockfile(base, nil)
			out = append(out, parsedLockfile{Path: rel, Result: res})
			return nil
		}
		// Through manifest.ReadManifest (PR #212 review): a UTF-8 BOM or UTF-16 lockfile
		// is decoded like its manifest, not failed whole (SR-17).
		data, readErr := manifest.ReadManifest(path)
		if readErr != nil {
			logger.Warn("failed to read lockfile — its previous rows are kept", "path", rel, "error", readErr)
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedRead})
			return nil
		}
		res, parseErr := ParseLockfile(base, data)
		if parseErr != nil {
			logger.Warn("failed to parse lockfile — its previous rows are kept", "path", rel, "error", parseErr)
			failed = append(failed, model.ManifestFailure{Path: rel, Kind: base, Reason: model.ManifestFailedParse})
			return nil
		}
		out = append(out, parsedLockfile{Path: rel, Result: res})
		return nil
	})
	if walkErr != nil {
		logger.Warn("lockfile walk failed — the stored lockfile snapshot is left as it is", "dir", workDir, "error", walkErr)
	}
	return lockfileSet{Parsed: out, Failed: failed, WalkErr: walkErr}
}

// lockfileSet is what one walk of a clone found: the lockfiles that parsed,
// those that exist and could not be read or parsed, and the walk's own
// error when it could not read the clone's root. It is collected once,
// ahead of the libyear phase (parsing only; nothing is stored), and handed
// to the lockfile phase.
type lockfileSet struct {
	Parsed  []parsedLockfile
	Failed  []model.ManifestFailure
	WalkErr error
}

// scanLockfiles is Phase 2b of AnalyzeRepo: walk + parse lockfiles,
// filter entries to the repo's DIRECT declared dependencies (the
// repo_deps_libyear rows Phase 2 just refreshed — bounding storage;
// full transitive storage is the deferred Phase C), and
// snapshot-replace both tables in one transaction.
//
// Multi-lockfile repos: a package resolving to DIFFERENT versions in
// different lockfiles is legitimate (two apps in one monorepo) — all
// distinct (package, version) pairs are kept.
func (ac *AnalysisCollector) scanLockfiles(ctx context.Context, repoID int64, workDir string, locks lockfileSet, result *AnalysisResult) error {
	if locks.WalkErr != nil {
		// Nothing is known about the lockfiles: the snapshot is not
		// replaced (SR-5: an unreadable clone is not "no lockfiles").
		return fmt.Errorf("lockfile walk failed (snapshot NOT replaced): %w", locks.WalkErr)
	}
	parsed := locks.Parsed

	deps, err := ac.store.GetRepoDepsForVulnScan(ctx, repoID)
	if err != nil {
		return err
	}
	declared := make(map[string]bool, len(deps))
	for _, d := range deps {
		declared[pkgid.MatchKey(d.PackageManager, d.Name)] = true
	}

	var inventory []*db.RepoLockfileInfo
	var packages []*model.RepoLockfilePackage
	var edges []*db.RepoLockfileEdge
	// v0.29.175: entries whose "version" was not a version, over every
	// lockfile of the repository; one line, not one per entry.
	var aliased, notRegistry, notAVersion int
	for _, pl := range parsed {
		aliased += pl.Result.Aliased
		notRegistry += pl.Result.NotRegistry
		notAVersion += pl.Result.NotAVersion
	}
	if aliased+notRegistry+notAVersion > 0 {
		ac.logger.Info("lockfile entries whose version field is not a version",
			"repo_id", repoID, "npm_aliases_recorded_under_the_real_package", aliased,
			"git_or_url_sources_left_out", notRegistry, "requirement_strings_left_out", notAVersion)
	}
	for _, pl := range parsed {
		res := pl.Result
		// v0.27.133 C2: dependency edges — the attribution substrate —
		// are stored ONLY under the same vuln_scan_transitive knob that
		// gates transitive package rows (edges without the transitive
		// closure have nothing to attribute; knob off keeps the exact
		// pre-C2 write set).
		if ac.TransitiveLockfiles {
			edgeSeen := map[string]bool{}
			for _, ed := range res.Edges {
				key := ed.ParentName + "@" + ed.ParentVersion + ">" + ed.ChildName
				if edgeSeen[key] {
					continue
				}
				edgeSeen[key] = true
				edges = append(edges, &db.RepoLockfileEdge{
					Ecosystem:       res.Ecosystem,
					LockfilePath:    pl.Path,
					ParentName:      ed.ParentName,
					ParentVersion:   ed.ParentVersion,
					ChildName:       ed.ChildName,
					ChildConstraint: ed.Constraint,
				})
			}
		}
		matched := 0
		seen := map[string]bool{}
		directFlagged := 0
		for _, e := range res.Entries {
			if e.Direct {
				directFlagged++
			}
			// By the entry's own ecosystem: a Bioconductor entry is not the
			// resolution of a declared CRAN dependency of its name.
			if !declared[pkgid.MatchKey(entryEcosystem(res, e), e.Name)] {
				// v0.27.21 C1: with vuln_scan_transitive on, the FULL
				// entry set is stored (direct=FALSE rows are the
				// transitive closure the vuln scan targets). Knob off:
				// pre-C1 behavior — declared-matched rows only.
				if !ac.TransitiveLockfiles {
					continue
				}
				key := e.Name + "@" + e.Version
				if seen[key] {
					continue
				}
				seen[key] = true
				packages = append(packages, &model.RepoLockfilePackage{
					Ecosystem:       entryEcosystem(res, e),
					PackageName:     e.Name,
					ResolvedVersion: e.Version,
					LockfilePath:    pl.Path,
					Namespace:       e.Namespace,
					Direct:          false,
					Scope:           e.Scope,
				})
				continue
			}
			key := e.Name + "@" + e.Version
			if seen[key] {
				continue
			}
			seen[key] = true
			matched++
			packages = append(packages, &model.RepoLockfilePackage{
				Ecosystem:       entryEcosystem(res, e),
				PackageName:     e.Name,
				ResolvedVersion: e.Version,
				LockfilePath:    pl.Path,
				// Stored 'direct' means "resolution of a repo-level
				// declared dependency" — exactly the pre-C1 row set.
				Namespace: e.Namespace,
				Direct:    true,
				Scope:     e.Scope,
			})
		}
		// direct_count: what the format itself flags as direct when it
		// distinguishes; else the declared-dep matches (best proxy) —
		// the Phase-C sizing pair alongside entry_count.
		directCount := directFlagged
		if !res.DirectKnown {
			directCount = matched
		}
		inventory = append(inventory, &db.RepoLockfileInfo{
			Ecosystem:    res.Ecosystem,
			LockfilePath: pl.Path,
			LockfileKind: res.Kind,
			EntryCount:   len(res.Entries),
			DirectCount:  directCount,
		})
	}

	// v0.27.133 C2: Go transitive closure via the toolchain — same knob,
	// same snapshot transaction. Declared go deps are the direct set.
	//
	// v0.27.150 (round 29): the expansion is best-effort, but the
	// snapshot REPLACE below is not — an incomplete run (missing
	// toolchain, module failure, repo budget exhausted) used to wipe
	// the previously valid Go closure, silently shrinking vuln and
	// SBOM output and FALSE-RESOLVING findings whose packages
	// vanished (they then flip-flopped back on the next healthy
	// scan). On incomplete: discard this run's partial Go rows and
	// carry the PRIOR snapshot's Go closure forward unchanged —
	// whole-and-stale beats partial-and-mixed. A failed prior-rows
	// read propagates (SR-5): failing the phase leaves the prior
	// snapshot intact, which is strictly better than replacing it
	// with a shrunken one.
	if ac.TransitiveLockfiles {
		goPkgs, goEdges, goComplete := ac.scanGoModGraph(ctx, repoID, workDir, declared)
		if goComplete {
			packages = append(packages, goPkgs...)
			edges = append(edges, goEdges...)
		} else {
			prevPkgs, prevEdges, perr := ac.store.GetRepoGoClosureRows(ctx, repoID)
			if perr != nil {
				return fmt.Errorf("go expansion incomplete and prior-closure read failed (snapshot NOT replaced): %w", perr)
			}
			ac.logger.Warn("go mod graph expansion incomplete — preserving the prior snapshot's Go closure",
				"repo_id", repoID,
				"prior_packages", len(prevPkgs), "prior_edges", len(prevEdges),
				"discarded_partial_packages", len(goPkgs), "discarded_partial_edges", len(goEdges))
			packages = append(packages, prevPkgs...)
			edges = append(edges, prevEdges...)
		}
	}

	// A lockfile that exists and failed this cycle keeps its previous
	// answer (0.29.176; summary/70, 4.3). The replace below removes every
	// lockfile row of the repository, so the failed paths' rows are read
	// first and re-inserted through the same insert as fresh rows. Only the
	// parsed columns are old: the direct flag comes from THIS cycle's
	// declared set, by the same filter as a fresh row (an old flag would
	// leave a removed declaration's row targeted by nothing, or add a
	// floor-version target beside the locked one). A failed read of the
	// previous rows fails the phase: the snapshot stands (SR-5).
	failed := append([]model.ManifestFailure(nil), locks.Failed...)
	if len(failed) > 0 {
		paths := make([]string, len(failed))
		for i, f := range failed {
			paths[i] = f.Path
		}
		prevInv, prevPkgs, prevEdges, perr := ac.store.GetRepoLockfileRowsAtPaths(ctx, repoID, paths)
		if perr != nil {
			return fmt.Errorf("lockfiles failed and their previous rows could not be read (snapshot NOT replaced): %w", perr)
		}
		carried := map[string]int{}
		for _, p := range prevPkgs {
			p.Direct = declared[pkgid.MatchKey(p.Ecosystem, p.PackageName)]
			if !p.Direct && !ac.TransitiveLockfiles {
				continue
			}
			carried[p.LockfilePath]++
			packages = append(packages, p)
		}
		inventory = append(inventory, prevInv...)
		if ac.TransitiveLockfiles {
			edges = append(edges, prevEdges...)
		}
		for i := range failed {
			failed[i].RowsCarried = carried[failed[i].Path]
		}
	}

	if err := ac.store.ReplaceRepoLockfileSnapshot(ctx, repoID, inventory, packages, edges, failed); err != nil {
		return err
	}
	if len(inventory) > 0 {
		// Round-21 (v0.27.138): with transitive scanning on by default,
		// packages holds direct + transitive + the Go build list —
		// log the SPLIT, not a mislabeled total (the house
		// log-the-effective-value rule).
		direct := 0
		for _, p := range packages {
			if p.Direct {
				direct++
			}
		}
		ac.logger.Info("lockfile scan complete",
			"repo_id", repoID,
			"lockfiles", len(inventory),
			"direct_resolutions", direct,
			"transitive_resolutions", len(packages)-direct,
			"edges", len(edges))
	}
	result.Lockfiles = len(inventory)
	result.LockfilePackages = len(packages)
	return nil
}

// lockfilesInUse is what the libyear phase reads to give an inventory row
// its locked version (0.29.177): every lockfile that parsed this cycle, and,
// for one that exists and failed, the rows it keeps from the previous
// snapshot (0.29.176), so a row does not fall back from its locked version
// to the declared one for as long as the lockfile is broken. If those
// previous rows cannot be read the failed lockfiles are simply not offered:
// their dependencies report the declared version this cycle, and the scan's
// targets, which come from the lockfile table, are unaffected.
func (ac *AnalysisCollector) lockfilesInUse(ctx context.Context, repoID int64, locks lockfileSet) []versionuse.Lockfile {
	byPath := map[string]*versionuse.Lockfile{}
	var order []string
	add := func(path, ecosystem, name, version string) {
		lf := byPath[path]
		if lf == nil {
			lf = &versionuse.Lockfile{Path: path, Ecosystem: ecosystem, Versions: map[string][]string{}}
			byPath[path] = lf
			order = append(order, path)
		}
		if name == "" {
			return
		}
		key := pkgid.MatchKey(ecosystem, name)
		if !slices.Contains(lf.Versions[key], version) {
			lf.Versions[key] = append(lf.Versions[key], version)
		}
	}
	for _, pl := range locks.Parsed {
		add(pl.Path, pl.Result.Ecosystem, "", "")
		for _, e := range pl.Result.Entries {
			// An entry of another ecosystem (renv.lock's Bioconductor
			// packages) locks no dependency of the lockfile's: the CRAN
			// dependency of that name is another package (SR-6).
			if entryEcosystem(pl.Result, e) != pl.Result.Ecosystem {
				continue
			}
			add(pl.Path, pl.Result.Ecosystem, e.Name, e.Version)
		}
	}
	if len(locks.Failed) > 0 {
		paths := make([]string, len(locks.Failed))
		for i, f := range locks.Failed {
			paths[i] = f.Path
		}
		_, prev, _, err := ac.store.GetRepoLockfileRowsAtPaths(ctx, repoID, paths)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				ac.logger.Warn("the previous rows of lockfiles that failed could not be read — their dependencies report the declared version this cycle",
					"repo_id", repoID, "lockfiles", len(paths), "error", err)
			}
		}
		for _, p := range prev {
			if p.Ecosystem == bioconductorEcosystem {
				continue // as above, for a carried row
			}
			add(p.LockfilePath, p.Ecosystem, p.PackageName, p.ResolvedVersion)
		}
	}
	out := make([]versionuse.Lockfile, 0, len(order))
	for _, p := range order {
		out = append(out, *byPath[p])
	}
	return out
}
